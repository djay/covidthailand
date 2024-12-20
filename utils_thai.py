import datetime
import difflib
import functools
import json
import math
import os
import re

import numpy as np
import pandas as pd
import pythainlp.tokenize
from dateutil.parser import parse as d

from utils_pandas import export
from utils_pandas import fuzzy_join
from utils_pandas import import_csv
from utils_pandas import rearrange
from utils_pandas import sensible_precision
from utils_scraping import logger
from utils_scraping import remove_prefix
from utils_scraping import remove_suffix
from utils_scraping import web_files


DISTRICT_RANGE_SIMPLE = [str(i) for i in range(1, 14)]
DISTRICT_RANGE = DISTRICT_RANGE_SIMPLE + ["Prison"]
DISTRICT_RANGE_UNKNOWN = [str(i) for i in range(1, 14)] + ["Prison", "Unknown"]
POS_COLS = [f"Pos Area {i}" for i in DISTRICT_RANGE_SIMPLE]
TEST_COLS = [f"Tests Area {i}" for i in DISTRICT_RANGE_SIMPLE]


REG_COLS = ["Bangkok Metropolitan Region", "Central", "Eastern", "Western", "Northeastern", "Northern", "Southern"]
REG_LEG = ["Bangkok Region", "Central", "Eastern", "Western", "Northeastern", "Northern", "Southern"]
REG_COLOURS = "Set2"


prov_guesses = pd.DataFrame(columns=["Province", "ProvinceEn", "count"])


###############
# Date helpers
###############
AREA_LEGEND_ORDERED = [
    "1: UpN: C.Mai, C.Rai, MHS, Lampang, Lamphun, Nan, Phayao, Phrae",
    "2: LoN: Tak, Phitsanulok, Phetchabun, Sukhothai, Uttaradit",
    "3: UpC: Kamphaeng Phet, Nakhon Sawan, Phichit, Uthai Thani, Chai Nat",
    "4: MidC: Nonthaburi, P.Thani, Ayutthaya, Saraburi, Lopburi, Sing Buri, Ang Thong, N.Nayok",
    "5: LoC: S.Sakhon, Kanchanaburi, N.Pathom, Ratchaburi, Suphanburi, PKK, Phetchaburi, S.Songkhram",
    "6: E: Trat, Rayong, Chonburi, S.Prakan, Chanthaburi, Prachinburi, Sa Kaeo, Chachoengsao",
    "7: MidNE: Khon Kaen, Kalasin, Maha Sarakham, Roi Et",
    "8: UpNE: S.Nakhon, Loei, U.Thani, Nong Khai, NBL, Bueng Kan, N.Phanom, Mukdahan",
    "9: LoNE: Korat, Buriram, Surin, Chaiyaphum",
    "10: ENE: Yasothon, Sisaket, Amnat Charoen, Ubon Ratchathani",
    "11: SW: Phuket, Krabi, Ranong, Phang Nga, S.Thani, Chumphon, N.S.Thammarat",
    "12: SE: Narathiwat, Satun, Trang, Songkhla, Pattani, Yala, Phatthalung",
    "13: MidC: Bangkok",
]

FIRST_AREAS = [13, 4, 5, 6, 1]  # based on size-ish
AREA_LEGEND = rearrange(AREA_LEGEND_ORDERED, *FIRST_AREAS) + ["Prison"]
AREA_LEGEND_SIMPLE = rearrange(AREA_LEGEND_ORDERED, *FIRST_AREAS)

THAI_ABBR_MONTHS = [
    "ม.ค.",
    "ก.พ.",
    "มี.ค.",
    "เม.ย.",
    "พ.ค.",
    "มิ.ย.",
    "ก.ค.",
    "ส.ค.",
    "ก.ย.",
    "ต.ค.",
    "พ.ย.",
    "ธ.ค.",
]
THAI_FULL_MONTHS = [
    "มกราคม",
    "กุมภาพันธ์",
    "มีนาคม",
    "เมษายน",
    "พฤษภาคม",
    "มิถุนายน",
    "กรกฎาคม",  # กรกฎำคม
    "สิงหาคม",
    "กันยายน",
    "ตุลาคม",
    "พฤศจิกายน",
    "ธันวาคม",
]


def today() -> datetime.datetime:
    """Return today's date and time"""
    return datetime.datetime.today()


def to_gregyear(thai, short=False, guess=False):
    """
    >>> to_gregyear("2565")
    2022
    >>> to_gregyear("2565", guess=True)
    2022
    >>> to_gregyear("2022", guess=True)
    2022

    """
    thai = thai if type(thai) != str else int(thai)
    if not guess or thai > 2500 or (60 < thai < 100):
        thai += (2500 if thai < 100 else 0) - 543
    return thai if not short else thai - 2000


def to_thaiyear(year, short=False):
    year = year if type(year) != str else int(year)
    year += (2000 if year < 100 else 0) + 543
    return year if not short else year - 2500


def file2date(file):
    """
    return date of either

    >>> str(file2date('files/report-100264.pdf'))
    '2021-02-10 00:00:00'

    >>> str(file2date('Daily Report 20220118'))
    '2022-01-18 00:00:00'

    >>> str(file2date('inputs/testing_moph/2023.01.28_แยกประเภทของผล-รายจังหวัด.xlsx'))
    '2023-01-28 00:00:00'

    # >>> file2date('files/10-02-21.json')
    # datetime.datetime(2021, 2, 10, 0, 0)


    """

    file = os.path.basename(file)
    file, *_ = file.rsplit(".", 1)
    if m := re.search(r"\d{4}-\d{1,2}-\d{1,2}", file):
        return d(m.group(0))
    elif m := re.search(r"\d{4}\.\d{1,2}\.\d{1,2}", file):
        return d(m.group(0).replace(".", "-"))
    elif m := re.search(r"(?:\d{8}|\d{6})", file):
        date = m.group(0)
        if len(date) == 8:
            # assume non-thai year for 8 digits?
            try:
                return pd.to_datetime(date)
            except:
                return None
        else:
            day, month, year = date[0:2], date[2:4], date[4:6]
        # Try year at the end. thai year
        if year[0] == "6":
            year = int(year) - 43 + 2000
        else:
            year = int(year) + 2000
        return datetime.datetime(day=int(day), month=int(month), year=year)
    return None


def find_dates(content, thai=True):
    # 7 - 13/11/2563
    dates = re.findall(r"([0-9]{2})/([0-9]{2})/(2[0-9]{3})", content)
    dates = set(
        [
            datetime.datetime(day=int(date[0]), month=int(date[1]), year=int(date[2]) - 543 if thai else int(date[2]))
            for date in dates
        ]
    )
    return sorted([d for d in dates])


def to_switching_date(dstr):
    "turning str 2021-01-02 into date but where m and d need to be switched"
    if not dstr:
        return None
    date = d(dstr).date()
    if date.day < 13 and date.month < 13:
        date = datetime.date(date.year, date.day, date.month)
    return date


def previous_date(end, day):
    "return a date before {end} by {day} days"
    start = end
    while start.day != int(day):
        start = start - datetime.timedelta(days=1)
    return start


def find_thai_date(content, remove=False, all=False):
    """
    find thai date in a string

    Abbreviated dates
    >>> print(find_thai_date('17 เม.ย. 2563'))
    2020-04-17 00:00:00

    Won't get confused if its a date range
    >>> print(find_thai_date('28 กุมภำพันธ์  – 18 กรกฎำคม 2564'))
    2021-07-18 00:00:00

    >>> print(find_thai_date("20 ต.ค. 64"))
    2021-10-20 00:00:00

    Can find inside a string
    >>> print(find_thai_date("สำหรับจำนวนผู้ได้รับวัคซีนโควิด 19 ในวันที่ 10 พฤษภาคม 2564 ผู้ได้รับวัคซีนทั้งหมด 88,560 โดส "))
    2021-05-10 00:00:00

    remove the date from the string
    >>> print(find_thai_date("สำหรับจำนวนผู้ได้รับวัคซีนโควิด 19 ในวันที่ 10 พฤษภาคม 2564 ผู้ได้รับวัคซีนทั้งหมด 88,560 โดส", remove=True)[1])
    สำหรับจำนวนผู้ได้รับวัคซีนโควิด 19 ในวันที่   ผู้ได้รับวัคซีนทั้งหมด 88,560 โดส

    can handle mispellings
    >>> print(find_thai_date("10 พฤษภาม 2564"))
    2021-05-10 00:00:00

    can handle mispellings
    >>> print(find_thai_date("10 พฤษ 2564"))
    2021-05-10 00:00:00

    you can get all dates
    >>> print(find_thai_date("สะสมตั้งแต่วันที่ 28 กุมภำพันธ์ 2564 – 10 มกรำคม 2565", all=True)[1])
    2022-01-10 00:00:00

    >>> print(find_thai_date("27 มกราคม 2566", all=True)[0])
    2023-01-27 00:00:00

    >>> print(find_thai_date("nวนัที่ 23 - 27 มกราคม 2566"))
    2023-01-27 00:00:00

    >>> [str(d) for d in find_thai_date("28 กุมภำพันธ์ 2564 ผลกำรให้บริกำรวัคซีน ณ วันที่ 27 มกรำคม 2566 เวลำ 16.00", all=True)]
    ['2021-02-28 00:00:00', '2023-01-27 00:00:00']
    """
    # TODO: prevent it finding numbers for the month name? finds too many
    results = []
    for m3 in re.finditer(r"([0-9]+)(?=\s*([^ ]+)\s*((?:25)?[0-9][0-9]))", content):
        d2, month, year = m3.groups()
        if int(d2) > 31:
            continue
        if len(year) == 2:
            year = "25" + year
        closest = difflib.get_close_matches(month, THAI_FULL_MONTHS + THAI_ABBR_MONTHS, 1, cutoff=0.60)
        month = closest[0] if closest else None

        month = (
            THAI_ABBR_MONTHS.index(month) + 1
            if month in THAI_ABBR_MONTHS
            else THAI_FULL_MONTHS.index(month) + 1
            if month in THAI_FULL_MONTHS
            else None
        )
        if month is None:
            continue
        date = datetime.datetime(year=int(year) - 543, month=month, day=int(d2))
        content = content[:m3.start()] + " " + content[m3.end(m3.lastindex):]
        results.append(date)
        if not all:
            return (date, content) if remove else date
    if all:
        return (results, content) if remove else results
    else:
        return (None, content) if remove else None


def to_month(month):
    closest = next(iter(difflib.get_close_matches(month, THAI_FULL_MONTHS + THAI_ABBR_MONTHS, 1, cutoff=0.85)), None)
    if closest:
        return (
            THAI_ABBR_MONTHS.index(closest) + 1
            if closest in THAI_ABBR_MONTHS
            else THAI_FULL_MONTHS.index(closest) + 1
            if closest in THAI_FULL_MONTHS
            else None
        )


def find_date_range(content):
    """
    >>> p = lambda x: tuple(str(d.date()) for d in x)

    Parse thai date ranges like
    >>> p(find_date_range('11-17 เม.ย. 2563'))
    ('2020-04-11', '2020-04-17')

    >>> p(find_date_range('10 กรกฎาคม ถึง 16 กรกฎาคม 2565'))
    ('2022-07-10', '2022-07-16')

    >>> p(find_date_range('04/04/2563 - 12/06/2563'))
    ('2020-04-04', '2020-06-12')

    >>> p(find_date_range('27/02/2565 - 05-03/2565'))
    ('2022-02-27', '2022-03-05')

    Will handle gregorian too
    >>> p(find_date_range('01/04/2021 – 04/03/2022'))
    ('2021-04-01', '2022-03-04')

    #>>> p(find_date_range('26 FEB – 04 \nMAR 22'))
    #('2021-04-01', '2022-03-04')
    """
    m1 = re.search(
        r"([0-9]+)[/-]([0-9]+)[/-]([0-9]+) *[-–] *([0-9]+)[/-]([0-9]+)[/-]([0-9]+)", content
    )
    m2 = re.search(r"(?<!/)([0-9]+) *[-–] *([0-9]+)/([0-9]+)/(25[0-9][0-9])", content)
    m3 = re.search(r"(?<!/)([0-9]+) *[-–] *([0-9]+) *([^ ]+) *(25[0-9][0-9])", content)
    m4 = re.search(r"(?<!/)([0-9]+) *([^\s\d]+) *(-|–|to|ถึง) *([0-9]+) *([^\s\d]+) *(25[0-9][0-9])", content)
    if m1:
        d1, m1, y1, d2, m2, y2 = m1.groups()
        start = datetime.datetime(day=int(d1), month=int(m1), year=to_gregyear(y1, guess=True))
        end = datetime.datetime(day=int(d2), month=int(m2), year=to_gregyear(y2, guess=True))
        return start, end
    elif m2:
        d1, d2, month, year = m2.groups()
        end = datetime.datetime(year=to_gregyear(year, guess=True), month=int(month), day=int(d2))
        start = previous_date(end, d1)
        return start, end
    elif m3:
        d1, d2, month, year = m3.groups()
        month = to_month(month)
        if not month:
            return None, None
        end = datetime.datetime(year=to_gregyear(year), month=month, day=int(d2))
        start = previous_date(end, d1)
        return start, end
    elif m4:
        d1, m1, sep, d2, m2, year = m4.groups()
        start = datetime.datetime(year=to_gregyear(year), month=to_month(m1), day=int(d1))
        end = datetime.datetime(year=to_gregyear(year), month=to_month(m2), day=int(d2))
        return start, end
    else:
        return None, None


def parse_gender(x):
    return "Male" if "ชาย" in x else "Female"


def thaipop(num: float, pos: int) -> str:
    pp = sensible_precision(num / 69630000 * 100)
    num = sensible_precision(num / 1000000)
    return '0%' if num == '0' else f'{pp}%\n{num}M'


def thaipop2(num: float, pos: int) -> str:
    pp = sensible_precision(num / 69630000 / 2 * 100)
    num = sensible_precision(num / 1000000)
    return '0%' if num == '0' else f'{pp}% {num}M'


@functools.lru_cache(maxsize=100, typed=False)
def get_provinces():
    def __get_alt_name_mappings(df):
        """ Return dict of alternative name lookup keys for provinces from the Complete Provinces + Alt Names
            dataframe/ dataset.
            Format: {AltName->Province,..}
        """
        alt_names_lookup_dict = df.set_index('Name')[['Alt_names']].to_dict()['Alt_names']
        r = {}
        for prov_en, altnames in alt_names_lookup_dict.items():
            altnames = eval(altnames)
            if type(altnames) is not list or len(altnames) <= 0:  # Is a list and has entries, therefore add them:
                break
            for name in altnames:
                if type(name) is not str or len(name) < 1:  #
                    raise ValueError(
                        f"Error in alt name: '{name}'. Unexpected error while iterating over "
                        f"mappings: {name}<-{altnames} for Province: {prov_en}"
                    )
                elif name not in r:
                    r[name] = prov_en
                elif name in r:
                    logger.info("Warning: duplicate entry of {} for Province: {} from Alt Names set: {}", name, prov_en, altnames)
                else:
                    raise ValueError(
                        f"Unexpected error while iterating over mappings: {name}<-{altnames} for Province: {prov_en}")
        return r

    df = pd.read_csv('province_mapping.csv', header=0)
    map_data = __get_alt_name_mappings(df)
    map_data = [(k, v) for k, v in map_data.items()]
    df2 = pd.DataFrame.from_records(map_data, columns=['Alt_names', 'ProvinceEn'])
    df2 = df2.set_index('ProvinceEn')
    df3 = df2.join(df.set_index('Name')[['district_num', 'Name(in Thai)', 'Population (2019)[1]', 'Area (km²)[2]']])
    df3 = df3.reset_index().rename(columns={
        'index': 'ProvinceEn', 'district_num': 'Health District Number',
        'Name(in Thai)': 'ProvinceTh', 'Population (2019)[1]': 'Population',
        'Area (km²)[2]': 'Area_km2'}).set_index('Alt_names')
    df4 = prov_mapping_subdistricts(df3)
    regions = prov_regions_wealth()  # Working locally but no longer on actions?
    df5 = df4.join(regions, on="ProvinceEn")
    return df5


def prov_mapping_subdistricts(provinces):
    url = "https://raw.githubusercontent.com/codesanook/thailand-administrative-division-province-district-subdistrict-sql/master/source-data.csv"  # noqa
    file, _, _ = next(web_files(url, dir="inputs/json", check=False))
    subs = pd.read_csv(file)
    subs = subs.groupby(['AMPHOE_T', 'CHANGWAT_T']).count().reset_index()
    subs['AMPHOE_T'] = subs['AMPHOE_T'].str.replace(r"^อ. ", "", regex=True)
    subs['CHANGWAT_T'] = subs['CHANGWAT_T'].str.replace(r"^จ. ", "", regex=True)
    subs = join_provinces(subs, on="CHANGWAT_T", provinces=provinces)
    altnames = subs[['AMPHOE_T', 'CHANGWAT_T']].merge(provinces, right_index=True, left_on="CHANGWAT_T")
    # AMPHOE_T
    provinces = provinces.combine_first(
        altnames.rename(columns=dict(
            AMPHOE_T="ProvinceAlt")).set_index("ProvinceAlt")).drop(columns=["CHANGWAT_T"])
    return provinces


def prov_mapping_from_cases(provinces):
    # use the case data as it has a mapping between thai and english names
    _, cases = next(web_files("https://covid19.th-stat.com/api/open/cases", dir="json", check=False))
    cases = pd.DataFrame(json.loads(cases)["Data"])
    cases = cases.rename(columns=dict(Province="ProvinceTh", ProvinceAlt="Provinces"))
    lup_province = cases.groupby(
        ['ProvinceId', 'ProvinceTh',
         'ProvinceEn']).size().reset_index().rename({
             0: 'count'
         }, axis=1).sort_values('count',
                                ascending=False).set_index("ProvinceEn")
    # get the proper names from provinces
    lup_province = lup_province.reset_index().rename(columns=dict(ProvinceEn="ProvinceAlt"))
    lup_province = lup_province.set_index("ProvinceAlt").join(provinces)
    lup_province = lup_province.drop(index="Unknown")
    lup_province = lup_province.set_index("ProvinceTh").drop(columns="count")
    # now bring in the thainames as extra altnames
    return provinces.combine_first(lup_province)


def prov_mapping_from_kristw(provinces):
    # bring in some appreviations
    lupurl = "https://raw.githubusercontent.com/kristw/gridmap-layout-thailand/master/src/input/provinces.csv"
    file, _ = next(web_files(lupurl, dir="inputs/json", check=False))
    abr = pd.read_csv(file)
    on_enname = abr.merge(provinces, right_index=True, left_on="enName")
    provinces = provinces.combine_first(
        on_enname.rename(columns=dict(
            thName="ProvinceAlt")).set_index("ProvinceAlt").drop(
                columns=["enAbbr", "enName", "thAbbr"]))
    provinces = provinces.combine_first(
        on_enname.rename(columns=dict(
            thAbbr="ProvinceAlt")).set_index("ProvinceAlt").drop(
                columns=["enAbbr", "enName", "thName"]))

    on_thai = abr.merge(provinces, right_index=True, left_on="thName")
    provinces = provinces.combine_first(
        on_thai.rename(columns=dict(
            enName="ProvinceAlt")).set_index("ProvinceAlt").drop(
                columns=["enAbbr", "thName", "thAbbr"]))
    provinces = provinces.combine_first(
        on_thai.rename(columns=dict(
            thAbbr="ProvinceAlt")).set_index("ProvinceAlt").drop(
                columns=["enAbbr", "enName", "thName"]))
    provinces = provinces.combine_first(
        on_thai.rename(columns=dict(
            enAbbr="ProvinceAlt")).set_index("ProvinceAlt").drop(
                columns=["thAbbr", "enName", "thName"]))

    # TODO: another source of alternative names
    # https://raw.githubusercontent.com/codesanook/thailand-administrative-division-province-district-subdistrict-sql/master/source-data.csv
    return provinces


def prov_regions_wealth():
    # TODO: Use 4 regions + greater bangkok instead
    # https://data.go.th/dataset/proviceandregionthailand - has 4, not 5.
    df = import_csv("provinces_regions", dir=".", index=["province"], date_cols=None)
    df = df.drop(columns="index")
    return df

    def clean_column_name(col):
        return (''.join(c for c in col if c not in '?:!/;()%$฿')).strip().replace(' ', '_').replace('-', '_').lower()

    # TODO: seems to have problems now from github actions?
    url = "https://en.wikipedia.org/wiki/List_of_Thai_provinces_by_GPP"
    file, _, _ = next(web_files(url, dir="inputs/html", check=False), None)
    df = pd.read_html(file)[0]

    df.columns = [clean_column_name(x) for x in df.columns]

    columns_of_interest = ['id', 'province', 'population_millions', 'gppbillions', 'gppbillionsus_nominal']
    df = df[columns_of_interest].rename({
        'gppbillions': 'gpp_billions_thb',
        'gppbillionsus_nominal': 'gpp_billions_usd'
    }, axis=1)

    df['region_id'] = np.floor(df['id'] / 100)

    df_region = df[df['id'] % 100 == 0][['region_id', 'province']].rename({'province': 'region'}, axis=1)

    df = df.merge(df_region, how='left', on='region_id')

    df = df[~(df['id'] % 100 == 0)]

    # Remap a couple of spellings
    df['province'].replace({'Chainat': 'Chai Nat', 'Phattalung': 'Phatthalung'}, inplace=True)

    # Add some extras
    df = df.combine_first(pd.DataFrame([{
        'id': 901,
        'province': 'Prison',
        'region': 'Other'
    }, {
        'id': 902,
        'province': 'Unknown',
        'region': 'Other'
    }]))
    df = df.drop(columns=["id"])
    export(df, "provinces_regions", dir=".")

    return df


@functools.lru_cache(maxsize=500, typed=False)
def get_province(prov, ignore_error=False, cutoff=0.74, split=False):
    prov = remove_prefix(prov.strip().strip(".").replace(" ", ""), "จ.")
    provinces = get_provinces()
    try:
        match = provinces.loc[prov]['ProvinceEn']
        return match if not split else [match]
    except KeyError:
        try:
            close = difflib.get_close_matches(prov, provinces.index, 1, cutoff=cutoff)[0]
        except IndexError:
            if split:
                # Might be that we have no spaces. Try divide up and see if we get a result? Giant hack.
                try_provs = [
                    get_province(p, ignore_error=True, cutoff=cutoff) for p in pythainlp.tokenize.word_tokenize(prov)
                ]
                if None in try_provs:
                    return []
                else:
                    return try_provs
                # hack way to split. just divide up
                # for i in range(2, 4):
                #     n = math.ceil(len(prov) / i)
                #     split_provs = [prov[i:i + n] for i in range(0, len(prov), n)]
                #     try_provs = [get_province(p, ignore_error=True, cutoff=cutoff) for p in split_provs]
                #     if None in try_provs:
                #         return []
                #     else:
                #         try_provs

            if ignore_error:
                return None
            else:
                raise KeyError(f"Province {prov} can't be guessed")
        proven = provinces.loc[close]['ProvinceEn']  # get english name here so we know we got it
        prov_guesses.loc[(prov_guesses.last_valid_index() or 0) + 1] = dict(Province=prov, ProvinceEn=proven, count=1)
        return proven if not split else [proven]


def prov_trim(p):
    return remove_suffix(remove_prefix(p, "จ.", "จังหวัด").strip(' .'), " Province").strip()


def join_provinces(df, on, extra=["Health District Number"], provinces=None):
    global prov_guesses
    if provinces is None:
        provinces = get_provinces()
    joined, guess = fuzzy_join(
        df.drop(columns=extra, errors="ignore"),
        provinces[extra + ["ProvinceEn"]],
        on,
        True,
        prov_trim,
        "ProvinceEn",
        return_unmatched=True)
    if not guess.empty:
        prov_guesses = guess.reset_index().rename(columns={on: "Province"})[['Province', 'ProvinceEn', 'count']]
        for i, row in prov_guesses.iterrows():
            prov_guesses.loc[(prov_guesses.last_valid_index() or 0) + 1] = row

    return joined


def get_fuzzy_provinces():
    "return dataframe of all the fuzzy matched province names"
    if not prov_guesses.empty:
        return prov_guesses.groupby(["Province", "ProvinceEn"]).sum().sort_values("count", ascending=False)
    else:
        return pd.DataFrame(columns=["Province", "ProvinceEn", "count"])


def area_crosstab(df, col, suffix="", aggfunc="sum"):
    given_2 = df.reset_index()[[
        'Date', col + suffix, 'Health District Number'
    ]]
    given_by_area_2 = pd.crosstab(given_2['Date'],
                                  given_2['Health District Number'],
                                  values=given_2[col + suffix],
                                  aggfunc=aggfunc)
    given_by_area_2.columns = [
        f"{col} Area {c}{suffix}" for c in given_by_area_2.columns
    ]
    return given_by_area_2


def region_crosstab(df, col, suffix="", aggfunc="sum"):
    given_2 = df.reset_index()[[
        'Date', col + suffix, 'region'
    ]]
    given_by_area_2 = pd.crosstab(given_2['Date'],
                                  given_2['region'],
                                  values=given_2[col + suffix],
                                  aggfunc=aggfunc)
    given_by_area_2.columns = [
        f"{col} Region: {c}{suffix}" for c in given_by_area_2.columns
    ]
    return given_by_area_2


def trend_table(table_provinces, sensitivity=25, style="green_up", ma_days=7):
    """Given Series indexed by date,province with a single value.
    Return latest values indexed by province with trend between (-1, +1)
    """
    # 14day MA just for cases
    #ma = table_provinces[['Cases','region']]
    ma = table_provinces.groupby("Province", group_keys=False).apply(lambda df: df.rolling(
        ma_days, min_periods=int(ma_days / 2), center=True).mean())
    # Too sensitive to changes
    # trend = table_provinces.groupby("Province", group_keys=False).apply(increasing(lambda df: df, 3)).to_frame("Trend")

    # Works ok but tends to make places that had a big peak in the past appear flat
    # trend = ma.groupby("Province").apply(lambda df: ((df - df.shift(7)) / df.max())) * 6

    # Use the per population number
    if "abs" in style:
        trend = ma * sensitivity
    elif "rank" in style:
        rank = ma.groupby("Date", group_keys=False).apply(lambda df: df.rank())
        peak = rank.max().max()
        trend = rank.groupby("Province", group_keys=False).apply(lambda df: (
            df - df.shift(int(math.ceil(ma_days / 2)))) / peak * sensitivity)
    else:
        ma_pop = ma.to_frame("Value").join(get_provinces()['Population'], on='Province')
        peak = ma.max().max() / ma_pop['Population'].max().max()
        trend = ma_pop.groupby("Province", group_keys=False).apply(
            lambda df: ((df['Value'] - df['Value'].shift(int(ma_days / 2))) / df['Population'])
        ) / peak * sensitivity

    trend = trend[~trend.index.duplicated()]  # TODO: not sure why increasing puts duplicates in?
    ma = ma.to_frame("MA").assign(
        Trend=trend,
        Value=table_provinces
    )

    ma = ma.reset_index("Province")
    last_day = ma.loc[ma.last_valid_index()]
    last_day = join_provinces(last_day, "Province", ["region"])
    last_day = last_day.reset_index().set_index("Province").drop(columns="Date")
    last_day["Trend_style"] = style

    return last_day
