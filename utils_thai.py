import datetime
import functools
from dateutil.parser import parse as d
import difflib
import json
import os
import re
import pythainlp.tokenize

import pandas as pd

from utils_pandas import fuzzy_join, rearrange
from utils_scraping import remove_prefix, remove_suffix, web_files


DISTRICT_RANGE_SIMPLE = [str(i) for i in range(1, 14)]
DISTRICT_RANGE = DISTRICT_RANGE_SIMPLE + ["Prison"]
DISTRICT_RANGE_UNKNOWN = [str(i) for i in range(1, 14)] + ["Prison", "Unknown"]
POS_COLS = [f"Pos Area {i}" for i in DISTRICT_RANGE_SIMPLE]
TEST_COLS = [f"Tests Area {i}" for i in DISTRICT_RANGE_SIMPLE]

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
    "กรกฎาคม",
    "สิงหาคม",
    "กันยายน",
    "ตุลาคม",
    "พฤศจิกายน",
    "ธันวาคม",
]


def today() -> datetime.datetime:
    """Return today's date and time"""
    return datetime.datetime.today()


def to_gregyear(thai, short=False):
    thai = thai if type(thai) != str else int(thai)
    thai += (2500 if thai < 100 else 0) - 543
    return thai if not short else thai - 2000


def to_thaiyear(year, short=False):
    year = year if type(year) != str else int(year)
    year += (2000 if year < 100 else 0) + 543
    return year if not short else year - 2500


def file2date(file):
    "return date of either for '10-02-21' or '100264'"
    file = os.path.basename(file)
    file, *_ = file.rsplit(".", 1)
    if m := re.search(r"\d{4}-\d{2}-\d{2}", file):
        return d(m.group(0))
    # date = file.rsplit(".pdf", 1)[0]
    # if "-" in file:
    #     date = file.rsplit("-", 1).pop()
    # else:
    #     date = file.rsplit("_", 1).pop()
    if m := re.search(r"\d{6}", file):
        # thai date in briefing filenames
        date = m.group(0)
        return datetime.datetime(
            day=int(date[0:2]), month=int(date[2:4]), year=int(date[4:6]) - 43 + 2000
        )
    return None


def find_dates(content):
    # 7 - 13/11/2563
    dates = re.findall(r"([0-9]+)/([0-9]+)/([0-9]+)", content)
    dates = set(
        [
            datetime.datetime(day=int(d[0]), month=int(d[1]), year=int(d[2]) - 543)
            for d in dates
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


def find_thai_date(content, remove=False):
    "find thai date like '17 เม.ย. 2563' "
    thai_date = re.compile(r"([0-9]+) *([^ ]+) *(25[0-9][0-9])")
    m3 = thai_date.search(content)
    if m3 is None and remove:
        return None, content
    elif m3 is None:
        return None
    d2, month, year = m3.groups()
    month = (
        THAI_ABBR_MONTHS.index(month) + 1
        if month in THAI_ABBR_MONTHS
        else THAI_FULL_MONTHS.index(month) + 1
        if month in THAI_FULL_MONTHS
        else None
    )
    date = datetime.datetime(year=int(year) - 543, month=month, day=int(d2))
    if remove:
        return date, thai_date.sub(" ", content)
    else:
        return date


def find_date_range(content):
    "Parse thai date ranges line '11-17 เม.ย. 2563' or '04/04/2563 12/06/2563'"
    m1 = re.search(
        r"([0-9]+)/([0-9]+)/([0-9]+) [-–] ([0-9]+)/([0-9]+)/([0-9]+)", content
    )
    m2 = re.search(r"([0-9]+) *[-–] *([0-9]+)/([0-9]+)/(25[0-9][0-9])", content)
    m3 = re.search(r"([0-9]+) *[-–] *([0-9]+) *([^ ]+) *(25[0-9][0-9])", content)
    if m1:
        d1, m1, y1, d2, m2, y2 = m1.groups()
        start = datetime.datetime(day=int(d1), month=int(m1), year=int(y1) - 543)
        end = datetime.datetime(day=int(d2), month=int(m2), year=int(y2) - 543)
        return start, end
    elif m2:
        d1, d2, month, year = m2.groups()
        end = datetime.datetime(year=int(year) - 543, month=int(month), day=int(d2))
        start = previous_date(end, d1)
        return start, end
    elif m3:
        d1, d2, month, year = m3.groups()
        month = (
            THAI_ABBR_MONTHS.index(month) + 1
            if month in THAI_ABBR_MONTHS
            else THAI_FULL_MONTHS.index(month) + 1
            if month in THAI_FULL_MONTHS
            else None
        )
        end = datetime.datetime(year=int(year) - 543, month=month, day=int(d2))
        start = previous_date(end, d1)
        return start, end
    else:
        return None, None


def parse_gender(x):
    return "Male" if "ชาย" in x else "Female"


def thaipop(num: float, pos: int) -> str:
    pp = num / 69630000 * 100
    num = num / 1000000
    return f'{num:.1f}M / {pp:.1f}%'


def thaipop2(num: float, pos: int) -> str:
    pp = num / 69630000 / 2 * 100
    num = num / 1000000
    return f'{num:.1f}M / {pp:.1f}%'


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
                if type(name) is not str or len(name) <= 1:  #
                    raise ValueError(
                        f"Error in alt name: '{name}'. Unexpected error while iterating over "
                        f"mappings: {name}<-{altnames} for Province: {prov_en}"
                    )
                elif name not in r:
                    r[name] = prov_en
                elif name in r:
                    print(f"Warning: duplicate entry of {name} for Province: {prov_en} from Alt Names set: {altnames}")
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

    return df4


def prov_mapping_subdistricts(provinces):
    url = "https://raw.githubusercontent.com/codesanook/thailand-administrative-division-province-district-subdistrict-sql/master/source-data.csv"  # noqa
    file, _, _ = next(web_files(url, dir="json", check=False))
    subs = pd.read_csv(file)
    subs = subs.groupby(['AMPHOE_T', 'CHANGWAT_T']).count().reset_index()
    subs['AMPHOE_T'] = subs['AMPHOE_T'].str.replace(r"^อ. ", "", regex=True)
    subs['CHANGWAT_T'] = subs['CHANGWAT_T'].str.replace(r"^จ. ", "", regex=True)
    subs = join_provinces(subs, on="CHANGWAT_T", provinces=provinces)
    altnames = subs[['AMPHOE_T', 'CHANGWAT_T']].merge(provinces, right_index=True, left_on="CHANGWAT_T")
    # AMPHOE_T
    provinces = provinces.combine_first(
        altnames.rename(columns=dict(
            AMPHOE_T="ProvinceAlt")).set_index("ProvinceAlt"))
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
    file, _ = next(web_files(lupurl, dir="json", check=False))
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


def area_crosstab(df, col, suffix):
    given_2 = df.reset_index()[[
        'Date', col + suffix, 'Health District Number'
    ]]
    given_by_area_2 = pd.crosstab(given_2['Date'],
                                  given_2['Health District Number'],
                                  values=given_2[col + suffix],
                                  aggfunc='sum')
    given_by_area_2.columns = [
        f"{col} Area {c}{suffix}" for c in given_by_area_2.columns
    ]
    return given_by_area_2
