import datetime
from dateutil.parser import parse as d
import difflib
import json
import os
import re

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
    "1: U-N: C.Mai, C.Rai, MHS, Lampang, Lamphun, Nan, Phayao, Phrae",
    "2: L-N: Tak, Phitsanulok, Phetchabun, Sukhothai, Uttaradit",
    "3: U-C: Kamphaeng Phet, Nakhon Sawan, Phichit, Uthai Thani, Chai Nat",
    "4: M-C: Nonthaburi, P.Thani, Ayutthaya, Saraburi, Lopburi, Sing Buri, Ang Thong, N.Nayok",
    "5: L-C: S.Sakhon, Kanchanaburi, N.Pathom, Ratchaburi, Suphanburi, PKK, Phetchaburi, S.Songkhram",
    "6: E: Trat, Rayong, Chonburi, S.Prakan, Chanthaburi, Prachinburi, Sa Kaeo, Chachoengsao",
    "7: M-NE: Khon Kaen, Kalasin, Maha Sarakham, Roi Et",
    "8: U-NE: S.Nakhon, Loei, U.Thani, Nong Khai, NBL, Bueng Kan, N.Phanom, Mukdahan",
    "9: L-NE: Korat, Buriram, Surin, Chaiyaphum",
    "10: E-NE: Yasothon, Sisaket, Amnat Charoen, Ubon Ratchathani",
    "11: SE: Phuket, Krabi, Ranong, Phang Nga, S.Thani, Chumphon, N.S.Thammarat",
    "12: SW: Narathiwat, Satun, Trang, Songkhla, Pattani, Yala, Phatthalung",
    "13: C: Bangkok",
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


def find_thai_date(content):
    "find thai date like '17 เม.ย. 2563' "
    m3 = re.search(r"([0-9]+) *([^ ]+) *(25[0-9][0-9])", content)
    d2, month, year = m3.groups()
    month = (
        THAI_ABBR_MONTHS.index(month) + 1
        if month in THAI_ABBR_MONTHS
        else THAI_FULL_MONTHS.index(month) + 1
        if month in THAI_FULL_MONTHS
        else None
    )
    date = datetime.datetime(year=int(year) - 543, month=month, day=int(d2))
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


def get_provinces():
    url = "https://en.wikipedia.org/wiki/Healthcare_in_Thailand#Health_Districts"
    file, _ = next(web_files(url, dir="html", check=False))
    areas = pd.read_html(file)[0]
    provinces = areas.assign(Provinces=areas['Provinces'].str.split(", ")).explode("Provinces")
    provinces['Provinces'] = provinces['Provinces'].str.strip()
    provinces = provinces.rename(columns=dict(Provinces="ProvinceEn")).drop(columns="Area Code")
    provinces['ProvinceAlt'] = provinces['ProvinceEn']
    provinces = provinces.set_index("ProvinceAlt")
    provinces.loc["Bangkok"] = [13, "Central", "Bangkok"]
    provinces.loc["Unknown"] = ["Unknown", "", "Unknown"]
    provinces.loc["Prison"] = ["Prison", "", "Prison"]
    provinces['Health District Number'] = provinces['Health District Number'].astype(str)

    # extra spellings for matching
    provinces.loc['Korat'] = provinces.loc['Nakhon Ratchasima']
    provinces.loc['Khorat'] = provinces.loc['Nakhon Ratchasima']
    provinces.loc['Suphanburi'] = provinces.loc['Suphan Buri']
    provinces.loc["Ayutthaya"] = provinces.loc["Phra Nakhon Si Ayutthaya"]
    provinces.loc["Phathum Thani"] = provinces.loc["Pathum Thani"]
    provinces.loc["Pathom Thani"] = provinces.loc["Pathum Thani"]
    provinces.loc["Ubon Thani"] = provinces.loc["Udon Thani"]
    provinces.loc["Bung Kan"] = provinces.loc["Bueng Kan"]
    provinces.loc["Chainat"] = provinces.loc["Chai Nat"]
    provinces.loc["Chon Buri"] = provinces.loc["Chonburi"]
    provinces.loc["ลาปาง"] = provinces.loc["Lampang"]
    provinces.loc["หนองบัวลาภู"] = provinces.loc["Nong Bua Lamphu"]
    provinces.loc["ปทุุมธานี"] = provinces.loc["Pathum Thani"]
    provinces.loc["เพชรบุรีี"] = provinces.loc["Phetchaburi"]
    provinces.loc["เพชรบุรีี"] = provinces.loc["Phetchaburi"]
    provinces.loc["เพชรบุุรี"] = provinces.loc["Phetchaburi"]

    provinces.loc["สมุุทรสาคร"] = provinces.loc["Samut Sakhon"]
    provinces.loc["สมุทธสาคร"] = provinces.loc["Samut Sakhon"]
    provinces.loc["กรุงเทพฯ"] = provinces.loc["Bangkok"]
    provinces.loc["กรุงเทพ"] = provinces.loc["Bangkok"]
    provinces.loc["กรงุเทพมหานคร"] = provinces.loc["Bangkok"]
    provinces.loc["พระนครศรีอยุธา"] = provinces.loc["Ayutthaya"]
    provinces.loc["อยุธยา"] = provinces.loc["Ayutthaya"]
    provinces.loc["สมุุทรสงคราม"] = provinces.loc["Samut Songkhram"]
    provinces.loc["สมุุทรปราการ"] = provinces.loc["Samut Prakan"]
    provinces.loc["สระบุุรี"] = provinces.loc["Saraburi"]
    provinces.loc["พม่า"] = provinces.loc["Nong Khai"]  # it's really burma, but have to put it somewhere
    provinces.loc["ชลบุุรี"] = provinces.loc["Chon Buri"]
    provinces.loc["นนทบุุรี"] = provinces.loc["Nonthaburi"]
    # from prov table in briefings
    provinces.loc["เชยีงใหม่"] = provinces.loc["Chiang Mai"]
    provinces.loc['จนัทบรุ'] = provinces.loc['Chanthaburi']
    provinces.loc['บรุรีมัย'] = provinces.loc['Buriram']
    provinces.loc['กาญจนบรุ'] = provinces.loc['Kanchanaburi']
    provinces.loc['Prachin Buri'] = provinces.loc['Prachinburi']
    provinces.loc['ปราจนีบรุ'] = provinces.loc['Prachin Buri']
    provinces.loc['ลพบรุ'] = provinces.loc['Lopburi']
    provinces.loc['พษิณุโลก'] = provinces.loc['Phitsanulok']
    provinces.loc['นครศรธีรรมราช'] = provinces.loc['Nakhon Si Thammarat']
    provinces.loc['เพชรบรูณ์'] = provinces.loc['Phetchabun']
    provinces.loc['อา่งทอง'] = provinces.loc['Ang Thong']
    provinces.loc['ชยัภมู'] = provinces.loc['Chaiyaphum']
    provinces.loc['รอ้ยเอ็ด'] = provinces.loc['Roi Et']
    provinces.loc['อทุยัธานี'] = provinces.loc['Uthai Thani']
    provinces.loc['ชลบรุ'] = provinces.loc['Chon Buri']
    provinces.loc['สมทุรปราการ'] = provinces.loc['Samut Prakan']
    provinces.loc['นราธวิาส'] = provinces.loc['Narathiwat']
    provinces.loc['ประจวบครีขีนัธ'] = provinces.loc['Prachuap Khiri Khan']
    provinces.loc['สมทุรสาคร'] = provinces.loc['Samut Sakhon']
    provinces.loc['ปทมุธานี'] = provinces.loc['Pathum Thani']
    provinces.loc['สระแกว้'] = provinces.loc['Sa Kaeo']
    provinces.loc['นครราชสมีา'] = provinces.loc['Nakhon Ratchasima']
    provinces.loc['นนทบรุ'] = provinces.loc['Nonthaburi']
    provinces.loc['ภเูก็ต'] = provinces.loc['Phuket']
    provinces.loc['สพุรรณบรุี'] = provinces.loc['Suphan Buri']
    provinces.loc['อดุรธานี'] = provinces.loc['Udon Thani']
    provinces.loc['พระนครศรอียธุยา'] = provinces.loc['Ayutthaya']
    provinces.loc['สระบรุ'] = provinces.loc['Saraburi']
    provinces.loc['เพชรบรุ'] = provinces.loc['Phetchaburi']
    provinces.loc['ราชบรุ'] = provinces.loc['Ratchaburi']
    provinces.loc['เชยีงราย'] = provinces.loc['Chiang Rai']
    provinces.loc['อบุลราชธานี'] = provinces.loc['Ubon Ratchathani']
    provinces.loc['สรุาษฎรธ์านี'] = provinces.loc['Surat Thani']
    provinces.loc['ฉะเชงิเทรา'] = provinces.loc['Chachoengsao']
    provinces.loc['สมทุรสงคราม'] = provinces.loc['Samut Songkhram']
    provinces.loc['แมฮ่อ่งสอน'] = provinces.loc['Mae Hong Son']
    provinces.loc['สโุขทยั'] = provinces.loc['Sukhothai']
    provinces.loc['นา่น'] = provinces.loc['Nan']
    provinces.loc['อตุรดติถ'] = provinces.loc['Uttaradit']
    provinces.loc['Nong Bua Lam Phu'] = provinces.loc['Nong Bua Lamphu']
    provinces.loc['หนองบวัล'] = provinces.loc['Nong Bua Lam Phu']
    provinces.loc['พงังา'] = provinces.loc['Phang Nga']
    provinces.loc['สรุนิทร'] = provinces.loc['Surin']
    provinces.loc['Si Sa Ket'] = provinces.loc['Sisaket']
    provinces.loc['ศรสีะเกษ'] = provinces.loc['Si Sa Ket']
    provinces.loc['ตรงั'] = provinces.loc['Trang']
    provinces.loc['พจิติร'] = provinces.loc['Phichit']
    provinces.loc['ปตัตานี'] = provinces.loc['Pattani']
    provinces.loc['ชยันาท'] = provinces.loc['Chai Nat']
    provinces.loc['พทัลงุ'] = provinces.loc['Phatthalung']
    provinces.loc['มกุดาหาร'] = provinces.loc['Mukdahan']
    provinces.loc['บงึกาฬ'] = provinces.loc['Bueng Kan']
    provinces.loc['กาฬสนิธุ'] = provinces.loc['Kalasin']
    provinces.loc['สงิหบ์รุ'] = provinces.loc['Sing Buri']
    provinces.loc['ปทมุธาน'] = provinces.loc['Pathum Thani']
    provinces.loc['สพุรรณบรุ'] = provinces.loc['Suphan Buri']
    provinces.loc['อดุรธาน'] = provinces.loc['Udon Thani']
    provinces.loc['อบุลราชธาน'] = provinces.loc['Ubon Ratchathani']
    provinces.loc['สรุาษฎรธ์าน'] = provinces.loc['Surat Thani']
    provinces.loc['นครสวรรค'] = provinces.loc['Nakhon Sawan']
    provinces.loc['ล าพนู'] = provinces.loc['Lamphun']
    provinces.loc['ล าปาง'] = provinces.loc['Lampang']
    provinces.loc['เพชรบรูณ'] = provinces.loc['Phetchabun']
    provinces.loc['อทุยัธาน'] = provinces.loc['Uthai Thani']
    provinces.loc['ก าแพงเพชร'] = provinces.loc['Kamphaeng Phet']
    provinces.loc['Lam Phu'] = provinces.loc['Nong Bua Lamphu']
    provinces.loc['หนองบวัล าภู'] = provinces.loc['Lam Phu']
    provinces.loc['ปตัตาน'] = provinces.loc['Pattani']
    provinces.loc['บรุรีัมย'] = provinces.loc['Buriram']
    provinces.loc['Buri Ram'] = provinces.loc['Buriram']
    provinces.loc['บรุรัีมย'] = provinces.loc['Buri Ram']
    provinces.loc['จันทบรุ'] = provinces.loc['Chanthaburi']
    provinces.loc['ชมุพร'] = provinces.loc['Chumphon']
    provinces.loc['อทุัยธาน'] = provinces.loc['Uthai Thani']
    provinces.loc['อ านาจเจรญ'] = provinces.loc['Amnat Charoen']
    provinces.loc['สโุขทัย'] = provinces.loc['Sukhothai']
    provinces.loc['ปัตตาน'] = provinces.loc['Pattani']
    provinces.loc['พัทลงุ'] = provinces.loc['Phatthalung']
    provinces.loc['ขอนแกน่'] = provinces.loc['Khon Kaen']
    provinces.loc['อทัุยธาน'] = provinces.loc['Uthai Thani']
    provinces.loc['หนองบัวล าภู'] = provinces.loc['Nong Bua Lam Phu']
    provinces.loc['สตลู'] = provinces.loc['Satun']
    provinces.loc['ปทุมธาน'] = provinces.loc['Pathum Thani']
    provinces.loc['ชลบุร'] = provinces.loc['Chon Buri']
    provinces.loc['จันทบุร'] = provinces.loc['Chanthaburi']
    provinces.loc['นนทบุร'] = provinces.loc['Nonthaburi']
    provinces.loc['เพชรบุร'] = provinces.loc['Phetchaburi']
    provinces.loc['ราชบุร'] = provinces.loc['Ratchaburi']
    provinces.loc['ลพบุร'] = provinces.loc['Lopburi']
    provinces.loc['สระบุร'] = provinces.loc['Saraburi']
    provinces.loc['สิงห์บุร'] = provinces.loc['Sing Buri']
    provinces.loc['ปราจีนบุร'] = provinces.loc['Prachin Buri']
    provinces.loc['สุราษฎร์ธาน'] = provinces.loc['Surat Thani']
    provinces.loc['ชัยภูม'] = provinces.loc['Chaiyaphum']
    provinces.loc['สุรินทร'] = provinces.loc['Surin']
    provinces.loc['อุบลราชธาน'] = provinces.loc['Ubon Ratchathani']
    provinces.loc['ประจวบคีรีขันธ'] = provinces.loc['Prachuap Khiri Khan']
    provinces.loc['อุตรดิตถ'] = provinces.loc['Uttaradit']
    provinces.loc['อ านาจเจริญ'] = provinces.loc['Amnat Charoen']
    provinces.loc['อุดรธาน'] = provinces.loc['Udon Thani']
    provinces.loc['เพชรบูรณ'] = provinces.loc['Phetchabun']
    provinces.loc['บุรีรัมย'] = provinces.loc['Buri Ram']
    provinces.loc['กาฬสินธุ'] = provinces.loc['Kalasin']
    provinces.loc['สุพรรณบุร'] = provinces.loc['Suphanburi']
    provinces.loc['กาญจนบุร'] = provinces.loc['Kanchanaburi']
    provinces.loc['ล าพูน'] = provinces.loc['Lamphun']
    provinces.loc["บึงกาฬ"] = provinces.loc["Bueng Kan"]
    provinces.loc['สมทุรสำคร'] = provinces.loc['Samut Sakhon']
    provinces.loc['กรงุเทพมหำนคร'] = provinces.loc['Bangkok']
    provinces.loc['สมทุรปรำกำร'] = provinces.loc['Samut Prakan']
    provinces.loc['อำ่งทอง'] = provinces.loc['Ang Thong']
    provinces.loc['ปทมุธำน'] = provinces.loc['Pathum Thani']
    provinces.loc['สมทุรสงครำม'] = provinces.loc['Samut Songkhram']
    provinces.loc['พระนครศรอียธุยำ'] = provinces.loc['Ayutthaya']
    provinces.loc['ตำก'] = provinces.loc['Tak']
    provinces.loc['ตรำด'] = provinces.loc['Trat']
    provinces.loc['รำชบรุ'] = provinces.loc['Ratchaburi']
    provinces.loc['ฉะเชงิเทรำ'] = provinces.loc['Chachoengsao']
    provinces.loc['Mahasarakham'] = provinces.loc['Maha Sarakham']
    provinces.loc['มหำสำรคำม'] = provinces.loc['Mahasarakham']
    provinces.loc['สรุำษฎรธ์ำน'] = provinces.loc['Surat Thani']
    provinces.loc['นครรำชสมีำ'] = provinces.loc['Nakhon Ratchasima']
    provinces.loc['ปรำจนีบรุ'] = provinces.loc['Prachinburi']
    provinces.loc['ชยันำท'] = provinces.loc['Chai Nat']
    provinces.loc['กำญจนบรุ'] = provinces.loc['Kanchanaburi']
    provinces.loc['อบุลรำชธำน'] = provinces.loc['Ubon Ratchathani']
    provinces.loc['นครศรธีรรมรำช'] = provinces.loc['Nakhon Si Thammarat']
    provinces.loc['นครนำยก'] = provinces.loc['Nakhon Nayok']
    provinces.loc['ล ำปำง'] = provinces.loc['Lampang']
    provinces.loc['นรำธวิำส'] = provinces.loc['Narathiwat']
    provinces.loc['สงขลำ'] = provinces.loc['Songkhla']
    provinces.loc['ล ำพนู'] = provinces.loc['Lamphun']
    provinces.loc['อ ำนำจเจรญ'] = provinces.loc['Amnat Charoen']
    provinces.loc['หนองคำย'] = provinces.loc['Nong Khai']
    provinces.loc['หนองบวัล ำภู'] = provinces.loc['Nong Bua Lam Phu']
    provinces.loc['อดุรธำน'] = provinces.loc['Udon Thani']
    provinces.loc['นำ่น'] = provinces.loc['Nan']
    provinces.loc['เชยีงรำย'] = provinces.loc['Chiang Rai']
    provinces.loc['ก ำแพงเพชร'] = provinces.loc['Kamphaeng Phet']
    provinces.loc['พทัลงุ*'] = provinces.loc['Phatthalung']
    provinces.loc['พระนครศรอียุธยำ'] = provinces.loc['Ayutthaya']
    provinces.loc['เชีียงราย'] = provinces.loc['Chiang Rai']
    provinces.loc['เวียงจันทร์'] = provinces.loc['Unknown']  # TODO: it's really vientiane
    provinces.loc['อุทัยธาน'] = provinces.loc['Uthai Thani']
    provinces.loc['ไม่ระบุ'] = provinces.loc['Unknown']
    provinces.loc['สะแก้ว'] = provinces.loc['Sa Kaeo']
    provinces.loc['ปรมิณฑล'] = provinces.loc['Bangkok']
    provinces.loc['เพชรบรุี'] = provinces.loc['Phetchaburi']
    provinces.loc['ปตัตำนี'] = provinces.loc['Pattani']
    provinces.loc['นครสวรรค์'] = provinces.loc['Nakhon Sawan']
    provinces.loc['เพชรบุรี'] = provinces.loc['Phetchaburi']
    provinces.loc['สมุทรปราการ'] = provinces.loc['Samut Prakan']
    provinces.loc['กทม'] = provinces.loc['Bangkok']
    provinces.loc['สระบุรี'] = provinces.loc['Saraburi']
    provinces.loc['ชัยภูมิ'] = provinces.loc['Chaiyaphum']
    provinces.loc['กัมพูชา'] = provinces.loc['Unknown']  # Cambodia
    provinces.loc['มาเลเซีย'] = provinces.loc['Unknown']  # Malaysia
    provinces.loc['เรอืนจา/ทีต่อ้งขงั'] = provinces.loc['Prison']
    provinces.loc['เรอืนจาฯ'] = provinces.loc["Prison"]  # Rohinja?
    provinces.loc['อานาจเจรญ'] = provinces.loc["Amnat Charoen"]
    provinces.loc['ลาพนู'] = provinces.loc["Lamphun"]
    provinces.loc['กาแพงเพชร'] = provinces.loc["Kamphaeng Phet"]
    provinces.loc['หนองบวัลาภู'] = provinces.loc["Nong Bua Lamphu"]
    provinces.loc['จนัทบุร'] = provinces.loc["Chanthaburi"]
    provinces.loc['กทม'] = provinces.loc["Bangkok"]
    provinces.loc['สพุรรณบุร'] = provinces.loc["Suphan Buri"]
    provinces.loc['สงิหบ์ุร'] = provinces.loc["Sing Buri"]
    provinces.loc['บุรรีมัย'] = provinces.loc["Buriram"]
    provinces.loc['ปราจนีบุร'] = provinces.loc["Prachinburi"]
    provinces.loc['พระนครศรอียุธยา'] = provinces.loc["Phra Nakhon Si Ayutthaya"]
    provinces.loc['เรอืนจาและทีต่อ้งขงั'] = provinces.loc["Prison"]

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
    provinces = provinces.combine_first(lup_province)

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

    # https://raw.githubusercontent.com/codesanook/thailand-administrative-division-province-district-subdistrict-sql/master/source-data.csv

    # Add in population data
    # popurl = "http://mis.m-society.go.th/tab030104.php?y=2562&p=00&d=0000&xls=y"
    popurl = "https://en.wikipedia.org/wiki/Provinces_of_Thailand"
    file, _ = next(web_files(popurl, dir="html", check=False))
    pop = pd.read_html(file)[2]
    pop = pop.join(provinces,
                   on="Name(in Thai)").set_index("ProvinceEn").rename(
                       columns={"Population (2019)[1]": "Population"})

    provinces = provinces.join(pop["Population"], on="ProvinceEn")

    return provinces


PROVINCES = get_provinces()


def get_province(prov, ignore_error=False):
    prov = remove_prefix(prov.strip().strip(".").replace(" ", ""), "จ.")
    try:
        return PROVINCES.loc[prov]['ProvinceEn']
    except KeyError:
        try:
            close = difflib.get_close_matches(prov, PROVINCES.index)[0]
        except IndexError:
            if ignore_error:
                return None
            else:
                print(f"provinces.loc['{prov}'] = provinces.loc['x']")
                raise Exception(f"provinces.loc['{prov}'] = provinces.loc['x']")
        proven = PROVINCES.loc[close]['ProvinceEn']  # get english name here so we know we got it
        prov_guesses.loc[(prov_guesses.last_valid_index() or 0) + 1] = dict(Province=prov, ProvinceEn=proven, count=1)
        return proven


def prov_trim(p):
    return remove_suffix(remove_prefix(p, "จ.").strip(' .'), " Province")


def join_provinces(df, on):
    joined, guess = fuzzy_join(
        df,
        PROVINCES[["Health District Number", "ProvinceEn"]],
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
