from json.decoder import JSONDecodeError, JSONDecoder
from typing import OrderedDict
import requests
import tabula
import os
import shutil
from tika import parser
import re
import urllib.parse
import pandas as pd
import matplotlib
import matplotlib.pyplot as plt
import datetime
from io import StringIO
from bs4 import BeautifulSoup
from pptx import Presentation
from urllib3.util.retry import Retry
import dateutil
from requests.adapters import HTTPAdapter, Retry
from webdav3.client import Client
import json
from pytwitterscraper import TwitterScraper
def TODAY(): return datetime.datetime.today()
from itertools import tee, islice, compress, cycle
import camelot

CHECK_NEWER = bool(os.environ.get("CHECK_NEWER", False))

requests.adapters.DEFAULT_RETRIES = 5  # for other tools that use requests internally
s = requests.Session()
RETRY = Retry(
    total=10, backoff_factor=1
)  # should make it more reliable as ddc.moph.go.th often fails
s.mount("http://", HTTPAdapter(max_retries=RETRY))
s.mount("https://", HTTPAdapter(max_retries=RETRY))


###############
# Date helpers
###############


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


def file2date(file):
    file = os.path.basename(file)
    date = file.rsplit(".pdf", 1)[0]
    if "-" in date:
        date = date.rsplit("-", 1).pop()
    else:
        date = date.rsplit("_", 1).pop()

    date = datetime.datetime(
        day=int(date[0:2]), month=int(date[2:4]), year=int(date[4:6]) - 43 + 2000
    )
    return date


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
    if date.day <13 and date.month <13:
        date = datetime.date(date.year, date.day, date.month)
    return date


def previous_date(end, day):
    start = end
    while start.day != int(day):
        start = start - datetime.timedelta(days=1)
    return start

def find_thai_date(content):
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
    # 11-17 เม.ย. 2563 or 04/04/2563 12/06/2563
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


def daterange(start_date, end_date, offset=0):
    for n in range(int((end_date - start_date).days) + offset):
        yield start_date + datetime.timedelta(n)


def spread_date_range(start, end, row, columns):
    r = list(daterange(start, end, offset=1))
    stats = [float(p) / len(r) for p in row]
    results = pd.DataFrame(
        [
            [
                date,
            ]
            + stats
            for date in r
        ],
        columns=columns,
    ).set_index("Date")
    return results


####################
# Extraction helpers
#####################


def parse_file(filename, html=False, paged=True):
    pages_txt = []

    # Read PDF file
    data = parser.from_file(filename, xmlContent=True)
    xhtml_data = BeautifulSoup(data["content"], features="lxml")
    pages = xhtml_data.find_all("div", attrs={"class": ["page", "slide-content"]})
    # TODO: slides are divided by slide-content and slide-master-content rather than being contained
    for i, content in enumerate(pages):
        # Parse PDF data using TIKA (xml/html)
        # It's faster and safer to create a new buffer than truncating it
        # https://stackoverflow.com/questions/4330812/how-do-i-clear-a-stringio-object
        _buffer = StringIO()
        _buffer.write(str(content))
        parsed_content = parser.from_buffer(_buffer.getvalue())
        if parsed_content["content"] is None:
            continue

        # Add pages
        text = parsed_content["content"].strip()
        if html:
            pages_txt.append(repr(content))
        else:
            pages_txt.append(text)
    if paged:
        return pages_txt
    else:
        return '\n\n\n'.join(pages_txt)


def get_next_numbers(content, *matches, debug=False, before=False):
    if len(matches) == 0:
        matches = [""]
    for match in matches:
        ahead, *behind = re.split(match, content,1) if match else ("", content)
        if not behind:
            continue
        found, *rest = behind if not before else [ahead]+behind
        numbers = re.findall(r"[,0-9]+", found)
        numbers = [n.replace(",", "") for n in numbers]
        numbers = [int(n) for n in numbers if n]
        numbers = numbers if not before else list(reversed(numbers))
        return numbers, match + " " + found
    if debug and matches:
        print("Couldn't find '{}'".format(match))
        print(content)
    return [], content


def slide2text(slide):
    text = ""
    if slide.shapes.title:
        text += slide.shapes.title.text
    for shape in slide.shapes:
        if shape.has_text_frame:
            # for p in shape.text_frame:
            text += "\n" + shape.text
    return text


def slide2chartdata(slide):
    for shape in slide.shapes:
        if not shape.has_chart:
            continue
        chart = shape.chart
        if chart is None:
            continue
        title = chart.chart_title.text_frame.text if chart.has_title else ""
        start, end = find_date_range(title)
        if start is None:
            continue
        series = dict([(s.name, s.values) for s in chart.series])

        yield chart, title, start, end, series


####################
# Download helpers
####################


def is_remote_newer(file, remote_date, check=True):
    if not os.path.exists(file):
        print(f"Missing: {file}")
        return True
    elif os.stat(file).st_size == 0:
        return True
    elif not check:
        return False
    elif remote_date is None:
        return True  # TODO: should we always keep cached?
    if type(remote_date) == str:
        remote_date = dateutil.parser.parse(remote_date).astimezone()
    fdate = datetime.datetime.fromtimestamp(os.path.getmtime(file)).astimezone()
    if remote_date > fdate:
        timestamp = fdate.strftime("%Y%m%d-%H%M%S")
        os.rename(file, f"{file}.{timestamp}")
        return True
    return False

def web_links(*index_urls, ext=".pdf", dir="html"):
    for index_url in index_urls:
        for file, index in web_files(index_url, dir=dir, check=True):
            # if index.status_code > 399: 
            #     continue
            links = re.findall("href=[\"'](.*?)[\"']", index.decode("utf-8"))
            for link in [urllib.parse.urljoin(index_url, l) for l in links if ext in l]:
                yield link

def web_files(*urls, dir=os.getcwd(), check=CHECK_NEWER):
    "if check is None, then always download"
    for url in urls:
        modified = s.head(url).headers.get("last-modified") if check else None
        file = url.rsplit("/", 1)[-1]
        file = os.path.join(dir, file)
        os.makedirs(os.path.dirname(file), exist_ok=True)
        if is_remote_newer(file, modified, check):
            r = s.get(url)
            if r.status_code != 200:
                continue
            os.makedirs(os.path.dirname(file), exist_ok=True)
            with open(file, "wb") as f:
                for chunk in r.iter_content(chunk_size=512 * 1024):
                    if chunk:  # filter out keep-alive new chunks
                        f.write(chunk)
        with open(file, "rb") as f:
            content = f.read()
        yield file, content


def dav_files(
    url="http://nextcloud.dmsc.moph.go.th/public.php/webdav", 
    username="wbioWZAQfManokc",
    password="null", 
    ext=".pdf .pptx", 
    dir="testing_moph",
    ):

    options = {
        "webdav_hostname": url,
        "webdav_login": username,
        "webdav_password": password,
    }
    client = Client(options)
    client.session.mount("http://", HTTPAdapter(max_retries=RETRY))
    client.session.mount("https://", HTTPAdapter(max_retries=RETRY))
    # important we get them sorted newest files first as we only fill in NaN from each additional file
    files = sorted(
        client.list(get_info=True),
        key=lambda info: dateutil.parser.parse(info["modified"]),
        reverse=True,
    )
    for info in files:
        file = info["path"].split("/")[-1]
        if not any([ext == file[-len(ext) :] for ext in ext.split()]):
            continue
        target = os.path.join(dir, file)
        os.makedirs(os.path.dirname(target), exist_ok=True)
        if is_remote_newer(target, info["modified"]):
            client.download_file(file, target)
        yield target


##########################################
# download and parse thailand covid data
##########################################
d = dateutil.parser.parse

def situation_cases_cum(parsedPDF, date):
    _,rest = get_next_numbers(parsedPDF, "The Disease Situation in Thailand", debug=True)
    cases, rest = get_next_numbers(
        rest, 
        "Total number of confirmed cases",
        "Characteristics of Infection in Confirmed cases",
        "Confirmed cases",
        debug=False
    )
    if not cases:
        return
    cases, *_ = cases
    if date < d("2020-04-09"):
        return pd.DataFrame([(date, cases)],columns=["Date", "Cases Cum"]).set_index("Date")
    outside_quarantine, _ = get_next_numbers(
        rest,
        "Cases found outside (?:the )?(?:state )?quarantine (?:facilities|centers)",
        debug=False
    )
    if outside_quarantine:
        outside_quarantine, *_ = outside_quarantine
        # 2647.0 # 2021-02-15
        # if date > d("2021-01-25"):
        #    # thai graphic says imported is 2396 instead of en 4195 on 2021-01-26
        #    outside_quarantine = outside_quarantine -  (4195 - 2396) 

        quarantine, _ = get_next_numbers(
            rest, 
            "Cases found in (?:the )?(?:state )?quarantine (?:facilities|centers)",
            "Staying in [^ ]* quarantine",
            debug=True)
        quarantine, *_ = quarantine
        quarantine = 1903 if quarantine == 19003 else quarantine # "2021-02-05"
        # TODO: work out date when it flips back again.
        if date < d("2020-12-28") or (date > d("2021-01-25") and outside_quarantine > quarantine):
            imported = outside_quarantine # It's mislabeled (new daily is correct however)
            imported = 2647 if imported == 609 else imported # "2021-02-17")
            imported = None if imported == 610 else imported # 2021-02-20 - 2021-03-01
            if imported is not None: 
                outside_quarantine = imported -  quarantine
            else:
                outside_quarantine = None
        else:
            imported = outside_quarantine + quarantine
    else:
        quarantine, _ = get_next_numbers(
            rest, 
            "(?i)d.?e.?signated quarantine",
            debug=False)
        quarantine, *_ = quarantine if quarantine else [None]
        quarantine = 562 if quarantine == 5562 else quarantine # "2021-09-19"
        imported, _ = get_next_numbers(
            rest, 
            "(?i)Imported Case(?:s)?",
            "(?i)Cases were imported from overseas",
            debug=False)
        imported, *_ = imported if imported else [None]
        if imported and quarantine:
            outside_quarantine = imported - quarantine
        else:
            outside_quarantine = None #TODO: can we get imported from total - quarantine - local?
    if quarantine:
        active, _ = get_next_numbers(
            rest,
            "(?i)Cases found from active case finding",
            "(?i)Cases were (?:infected )?migrant workers",
            debug=False
        )
        active, *_ = active if active else [None]

        # TODO: cum local really means all local ie walkins+active testing 
        local, _ = get_next_numbers(rest, "(?i)(?:Local )?Transmission", debug=False)
        local, *_ = local if local else [None]
        # TODO: 2021-01-25. Local 6629.0 -> 12250.0, quarantine 597.0 -> 2396.0 active 4684.0->5532.0
        if imported is None:
            pass
        elif cases-imported == active:
            walkin = local
            local = cases-imported
            active = local - walkin
        elif active is None:
            pass
        elif local + active == cases-imported:
            # switched to different definition?
            walkin = local
            local = walkin + active
        elif date <= d("2021-01-25") or d("2021-02-16") <= date <= d("2021-03-01"):
            walkin = local
            local = walkin + active
    else:
        local, active = None, None
    
    #assert cases == (local+imported) # Too many mistakes
    return pd.DataFrame(
        [(date, cases, local, imported, quarantine, outside_quarantine, active)],
        columns=["Date", "Cases Cum", "Cases Local Transmission Cum", "Cases Imported Cum", "Cases In Quarantine Cum", "Cases Outside Quarantine Cum", "Cases Proactive Cum"]
        ).set_index("Date")

def situation_cases_new(parsedPDF, date):
    if date < d("2020-11-02"):
        return
    _,rest = get_next_numbers(
        parsedPDF, 
        "The Disease Situation in Thailand", 
        "(?i)Type of case Total number Rate of Increase",
        debug=False)
    cases, rest = get_next_numbers(
        rest, 
        "(?i)number of new case(?:s)?",
        debug=False
    )
    if not cases or date < d("2020-05-09"):
        return
    cases, *_ = cases
    local, _ = get_next_numbers(rest, "(?i)(?:Local )?Transmission", debug=False)
    local, *_ = local if local else [None]
    quarantine, _ = get_next_numbers(
        rest, 
        "Cases found (?:positive from |in )(?:the )?(?:state )?quarantine",
        #"Staying in [^ ]* quarantine",
        debug=False)
    quarantine, *_ = quarantine
    quarantine = {d("2021-01-27"):11}.get(date, quarantine) # corrections from thai doc
    outside_quarantine, _ = get_next_numbers(
        rest,
        "(?i)Cases found (?:positive )?outside (?:the )?(?:state )?quarantine",
        debug=False
    )
    outside_quarantine, *_ = outside_quarantine if outside_quarantine else [None]
    if outside_quarantine is not None:
        imported = quarantine + outside_quarantine
        active, _ = get_next_numbers(
            rest,
            "(?i)active case",
            debug=True
        )
        active, *_ = active if active else [None]
        if date <= d("2020-12-24"): # starts getting cum values
            active = None
        # local really means walkins. so need add it up
        if active:
            local = local + active
    else:
        imported, active = None, None
    cases = {d("2021-03-31"):42}.get(date, cases)
    #if date not in [d("2020-12-26")]:
    #    assert cases == (local+imported) # except 2020-12-26 - they didn't include 30 proactive
    return pd.DataFrame(
        [(date, cases, local, imported, quarantine, outside_quarantine, active)],
        columns=["Date", "Cases", "Cases Local Transmission", "Cases Imported", "Cases In Quarantine", "Cases Outside Quarantine", "Cases Proactive"]
        ).set_index("Date")


def situation_pui(parsedPDF, date):
    numbers, _ = get_next_numbers(
        parsedPDF, "Total +number of laboratory tests", debug=False
    )
    if numbers:
        tests_total, pui, active_finding, asq, not_pui, pui, pui_port, *rest = numbers
    else:
        numbers, _ = get_next_numbers(
            parsedPDF, "Total number of people who met the criteria of patients", debug=False,
        )
        if date > dateutil.parser.parse("2020-01-30") and not numbers:
            raise Exception(f"Problem parsing {date}")
        elif not numbers:
            return
        tests_total, active_finding, asq, not_pui = [None] * 4
        pui, pui_airport, pui_seaport, pui_hospital, *rest = numbers
        pui_port = pui_airport + pui_seaport
    if pui in [1103858, 3891136, 433807, 96989]:  # mistypes?
        pui = None
    elif tests_total in [783679, 849874, 936458]:
        tests_total = None
    elif None in (tests_total, pui, active_finding, asq, not_pui) and date > d("2020-06-30"):
        raise Exception(f"Missing data at {date}")
    row = (tests_total, pui, active_finding, asq, not_pui)
    return pd.DataFrame(
        [(date, )+row],
        columns=["Date", "Tested Cum", "Tested PUI Cum", "Tested Proactive Cum", "Tested Quarantine Cum", "Tested Not PUI Cum"]
        ).set_index("Date")


def get_en_situation():
    results = pd.DataFrame(columns=["Date"]).set_index("Date")
    url = "https://ddc.moph.go.th/viralpneumonia/eng/situation.php"
    for file, _ in web_files(*web_links(url, ext=".pdf", dir="situation_en"), dir="situation_en"):
        parsedPDF = parse_file(file, html=False, paged=False)
        if "situation" not in os.path.basename(file):
            continue
        date = file2date(file)
        if date <= dateutil.parser.parse("2020-01-30"):
            continue # TODO: can manually put in numbers before this
        df = situation_pui(parsedPDF, date)
        if df is not None:
            results = results.combine_first(df)
        df = situation_cases_cum(parsedPDF, date)
        if df is not None:
            results = results.combine_first(df)
        df = situation_cases_new(parsedPDF, date)
        if df is not None:
            results = results.combine_first(df)
        cums = [c for c in results.columns if ' Cum' in c]
        # if len(results) > 1 and (results.iloc[0][cums] > results.iloc[1][cums]).any():
        #     print((results.iloc[0][cums] > results.iloc[1][cums]))
        #     print(results.iloc[0:2])
            #raise Exception("Cumulative data didn't increase")
        row = results.iloc[0].to_dict()
        print(
            file, 
            "p{Tested PUI Cum:.0f}\tc{Cases Cum:.0f}({Cases:.0f})\t"
            "l{Cases Local Transmission Cum:.0f}({Cases Local Transmission:.0f})\t"
            "a{Cases Proactive Cum:.0f}({Cases Proactive:.0f})\t"
            "i{Cases Imported Cum:.0f}({Cases Imported:.0f})\t"
            "q{Cases In Quarantine Cum:.0f}({Cases In Quarantine:.0f})\t"
            "".format(**row)
        )
    # Missing data. filled in from th infographic
    missing = [
        (d("2020-12-19"),2476,0, 0),
        (d("2020-12-20"),3011,516, 516),
        (d("2020-12-21"),3385,876, 360),
        (d("2020-12-22"),3798,1273, 397),
        (d("2020-12-23"),3837,1273, 0),
        (d("2020-12-24"),3895,1273, 0),
        (d("2020-12-25"),3976,1308, 35),
    ]
    missing = pd.DataFrame(
        missing,
        columns=["Date","Cases Local Transmission Cum","Cases Proactive Cum", "Cases Proactive"]
    ).set_index("Date")
    results = missing[["Cases Local Transmission Cum","Cases Proactive Cum",]].combine_first(results)
    return results

def get_situation_today():
    _, page = next(web_files("https://ddc.moph.go.th/viralpneumonia/index.php", dir="situation_th", check=True))
    text = BeautifulSoup(page, 'html.parser').get_text()
    numbers, rest = get_next_numbers(text, "ผู้ป่วยเข้าเกณฑ์เฝ้าระวัง")
    pui_cum, pui = numbers[:2]
    numbers, rest = get_next_numbers(text, "กักกันในพื้นที่ที่รัฐกำหนด")
    imported_cum, imported = numbers[:2]
    numbers, rest = get_next_numbers(text, "ผู้ป่วยยืนยัน")
    cases_cum, cases = numbers[:2]
    numbers, rest = get_next_numbers(text, "สถานการณ์ในประเทศไทย")
    date = find_thai_date(rest).date()
    row = [cases_cum, cases, pui_cum, pui, imported_cum, imported]
    return pd.DataFrame(
        [[date,]+row],
        columns=["Date", "Cases Cum", "Cases", "Tested PUI Cum", "Tested PUI", "Cases Imported Cum", "Cases Imported"]
    ).set_index("Date")
    
    


def situation_pui_th(parsedPDF, date):
    tests_total, active_finding, asq, not_pui = [None] * 4
    numbers, content = get_next_numbers(
        parsedPDF,
        "ด่านโรคติดต่อระหว่างประเทศ",
        "ด่านโรคติดต่อระหวา่งประเทศ",  # 'situation-no346-141263n.pdf'
        "นวนการตรวจทาง\S+องปฏิบัติการ",
        "ด่านควบคุมโรคติดต่อระหว่างประเทศ",
    )
    # cases = None
    if len(numbers) > 6:
        (
            screened_port,
            screened_cw,
            tests_total,
            pui,
            active_finding,
            asq,
            not_pui,
            *rest,
        ) = numbers
        if tests_total < 30000:
            tests_total, pui, active_finding, asq, not_pui, *rest = numbers
            if pui == 4534137:
                pui = 453413  # situation-no273-021063n.pdf
    else:
        numbers, content = get_next_numbers(
            parsedPDF,
#            "ผู้ป่วยที่มีอาการเข้าได้ตามนิยาม",
            "ตาราง 2 ผลดำ",
            "ตาราง 1",  # situation-no172-230663.pdf #'situation-no83-260363_1.pdf'
        )
        if len(numbers) > 0:
            pui, *rest = numbers
    if date > dateutil.parser.parse("2020-03-26") and not numbers:
        raise Exception(f"Problem parsing {date}")
    elif not numbers:
        return
    if tests_total == 167515: # situation-no447-250364.pdf
        tests_total = 1675125

    if (
        tests_total is not None
        and tests_total > 2000000 < 30000
        or pui > 1500000 < 100000
    ):
        raise Exception(f"Bad data in {date}")
    # merge(file, date, (date, tests_total, pui, active_finding, asq, not_pui, None))
    row = (tests_total, pui, active_finding, asq, not_pui)
    if None in row and date > d("2020-06-30"):
        raise Exception(f"Missing data at {date}")
    return pd.DataFrame(
        [(date,)+row],
        columns=["Date", "Tested Cum", "Tested PUI Cum", "Tested Proactive Cum", "Tested Quarantine Cum", "Tested Not PUI Cum"]
    ).set_index("Date")
     
def get_thai_situation():
    results = pd.DataFrame(columns=["Date"]).set_index("Date")
    for file, parsedPDF in web_files(
        *web_links(
            "https://ddc.moph.go.th/viralpneumonia/situation.php",
            "https://ddc.moph.go.th/viralpneumonia/situation_more.php",
            ext=".pdf",
            dir="situation_th"
        ),
        dir="situation_th",
    ):
        parsedPDF = parse_file(file, html=False, paged=False)
        if "situation" not in os.path.basename(file):
            continue
        if "Situation Total number of PUI" in parsedPDF:
            # english report mixed up? - situation-no171-220663.pdf
            continue
        date = file2date(file)
        df = situation_pui_th(parsedPDF, date)
        if df is not None:
            results = results.combine_first(df)
            print(
                file, 
                "p{Tested PUI Cum:.0f}\t"
                "t{Tested Cum:.0f}\t"
                "{Tested Proactive Cum:.0f}\t"
                "{Tested Quarantine Cum:.0f}\t" 
                "{Tested Not PUI Cum:.0f}\t"
                "".format(**results.iloc[0].to_dict()))

    print(results)
    return results

def cum2daily(results):
    cum = results[(c for c in results.columns if " Cum" in c)]
    all_days = pd.date_range(cum.index.min(), cum.index.max(), name="Date")
    cum = cum.reindex(all_days) # put in missing days with NaN
    #cum = cum.interpolate(limit_area="inside") # missing dates need to be filled so we don't get jumps
    cum = cum - cum.shift(+1)  # we got cumilitive data
    renames = dict((c,c.rstrip(' Cum')) for c in list(cum.columns) if 'Cum' in c)
    cum = cum.rename(columns=renames)
    return cum

   

def get_situation():
    today_situation = get_situation_today()
    en_situation = get_en_situation()
    th_situation = get_thai_situation()
    situation = th_situation.combine_first(en_situation)
    situation = situation.combine_first(today_situation)
    cum = cum2daily(situation)
    situation = situation.combine_first(cum) # any direct non-cum are trusted more

    os.makedirs("api", exist_ok=True)
    situation.reset_index().to_json(
        "api/situation_reports", 
        date_format="iso",
        indent=3,
        orient="records",
    )
    situation.reset_index().to_csv(
        "api/situation_reports.csv", 
        index=False 
    )
    return situation


def get_cases():
    try:
        timeline = s.get("https://covid19.th-stat.com/api/open/timeline").json()["Data"]
    except JSONDecodeError:
        return pd.DataFrame()

    results = []
    for d in timeline:
        date = datetime.datetime.strptime(d["Date"], "%m/%d/%Y")
        cases = d["NewConfirmed"]
        # merge('timeline', date, (date, None, None, None, None, None, cases))
        results.append((date, cases))
    data = pd.DataFrame(results, columns=["Date", "Cases"]).set_index("Date")
    print(data)
    return data

def get_tests_by_day():
    file = next(dav_files(ext="xlsx"))
    tests = pd.read_excel(file, parse_dates=True, usecols=[0, 1, 2])
    tests.dropna(how="any", inplace=True)  # get rid of totals row
    tests = tests.set_index("Date")
    pos = tests.loc["Cannot specify date"].Pos
    total = tests.loc["Cannot specify date"].Total
    tests.drop("Cannot specify date", inplace=True)
    # Need to redistribute the unknown values across known values
    # Documentation tells us it was 11 labs and only before 3 April
    unknown_end_date = datetime.datetime(day=3, month=4, year=2020)
    all_pos = tests["Pos"][:unknown_end_date].sum()
    all_total = tests["Total"][:unknown_end_date].sum()
    for index, row in tests.iterrows():
        if index > unknown_end_date:
            continue
        row.Pos = float(row.Pos) + row.Pos / all_pos * pos
        row.Total = float(row.Total) + row.Total / all_total * total
    # TODO: still doesn't redistribute all missing values due to rounding. about 200 left
    print(tests["Pos"].sum(), pos + all_pos)
    print(tests["Total"].sum(), total + all_total)
    # fix datetime
    tests.reset_index(drop=False, inplace=True)
    tests["Date"] = pd.to_datetime(tests["Date"])
    tests.set_index("Date", inplace=True)

    tests.rename(columns=dict(Pos="Pos XLS", Total="Tests XLS"), inplace=True)

    return tests


POS_AREA_COLS = ["Pos Area {}".format(i + 1) for i in range(13)]
TESTS_AREA_COLS = ["Tests Area {}".format(i + 1) for i in range(13)]


def get_tests_by_area():
    columns = ["Date"] + POS_AREA_COLS + TESTS_AREA_COLS + ["Pos Area", "Tests Area"]
    raw_cols = ["Start", "End",] + POS_AREA_COLS + TESTS_AREA_COLS
    data = pd.DataFrame()
    raw = pd.DataFrame()

    for file in dav_files(ext=".pptx"):
        prs = Presentation(file)
        for chart in (
            chart for slide in prs.slides for chart in slide2chartdata(slide)
        ):
            chart, title, start, end, series = chart
            if not "เริ่มเปิดบริการ" in title and any(
                t in title for t in ["เขตสุขภาพ", "เขตสุขภำพ"]
            ):
                # the graph for X period split by health area.
                # Need both pptx and pdf as one pdf is missing
                pos = list(series["จำนวนผลบวก"])
                tests = list(series["จำนวนตรวจ"])
                row = pos + tests + [sum(pos), sum(tests)]
                results = spread_date_range(start, end, row, columns)
                print(results)
                data = data.combine_first(results)
                raw = raw.combine_first(pd.DataFrame(
                    [[start,end,]+pos + tests],
                    columns=raw_cols
                ).set_index("Start"))
    # Also need pdf copies becaus of missing pptx
    for file in dav_files(ext=".pdf"):
        pages = parse_file(file, html=False, paged=True)
        not_whole_year = [page for page in pages if "เริ่มเปิดบริการ" not in page]
        by_area = [
            page
            for page in not_whole_year
            if "เขตสุขภาพ" in page or "เขตสุขภำพ" in page
        ]
        # Can't parse '35_21_12_2020_COVID19_(ถึง_18_ธันวาคม_2563)(powerpoint).pptx' because data is a graph
        # no pdf available so data missing
        # Also missing 14-20 Nov 2020 (no pptx or pdf)

        for page in by_area:
            start, end = find_date_range(page)
            if start is None:
                continue
            if "349585" in page:
                page = page.replace("349585", "349 585")
            # if '16/10/2563' in page:
            #     print(page)
            # First line can be like จดัท ำโดย เพญ็พชิชำ ถำวงศ ์กรมวิทยำศำสตณก์ำรแพทย์ วันที่ท ำรำยงำน 15/02/2564 เวลำ 09.30 น.
            first, rest = page.split("\n", 1)
            page = (
                rest if "เพญ็พชิชำ" in first or "/" in first else page
            )  # get rid of first line that sometimes as date and time in it
            numbers, content = get_next_numbers(page, "", debug=True)  # "ภาคเอกชน",
            # ภาครัฐ
            # ภาคเอกชน
            # จดัท ำโดย เพญ็พชิชำ ถำวงศ ์กรมวิทยำศำสตณก์ำรแพทย์
            # print(numbers)
            # TODO: should really find and parse X axis labels which contains 'เขต' and count
            tests_start = 13 if "total" not in page else 14
            pos = numbers[0:13]
            tests = numbers[tests_start : tests_start + 13]
            row = pos + tests + [sum(pos), sum(tests)]
            results = spread_date_range(start, end, row, columns)
            print(results)
            data = data.combine_first(results)
            raw = raw.combine_first(pd.DataFrame(
                [[start,end,]+pos + tests],
                columns=raw_cols
            ).set_index("Start"))
    os.makedirs("api", exist_ok=True)
    raw.reset_index().to_json(
        "api/tests_by_area", 
        date_format="iso",
        indent=3,
        orient="records",
    )
    raw.reset_index().to_csv(
        "api/tests_by_area.csv",
        index=False 
    )
    return data


def get_tests_private_public():
    data = pd.DataFrame()

    # some additional data from pptx files
    for file in dav_files(ext=".pptx"):
        prs = Presentation(file)
        for chart in (
            chart for slide in prs.slides for chart in slide2chartdata(slide)
        ):
            chart, title, start, end, series = chart
            if not "เริ่มเปิดบริการ" in title and any(
                t in title for t in ["เขตสุขภาพ", "เขตสุขภำพ"]
            ):
                # area graph
                continue
            elif "และอัตราการตรวจพบ" in title and "รายสัปดาห์" not in title:
                # The graphs at the end with all testing numbers private vs public
                private = "Private" if "ภาคเอกชน" in title else "Public"

                # pos = series["Pos"]
                if "จำนวนตรวจ" not in series:
                    continue
                tests = series["จำนวนตรวจ"]
                positivity = series["% Detection"]
                dates = list(daterange(start, end, 1))
                df = pd.DataFrame(
                    {
                        "Date": dates,
                        f"Tests {private}": tests,
                        f"% Detection {private}": positivity,
                    }
                ).set_index("Date")
                df[f"Pos {private}"] = (
                    df[f"Tests {private}"] * df[f"% Detection {private}"] / 100.0
                )
                print(df)
                data = data.combine_first(df)
            # TODO: There is also graphs splt by hospital
    os.makedirs("api", exist_ok=True)
    data.reset_index().to_json(
        "api/tests_pubpriv",
        date_format="iso",
        indent=3,
        orient="records",
    )
    data.reset_index().to_csv(
        "api/tests_pubpriv.csv",
        index=False 
    )
    return data

def get_provinces():
    #_, districts = next(web_files("https://en.wikipedia.org/wiki/Healthcare_in_Thailand#Health_Districts", dir="html"))
    areas = pd.read_html("https://en.wikipedia.org/wiki/Healthcare_in_Thailand#Health_Districts")[0]
    provinces = areas.assign(Provinces=areas['Provinces'].str.split(",")).explode("Provinces")
    provinces['Provinces'] = provinces['Provinces'].str.strip()
    provinces = provinces.rename(columns=dict(Provinces="ProvinceEn")).drop(columns="Area Code")
    provinces['ProvinceAlt'] = provinces['ProvinceEn']
    provinces = provinces.set_index("ProvinceAlt")
    provinces.loc["Bangkok"] = [13, "Central", "Bangkok"]

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
    provinces.loc["พม่า"] = provinces.loc["Nong Khai"] # it's really burma, but have to put it somewhere
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
    provinces.loc['เพชรบรุ'] = provinces.loc['Phetchabun']
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
    provinces.loc['ปตัตานี'] = provinces.loc['Phichit']
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

    # use the case data as it has a mapping between thai and english names
    _, cases = next(web_files("https://covid19.th-stat.com/api/open/cases", dir="json", check=False))
    cases = pd.DataFrame(json.loads(cases)["Data"])
    cases = cases.rename(columns=dict(Province="ProvinceTh", ProvinceAlt="Provinces"))
    lup_province = cases.groupby(['ProvinceId', 'ProvinceTh', 'ProvinceEn']).size().reset_index().rename({0:'count'}, axis=1).sort_values('count', ascending=False).set_index("ProvinceEn")
    # get the proper names from provinces
    lup_province = lup_province.reset_index().rename(columns=dict(ProvinceEn="ProvinceAlt"))
    lup_province = lup_province.set_index("ProvinceAlt").join(provinces)
    lup_province = lup_province.drop(index="Unknown")
    lup_province = lup_province.set_index("ProvinceTh").drop(columns="count")

    # now bring in the thainames as extra altnames
    provinces = provinces.combine_first(lup_province)

    # bring in some appreviations
    abr = pd.read_csv("https://raw.githubusercontent.com/kristw/gridmap-layout-thailand/master/src/input/provinces.csv")
    on_enname = abr.merge(provinces, right_index=True, left_on="enName")
    provinces = provinces.combine_first(on_enname.rename(columns=dict(thName="ProvinceAlt")).set_index("ProvinceAlt").drop(columns=["enAbbr", "enName","thAbbr"]))
    provinces = provinces.combine_first(on_enname.rename(columns=dict(thAbbr="ProvinceAlt")).set_index("ProvinceAlt").drop(columns=["enAbbr", "enName","thName"]))

    on_thai = abr.merge(provinces, right_index=True, left_on="thName")
    provinces = provinces.combine_first(on_thai.rename(columns=dict(enName="ProvinceAlt")).set_index("ProvinceAlt").drop(columns=["enAbbr", "thName","thAbbr"]))
    provinces = provinces.combine_first(on_thai.rename(columns=dict(thAbbr="ProvinceAlt")).set_index("ProvinceAlt").drop(columns=["enAbbr", "enName","thName"]))
    provinces = provinces.combine_first(on_thai.rename(columns=dict(enAbbr="ProvinceAlt")).set_index("ProvinceAlt").drop(columns=["thAbbr", "enName","thName"]))

    # https://raw.githubusercontent.com/codesanook/thailand-administrative-division-province-district-subdistrict-sql/master/source-data.csv

    return provinces

PROVINCES = get_provinces()


def get_cases_by_area_type():
    briefings, cases = get_cases_by_prov_briefings()
    dfprov,twcases = get_cases_by_prov_tweets()
    cases = cases.combine_first(twcases)
    dfprov = briefings.combine_first(dfprov) # TODO: check they aggree
    dfprov = dfprov.join(PROVINCES['Health District Number'], on="Province")
    # Now we can save raw table of provice numbers
    dfprov.reset_index().to_json(
        "api/cases_by_province",
        date_format="iso",
        indent=3,
        orient="records",
    )
    dfprov.reset_index().to_csv(
        "api/cases_by_province.csv",
        index=False 
    )

    # Reduce down to health areas
    dfprov_grouped = dfprov.groupby(["Date","Health District Number"]).sum(min_count=1).reset_index()
    dfprov_grouped = dfprov_grouped.pivot(index="Date",columns=['Health District Number'])
    dfprov_grouped = dfprov_grouped.rename(columns=dict((i,f"Area {i}") for i in range(1,14)))
    #cols = dict((f"Area {i}", f"Cases Area {i}") for i in range(1,14))
    #by_area = dfprov_grouped["Cases"].groupby(['Health District Number'],axis=1).sum(min_count=1).rename(columns=cols)
    #cols = dict((f"Area {i}", f"Cases Proactive Area {i}") for i in range(1,14))
    by_type = dfprov_grouped.groupby(level=0, axis=1).sum(min_count=1)
    # Collapse columns to "Cases Proactive Area 13" etc
    dfprov_grouped.columns = dfprov_grouped.columns.map(' '.join).str.strip()
    by_area = dfprov_grouped.combine_first(by_type)
    by_area = by_area.combine_first(cases) # imported, proactive total etc

    # Ensure we have all areas
    for i in range(1,14):
        col = f"Cases Walkin Area {i}"
        if col not in by_area:
            by_area[col] = by_area.get(col, pd.Series(index=by_area.index, name=col))
        col = f"Cases Proactive Area {i}"
        if col not in by_area:
            by_area[col] = by_area.get(col, pd.Series(index=by_area.index, name=col))
    return by_area


def get_case_details_csv():
    url = "https://data.go.th/dataset/covid-19-daily"
    links = [l for l in web_links(url, ext=".csv") if "pm-" in l]
    file, _ = next(web_files(*links, dir="json"))
    cases = pd.read_csv(file)
    cases['announce_date'] = pd.to_datetime(cases['announce_date'], dayfirst=True)
    cases['Notified date'] = pd.to_datetime(cases['Notified date'], dayfirst=True,)

    return cases

def get_case_details_api():
#    _, cases = next(web_files("https://covid19.th-stat.com/api/open/cases", dir="json"))
    url = "https://data.go.th/api/3/action/datastore_search?resource_id=329f684b-994d-476b-91a4-62b2ea00f29f&limit=1000&offset="
    records = []
    def get_page(i, check=False):
        _, cases = next(web_files(f"{url}{i}", dir="json", check=check))
        return json.loads(cases)['result']['records']

    for i in range(0,100000,1000):
        data = get_page(i, False)
        if len(data) < 1000:
            data = get_page(i, True)
            if len(data) < 1000:
                break
        records.extend(data)
    # they screwed up the date conversion. d and m switched sometimes
    # TODO: bit slow. is there way to do this in pandas?
    for record in records:
        record['announce_date'] = to_switching_date(record['announce_date'])
        record['Notified date'] = to_switching_date(record['Notified date'])
    cases = pd.DataFrame(records)
    return cases

def get_cases_by_area_api():
    cases = get_case_details_csv()
#    cases['Date'] = pd.to_datetime(cases['announce_date'], format='%Y-%d-%m',errors='coerce')
#    cases['Notified date'] = pd.to_datetime(cases['Notified date'], format='%Y-%d-%m',)
    cases["province_of_onset"] = cases["province_of_onset"].str.strip(".")
    cases = cases.join(PROVINCES["Health District Number"], on="province_of_onset")
    unjoined = cases.loc[(cases["Health District Number"].isnull()) & (cases["province_of_onset"].notnull())]
    assert unjoined.empty, f"Missing prov: {list(unjoined.index)}"
    cases = cases.rename(columns=dict(announce_date="Date"))
    case_areas = pd.crosstab(pd.to_datetime(cases['Date']).dt.date,cases['Health District Number'])
    case_areas = case_areas.rename(columns=dict((i,f"Cases Area {i}") for i in range(1,14)))
    return case_areas

def get_cases_by_area():
    case_areas = get_cases_by_area_api() # can be very wrong for the last days

    # we will add in the tweet data for the export
    case_tweets = get_cases_by_area_type()

    case_areas = case_tweets.combine_first(case_areas)

    os.makedirs("api", exist_ok=True)
    case_areas.reset_index().to_json(
        "api/cases_by_area",
        date_format="iso",
        indent=3,
        orient="records",
    )
    case_areas.reset_index().to_csv(
        "api/cases_by_area.csv",
        index=False 
    )

    
    return case_areas


def parse_tweet(tw, tweet, found, *matches):
    is_match = lambda tweet, *matches: any(m in tweet for m in matches)
    if not is_match(tweet.get('text',tweet.get("comment","")), *matches):
        return ""
    text = tw.get_tweetinfo(tweet['id']).contents['text']
    if any(text in t for t in found):
        return ""
    # TODO: ensure tweets are [1/2] etc not just "[" and by same person
    if "[" not in text:
        return text
    for t in sorted(tw.get_tweetcomments(tweet['id']).contents, key=lambda t:t['id']):
        rest = parse_tweet(tw, t, found+[text], *matches)
        if rest and rest not in text:
            text += " " + rest 
    return text

def get_tweets_from(userid, datefrom, dateto, *matches):
    import pickle
    tw = TwitterScraper()
    filename = f"tweets/tweets2_{userid}.pickle"
    os.makedirs("tweets", exist_ok=True)
    #is_match = lambda tweet, *matches: any(m in tweet for m in matches)
    try:
        with open(filename,"rb") as fp:
            tweets = pickle.load(fp)
    except:
        tweets = {}
    latest = max(tweets.keys()) if tweets else None
    if latest and dateto and latest >= (datetime.datetime.today() if not dateto else dateto).date():
        return tweets
    for limit in ([50,2000,5000] if tweets else [5000]):
        print(f"Getting {limit} tweets")       
        for tweet in sorted(tw.get_tweets(userid, count=limit).contents,key=lambda t:t['id']):
            date = tweet['created_at'].date()
            text = parse_tweet(tw, tweet, tweets.get(date,[]), *matches)
            if text:
                tweets[date] = tweets.get(date,[]) + [text]
            # if not is_match(tweet['text'], *matches):
            #     continue
            # text = tw.get_tweetinfo(tweet['id']).contents['text']
            # if text not in tweets.get(date,[]):
            #     tweets[date] = tweets.get(date,[]) + [text]
            # # TODO: ensure tweets are [1/2] etc not just "[" and by same person
            # if "[" not in text:
            #     continue
            # rest = [t for t in tw.get_tweetcomments(tweet['id']).contents if is_match(t['comment'],"[", *matches)]
            # for t in rest:
            #     text = tw.get_tweetinfo(t['id']).contents['text'] 
            #     if text not in tweets.get(date,[]):
            #         tweets[date] = tweets.get(date,[]) + [text]

                
        earliest = min(tweets.keys())
        latest = max(tweets.keys())
        print(f"got tweets {earliest} to {latest} {len(tweets)}")
        if earliest <= datefrom.date(): #TODO: ensure we have every tweet in sequence?
            break
        else:
            print(f"Retrying: Earliest {earliest}")
    with open(filename,"wb") as fp:
        pickle.dump(tweets, fp)

    # # join tweets
    # for date,lines in tweets.items():
    #     newlines = []
    #     tomerge = []
    #     i=1
    #     for line in lines:
    #         m = re.search(r"\[([0-9]+)\/([0-9]+)\]", line)
    #         if m:
    #             i = int(m.group(1))
    #             of = int(m.group(2))
    #             tomerge.append( (i,of, line))
    #         elif "[" in line:
    #             tomerge.append((i,0,line))
    #             i+=1
    #         else:
    #             newlines.append(line)
    #     # TODO: somethings he forgets to put in [2/2]. need to use threads
    #     if tomerge:        
    #         tomerge.sort()
    #         text = ' '.join(text for i,of,text in tomerge)
    #         newlines.append(text)
    #     tweets[date] = newlines
    return tweets



def get_cases_by_prov_tweets():
    #tw = TwitterScraper()

    # Get tweets
    # 2021-03-01 and 2021-03-05 are missing
    new = get_tweets_from(531202184, d("2021-04-03"), None, "Official #COVID19 update", "📍")
    #old = get_tweets_from(72888855, d("2021-01-14"), d("2021-04-02"), "Official #COVID19 update", "📍")
    old = get_tweets_from(72888855, d("2021-02-21"), None, "Official #COVID19 update", "📍")
    
    officials = {}
    provs = {}
    for date,tweets in list(new.items())+list(old.items()):
        for tweet in tweets:
            if "RT @RichardBarrow" in tweet:
                continue
            if "Official #COVID19 update" in tweet:
                officials[date] = tweet
            elif "👉" in tweet and "📍" in tweet:
                if tweet in provs.get(date,""):
                    continue
                provs[date] = provs.get(date,"") + " " + tweet

    # Get imported vs walkin totals
    df = pd.DataFrame()
    def toint(s):
        return int(s.replace(',','')) if s else None

    for date, text in officials.items():
        imported = toint(re.search("\+([0-9,]+) imported", text).group(1))
        local = toint(re.search("\+([0-9,]+) local", text).group(1))
        cols = ["Date", "Cases Imported", "Cases Local Transmission"]
        row = [date,imported,local]
        df = df.combine_first(pd.DataFrame([row], columns=cols).set_index("Date"))    

    # get walkin vs proactive by area
    walkins = {}
    proactive = {}
    for date, text in provs.items():
        if "📍" not in text:
            continue
        start,*lines = text.split("👉",2)
        if len(lines) < 2:
            raise Exception()
        for line in lines:
            prov_matches = re.findall("📍([\s\w,&;]+) ([0-9]+)", line)
            prov = dict((p.strip(),toint(v)) for ps,v in prov_matches for p in re.split("(?:,|&amp;)",ps))
            if d("2021-04-08").date() == date:
                if prov["Bangkok"] == 147: #proactive
                    prov["Bangkok"] = 47
                elif prov["Phuket"] == 3: #Walkins
                    prov["Chumphon"] = 3
                    prov['Khon Kaen'] = 3
                    prov["Ubon Thani"] = 7
                    prov["Nakhon Pathom"] = 6
                    prov["Phitsanulok"] = 4

            label = re.findall('^ *([0-9]+)([^📍👉👇\[]*)', line)
            if label:
                total,label = label[0]
                #label = label.split("👉").pop() # Just in case tweets get muddled 2020-04-07
                total = toint(total)
            else:
                raise Exception(f"Couldn't find case type in: {date} {line}")
            if total is None:
                raise Exception(f"Couldn't parse number of cases in: {date} {line}")
            elif total != sum(prov.values()):
                raise Exception(f"bad parse of {date} {total}!={sum(prov.values())}: {text}")
            if "proactive" in label:
                proactive.update(dict(((date,k),v) for k,v in prov.items()))
                proactive[(date,"All")] = total                                  
            elif "walk-in" in label:
                walkins.update(dict(((date,k),v) for k,v in prov.items()))
                walkins[(date,"All")] = total
            else:
                raise Exception()
    # Add in missing data
    date = d("2021-03-01")
    p = {"All":36, "Pathum Thani": 35, "Nonthaburi": 1}
    proactive.update(((date,k),v) for k,v in p.items())
    w = {"All":28, "Samut Sakhon": 19, "Tak": 3, "Nakhon Pathom": 2, "Bangkok":2, "Chonburi": 1, "Ratchaburi": 1}
    walkins.update(((date,k),v) for k,v in w.items())
                
    cols = ["Date", "Province", "Cases Walkin", "Cases Proactive"]
    rows = []
    for date,province in set(walkins.keys()).union(set(proactive.keys())):
        rows.append([date,province,walkins.get((date,province)),proactive.get((date,province))])
    dfprov = pd.DataFrame(rows, columns=cols)
    index = pd.MultiIndex.from_frame(dfprov[['Date','Province']])
    dfprov = dfprov.set_index(index)[["Cases Walkin", "Cases Proactive"]]
    return dfprov, df

def seperate(seq, condition):
    a, b = [], []
    for item in seq:
        (a if condition(item) else b).append(item)
    return a, b

def split(seq, condition, maxsplit=0):
    run = []
    last = False
    splits = 0
    for i in seq:
        if (maxsplit and splits >= maxsplit) or bool(condition(i)) == last:
            run.append(i)
        else:
            splits += 1
            yield run
            run = [i]
            last = not last            
    yield run

# def nwise(iterable, n=2):                                                      
#     iters = tee(iterable, n)                                                     
#     for i, it in enumerate(iters):                                               
#         next(islice(it, i, i), None)                                               
#     return zip(*iters)   

def pairwise(lst):
    lst = list(lst)
    return list(zip(compress(lst, cycle([1, 0])), compress(lst, cycle([0, 1]))))    

#in_any()

def briefing_case_detail(date, pages):

    num_people = re.compile(r"([0-9]+) *ราย")
    title_num = re.compile(r"([0-9]+\.(?:[0-9]+))")
    #allprov = [p for p in PROVINCES['ProvinceTh'] if type(p)==str]
    #is_prov = re.compile(f"({'|'.join(allprov)})")
#    is_pcell = lambda c: is_prov.search(c) and num_people.search(c)
    is_pcell = re.compile(r"((?:[\u0E00-\u0E7Fa-zA-Z' ]+)\.? *\n *\( *(?:[0-9]+) *ราย *\))")
    is_header = lambda x: "ลักษณะผู้ติดเชื้อ" in x

    totals = dict() # groupname -> running total
    all_cells = {}
    rows = []
    if date < d("2021-03-24"):
        pages = []
    for soup in pages:
        text = soup.get_text()
        if "ผู้ป่วยรายใหม่ประเทศไทย" not in text:
            continue
        parts = soup.find_all('p')
        parts = [c for c in [c.strip() for c in [c.get_text() for c in parts]] if c]
        maintitle, parts = seperate(parts, lambda x: "วันที่" in x)
        footer, parts = seperate(parts, lambda x: "กรมควบคุมโรค กระทรวงสาธารณสุข" in x)
        table = list(split(parts, lambda x: re.match("^\w*[0-9]+\.", x)))
        if len(table) == 2:
            # titles at the end
            table, titles = table
            table = [titles, table]
        else:
            extras = table.pop(0)
        # if only one table we can use camelot to get the table. will be slow but less problems
        #ctable = camelot.read_pdf(file, pages="6", process_background=True)[0].df
            
        for titles,cells in pairwise(table):
            title = titles[0].strip("(ต่อ)").strip()
            # groupnum, groupname, total = title_re.search(title).groups()
            # if total:
            #     total = int(total.group(1))
            if "การคัดกรองเชิงรุก" in title:
                case_type = "Proactive"
            elif "เดินทางมาจากต่างประเทศ" in title:
                case_type = "Quarantine"
                continue # just care about province cases for now
            #if re.search("(จากระบบเฝ้าระวัง|ติดเชื้อในประเทศ)", title):
            else:
                case_type = "Walkin"
            header, cells = seperate(cells, is_header)
            #lines = pairwise(islice(split(cells, is_pcell),1,None))
            # "อยู่ระหว่างสอบสวน (93 ราย)" on 2021-04-05 screws things up as its not a province
            lines = pairwise(islice(is_pcell.split("\n".join(cells)),1,None)) # beacause can be split over <p>
            all_cells.setdefault(title,[]).append(lines)
            print(title,case_type)
            for prov, rest in lines:
                #for prov in provs: # TODO: should really be 1. make split only split 1.
                    # TODO: sometimes cells/data seperated by "-" 2021-01-03
                    prov,num = prov.strip().split("(",1)
                    prov = prov.strip().strip(".").replace(" ", "")
                    prov = PROVINCES.loc[prov]['ProvinceEn'] # get english name here so we know we got it
                    num = int(num_people.search(num).group(1))
                    totals[title] = totals.get(title,0) + num
                    print(num,prov)
                    rows.append((date, prov,case_type, num,))
    # checksum on title totals
    for title, total in totals.items():
        m = num_people.search(title)
        if m:
            assert total==int(m.group(1))
    df = pd.DataFrame(rows, columns=["Date", "Province", "Case Type", "Cases",]).set_index(['Date', 'Province'])

    return df

def briefing_case_types(date, pages):
    rows = []
    if date < d("2021-02-01"):
        pages = []
    for i,soup in enumerate(pages):
        text = soup.get_text()
        if not "รายงานสถานการณ์" in text:
            continue
        numbers, rest = get_next_numbers(text, "รวม")
        cases, walkins, proactive, quarantine, *_ = numbers
        numbers, rest = get_next_numbers(text, "ช่องเส้นทางธรรมชาติ","รายผู้ที่เดินทางมาจากต่างประเทศ", before=True)
        if len(numbers) > 0:
            ports = numbers[0]
        else:
            ports = 0
        imported = ports + quarantine

        assert cases == walkins + proactive + imported, f"{date}: briefing case types don't match"
        rows.append([date, cases, walkins, proactive, imported])
        break
    df = pd.DataFrame(rows, columns=["Date", "Cases", "Cases Walkin", "Cases Proactive", "Cases Imported",]).set_index(['Date'])
    print(df.to_string(header=False))
    return df

NUM_OR_DASH = re.compile("([0-9\,-]+)")
def briefing_province_cases(file, date, pages):
    if date < d("2021-01-13"):
        pages = []
    rows = {}
    for i,soup in enumerate(pages):
        #camelot.read_pdf(file,pages=' '.join([str(i) for i in [i]]))
        if "อโควิดในประเทศรายใหม่" not in str(soup):
            continue
        parts = [l.get_text() for l in soup.find_all("p")]
        parts = [l for l in parts if l]
        #parts = list(split(parts, lambda x: "รวม" in x))[-1]
        title, *parts = parts
        while parts and "รวม" in parts[0]:
            header, *parts = parts
        #lines = [c.strip() for c in re.split("((?:[^0-9,\- ]+) *)+", "\n".join(lines)) if c.strip()]
        parts = [c.strip() for c in NUM_OR_DASH.split("\n".join(parts)) if c.strip()]
        while True:
            if len(parts) < 9:
                break
            linenum, prov, *parts = parts
            numbers, parts = parts[:9], parts[9:]
            #_, prov, *_ = re.split("[0-9-]+[ ,]", line)
            thai = prov.strip().strip(" ี").strip(" ์").strip(" ิ")
            try:
                prov = PROVINCES["ProvinceEn"].loc[thai]
            except KeyError:
                print(f"provinces.loc['{thai}'] = provinces.loc['x']")
                continue
            #numbers = [float(i.replace(",","")) if i!="-" else 0 for i in re.findall("([0-9-,]+)", row)]
            numbers = [float(i.replace(",","")) if i!="-" else 0 for i in numbers]
            #numbers, rest = get_next_numbers(line, "\w*")
            #linenum = numbers[0]
            numbers = numbers[1:-1] # last is total. first is previous days
            assert len(numbers) == 7
            for i, cases in enumerate(reversed(numbers)):
                if i > 3: # 2021-01-11 they use earlier cols for date ranges
                    break
                olddate = date-datetime.timedelta(days=i)
                #cases = numbers[-1]
                #df.loc[(olddate,prov), "Cases"] = cases
                rows[(olddate,prov)] = cases
                if False and olddate == date:
                    if cases > 0:
                        print(date,linenum, thai, PROVINCES["ProvinceEn"].loc[prov], cases)
                    else:
                        print("no cases", linenum, thai, *numbers)
    df = pd.DataFrame(((d,p,c) for (d,p), c in rows.items()), columns=["Date", "Province", "Cases"]).set_index(["Date","Province"])

    return df



def get_cases_by_prov_briefings():
    types = pd.DataFrame(columns=["Date",]).set_index(['Date',])
    date_prov = pd.DataFrame(columns=["Date", "Province"]).set_index(['Date', 'Province'])
    date_prov_types = pd.DataFrame(columns=["Date", "Province", "Case Type"]).set_index(['Date', 'Province'])
    url = "http://media.thaigov.go.th/uploads/public_img/source/"
    start = d("2021-01-13") #12th gets a bit messy but could be fixed
    #start = d("2021-04-07")
    end = TODAY()
    #end = d("2021-04-09")
    links = (f"{url}{f.day:02}{f.month:02}{f.year-1957}.pdf" for f in daterange(start, end,1))
    links = reversed(list(links))
    for file, text in web_files(*links, dir="briefings"):
        pages = parse_file(file, html=True, paged=True)
        pages = [BeautifulSoup(page, 'html.parser') for page in pages]
        date = file2date(file)
        df = briefing_case_detail(date, pages)
        date_prov_types = date_prov_types.combine_first(df)
        today_types = briefing_case_types(date, pages)
        types = types.combine_first(today_types)
        prov = briefing_province_cases(file, date, pages)
        date_prov = date_prov.combine_first(prov)
        today_total = today_types[['Cases Proactive', "Cases Walkin"]].sum().sum()
        prov_total = prov.groupby("Date").sum()['Cases'].loc[date]
        if today_total != prov_total:
            print(f"WARNING: briefing provs={prov_total}, cases={today_total}")
        # Phetchabun                  1.0 extra
    # ขอนแกน่ 12 missing
    # ชุมพร 1 missing


    if not date_prov_types.empty:
        date_prov_types = date_prov_types.groupby(['Date','Province','Case Type']).sum() # we often have multiple walkin events
        date_prov_types = date_prov_types.reset_index().pivot(index=["Date", "Province"],columns=['Case Type'])
        date_prov_types.columns = [f"Cases {c}" for c in date_prov_types.columns.get_level_values(1)]
        date_prov = date_prov.combine_first(date_prov_types)

    return date_prov, types


### Combine and plot

def scrape_and_combine():

    cases_by_area = get_cases_by_area()
    situation = get_situation()
    print(cases_by_area)
    print(situation)

    tests = get_tests_by_day()
    print(tests)
    tests_by_area = get_tests_by_area()
    cases = get_cases()
    print(cases)
    privpublic = get_tests_private_public()

    df = cases # cases from situation can go wrong
    df = df.combine_first(cases_by_area)
    df = df.combine_first(situation)
    df = df.combine_first(tests_by_area)
    df = df.combine_first(tests)
    df = df.combine_first(privpublic)
    print(df)

    os.makedirs("api", exist_ok=True)
    df.reset_index().to_json(
        "api/combined",
        date_format="iso",
        indent=3,
        orient="records",
    )
    df.reset_index().to_csv(
        "api/combined.csv",
        index=False 
    )
    return df


def calc_cols(df):
    # adding in rolling average to see the trends better
    df["Tested (MA)"] = df["Tested"].rolling(7).mean()
    df["Tested PUI (MA)"] = df["Tested PUI"].rolling(7).mean()
    df["Cases (MA)"] = df["Cases"].rolling(7).mean()
    df["Tests Area (MA)"] = df["Tests Area"].rolling(7).mean()
    df["Pos Area (MA)"] = df["Pos Area"].rolling(7).mean()
    df["Tests XLS (MA)"] = df["Tests XLS"].rolling(7).mean()
    df["Pos XLS (MA)"] = df["Pos XLS"].rolling(7).mean()
    df["Pos Public (MA)"] = df["Pos Public"].rolling(7).mean()
    df["Pos Private (MA)"] = df["Pos Private"].rolling(7).mean()
    df["Tests Public (MA)"] = df["Tests Public"].rolling(7).mean()
    df["Tests Private (MA)"] = df["Tests Private"].rolling(7).mean()

    # Calculate positive rate
    df["Positivity Tested (MA)"] = df["Cases (MA)"] / df["Tested (MA)"] * 100
    df["Positivity PUI (MA)"] = df["Cases (MA)"] / df["Tested PUI (MA)"] * 100
    df["Positivity PUI"] = df["Cases"] / df["Tested PUI"] * 100
    df["Positivity"] = df["Cases"] / df["Tested"] * 100
    df["Positivity Area (MA)"] = df["Pos Area (MA)"] / df["Tests Area (MA)"] * 100
    df["Positivity Area"] = df["Pos Area"] / df["Tests Area"] * 100
    df["Positivity XLS (MA)"] = df["Pos XLS (MA)"] / df["Tests XLS (MA)"] * 100
    df["Positivity XLS"] = df["Pos XLS"] / df["Tests XLS"] * 100
    df["Positivity Cases/Tests (MA)"] = df["Cases (MA)"] / df["Tests XLS (MA)"] * 100

    # Combined data
    df["Pos Corrected+Private (MA)"] = df["Pos Private (MA)"] + df["Pos XLS (MA)"]
    df["Tests Private+Public (MA)"] = df["Tests Public (MA)"] + df["Tests Private (MA)"]
    df["Tests Corrected+Private (MA)"] = df["Tests XLS (MA)"] + df["Tests Private (MA)"]

    df["Positivity Private (MA)"] = (
        df["Pos Private (MA)"] / df["Tests Private (MA)"] * 100
    )
    df["Positivity Public+Private (MA)"] = (
        df["Pos Corrected+Private (MA)"] / df["Tests Corrected+Private (MA)"] * 100
    )

    # Computed data
    # TODO: has a problem if we have local transmission but no proactive
    walkins = pd.DataFrame(df["Cases Local Transmission"] - df["Cases Proactive"], columns=['Cases Walkin'])
    df = df.combine_first(walkins)

    # Work out unknown areas or types
    cols = [f"Cases Walkin Area {area}" for area in range(1, 14)]
    df['Cases Walkin Area 14'] = df['Cases Walkin'].sub(df[cols].sum(axis=1), fill_value=0)
    assert df[cols][(df['Cases Walkin Area 14'] < 0)].empty
    cols = [f"Cases Proactive Area {area}" for area in range(1, 14)]
    df['Cases Proactive Area 14'] = df['Cases Proactive'].sub(df[cols].sum(axis=1), fill_value=0)
    assert df[cols][(df['Cases Proactive Area 14'] < 0)].empty
    cols = [f"Tests Area {area}" for area in range(1, 14)]
    df['Tests Area 14'] = df['Tests Public (MA)'].sub(df[cols].sum(axis=1), fill_value=0).clip(lower=0) # 97 lower than 0
    assert df[cols][(df['Tests Area 14'] < 0)].empty
    cols = [f"Pos Area {area}" for area in range(1, 14)]
    df['Pos Area 14'] = df['Pos Public (MA)'].sub(df[cols].sum(axis=1), fill_value=0).clip(lower=0) # 139 rows < 0
    assert df[cols][(df['Pos Area 14'] < 0)].empty
    # TODO: skip for now until we work out how to not interpolate
    df['Cases Unknown'] = df['Cases'].sub(df[["Cases Walkin","Cases Proactive","Cases Imported"]].sum(axis=1), fill_value=0).clip(lower=0) # 3 dates lower than 0
    assert df[["Cases", "Cases Walkin","Cases Proactive","Cases Imported","Cases Unknown"]][(df['Cases Unknown'] < 0)].empty 

    return df

# df = df.cumsum()
AREA_LEGEND = [
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


def rearrange(l, *first):
    l = list(l)
    result = []
    for f in first:
        result.append(l[f-1])
        l[f-1] = None
    return result + [i for i in l if i is not None]

FIRST_AREAS = [13, 4, 6, 1, 5, 12] # based on size-ish
AREA_LEGEND = rearrange(AREA_LEGEND, *FIRST_AREAS)
AREA_LEGEND_UNKNOWN = AREA_LEGEND + ["Unknown District"]
TESTS_AREA_SERIES = rearrange(TESTS_AREA_COLS, *FIRST_AREAS)
POS_AREA_SERIES = rearrange(POS_AREA_COLS, *FIRST_AREAS)


def save_plots(df):
    matplotlib.use("AGG")
    plt.style.use('seaborn-whitegrid')
    fig, ax = plt.subplots()
    df.plot(
        ax=ax,
        use_index=True,
        kind="line",
        figsize=[20, 10],
        title="Tests vs PUI (7 day rolling average) - Thailand Covid",
        y=[
            "Tested PUI (MA)",
            "Tests XLS (MA)",
            "Tests Corrected+Private (MA)",
        ],
    )
    ax.legend(
        [
            "PUI",
            "Tests Performed (Public)",
            "Tests Performed (All)",
        ]
    )
    plt.tight_layout()
    plt.savefig("tests.png")


    fig, ax = plt.subplots()
    df.plot(
        ax=ax,
        use_index=True,
        kind="line",
        figsize=[20, 10],
        title="Situation Reports PUI - Thailand Covid",
        y=[
            'Tested Cum', 
            "Tested PUI Cum", 
            "Tested Not PUI Cum", 
            "Tested Proactive Cum",
            "Tested Quarantine Cum",
        ],
    )
    plt.tight_layout()
    plt.savefig("tested_pui.png")




    fig, ax = plt.subplots()
    df.plot(
        ax=ax,
        use_index=True,
        kind="line",
        figsize=[20, 10],
        y=[
            "Positivity PUI (MA)",
            "Positivity XLS (MA)",
            "Positivity Public+Private (MA)",
        ],
        title="Positive Rate (7 day rolling average) - Thailand Covid",
    )
    ax.legend(
        [
            "Confirmed Cases / PUI",
            "Positive Results / Tests Performed (Public)",
            "Positive Results / Tests Performed (All)",
        ]
    )
    plt.tight_layout()
    plt.savefig("positivity.png")

    df["Positivity Walkins/PUI"] = df["Cases Walkin"] / df["Tested PUI"]

    fig, ax = plt.subplots()
    df["2020-12-12":].plot(
        ax=ax,
        use_index=True,
        kind="line",
        figsize=[20, 10],
        y=[
            "Positivity PUI",
            "Positivity Walkins/PUI",
            "Positivity Public+Private (MA)",
        ],
        title="Is enough testing happening: Positive Rate - Thailand Covid",
    )
    ax.legend(
        [
            "Share of PUI that have Covid",
            "Whare of PUI that have Covid found via Walkin",
            "Share of PCR tests that are postitive",
        ]
    )
    plt.tight_layout()
    plt.savefig("positivity_2.png")

    df["PUI per Case"] = df["Tested PUI (MA)"] / df["Cases (MA)"] 
    df["PUI3 per Case"] = df["Tested PUI (MA)"]*3 / df["Cases (MA)"] 
    df["PUI per Walkin"] = df["Tested PUI"].shift(-3).rolling(3).mean()/ df["Cases Walkin"].rolling(3).mean()
    df["Tests per case"] = df["Tests Corrected+Private (MA)"] / df["Cases (MA)"]

    fig, ax = plt.subplots()
    df["2020-12-12":].plot(
        ax=ax,
        use_index=True,
        kind="line",
        figsize=[20, 10],
        y=[
            "PUI per Case",
            "PUI3 per Case",
            "Tests per case",
        ],
        title="Tests & PUI per Confirmed Case (7 day rolling average)",
    )
    ax.legend(
        [
            "PUI per Case",
            "Predicted Tests per Case (PUI*3)",
            "PCR Tests per case",
        ]
    )
    plt.tight_layout()
    plt.savefig("tests_per_case.png")


    fig, ax = plt.subplots()
    df.plot(
        ax=ax,
        use_index=True,
        kind="line",
        figsize=[20, 10],
        y=[
            "Positivity PUI (MA)",
            "Positivity Cases/Tests (MA)",
            "Positivity XLS (MA)",
            "Positivity Private (MA)",
            "Positivity Public+Private (MA)",
        ],
        title="Positive Rate (7day rolling average) - Thailand Covid",
    )
    ax.legend(
        [
            "Confirmed Cases / PUI",
            "Confirmed Cases / Tests Performed (Public)",
            "Positive Results / Tests Performed (Public)",
            "Positive Results / Tests Performed (Private)",
            "Positive Results / Tests Performed (All)",
        ]
    )
    plt.tight_layout()
    plt.savefig("positivity_all.png")

    fig, ax = plt.subplots()
    df.plot(
        ax=ax,
        use_index=True,
        kind="line",
        figsize=[20, 10],
        y=[
            "Cases (MA)",
            "Pos XLS (MA)",
            "Pos Corrected+Private (MA)",
        ],
        title="Confirmed Cases vs Positive Results (7 day rolling average) - Thailand Covid",
    )
    ax.legend(
        [
            "Confirmed Cases",
            "Positive Test Results (Public)",
            "Positive Test Results (All)",
        ]
    )
    plt.tight_layout()
    plt.savefig("cases.png")

    fig, ax = plt.subplots()
    df.plot(
        ax=ax,
        use_index=True,
        kind="line",
        figsize=[20, 10],
        y=[
            "Cases (MA)",
            "Pos Area (MA)",
            "Pos XLS (MA)",
            "Pos Public (MA)",
            "Pos Private (MA)",
            "Pos Corrected+Private (MA)",
        ],
        title="Confirmed Cases vs Positive Results (7 day rolling average) - Thailand Covid",
    )
    plt.tight_layout()
    plt.savefig("cases_all.png")



    fig, ax = plt.subplots()
    df.loc["2020-12-12":].plot(
        ax=ax,
        y=["Cases Imported","Cases Walkin", "Cases Proactive", "Cases Unknown"],
        use_index=True,
        kind="area",
        figsize=[20, 10],
        title="Cases by source - Thailand Covid",
    )
    plt.tight_layout()
    plt.savefig("cases_types.png")


    ##########################
    # Tests by area
    ##########################

    cols = [f"Tests Area {area}" for area in range(1, 15)]
    fig, ax = plt.subplots()
    df.plot(
        ax=ax,
        use_index=True,
        y=rearrange(cols, *FIRST_AREAS),
        kind="area",
        figsize=[20, 10],
        title="Test performed by Thailand Health Area",
    )
    ax.legend(AREA_LEGEND_UNKNOWN)
    #ax.subtitle("Excludes proactive & private tests")
    plt.tight_layout()
    plt.savefig("tests_area.png")

    cols = [f"Pos Area {area}" for area in range(1, 15)]
    fig, ax = plt.subplots()
    df.plot(
        ax=ax,
        #use_index=True,
        y=rearrange(cols, *FIRST_AREAS),
        kind="area",
        figsize=[20, 10],
        title="Positive Test results by Thailand Health Area",
    )
    ax.legend(AREA_LEGEND_UNKNOWN)
    #ax.subtitle("Excludes proactive & private tests")
    plt.tight_layout()
    plt.savefig("pos_area.png")


    cols = [f"Tests Daily {area}" for area in range(1, 14)]
    test_cols = [f"Tests Area {area}" for area in range(1, 14)]
    for area in range(1, 14):
        df[f"Tests Daily {area}"] = (
            df[f"Tests Area {area}"]
            / df[test_cols].sum(axis=1)
            * df["Tests Public (MA)"]
        )
    df['Tests Daily 14'] = df['Tests Public (MA)'].sub(df[cols].sum(axis=1), fill_value=0).clip(lower=0)
    fig, ax = plt.subplots()
    df["2020-12-12":].plot(
        ax=ax,
        use_index=True,
        y=rearrange(cols+['Tests Daily 14'], *FIRST_AREAS),
        kind="area",
        figsize=[20, 10],
        title="Public Tests performed by Thailand Health Area (ex. some proactive, 7 day rolling average)",
    )
    ax.legend(AREA_LEGEND_UNKNOWN)
    #ax.subtitle("Excludes proactive & private tests")
    plt.tight_layout()
    plt.savefig("tests_area_daily.png")

    cols = [f"Pos Daily {area}" for area in range(1, 14)]
    pos_cols = [f"Pos Area {area}" for area in range(1, 14)]
    for area in range(1, 14):
        df[f"Pos Daily {area}"] = (
            df[f"Pos Area {area}"]
            / df[pos_cols].sum(axis=1)
            * df["Pos Public (MA)"]
        )
    df['Pos Daily 14'] = df['Pos Public (MA)'].sub(df[cols].sum(axis=1), fill_value=0).clip(lower=0)
    fig, ax = plt.subplots()
    df["2020-12-12":].plot(
        ax=ax,
        use_index=True,
        y=rearrange(cols+["Pos Daily 14"], *FIRST_AREAS),
        kind="area",
        figsize=[20, 10],
        title="Public Positive Test results by Thailand Health Area (ex. some proactive, 7 day rolling average)",
    )
    ax.legend(AREA_LEGEND_UNKNOWN)
    #ax.subtitle("Excludes proactive & private tests")
    plt.tight_layout()
    plt.savefig("pos_area_daily.png")


    # Workout positivity for each area as proportion of positivity for that period
    for area in range(1, 14):
        df[f"Positivity {area}"] = (
            df[f"Pos Area {area}"] / df[f"Tests Area {area}"] * 100
        )
    cols = [f"Positivity {area}" for area in range(1, 14)]
    df["Total Positivity Area"] = df[cols].sum(axis=1)
    for area in range(1, 14):
        df[f"Positivity {area}"] = (
            df[f"Positivity {area}"]
            / df["Total Positivity Area"]
            * df["Positivity XLS (MA)"]
        )
    df['Positivity 14'] = df['Positivity XLS (MA)'].sub(df[cols].sum(axis=1), fill_value=0).clip(lower=0)

    print(
        df[
            ["Total Positivity Area", "Positivity Area", "Pos Area", "Tests Area"]
            + cols
        ]
    )

    fig, ax = plt.subplots()
    df.plot(
        ax=ax,
        use_index=True,
        y=rearrange(cols+['Positivity 14'], *FIRST_AREAS),
        kind="area",
        figsize=[20, 10],
        title="Positive Rate by Health Area in proportion to Thailand positive rate (exludes private and some proactive tests)",
    )
    ax.legend(AREA_LEGEND_UNKNOWN)
    #ax.subtitle("Excludes proactive & private tests")
    plt.tight_layout()
    plt.savefig("positivity_area.png")

    fig, ax = plt.subplots()
    df.loc["2020-12-12":].plot(
        ax=ax,
        use_index=True,
        y=rearrange(cols+['Positivity 14'], *FIRST_AREAS),
        kind="area",
        figsize=[20, 10],
        title="Positive Rate by Health Area in proportion to Thailand positive rate (exludes private and some proactive tests)",
    )
    ax.legend(AREA_LEGEND_UNKNOWN)
    #ax.subtitle("Excludes proactive & private tests")
    plt.tight_layout()
    plt.savefig("positivity_area_2.png")

    #########################
    # Case by area plots
    #########################

    cols = [f"Cases Area {area}" for area in range(1, 14)]+["Cases Imported"]
    df['Cases Area Unknown'] = df['Cases'].sub(df[cols].sum(axis=1), fill_value=0).clip(0) # TODO: 2 days values go below due to data from api
    cols = cols+['Cases Area Unknown']
    assert df[cols][(df['Cases Area Unknown'] < 0)].empty
    legend = AREA_LEGEND + ["Imported Cases", "Unknown District"]

    cols = rearrange(cols,*FIRST_AREAS)
    fig, ax = plt.subplots()
    df[:"2020-06-14"].plot(
        ax=ax,
        y=cols,
        kind="area",
        figsize=[20, 10],
        title="Cases by health area"
    )
    ax.legend(legend)
    plt.tight_layout()
    plt.savefig("cases_areas_1.png")

    fig, ax = plt.subplots()
    df["2020-12-12":].plot(
        ax=ax,
        y=cols,
        kind="area",
        figsize=[20, 10],
        title="Cases by health area"
    )
    ax.legend(legend)
    plt.tight_layout()
    plt.savefig("cases_areas_2.png")


    cols = rearrange([f"Cases Walkin Area {area}" for area in range(1, 15)],*FIRST_AREAS)
    fig, ax = plt.subplots()
    df["2021-02-16":].plot(
        ax=ax,
        y=cols,
        kind="area",
        figsize=[20, 10],
        title="Walkin cases by health area - Thailand"
    )
    ax.legend(AREA_LEGEND_UNKNOWN)
    plt.tight_layout()
    plt.savefig("cases_areas_walkins.png")

    cols = rearrange([f"Cases Proactive Area {area}" for area in range(1, 15)],*FIRST_AREAS)
    fig, ax = plt.subplots()
    df["2021-02-16":].plot(
        ax=ax,
        y=cols,
        kind="area",
        figsize=[20, 10],
        title="Proactive cases by health area - Thailand"
    )
    ax.legend(AREA_LEGEND_UNKNOWN)
    plt.tight_layout()
    plt.savefig("cases_areas_proactive.png")


if __name__ == "__main__":
    df = scrape_and_combine()
    df = calc_cols(df)
    df = save_plots(df)
