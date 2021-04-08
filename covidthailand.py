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
from itertools import tee

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
        date = date.rsplit("-", 1)[1]
    else:
        date = date.rsplit("_", 1)[1]

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


def previous_date(end, day):
    start = end
    while start.day != int(day):
        start = start - datetime.timedelta(days=1)
    return start


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


def get_next_numbers(content, *matches, debug=False):
    if len(matches) == 0:
        matches = [""]
    for match in matches:
        _, *rest = re.split(match, content,1) if match else ("", content)
        if rest:
            rest, *_ = rest 
            numbers = re.findall(r"[,0-9]+", rest)
            numbers = [n.replace(",", "") for n in numbers]
            numbers = [int(n) for n in numbers if n]
            return numbers, match + " " + rest
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


def is_remote_newer(file, remote_date):
    if not os.path.exists(file):
        print(f"Missing: {file}")
        return True
    if remote_date is None:
        return False  # TODO: do we want to redownload each time?
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
    for url in urls:
        modified = s.head(url).headers.get("last-modified") if check else None
        file = url.rsplit("/", 1)[-1]
        file = os.path.join(dir, file)
        os.makedirs(os.path.dirname(file), exist_ok=True)
        if is_remote_newer(file, modified):
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
    cum = cum.interpolate(limit_area="inside") # missing dates need to be filled so we don't get jumps
    cum = cum - cum.shift(+1)  # we got cumilitive data
    renames = dict((c,c.rstrip(' Cum')) for c in list(cum.columns) if 'Cum' in c)
    cum = cum.rename(columns=renames)
    return cum

def get_situation():
    en_situation = get_en_situation()
    th_situation = get_thai_situation()
    situation = th_situation.combine_first(en_situation)
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
    areas = pd.read_html("https://en.wikipedia.org/wiki/Healthcare_in_Thailand#Health_Districts")[0]
    provinces = areas.assign(Provinces=areas['Provinces'].str.split(",")).explode("Provinces")
    provinces['Provinces'] = provinces['Provinces'].str.strip()
    missing = [
        ("Bangkok", 13, "Central", None),
    ]
    missing = pd.DataFrame(missing, columns=['Provinces','Health District Number', "Area of Thailand", "Area Code"]).set_index("Provinces")
    provinces = provinces.set_index("Provinces").combine_first(missing)
    provinces.loc['Korat'] = provinces.loc['Nakhon Ratchasima']
    provinces.loc['Khorat'] = provinces.loc['Nakhon Ratchasima']
    provinces.loc['Suphanburi'] = provinces.loc['Suphan Buri']
    provinces.loc["Ayutthaya"] = provinces.loc["Phra Nakhon Si Ayutthaya"]
    provinces.loc["Phathum Thani"] = provinces.loc["Pathum Thani"]
    provinces.loc["Pathom Thani"] = provinces.loc["Pathum Thani"]
    provinces.loc["Ubon Thani"] = provinces.loc["Udon Thani"]
   

    return provinces


def get_cases_by_area():
    cases = pd.DataFrame(json.loads(s.get("https://covid19.th-stat.com/api/open/cases").content)["Data"])
    provinces = get_provinces()
    cases = cases.join(provinces, on="ProvinceEn")
    cases = cases.rename(columns=dict(ConfirmDate="Date"))
    case_areas = pd.crosstab(pd.to_datetime(cases['Date']).dt.date,cases['Health District Number'])
    case_areas = case_areas.rename(columns=dict((i,f"Cases Area {i}") for i in range(1,14)))
    os.makedirs("api", exist_ok=True)

    # we will add in the tweet data for the export
    try:
        case_tweets = get_cases_by_area_tweets()
    except:
        # could be because of old data. refetch it
        #shutil.rmtree("tweets")
        #case_tweets = get_cases_by_area_tweets()
        raise

    case_areas = case_areas.combine_first(case_tweets)

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
    for limit in ([50,300,500,2000,5000] if tweets else [5000]):       
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
        if earliest <= datefrom.date(): #TODO: ensure we have every tweet in sequence?
            break
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



def get_cases_by_area_tweets():
    #tw = TwitterScraper()

    # Get tweets
    # 2021-03-01 and 2021-03-05 are missing
    new = get_tweets_from(531202184, d("2021-04-03"), None, "Official #COVID19 update", "📍")
    #old = get_tweets_from(72888855, d("2021-01-14"), d("2021-04-02"), "Official #COVID19 update", "📍")
    old = get_tweets_from(72888855, d("2021-02-18"), None, "Official #COVID19 update", "📍")
    
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
    provinces = get_provinces()
    dfprov = dfprov.join(provinces['Health District Number'], on="Province")
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
    dfprov_grouped = dfprov.groupby(["Date","Health District Number"]).sum().reset_index()
    dfprov_grouped = dfprov_grouped.pivot(index="Date",columns=['Health District Number'])
    dfprov_grouped = dfprov_grouped.rename(columns=dict((i,f"Area {i}") for i in range(1,14)))
    by_area = dfprov_grouped.groupby(['Health District Number'],axis=1).sum()
    by_area = by_area.rename(columns=dict((f"Area {i}", f"Cases Area {i}") for i in range(1,14)))
    by_type = dfprov_grouped.groupby(level=0, axis=1).sum()
    # Collapse columns to "Cases Proactive Area 13" etc
    dfprov_grouped.columns = dfprov_grouped.columns.map(' '.join).str.strip()
    by_area = by_area.combine_first(dfprov_grouped).combine_first(df).combine_first(by_type)

    # Ensure we have all areas
    for i in range(1,14):
        col = f"Cases Walkin Area {i}"
        if col not in by_area:
            by_area[col] = by_area.get(col, pd.Series(index=by_area.index, name=col))
        col = f"Cases Proactive Area {i}"
        if col not in by_area:
            by_area[col] = by_area.get(col, pd.Series(index=by_area.index, name=col))
    return by_area

def split(seq, condition):
    a, b = [], []
    for item in seq:
        (a if condition(item) else b).append(item)
    return a, b

def get_briefings():

    urls = ["http://media.thaigov.go.th/uploads/public_img/source/300364.pdf"]
    for file, text in web_files(*urls, dir="briefings"):
        pages = parse_file(file, html=True, paged=True)
        for page in pages:
            if "ผู้ป่วยรายใหม่ประเทศไทย" not in page:
                continue
            soup = BeautifulSoup(page, 'html.parser')
            cells = soup.find_all('p')
            cells = [c.get_text() for c in cells]
            titles, cells = split(cells, lambda x: re.search("^\w*[0-9]+.", x))
            maintitle, cells = split(cells, find_dates)
            header, cells = split(cells, lambda x: "จังหวัด" in x)

            # Find the titles
            tail = cells
            while True:
                head, *tail = tail    

            rows = []
            others = []
            while True:
                if not cells:
                    break
                if "ราย)" not in cells[0] or "\n" not in cells[0]:
                    others.append(cells.pop(0))
                    continue
                prov, demo, symp, hosp, *rest = cells
                #re.match(r"[\s\w]+\n\([0-9]+))", prov
                #prov = prov.strip()
                prov,num = prov.strip().split("\n")
                prov = prov.strip(".")
                num = int(re.search("([0-9]+)", num).group(1))
                rows.append((prov,num,demo,symp,hosp))
                cells = rest
            df = pd.DataFrame(rows, columns=["ProvinceTh", "Cases Proactive"])
            return df


### Combine and plot

def scrape_and_combine():

    get_briefings()
    cases_by_area = get_cases_by_area()
    print(cases_by_area)
    situation = get_situation()
    print(situation)

    tests = get_tests_by_day()
    print(tests)
    tests_by_area = get_tests_by_area()
    cases = get_cases()
    print(cases)
    privpublic = get_tests_private_public()

    df = cases # cases from situation can go wrong
    df = df.combine_first(situation)
    df = df.combine_first(cases_by_area)
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
    df['Cases Walkin'] = df["Cases Local Transmission"] - df["Cases Proactive"]

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
        result.append(l[f])
        l[f] = None
    return result + [i for i in l if i is not None]

FIRST_AREAS = [12, 3, 5, 0, 4] # based on size-ish
AREA_LEGEND = rearrange(AREA_LEGEND, *FIRST_AREAS)
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
    df.plot(
        ax=ax,
        use_index=True,
        y=rearrange(TESTS_AREA_COLS, *FIRST_AREAS),
        kind="area",
        figsize=[20, 10],
        title="Test performed by Thailand Health Area",
    )
    ax.legend(AREA_LEGEND)
    #ax.subtitle("Excludes proactive & private tests")
    plt.tight_layout()
    plt.savefig("tests_area.png")

    fig, ax = plt.subplots()
    df.plot(
        ax=ax,
        #use_index=True,
        y=rearrange(POS_AREA_COLS, *FIRST_AREAS),
        kind="area",
        figsize=[20, 10],
        title="Positive Test results by Thailand Health Area",
    )
    ax.legend(AREA_LEGEND)
    #ax.subtitle("Excludes proactive & private tests")
    plt.tight_layout()
    plt.savefig("pos_area.png")


    cols = [f"Tests Daily {area}" for area in range(1, 14)]
    df["Tests Total Area"] = df[TESTS_AREA_COLS].sum(axis=1)
    for area in range(1, 14):
        df[f"Tests Daily {area}"] = (
            df[f"Tests Area {area}"]
            / df["Tests Total Area"]
            * df["Tests Public (MA)"]
        )
    fig, ax = plt.subplots()
    df["2020-12-12":].plot(
        ax=ax,
        use_index=True,
        y=rearrange(cols, *FIRST_AREAS),
        kind="area",
        figsize=[20, 10],
        title="Public Tests performed by Thailand Health Area (ex. some proactive, 7 day rolling average)",
    )
    ax.legend(AREA_LEGEND)
    #ax.subtitle("Excludes proactive & private tests")
    plt.tight_layout()
    plt.savefig("tests_area_daily.png")

    cols = [f"Pos Daily {area}" for area in range(1, 14)]
    df["Pos Total Area"] = df[POS_AREA_COLS].sum(axis=1)
    for area in range(1, 14):
        df[f"Pos Daily {area}"] = (
            df[f"Pos Area {area}"]
            / df["Pos Total Area"]
            * df["Pos Public (MA)"]
        )
    fig, ax = plt.subplots()
    df["2020-12-12":].plot(
        ax=ax,
        use_index=True,
        y=rearrange(cols, *FIRST_AREAS),
        kind="area",
        figsize=[20, 10],
        title="Public Positive Test results by Thailand Health Area (ex. some proactive, 7 day rolling average)",
    )
    ax.legend(AREA_LEGEND)
    #ax.subtitle("Excludes proactive & private tests")
    plt.tight_layout()
    plt.savefig("pos_area_daily.png")


    # Workout positivity for each area as proportion of positivity for that period
    fig, ax = plt.subplots()
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
    print(
        df[
            ["Total Positivity Area", "Positivity Area", "Pos Area", "Tests Area"]
            + cols
        ]
    )

    df.plot(
        ax=ax,
        use_index=True,
        y=rearrange(cols, *FIRST_AREAS),
        kind="area",
        figsize=[20, 10],
        title="Positive Rate by Health Area in proportion to Thailand positive rate (exludes private and some proactive tests)",
    )
    ax.legend(AREA_LEGEND)
    #ax.subtitle("Excludes proactive & private tests")
    plt.tight_layout()
    plt.savefig("positivity_area.png")

    fig, ax = plt.subplots()
    df.loc["2020-12-12":].plot(
        ax=ax,
        use_index=True,
        y=rearrange(cols, *FIRST_AREAS),
        kind="area",
        figsize=[20, 10],
        title="Positive Rate by Health Area in proportion to Thailand positive rate (exludes private and some proactive tests)",
    )
    ax.legend(AREA_LEGEND)
    #ax.subtitle("Excludes proactive & private tests")
    plt.tight_layout()
    plt.savefig("positivity_area_2.png")




    fig, ax = plt.subplots()
    df.loc["2020-12-12":].plot(
        ax=ax,
        y=["Cases Imported","Cases Walkin", "Cases Proactive", ],
        use_index=True,
        kind="area",
        figsize=[20, 10],
        title="Cases by source - Thailand Covid",
    )
    plt.tight_layout()
    plt.savefig("cases_types.png")


    cols = rearrange([f"Cases Area {area}" for area in range(1, 14)],*FIRST_AREAS)
    fig, ax = plt.subplots()
    df[:"2020-06-14"].plot(
        ax=ax,
        y=cols,
        kind="area",
        figsize=[20, 10],
        title="Cases by health area"
    )
    ax.legend(AREA_LEGEND)
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
    ax.legend(AREA_LEGEND)
    plt.tight_layout()
    plt.savefig("cases_areas_2.png")


    cols = rearrange([f"Cases Walkin Area {area}" for area in range(1, 14)],*FIRST_AREAS)
    fig, ax = plt.subplots()
    df["2021-02-16":].plot(
        ax=ax,
        y=cols,
        kind="area",
        figsize=[20, 10],
        title="Walkin cases by health area - Thailand"
    )
    ax.legend(AREA_LEGEND)
    plt.tight_layout()
    plt.savefig("cases_areas_walkins.png")

    cols = rearrange([f"Cases Proactive Area {area}" for area in range(1, 14)],*FIRST_AREAS)
    fig, ax = plt.subplots()
    df["2021-02-16":].plot(
        ax=ax,
        y=cols,
        kind="area",
        figsize=[20, 10],
        title="Proactive cases by health area - Thailand"
    )
    ax.legend(AREA_LEGEND)
    plt.tight_layout()
    plt.savefig("cases_areas_proactive.png")


if __name__ == "__main__":
    df = scrape_and_combine()
    df = calc_cols(df)
    df = save_plots(df)
