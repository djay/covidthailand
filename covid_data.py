import datetime
import functools
import dateutil
from dateutil.parser import parse as d
from itertools import chain, islice
import json
import os
import re
import copy

from bs4 import BeautifulSoup
import camelot
import numpy as np
import pandas as pd
import requests
from requests.exceptions import ConnectionError

from utils_pandas import add_data, check_cum, cum2daily, daily2cum, daterange, export, fuzzy_join, import_csv, spread_date_range
from utils_scraping import CHECK_NEWER, USE_CACHE_DATA, any_in, dav_files, get_next_number, get_next_numbers, \
    get_tweets_from, pairwise, parse_file, parse_numbers, pptx2chartdata, seperate, split, strip, toint, unique_values,\
    web_files, web_links, all_in, NUM_OR_DASH
from utils_thai import DISTRICT_RANGE, area_crosstab, file2date, find_date_range, \
    find_thai_date, get_province, join_provinces, parse_gender, to_switching_date, today,  \
    get_fuzzy_provinces, POS_COLS, TEST_COLS


##########################################
# Situation reports/PUI
##########################################

def situation_cases_cum(parsed_pdf, date):
    _, rest = get_next_numbers(parsed_pdf, "Disease Situation in Thailand", debug=False)
    cases, rest = get_next_numbers(
        rest,
        "Total number of confirmed cases",
        "Characteristics of Infection in Confirmed cases",
        "Confirmed cases",
        debug=False
    )
    if not cases:
        return pd.DataFrame()
    cases, *_ = cases
    if date < d("2020-04-09"):
        return pd.DataFrame([(date, cases)], columns=["Date", "Cases Cum"]).set_index("Date")
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

        quarantine, _ = get_next_number(
            rest,
            "Cases found in (?:the )?(?:state )?quarantine (?:facilities|centers)",
            "Staying in [^ ]* quarantine",
            default=0, until="●")
        quarantine = 1903 if quarantine == 19003 else quarantine  # "2021-02-05"
        # TODO: work out date when it flips back again.
        if date == d("2021-05-17"):
            imported = quarantine = outside_quarantine
            outside_quarantine = 0
        elif date < d("2020-12-28") or (date > d("2021-01-25") and outside_quarantine > quarantine):
            imported = outside_quarantine  # It's mislabeled (new daily is correct however)
            imported = 2647 if imported == 609 else imported  # "2021-02-17")
            imported = None if imported == 610 else imported  # 2021-02-20 - 2021-03-01
            if imported is not None:
                outside_quarantine = imported - quarantine
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
        quarantine = 562 if quarantine == 5562 else quarantine  # "2021-09-19"
        imported, _ = get_next_number(
            rest,
            "(?i)Imported Case(?:s)?",
            "(?i)Cases were imported from overseas")
        if imported and quarantine:
            outside_quarantine = imported - quarantine
        else:
            outside_quarantine = None  # TODO: can we get imported from total - quarantine - local?
    if quarantine:
        active, _ = get_next_number(
            rest,
            "(?i)Cases found from active case finding",
            "(?i)Cases were (?:infected )?migrant workers",
        )
        prison, _ = get_next_number(rest, "Cases found in Prisons", default=0)
        if active is not None:
            active += prison

        # TODO: cum local really means all local ie walkins+active testing
        local, _ = get_next_number(rest, "(?i)(?:Local )?Transmission")
        # TODO: 2021-01-25. Local 6629.0 -> 12250.0, quarantine 597.0 -> 2396.0 active 4684.0->5532.0
        if imported is None:
            pass
        elif cases - imported == active:
            walkin = local
            local = cases - imported
            active = local - walkin
        elif active is None:
            pass
        elif local + active == cases - imported:
            # switched to different definition?
            walkin = local
            local = walkin + active
        elif date <= d("2021-01-25") or d("2021-02-16") <= date <= d("2021-03-01"):
            walkin = local
            local = walkin + active
    else:
        local, active = None, None

    # assert cases == (local+imported) # Too many mistakes
    return pd.DataFrame(
        [(date, cases, local, imported, quarantine, outside_quarantine, active)],
        columns=["Date", "Cases Cum", "Cases Local Transmission Cum", "Cases Imported Cum",
                 "Cases In Quarantine Cum", "Cases Outside Quarantine Cum", "Cases Proactive Cum"]
    ).set_index("Date")


def situation_cases_new(parsed_pdf, date):
    if date < d("2020-11-02"):
        return pd.DataFrame()
    _, rest = get_next_numbers(
        parsed_pdf,
        "The Disease Situation in Thailand",
        "(?i)Type of case Total number Rate of Increase",
        debug=False)
    cases, rest = get_next_numbers(
        rest,
        "(?i)number of new case(?:s)?",
        debug=False
    )
    if not cases or date < d("2020-05-09"):
        return pd.DataFrame()
    cases, *_ = cases
    local, _ = get_next_numbers(rest, "(?i)(?:Local )?Transmission", debug=False)
    local, *_ = local if local else [None]
    quarantine, _ = get_next_numbers(
        rest,
        "Cases found (?:positive from |in )(?:the )?(?:state )?quarantine",
        # "Staying in [^ ]* quarantine",
        debug=False)
    quarantine, *_ = quarantine
    quarantine = {d("2021-01-27"): 11}.get(date, quarantine)  # corrections from thai doc
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
        if date <= d("2020-12-24"):  # starts getting cum values
            active = None
        # local really means walkins. so need add it up
        if active:
            local = local + active
    else:
        imported, active = None, None
    cases = {d("2021-03-31"): 42}.get(date, cases)
    # if date not in [d("2020-12-26")]:
    #    assert cases == (local+imported) # except 2020-12-26 - they didn't include 30 proactive
    return pd.DataFrame(
        [(date, cases, local, imported, quarantine, outside_quarantine, active)],
        columns=["Date", "Cases", "Cases Local Transmission", "Cases Imported",
                 "Cases In Quarantine", "Cases Outside Quarantine", "Cases Proactive"]
    ).set_index("Date")


def situation_pui(parsed_pdf, date):
    numbers, _ = get_next_numbers(
        parsed_pdf, "Total +number of laboratory tests",
        until="Sought medical services on their own at hospitals",
        debug=False
    )
    if numbers:
        if len(numbers) == 7:
            tests_total, pui, active_finding, asq, not_pui, pui2, pui_port, *rest = numbers
        elif len(numbers) == 6:
            tests_total, pui, asq, active_finding, pui2, pui_port, *rest = numbers
            not_pui = None
        else:
            raise Exception(numbers)

        pui = {309371: 313813}.get(pui, pui)  # 2020-07-01
        # TODO: find 1529045 below and see which is correct 20201-04-26
        pui2 = pui if pui2 in [96989, 433807, 3891136, 385860, 326073, 1529045] else pui2
        assert pui == pui2
    else:
        numbers, _ = get_next_numbers(
            parsed_pdf, "Total number of people who met the criteria of patients", debug=False,
        )
        if date > dateutil.parser.parse("2020-01-30") and not numbers:
            raise Exception(f"Problem parsing {date}")
        elif not numbers:
            return pd.DataFrame()
        tests_total, active_finding, asq, not_pui = [None] * 4
        pui, pui_airport, pui_seaport, pui_hospital, *rest = numbers
        # pui_port = pui_airport + pui_seaport
    if pui in [1103858, 3891136]:  # mistypes? # 433807?
        pui = None
    elif tests_total in [783679, 849874, 936458]:
        tests_total = None
    elif None in (tests_total, pui, asq, active_finding) and date > d("2020-06-30"):
        raise Exception(f"Missing data at {date}")

    # walkin public vs private
    numbers, rest = get_next_numbers(parsed_pdf, "Sought medical services on their own at hospitals")
    if not numbers:
        pui_walkin_private, pui_walkin_public, pui_walkin = [None] * 3
    elif re.search("(?i)cases (in|at) private hospitals", rest):
        pui_walkin_private, pui_walkin_public, pui_walkin, *_ = numbers
        pui_walkin_public = {8628765: 862876}.get(pui_walkin_public, pui_walkin_public)
        # assert pui_walkin == pui_walkin_private + pui_walkin_public
    else:
        pui_walkin, *_ = numbers
        pui_walkin_private, pui_walkin_public = None, None
        pui_walkin = {853189: 85191}.get(pui_walkin, pui_walkin)  # by taking away other numbers
    assert pui_walkin is None or pui is None or (pui_walkin <= pui and 5000000 > pui_walkin > 0)
    assert pui_walkin_public is None or (5000000 > pui_walkin_public > 10000)

    if not_pui is not None:
        active_finding += not_pui
    row = (tests_total, pui, active_finding, asq, pui_walkin, pui_walkin_private, pui_walkin_public)
    return pd.DataFrame(
        [(date, ) + row],
        columns=[
            "Date",
            "Tested Cum",
            "Tested PUI Cum",
            "Tested Proactive Cum",
            "Tested Quarantine Cum",
            "Tested PUI Walkin Cum",
            "Tested PUI Walkin Private Cum",
            "Tested PUI Walkin Public Cum",
        ]
    ).set_index("Date")


def get_en_situation():
    results = pd.DataFrame(columns=["Date"]).set_index("Date")
    url = "https://ddc.moph.go.th/viralpneumonia/eng/situation.php"
    for file, _ in web_files(*web_links(url, ext=".pdf", dir="situation_en"), dir="situation_en"):
        parsed_pdf = parse_file(file, html=False, paged=False).replace("\u200b", "")
        if "situation" not in os.path.basename(file):
            continue
        date = file2date(file)
        if date <= dateutil.parser.parse("2020-01-30"):
            continue  # TODO: can manually put in numbers before this
        parsed_pdf = parsed_pdf.replace("DDC Thailand 1", "")  # footer put in teh wrong place

        pui = situation_pui(parsed_pdf, date)
        cases = situation_cases_cum(parsed_pdf, date)
        new_cases = situation_cases_new(parsed_pdf, date)
        row = pui.combine_first(cases).combine_first(new_cases)
        results = results.combine_first(row)
        # cums = [c for c in results.columns if ' Cum' in c]
        # if len(results) > 1 and (results.iloc[0][cums] > results.iloc[1][cums]).any():
        #     print((results.iloc[0][cums] > results.iloc[1][cums]))
        #     print(results.iloc[0:2])
        # raise Exception("Cumulative data didn't increase")
        # row = results.iloc[0].to_dict()
        print(date.date(), file, row.to_string(header=False, index=False))
        #     "p{Tested PUI Cum:.0f}\tc{Cases Cum:.0f}({Cases:.0f})\t"
        #     "l{Cases Local Transmission Cum:.0f}({Cases Local Transmission:.0f})\t"
        #     "a{Cases Proactive Cum:.0f}({Cases Proactive:.0f})\t"
        #     "i{Cases Imported Cum:.0f}({Cases Imported:.0f})\t"
        #     "q{Cases In Quarantine Cum:.0f}({Cases In Quarantine:.0f})\t"
        #     "".format(**row)
        # )
    # Missing data. filled in from th infographic
    missing = [
        (d("2020-12-19"), 2476, 0, 0),
        (d("2020-12-20"), 3011, 516, 516),
        (d("2020-12-21"), 3385, 876, 360),
        (d("2020-12-22"), 3798, 1273, 397),
        (d("2020-12-23"), 3837, 1273, 0),
        (d("2020-12-24"), 3895, 1273, 0),
        (d("2020-12-25"), 3976, 1308, 35),
    ]
    missing = pd.DataFrame(
        missing,
        columns=["Date", "Cases Local Transmission Cum", "Cases Proactive Cum", "Cases Proactive"]
    ).set_index("Date")
    results = missing[["Cases Local Transmission Cum", "Cases Proactive Cum", ]].combine_first(results)
    return results


def situation_pui_th(parsed_pdf, date, results):
    tests_total, active_finding, asq, not_pui = [None] * 4
    numbers, content = get_next_numbers(
        parsed_pdf,
        r"ด่านโรคติดต่อระหว่างประเทศ",
        r"ด่านโรคติดต่อระหวา่งประเทศ",  # 'situation-no346-141263n.pdf'
        r"นวนการตรวจทาง\S+องปฏิบัติการ",
        "ด่านควบคุมโรคติดต่อระหว่างประเทศ",
        until="(?:โรงพยาบาลด้วยตนเอง|ารับการรักษาท่ีโรงพยาบาลด|โรงพยาบาลเอกชน)"
    )
    # cases = None

    if len(numbers) == 7:  # numbers and numbers[2] < 30000:
        tests_total, pui, active_finding, asq, not_pui, *rest = numbers
        if pui == 4534137:
            pui = 453413  # situation-no273-021063n.pdf
    elif len(numbers) > 8:
        _, _, tests_total, pui, active_finding, asq, not_pui, *rest = numbers
    elif len(numbers) == 8:
        # 2021 - removed not_pui
        _, _, tests_total, pui, asq, active_finding, pui2, *rest = numbers
        assert pui == pui2
        not_pui = None
    elif len(numbers) == 6:  # > 2021-05-10
        tests_total, pui, asq, active_finding, pui2, screened = numbers
        assert pui == pui2
        not_pui = None
    else:
        numbers, content = get_next_numbers(
            parsed_pdf,
            # "ผู้ป่วยที่มีอาการเข้าได้ตามนิยาม",
            "ตาราง 2 ผลดำ",
            "ตาราง 1",  # situation-no172-230663.pdf #'situation-no83-260363_1.pdf'
        )
        if len(numbers) > 0:
            pui, *rest = numbers
    if date > dateutil.parser.parse("2020-03-26") and not numbers:
        raise Exception(f"Problem finding PUI numbers for date {date}")
    elif not numbers:
        return
    if tests_total == 167515:  # situation-no447-250364.pdf
        tests_total = 1675125
    if date in [d("2020-12-23")]:  # 1024567
        tests_total, not_pui = 997567, 329900
    if (tests_total is not None and tests_total > 2000000 < 30000 or pui > 1500000 < 100000):
        raise Exception(f"Bad data in {date}")
    pui = {d("2020-02-12"): 799, d("2020-02-13"): 804}.get(date, pui)  # Guess

    walkinsre = "(?:ษาที่โรงพยาบาลด้วยตนเอง|โรงพยาบาลด้วยตนเอง|ารับการรักษาท่ีโรงพยาบาลด|โรงพยาบาลดวยตนเอง)"
    _, line = get_next_numbers(parsed_pdf, walkinsre)
    pui_walkin_private, rest = get_next_number(line, f"(?s){walkinsre}.*?โรงพยาบาลเอกชน", remove=True)
    pui_walkin_public, rest = get_next_number(rest, f"(?s){walkinsre}.*?โรงพยาบาลรัฐ", remove=True)
    unknown, rest = get_next_number(rest, f"(?s){walkinsre}.*?(?:งการสอบสวน|ารสอบสวน)", remove=True)
    # rest = re.sub("(?s)(?:งการสอบสวน|ารสอบสวน).*?(?:อ่ืนๆ|อื่นๆ|อืน่ๆ|ผู้ป่วยยืนยันสะสม|88)?", "", rest,1)
    pui_walkin, rest = get_next_number(rest)
    assert pui_walkin is not None
    if date <= d("2020-03-10"):
        pui_walkin_private, pui_walkin, pui_walkin_public = [None] * 3  # starts going up again
    # pui_walkin_private = {d("2020-03-10"):2088}.get(date, pui_walkin_private)

    assert pui_walkin is None or pui is None or (pui_walkin <= pui and pui_walkin > 0)

    if not_pui is not None:
        active_finding += not_pui  # later reports combined it anyway
    row = (tests_total, pui, active_finding, asq, pui_walkin_private, pui_walkin_public, pui_walkin)
    if None in row and date > d("2020-06-30"):
        raise Exception(f"Missing data at {date}")
    df = pd.DataFrame(
        [(date,) + row],
        columns=[
            "Date",
            "Tested Cum",
            "Tested PUI Cum",
            "Tested Proactive Cum",
            "Tested Quarantine Cum",
            "Tested PUI Walkin Private Cum",
            "Tested PUI Walkin Public Cum",
            "Tested PUI Walkin Cum"]
    ).set_index("Date")
    assert check_cum(df, results)
    return df


def get_thai_situation():
    results = pd.DataFrame(columns=["Date"]).set_index("Date")
    links = web_links(
        "https://ddc.moph.go.th/viralpneumonia/situation.php",
        "https://ddc.moph.go.th/viralpneumonia/situation_more.php",
        ext=".pdf",
        dir="situation_th"
    )
    for file, _ in web_files(*links, dir="situation_th"):
        parsed_pdf = parse_file(file, html=False, paged=False)
        if "situation" not in os.path.basename(file):
            continue
        if "Situation Total number of PUI" in parsed_pdf:
            # english report mixed up? - situation-no171-220663.pdf
            continue
        date = file2date(file)
        df = situation_pui_th(parsed_pdf, date, results)
        if df is not None:
            results = results.combine_first(df)
            print(date.date(), file, df.to_string(header=False, index=False))
            # file,
            # "p{Tested PUI Cum:.0f}\t"
            # "t{Tested Cum:.0f}\t"
            # "{Tested Proactive Cum:.0f}\t"
            # "{Tested Quarantine Cum:.0f}\t"
            # "{Tested Not PUI Cum:.0f}\t"
            # "".format(**results.iloc[0].to_dict()))
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
        [[date, ] + row],
        columns=["Date", "Cases Cum", "Cases", "Tested PUI Cum", "Tested PUI", "Cases Imported Cum", "Cases Imported"]
    ).set_index("Date")


def get_situation():
    print("========Situation Reports==========")

    today_situation = get_situation_today()
    en_situation = get_en_situation()
    th_situation = get_thai_situation()
    situation = th_situation.combine_first(en_situation)
    cum = cum2daily(situation)
    situation = situation.combine_first(cum)  # any direct non-cum are trusted more

    # Only add in the live stats if they have been updated with new info
    today = today_situation.index.max()
    yesterday = today - datetime.timedelta(days=1)
    stoday = today_situation.loc[today]
    syesterday = situation.loc[str(yesterday)] if str(yesterday) in situation else None
    if syesterday is None:
        situation = situation.combine_first(today_situation)
    elif syesterday['Tested PUI Cum'] < stoday['Tested PUI Cum'] and \
            syesterday['Tested PUI'] != stoday['Tested PUI']:
        situation = situation.combine_first(today_situation)

    export(situation, "situation_reports")
    return situation


#################################
# Cases Apis
#################################

def get_cases():
    print("========Covid19 Timeline==========")
    try:
        file, text = next(
            web_files("https://covid19.th-stat.com/json/covid19v2/getTimeline.json", dir="json", check=True))
    except ConnectionError:
        # I think we have all this data covered by other sources. It's a little unreliable.
        return pd.DataFrame()
    data = pd.DataFrame(json.loads(text)['Data'])
    data['Date'] = pd.to_datetime(data['Date'])
    data = data.set_index("Date")
    cases = data[["NewConfirmed", "NewDeaths", "NewRecovered", "Hospitalized"]]
    cases = cases.rename(columns=dict(NewConfirmed="Cases", NewDeaths="Deaths", NewRecovered="Recovered"))
    return cases


@functools.lru_cache(maxsize=100, typed=False)
def get_case_details_csv():
    url = "https://data.go.th/dataset/covid-19-daily"
    file, text = next(web_files(url, dir="json", check=True))
    data = re.search(r"packageApp\.value\('meta',([^;]+)\);", text.decode("utf8")).group(1)
    apis = json.loads(data)
    links = [api['url'] for api in apis if "รายงานจำนวนผู้ติดเชื้อ COVID-19 ประจำวัน" in api['name']]
    # ensure csv is first pick but we can handle either if one is missing
    links = sorted([link for link in links if '.php' not in link], key=lambda l: l.split(".")[-1])
    file, _ = next(web_files(next(iter(links)), dir="json", check=False))
    if file.endswith(".xlsx"):
        cases = pd.read_excel(file)
    elif file.endswith(".csv"):
        cases = pd.read_csv(file)
    else:
        raise Exception(f"Unknown filetype for covid19daily {file}")
    cases['announce_date'] = pd.to_datetime(cases['announce_date'], dayfirst=True)
    cases['Notified date'] = pd.to_datetime(cases['Notified date'], dayfirst=True,)
    cases = cases.rename(columns=dict(announce_date="Date")).set_index("Date")
    print("Covid19daily", file, cases.reset_index().iloc[-1].to_string(header=False, index=False))
    return cases


def get_case_details_api():
    # _, cases = next(web_files("https://covid19.th-stat.com/api/open/cases", dir="json"))
    rid = "329f684b-994d-476b-91a4-62b2ea00f29f"
    url = f"https://data.go.th/api/3/action/datastore_search?resource_id={rid}&limit=1000&offset="
    records = []

    def get_page(i, check=False):
        _, cases = next(web_files(f"{url}{i}", dir="json", check=check))
        return json.loads(cases)['result']['records']

    for i in range(0, 100000, 1000):
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


def get_cases_by_demographics_api():
    print("========Covid19Daily Demographics==========")

    cases = get_case_details_csv().reset_index()
    # cases = cases.rename(columns=dict(announce_date="Date"))

    # age_groups = pd.cut(cases['age'], bins=np.arange(0, 100, 10))
    # cases = get_case_details_csv().reset_index()
    labels = ["Age 0-19", "Age 20-29", "Age 30-39", "Age 40-49", "Age 50-65", "Age 66-"]
    age_groups = pd.cut(cases['age'], bins=[0, 19, 29, 39, 49, 65, np.inf], labels=labels)
    case_ages = pd.crosstab(cases['Date'], age_groups)
    # case_areas = case_areas.rename(columns=dict((i,f"Cases Area {i}") for i in DISTRICT_RANGE))

    cases['risk'].value_counts()
    risks = {}
    risks['สถานบันเทิง'] = "Entertainment"
    risks['อยู่ระหว่างการสอบสวน'] = "Investigating"  # Under investication
    risks['การค้นหาผู้ป่วยเชิงรุกและค้นหาผู้ติดเชื้อในชุมชน'] = "Proactive Search"
    risks['State Quarantine'] = 'Imported'
    risks['ไปสถานที่ชุมชน เช่น ตลาดนัด สถานที่ท่องเที่ยว'] = "Community"
    risks['Cluster ผับ Thonglor'] = "Entertainment"
    risks['ผู้ที่เดินทางมาจากต่างประเทศ และเข้า ASQ/ALQ'] = 'Imported'
    risks['Cluster บางแค'] = "Community"  # bangkhee
    risks['Cluster ตลาดพรพัฒน์'] = "Community"  # market
    risks['Cluster ระยอง'] = "Entertainment"  # Rayong
    # work with forigners
    risks['อาชีพเสี่ยง เช่น ทำงานในสถานที่แออัด หรือทำงานใกล้ชิดสัมผัสชาวต่างชาติ เป็นต้น'] = "Work"
    risks['ศูนย์กักกัน ผู้ต้องกัก'] = "Prison"  # detention
    risks['คนไทยเดินทางกลับจากต่างประเทศ'] = "Imported"
    risks['สนามมวย'] = "Entertainment"  # Boxing
    risks['ไปสถานที่แออัด เช่น งานแฟร์ คอนเสิร์ต'] = "Community"  # fair/market
    risks['คนต่างชาติเดินทางมาจากต่างประเทศ'] = "Imported"
    risks['บุคลากรด้านการแพทย์และสาธารณสุข'] = "Work"
    risks['ระบุไม่ได้'] = "Unknown"
    risks['อื่นๆ'] = "Unknown"
    risks['พิธีกรรมทางศาสนา'] = "Community"  # Religous
    risks['Cluster บ่อนพัทยา/ชลบุรี'] = "Entertainment"  # gambling rayong
    risks['ผู้ที่เดินทางมาจากต่างประเทศ และเข้า HQ/AHQ'] = "Imported"
    risks['Cluster บ่อนไก่อ่างทอง'] = "Entertainment"  # cockfighting
    risks['Cluster จันทบุรี'] = "Entertainment"  # Chanthaburi - gambing?
    risks['Cluster โรงงาน Big Star'] = "Work"  # Factory
    r = {
        27: 'Cluster ชลบุรี:Entertainment',  # Chonburi - gambling
        28: 'Cluster เครือคัสเซ่อร์พีคโฮลดิ้ง (CPG,CPH):Work',
        29: 'ตรวจก่อนทำหัตถการ:Unknown',  # 'Check before the procedure'
        30: 'สัมผัสผู้เดินทางจากต่างประเทศ:Contact',  # 'touch foreign travelers'
        31: "Cluster Memory 90's กรุงเทพมหานคร:Entertainment",
        32: 'สัมผัสผู้ป่วยยืนยัน:Contact',
        33: 'ปอดอักเสบ (Pneumonia):Pneumonia',
        34: 'Cluster New Jazz กรุงเทพมหานคร:Entertainment',
        35: 'Cluster มหาสารคาม:Entertainment',  # Cluster Mahasarakham
        36: 'ผู้ที่เดินทางมาจากต่างประเทศ และเข้า OQ:Imported',
        37: 'Cluster สมุทรปราการ (โรงงาน บริษัทเมทัล โปรดักส์):Work',
        38: 'สัมผัสใกล้ชิดผู้ป่วยยันยันก่อนหน้า:Contact',
        39: 'Cluster ตลาดบางพลี:Work',
        40: 'Cluster บ่อนเทพารักษ์:Community',  # Bangplee Market'
        41: 'Cluster Icon siam:Community',
        42: 'Cluster The Lounge Salaya:Entertainment',
        43: 'Cluster ชลบุรี โรงเบียร์ 90:Entertainment',
        44: 'Cluster โรงงาน standard can:Work',
        45: 'Cluster ตราด:Community',  # Trat?
        46: 'Cluster สถานบันเทิงย่านทองหล่อ:Entertainment',
        47: 'ไปยังพื้นที่ที่มีการระบาด:Community',
        48: 'Cluster สมุทรสาคร:Work',  # Samut Sakhon
        49: 'สัมผัสใกล้ชิดกับผู้ป่วยยืนยันรายก่อนหน้านี้:Contact',
        51: 'อยู่ระหว่างสอบสวน:Unknown',
        20210510.1: 'Cluster คลองเตย:Community',  # Cluster Klongtoey, 77
        # Go to a community / crowded place, 17
        20210510.2: 'ไปแหล่งชุมชน/สถานที่คนหนาแน่น:Community',
        20210510.3: 'สัมผัสใกล้ชิดผู้ป่วยยืนยันก่อนหน้า:Contact',
        # Cluster Chonburi Daikin Company, 3
        20210510.4: 'Cluster ชลบุรี บริษัทไดกิ้น:Work',
        20210510.5: 'ร้านอาหาร:Entertainment',  # resturant
        # touch the infected person confirm Under investigation, 5
        20210510.6: 'สัมผัสผู้ติดเชื้อยืนยัน อยู่ระหว่างสอบสวน:Contact',
        # touch the infected person confirm Under investigation, 5
        20210510.7: 'สัมผัสผู้ป่วยยืนยัน อยู่ระหว่างสอบสวน:Contact',
        # Travelers from high-risk areas Bangkok, 2
        20210510.8: 'ผู้เดินทางมาจากพื้นที่เสี่ยง กรุงเทพมหานคร:Community',
        # to / from Epidemic area, Bangkok Metropolis, 1
        20210510.9: 'ไปยัง/มาจาก พื้นที่ระบาดกรุงเทพมหานครมหานคร:Community',
        20210510.11: 'ระหว่างสอบสวน:Investigating',
        # party pakchong https://www.bangkokpost.com/thailand/general/2103827/5-covid-clusters-in-nakhon-ratchasima
        20210510.12: 'Cluster ปากช่อง:Entertainment',
        20210512.1: 'Cluster คลองเตย:Community',  # klongtoey cluster
        20210512.2: 'อยู่ระหว่างสอบสวนโรค:Investigating',
        20210512.3: 'อื่น ๆ:Unknown',  # Other
        # African gem merchants dining after ramandan
        20210512.4: 'Cluster จันทบุรี (ชาวกินี ):Entertainment',
        20210516.0: 'Cluster เรือนจำกลางคลองเปรม:Prison',  # 894
        20210516.1: 'Cluster ตลาดสี่มุมเมือง:Community',  # 344 Four Corners Market
        20210516.2: 'Cluster สมุทรปราการ GRP Hightech:Work',  # 130
        20210516.3: 'Cluster ตลาดนนทบุรี:Community',  # Cluster Talat Nonthaburi, , 85
        20210516.4: 'Cluster โรงงาน QPP ประจวบฯ:Work',  # 69
        # 41 Cluster Special Prison Thonburi,
        20210516.5: 'Cluster เรือนจำพิเศษธนบุรี:Prison',
        # 26 Cluster Chanthaburi (Guinea),
        20210516.6: 'Cluster จันทบุรี (ชาวกินี):Entertainment',
        # 20210516.7: 'Cluster บริษัทศรีสวัสดิ์,Work',  #16
        20210516.8: 'อื่น:Unknown',  # 10
        20210516.9: 'Cluster เรือนจำพิเศษมีนบุรี:Prison',  # 5
        20210516.11: 'Cluster จนท. สนามบินสุวรรณภูมิ:Work',  # 4
        20210516.12: 'สัมผัสผู้ป่วยที่ติดโควิด:Contact',  # 4
        20210531.0: 'Cluster เรือนจำพิเศษกรุงเทพ:Prison',
        20210531.1: 'Cluster บริษัทศรีสวัสดิ์:Work',
        20210531.2: "สัมผัสผู้ป่วยยืนยัน อยู่ระหว่างสอบสวน:Contact",
        20210531.3: 'Cluster ตราด:Community',
        20210531.4: 'ผู้ที่เดินทางมาจากต่างประเทศ และเข้า AOQ:Imported',
        20210531.5: 'ผู้เดินทางมาจากพื้นที่เสี่ยง กรุงเทพมหานคร:Community',
        20210531.6: 'Cluster กรุงเทพมหานคร. คลองเตย:Community'
    }
    for v in r.values():
        key, cat = v.split(":")
        risks[key] = cat
    risks = pd.DataFrame(risks.items(), columns=[
                         "risk", "risk_group"]).set_index("risk")
    cases_risks, unmatched = fuzzy_join(cases, risks, on="risk", return_unmatched=True)
    matched = cases_risks[["risk", "risk_group"]]
    case_risks = pd.crosstab(cases_risks['Date'], cases_risks["risk_group"])
    case_risks.columns = [f"Risk: {x}" for x in case_risks.columns]

    # dump mappings to file so can be inspected
    export(matched.value_counts().to_frame("count"), "risk_groups", csv_only=True)
    export(unmatched, "risk_groups_unmatched", csv_only=True)

    return case_risks.combine_first(case_ages)


##################################
# RB Tweet Parsing
##################################


UNOFFICIAL_TWEET = re.compile("(?:🔴 BREAKING: |🔴 #COVID19)")
OFFICIAL_TWEET = re.compile("#COVID19 update")


def parse_official_tweet(df, date, text):
    imported, _ = get_next_number(text, "imported", before=True, default=0)
    local, _ = get_next_number(text, "local", before=True, default=0)
    cases = imported + local
    # cases_cum, _ = get_next_number(text, "Since Jan(?:uary)? 2020")
    deaths, _ = get_next_number(text, "dead +", "deaths +")
    serious, _ = get_next_number(text, "in serious condition", "in ICU", before=True)
    recovered, _ = get_next_number(text, "discharged", "left care", before=True)
    hospitalised, _ = get_next_number(text, "in care", before=True)
    vent, _ = get_next_number(text, "on ventilators", before=True)
    cols = [
        "Date",
        "Cases Imported",
        "Cases Local Transmission",
        "Cases",
        "Deaths",
        "Hospitalized",
        "Recovered",
        "Hospitalized Severe",
        "Hospitalized Respirator",
    ]
    row = [date, imported, local, cases, deaths]
    row2 = row + [hospitalised, recovered]
    if date <= d("2021-05-01").date():
        assert not any_in(row, None), f"{date} Missing data in Official Tweet {row}"
    else:
        assert not any_in(row2, None), f"{date} Missing data in Official Tweet {row}"
    row_opt = row2 + [serious, vent]
    tdf = pd.DataFrame([row_opt], columns=cols).set_index("Date")
    print(date, "Official:", tdf.to_string(index=False, header=False))
    return df.combine_first(tdf)


def parse_unofficial_tweet(df, date, text):
    deaths, _ = get_next_number(text, "deaths", before=True)
    cases, _ = get_next_number(text, "cases", before=True)
    prisons, _ = get_next_number(text, "prisons", before=True)
    if any_in([None], deaths, cases):
        return df
    cols = ["Date", "Deaths", "Cases", "Cases Area Prison"]
    row = [date, deaths, cases, prisons]
    tdf = pd.DataFrame([row], columns=cols).set_index("Date")
    print(date, "Breaking:", tdf.to_string(index=False, header=False))
    return df.combine_first(tdf)


def parse_case_prov_tweet(walkins, proactive, date, text):
    if "📍" not in text:
        return walkins, proactive
    if "ventilators" in text:  # after 2021-05-11 start using "👉" for hospitalisation
        return walkins, proactive
    start, *lines = text.split("👉", 2)
    if len(lines) < 2:
        raise Exception()
    for line in lines:
        prov_matches = re.findall(r"📍([\s\w,&;]+) ([0-9]+)", line)
        prov = dict((p.strip(), toint(v)) for ps, v in prov_matches for p in re.split("(?:,|&amp;)", ps))
        if d("2021-04-08").date() == date:
            if prov["Bangkok"] == 147:  # proactive
                prov["Bangkok"] = 47
            elif prov["Phuket"] == 3:  # Walkins
                prov["Chumphon"] = 3
                prov['Khon Kaen'] = 3
                prov["Ubon Thani"] = 7
                prov["Nakhon Pathom"] = 6
                prov["Phitsanulok"] = 4

        label = re.findall(r'^ *([0-9]+)([^📍👉👇\[]*)', line)
        if label:
            total, label = label[0]
            # label = label.split("👉").pop() # Just in case tweets get muddled 2020-04-07
            total = toint(total)
        else:
            raise Exception(f"Couldn't find case type in: {date} {line}")
        if total is None:
            raise Exception(f"Couldn't parse number of cases in: {date} {line}")
        elif total != sum(prov.values()):
            raise Exception(f"bad parse of {date} {total}!={sum(prov.values())}: {text}")
        if "proactive" in label:
            proactive.update(dict(((date, k), v) for k, v in prov.items()))
            print(date, "Proactive:", len(prov))
            # proactive[(date,"All")] = total
        elif "walk-in" in label:
            walkins.update(dict(((date, k), v) for k, v in prov.items()))
            print(date, "Walkins:", len(prov))
            # walkins[(date,"All")] = total
        else:
            raise Exception()
    return walkins, proactive


def get_cases_by_prov_tweets():
    print("========RB Tweets==========")
    # These are published early so quickest way to get data
    # previously also used to get per provice case stats but no longer published

    # Get tweets
    # 2021-03-01 and 2021-03-05 are missing
    new = get_tweets_from(531202184, d("2021-04-03"), None, OFFICIAL_TWEET, "📍")
    # old = get_tweets_from(72888855, d("2021-01-14"), d("2021-04-02"), "Official #COVID19 update", "📍")
    old = get_tweets_from(72888855, d("2021-02-21"), None, OFFICIAL_TWEET, "📍")
    unofficial = get_tweets_from(531202184, d("2021-04-03"), None, UNOFFICIAL_TWEET)
    officials = {}
    provs = {}
    breaking = {}
    for date, tweets in list(new.items()) + list(old.items()):
        for tweet in tweets:
            if "RT @RichardBarrow" in tweet:
                continue
            if OFFICIAL_TWEET.search(tweet):
                officials[date] = tweet
            elif "👉" in tweet and "📍" in tweet:
                if tweet in provs.get(date, ""):
                    continue
                provs[date] = provs.get(date, "") + " " + tweet
    for date, tweets in unofficial.items():
        for tweet in tweets:
            if UNOFFICIAL_TWEET.search(tweet):
                breaking[date] = tweet

    # Get imported vs walkin totals
    df = pd.DataFrame()

    for date, text in sorted(officials.items(), reverse=True):
        df = df.pipe(parse_official_tweet, date, text)

    for date, text in sorted(breaking.items(), reverse=True):
        if date in officials:
            # do unoffical tweets if no official tweet
            continue
        df = df.pipe(parse_unofficial_tweet, date, text)

    # get walkin vs proactive by area
    walkins = {}
    proactive = {}
    for date, text in provs.items():
        walkins, proactive = parse_case_prov_tweet(walkins, proactive, date, text)

    # Add in missing data
    date = d("2021-03-01")
    p = {"Pathum Thani": 35, "Nonthaburi": 1}  # "All":36,
    proactive.update(((date, k), v) for k, v in p.items())
    w = {"Samut Sakhon": 19, "Tak": 3, "Nakhon Pathom": 2, "Bangkok": 2, "Chonburi": 1, "Ratchaburi": 1}  # "All":28,
    walkins.update(((date, k), v) for k, v in w.items())
    cols = ["Date", "Province", "Cases Walkin", "Cases Proactive"]
    rows = []
    for date, province in set(walkins.keys()).union(set(proactive.keys())):
        rows.append([date, province, walkins.get((date, province)), proactive.get((date, province))])
    dfprov = pd.DataFrame(rows, columns=cols)
    index = pd.MultiIndex.from_frame(dfprov[['Date', 'Province']])
    dfprov = dfprov.set_index(index)[["Cases Walkin", "Cases Proactive"]]
    df = df.combine_first(cum2daily(df))
    return dfprov, df


def briefing_case_detail_lines(soup):
    parts = soup.find_all('p')
    parts = [c for c in [c.strip() for c in [c.get_text() for c in parts]] if c]
    maintitle, parts = seperate(parts, lambda x: "วันที่" in x)
    if not maintitle or "ผู้ป่วยรายใหม่ประเทศไทย" not in maintitle[0]:
        return
    # footer, parts = seperate(parts, lambda x: "กรมควบคุมโรค กระทรวงสาธารณสุข" in x)
    table = list(split(parts, re.compile(r"^\w*[0-9]+\.").match))
    if len(table) == 2:
        # titles at the end
        table, titles = table
        table = [titles, table]
    else:
        table.pop(0)

    # if only one table we can use camelot to get the table. will be slow but less problems
    # ctable = camelot.read_pdf(file, pages="6", process_background=True)[0].df

    for titles, cells in pairwise(table):
        title = titles[0].strip("(ต่อ)").strip()
        header, cells = seperate(cells, re.compile("ลักษณะผู้ติดเชื้อ").search)
        # "อยู่ระหว่างสอบสวน (93 ราย)" on 2021-04-05 screws things up as its not a province
        # have to use look behind
        thai = r"[\u0E00-\u0E7Fa-zA-Z'. ]+[\u0E00-\u0E7Fa-zA-Z'.]"
        not_prov = r"(?<!อยู่ระหว่างสอบสวน)(?<!ยู่ระหว่างสอบสวน)(?<!ระหว่างสอบสวน)"
        provish = f"{thai}{not_prov}"
        nl = " *\n* *"
        nu = "(?:[0-9]+)"
        is_pcell = re.compile(rf"({provish}(?:{nl}\({provish}\))?{nl}\( *{nu} *ราย *\))")
        lines = pairwise(islice(is_pcell.split("\n".join(cells)), 1, None))  # beacause can be split over <p>
        yield title, lines


def briefing_case_detail(date, pages):

    num_people = re.compile(r"([0-9]+) *ราย")

    totals = dict()  # groupname -> running total
    all_cells = {}
    rows = []
    if date <= d("2021-02-26"):  # missing 2nd page of first lot (1.1)
        pages = []
    for soup in pages:
        for title, lines in briefing_case_detail_lines(soup):
            if "ติดเชื้อจากต่างประเทศ" in title:  # imported
                continue
            elif "การคัดกรองเชิงรุก" in title:
                case_type = "Proactive"
            elif "เดินทางมาจากต่างประเทศ" in title:
                # case_type = "Quarantine"
                continue  # just care about province cases for now
            # if re.search("(จากระบบเฝ้าระวัง|ติดเชื้อในประเทศ)", title):
            else:
                case_type = "Walkin"
            all_cells.setdefault(title, []).append(lines)
            # print(title,case_type)

            for prov_num, line in lines:
                # for prov in provs: # TODO: should really be 1. make split only split 1.
                # TODO: sometimes cells/data seperated by "-" 2021-01-03

                prov, num = prov_num.strip().split("(", 1)
                prov = get_province(prov)
                num = int(num_people.search(num).group(1))
                totals[title] = totals.get(title, 0) + num

                _, rest = get_next_numbers(line, "(?:nผล|ผลพบ)")  # "result"
                asym, rest = get_next_number(
                    rest,
                    "(?s)^.*(?:ไม่มีอาการ|ไมมี่อาการ|ไม่มีอาการ)",
                    default=0,
                    remove=True
                )
                sym, rest = get_next_number(
                    rest,
                    "(?s)^.*(?<!(?:ไม่มี|ไมมี่|ไม่มี))(?:อาการ|อาการ)",
                    default=0,
                    remove=True
                )
                unknown, _ = get_next_number(
                    rest,
                    "อยู่ระหว่างสอบสวนโรค",
                    # "อยู่ระหว่างสอบสวน",
                    "อยู่ระหว่างสอบสวน",
                    "อยู่ระหว่างสอบสวน",
                    "ไม่ระบุ",
                    default=0)
                # unknown2 = get_next_number(
                #     rest,
                #     "อยู่ระหว่างสอบสวน",
                #     "อยู่ระหว่างสอบสวน",
                #     default=0)
                # if unknown2:
                #     unknown = unknown2

                # TODO: if 1, can be by itself
                if asym == 0 and sym == 0 and unknown == 0:
                    sym, asym, unknown = None, None, None
                else:
                    assert asym + sym + unknown == num
                rows.append((date, prov, case_type, num, asym, sym))
    # checksum on title totals
    for title, total in totals.items():
        m = num_people.search(title)
        if not m:
            continue
        if date in [d("2021-03-19")]:  # 1.1 64!=56
            continue
        assert total == int(m.group(1)), f"group total={total} instead of: {title}\n{all_cells[title]}"
    df = pd.DataFrame(
        rows,
        columns=["Date", "Province", "Case Type", "Cases", "Cases Asymptomatic", "Cases Symptomatic"]
    ).set_index(['Date', 'Province'])

    return df


def briefing_case_types(date, pages):
    rows = []
    if date < d("2021-02-01"):
        pages = []
    for i, soup in enumerate(pages):
        text = soup.get_text()
        if "รายงานสถานการณ์" not in text:
            continue
        # cases = get_next_number(text, "ติดเชื้อจาก", before=True)
        # walkins = get_next_number(text.split("รายผู้ที่เดิน")[0], "ในประเทศ", until="ราย")
        # quarantine = get_next_number(text, "ต่างประเทศ", until="ราย", default=0)
        if date == d("2021-05-17"):
            numbers, rest = get_next_numbers(text.split("อาการหนัก")[1], "ในประเทศ")
            local, cases, imported, prison, walkins, proactive, imported2, prison2, *_ = numbers
            assert local == walkins + proactive
            assert imported == imported2
            assert prison == prison2
        else:
            numbers, rest = get_next_numbers(text, "รวม", until="รายผู้ที่เดิน")
            cases, walkins, proactive, *quarantine = numbers
            quarantine = quarantine[0] if quarantine else 0
            ports, rest = get_next_number(
                text,
                "ช่องเส้นทางธรรมชาติ",
                "รายผู้ที่เดินทางมาจากต่างประเทศ",
                before=True,
                default=0
            )
            imported = ports + quarantine
            prison, _ = get_next_number(text.split("รวม")[1], "ที่ต้องขัง", default=0, until="ราย")
        proactive += prison  # not sure if they are going to add this category going forward?

        assert cases == walkins + proactive + imported, f"{date}: briefing case types don't match"

        # hospitalisations
        numbers, rest = get_next_numbers(text, "อาการหนัก")
        if numbers:
            severe, respirator, *_ = numbers
            hospital, _ = get_next_number(text, "ใน รพ.")
            field, _ = get_next_number(text, "รพ.สนาม")
            num, _ = get_next_numbers(text, "ใน รพ.", before=True)
            hospitalised = num[0]
            assert hospital + field == hospitalised
        else:
            hospital, field, severe, respirator, hospitalised = [None] * 5

        if date < d("2021-05-18"):
            recovered, _ = get_next_number(text, "(เพ่ิมขึ้น|เพิ่มขึ้น)", until="ราย")
        else:
            # 2021-05-18 Using single infographic with 3rd wave numbers?
            numbers, _ = get_next_numbers(text, "หายป่วยแล้ว", "หายป่วยแลว้")
            cum_recovered_3rd, recovered, *_ = numbers
            if cum_recovered_3rd < recovered:
                recovered = cum_recovered_3rd

        assert recovered is not None

        deaths, _ = get_next_number(text, "เสียชีวิตสะสม", "เสียชีวติสะสม", "เสียชีวติ", before=True)
        assert not any_in([None], cases, walkins, proactive, imported, recovered, deaths)
        if date > d("2021-04-23"):
            assert not any_in([None], hospital, field, severe, respirator, hospitalised)

        # cases by region
        # bangkok, _ = get_next_number(text, "กรุงเทพฯ และนนทบุรี")
        # north, _ = get_next_number(text, "ภาคเหนือ")
        # south, _ = get_next_number(text, "ภาคใต้")
        # east, _ = get_next_number(text, "ภาคตะวันออก")
        # central, _ = get_next_number(text, "ภาคกลาง")
        # all_regions = north+south+east+central
        # assert hospitalised == all_regions, f"Regional hospitalised {all_regions} != {hospitalised}"

        rows.append([
            date,
            cases,
            walkins,
            proactive,
            imported,
            prison,
            hospital,
            field,
            severe,
            respirator,
            hospitalised,
            recovered,
            deaths,
        ])
        break
    df = pd.DataFrame(rows, columns=[
        "Date",
        "Cases",
        "Cases Walkin",
        "Cases Proactive",
        "Cases Imported",
        "Cases Area Prison",  # Keep as Area so we don't repeat number.
        "Hospitalized Hospital",
        "Hospitalized Field",
        "Hospitalized Severe",
        "Hospitalized Respirator",
        "Hospitalized",
        "Recovered",
        "Deaths",
    ]).set_index(['Date'])
    if not df.empty:
        print(f"{date.date()} Briefing Cases:", df.to_string(header=False, index=False))
    return df


def briefing_province_cases(date, pages):
    # TODO: also can be got from https://ddc.moph.go.th/viralpneumonia/file/scoreboard/scoreboard_02062564.pdf
    # Seems updated around 3pm so perhaps not better than briefing
    if date < d("2021-01-13"):
        pages = []
    rows = {}
    for i, soup in enumerate(pages):
        text = str(soup)
        if "อโควิดในประเทศรายใหม่" not in text or "รวมท ัง้ประเทศ" in text:
            continue
        parts = [p.get_text() for p in soup.find_all("p")]
        parts = [line for line in parts if line]
        preamble, *tables = split(parts, re.compile(r"รวม\s*\(ราย\)").search)
        if len(tables) <= 1:
            continue  # Additional top 10 report. #TODO: better detection of right report
        else:
            title, parts = tables
        while parts and "รวม" in parts[0]:
            totals, *parts = parts
        parts = [c.strip() for c in NUM_OR_DASH.split("\n".join(parts)) if c.strip()]
        while True:
            if len(parts) < 9:
                # TODO: can be number unknown cases - e.g. หมายเหตุ : รอสอบสวนโรค จานวน 337 ราย
                break
            if NUM_OR_DASH.search(parts[0]):
                linenum, prov, *parts = parts
            else:
                # for some reason the line number doesn't show up? but its there in the pdf...
                break
            numbers, parts = parts[:9], parts[9:]
            thai = prov.strip().strip(" ี").strip(" ์").strip(" ิ")
            if thai in ['กทม. และปรมิ ณฑล', 'รวมจงัหวดัอนื่ๆ(']:
                # bangkok + subrubrs, resst of thailand
                break
            prov = get_province(thai)
            numbers = parse_numbers(numbers)
            numbers = numbers[1:-1]  # last is total. first is previous days
            assert len(numbers) == 7
            for i, cases in enumerate(reversed(numbers)):
                if i > 4:  # 2021-01-11 they use earlier cols for date ranges
                    break
                olddate = date - datetime.timedelta(days=i)
                rows[(olddate, prov)] = cases + rows.get((olddate, prov), 0)  # rare case where we need to merge
                # if False and olddate == date:
                #     if cases > 0:
                #         print(date, linenum, thai, PROVINCES["ProvinceEn"].loc[prov], cases)
                #     else:
                #         print("no cases", linenum, thai, *numbers)
    data = ((d, p, c) for (d, p), c in rows.items())
    df = pd.DataFrame(data, columns=["Date", "Province", "Cases"]).set_index(["Date", "Province"])
    assert date >= d(
        "2021-01-13") and not df.empty, f"Briefing on {date} failed to parse cases per province"
    return df


def briefing_deaths_provinces(text, date, total_deaths):

    # get rid of extra words in brakets to make easier
    text = re.sub("(ละ|/จังหวัด|จังหวัด|อย่างละ|ราย)", "", text)

    # Provinces are split between bullets with disease and risk.
    pre, rest = re.split("โควิด *-?19\n\n", text, 1)
    bullets_re = re.compile(r"(•[^\(]*?\( ?\d+ ?\)(?:[\n ]*\([^\)]+\))?)\n?")
    ptext1, b1, rest = bullets_re.split(rest, 1)
    *bullets, ptext2 = bullets_re.split(rest)
    ptext2, age_text = re.split("•", ptext2, 1)
    ptext = ptext1 + ptext2
    pcells = pairwise(strip(re.split(r"(\(?\d+\)?)", ptext)))

    province_count = {}
    last_provs = None

    def add_deaths(provinces, num):
        provs = [p.strip("() ") for p in provinces.split() if len(p) > 1 and p.strip("() ")]
        provs = [get_province(p, ignore_error=True) for p in provs]
        # TODO: unknown from another cell get in there. Work out how to remove it a better way
        provs = [p for p in provs if p and p != "Unknown"]
        for p in provs:
            province_count[p] = province_count.get(p, 0) + num

    for provinces, num in pcells:
        # len() < 2 because some stray modifier?
        text_num, rest = get_next_number(provinces, remove=True)
        num, _ = get_next_number(num)
        if num is None and text_num is not None:
            num = text_num
        elif num is None:
            raise Exception(f"No number of deaths found {date}: {text}")

        if rest.strip().startswith("("):
            # special case where some in that province are in prison
            # take them out of last prov and put into special province
            if not last_provs:
                raise Exception(f"subset of province can't be adjusted for {rest}")
            add_deaths(last_provs, -num)  # TODO: should only be prison. check this
        add_deaths(rest, num)
        last_provs = rest
    dfprov = pd.DataFrame(((date, p, c) for p, c in province_count.items()),
                          columns=["Date", "Province", "Deaths"]).set_index(["Date", "Province"])
    assert total_deaths == dfprov['Deaths'].sum()
    return dfprov


def briefing_deaths_summary(text, date):
    title_re = re.compile("(ผูป่้วยโรคโควดิ-19|ผู้ป่วยโรคโควิด-19)")
    if not title_re.search(text):
        return pd.DataFrame(), pd.DataFrame()
    # Summary of locations, reasons, medium age, etc

    # Congenital disease / risk factor The severity of the disease
    # congenital_disease = df[2][0]  # TODO: parse?
    # Risk factors for COVID-19 infection
    # risk_factors = df[3][0]
    numbers, *_ = get_next_numbers(text,
                                   "ค่ามัธยฐานของอายุ",
                                   "ค่ากลาง อายุ",
                                   "ค่ากลางอายุ",
                                   "ค่ากลางของอายุ",
                                   "• ค่ากลาง",
                                   ints=False)
    med_age, min_age, max_age, *_ = numbers
    numbers, *_ = get_next_numbers(text, "ชาย")
    male, female, *_ = numbers

    numbers, *_ = get_next_numbers(text, "ค่ากลางระยะเวลา")
    if numbers:
        period_death_med, period_death_max, *_ = numbers

    title_num, _ = get_next_numbers(text, title_re)
    day, year, deaths_title, *_ = title_num

    no_comorbidity, _ = get_next_number(text, "ไม่มีโรคประจ", "ปฏิเสธโรคประจ าตัว", default=0)
    risk_family, _ = get_next_number(text, "คนในครอบครัว", "ครอบครัว", "สัมผัสญาติติดเชื้อมาเยี่ยม", default=0)

    assert male + female == deaths_title
    # TODO: <= 2021-04-30. there is duration med, max and 7-21 days, 1-4 days, <1

    # TODO: what if they have more than one page?
    sum = pd.DataFrame([[date, male + female, med_age, min_age, max_age, male, female, no_comorbidity, risk_family]],
                       columns=[
                           "Date", "Deaths", "Deaths Age Median", "Deaths Age Min", "Deaths Age Max", "Deaths Male",
                           "Deaths Female", "Deaths Comorbidity None", "Deaths Risk Family"]
                       ).set_index("Date")
    dfprov = briefing_deaths_provinces(text, date, deaths_title)
    print(f"{date.date()} Deaths:", len(dfprov), "|", sum.to_string(header=False, index=False))
    return sum, dfprov


def briefing_deaths_cells(cells, date, all):
    rows = []
    for cell in cells:
        lines = [line for line in cell.split("\n") if line.strip()]
        if "รายละเอียดผู้เสีย" in lines[0]:
            lines = lines[1:]
        rest = '\n'.join(lines)
        death_num, rest = get_next_number(rest, "รายที่", "รายที", remove=True)
        age, rest = get_next_number(rest, "อายุ", "ผู้ป่ว", remove=True)
        num_2ndwave, rest = get_next_number(rest, "ระลอกใหม่", remove=True)
        numbers, _ = get_next_numbers(rest, "")
        if age is not None and death_num is not None:
            pass
        elif age:
            death_num, *_ = numbers
        elif death_num:
            age, *_ = numbers
        else:
            death_num, age, *_ = numbers
        assert 1 < age < 110
        assert 55 < death_num < 1500
        gender = parse_gender(cell)
        match = re.search(r"ขณะป่วย (\S*)", cell)  # TODO: work out how to grab just province
        if match:
            prov = match.group(1).replace("จังหวัด", "")
            province = get_province(prov)
        else:
            # handle province by itself on a line
            p = [get_province(word, True) for line in lines[:3] for word in line.split()]
            p = [pr for pr in p if pr]
            if p:
                province = p[0]
            else:
                raise Exception(f"no province found for death in: {cell}")
        rows.append([float(death_num), date, gender, age, province, None, None, None, None, None])
    df = \
        pd.DataFrame(rows, columns=['death_num', "Date", "gender", "age", "Province", "nationality",
                                    "congenital_disease", "case_history", "risk_factor_sickness",
                                    "risk_factor_death"]).set_index("death_num")
    return all.append(df, verify_integrity=True)


def briefing_deaths_table(orig, date, all):
    """death details per quadrant or page, turned into table by camelot"""
    df = orig.drop(columns=[0, 10])
    df.columns = ['death_num', "gender", "nationality", "age", "Province",
                  "congenital_disease", "case_history", "risk_factor_sickness", "risk_factor_death"]
    df['death_num'] = pd.to_numeric(df['death_num'], errors="coerce")
    df['age'] = pd.to_numeric(df['age'], errors="coerce")
    df = df.dropna(subset=["death_num"])
    df['Date'] = date
    df['gender'] = df['gender'].map(parse_gender)  # TODO: handle mispelling
    df = df.set_index("death_num")
    df = join_provinces(df, "Province")
    all = all.append(df, verify_integrity=True)
    # parts = [l.get_text() for l in soup.find_all("p")]
    # parts = [l for l in parts if l]
    # preamble, *tables = split(parts, re.compile("ปัจจัยเสี่ยงการ").search)
    # for header,lines in pairwise(tables):
    #     _, *row_pairs = split(lines, re.compile("(\d+\s*(?:ชาย|หญิง))").search)
    #     for first, rest in pairwise(row_pairs):
    #         row = ' '.join(first) + ' '.join(rest)
    #         case_num, age, *dates = get_next_numbers("")
    #         print(row)
    return all


def briefing_deaths(file, date, pages):
    # Only before the 2021-04-29
    all = pd.DataFrame()
    for i, soup in enumerate(pages):
        text = soup.get_text()
        # Latest version of deaths. Only gives summary info
        sum, dfprov = briefing_deaths_summary(text, date)
        if not sum.empty:
            return all, sum, dfprov

        if "วิตของประเทศไทย" not in text:
            continue
        orig = None
        if date <= d("2021-04-19"):
            cells = [soup.get_text()]
        else:
            # Individual case detail for death
            orig = camelot.read_pdf(file, pages=str(i + 2), process_background=True)[0].df
            if len(orig.columns) != 11:
                cells = [cell for r in orig.itertuples() for cell in r[1:] if cell]
            else:
                cells = []
        if cells:
            # Older style, not row per death
            all = briefing_deaths_cells(cells, date, all)
        elif orig is not None:  # <= 2021-04-27
            all = briefing_deaths_table(orig, date, all)
        else:
            raise Exception(f"Couldn't parse deaths {date}")

    if all.empty:
        print(f"{date.date()}: Deaths:  0")
        sum = \
            pd.DataFrame([[date, 0, None, None, None, 0, 0]],
                         columns=["Date", "Deaths", "Deaths Age Median", "Deaths Age Min", "Deaths Age Max",
                                  "Deaths Male", "Deaths Female"]).set_index("Date")
        dfprov = pd.DataFrame(columns=["Date", "Province", "Deaths"]).set_index(["Date", "Province"])

    else:
        # calculate daily summary stats
        med_age, min_age, max_age = all['age'].median(), all['age'].min(), all['age'].max()
        g = all['gender'].value_counts()
        male, female = g.get('Male', 0), g.get('Female', 0)
        sum = \
            pd.DataFrame([[date, male + female, med_age, min_age, max_age, male, female]],
                         columns=["Date", "Deaths", "Deaths Age Median", "Deaths Age Min", "Deaths Age Max",
                                  "Deaths Male", "Deaths Female"]).set_index("Date")
        print(f"{date.date()} Deaths: ", sum.to_string(header=False, index=False))
        dfprov = all[["Date", 'Province']].value_counts().to_frame("Deaths")

    # calculate per provice counts
    return all, sum, dfprov


def get_cases_by_prov_briefings():
    print("========Briefings==========")
    types = pd.DataFrame(columns=["Date", ]).set_index(['Date', ])
    date_prov = pd.DataFrame(columns=["Date", "Province"]).set_index(['Date', 'Province'])
    date_prov_types = pd.DataFrame(columns=["Date", "Province", "Case Type"]).set_index(['Date', 'Province'])
    deaths = pd.DataFrame()
    url = "http://media.thaigov.go.th/uploads/public_img/source/"
    start = d("2021-01-13")  # 12th gets a bit messy but could be fixed
    end = today()
    links = (f"{url}{f.day:02}{f.month:02}{f.year-1957}.pdf" for f in daterange(start, end, 1))
    for file, text in web_files(*reversed(list(links)), dir="briefings"):
        pages = parse_file(file, html=True, paged=True)
        pages = [BeautifulSoup(page, 'html.parser') for page in pages]
        date = file2date(file)

        today_types = briefing_case_types(date, pages)
        types = types.combine_first(today_types)

        case_detail = briefing_case_detail(date, pages)
        date_prov_types = date_prov_types.combine_first(case_detail)

        prov = briefing_province_cases(date, pages)

        each_death, death_sum, death_by_prov = briefing_deaths(file, date, pages)

        wrong_deaths_report = date in [
            d("2021-03-19"),  # 19th was reported on 18th
            d("2021-03-18"),
            d("2021-03-17"),  # 15th and 17th no details of death
            d("2021-03-15"),
            d("2021-02-24"),  # 02-24 infographic is image
            d("2021-02-19"),  # 02-19 death deatils is graphic (the doctor)
            d("2021-02-15"),  # no details of deaths (2)
            d("2021-02-10"),  # no details of deaths (1)
        ] or date < d("2021-02-01")  # TODO: check out why later
        ideaths, ddeaths = today_types['Deaths'], death_sum['Deaths']
        assert wrong_deaths_report or (ddeaths == ideaths).all(
        ), f"Death details {ddeaths} didn't match total {ideaths}"

        deaths = deaths.append(each_death, verify_integrity=True)
        date_prov = date_prov.combine_first(death_by_prov)
        types = types.combine_first(death_sum)

        date_prov = date_prov.combine_first(prov)

        # Do some checks across the data
        today_total = today_types[['Cases Proactive', "Cases Walkin"]].sum().sum()
        prov_total = prov.groupby("Date").sum()['Cases'].loc[date]
        warning = f"briefing provs={prov_total}, cases={today_total}"
        if today_total and prov_total:
            assert prov_total / today_total > 0.77, warning  # 2021-04-17 is very low but looks correct
        if today_total != prov_total:
            print(f"{date.date()} WARNING:", warning)
        # if today_total / prov_total < 0.9 or today_total / prov_total > 1.1:
        #     raise Exception(f"briefing provs={prov_total}, cases={today_total}")

        # Phetchabun                  1.0 extra
    # ขอนแกน่ 12 missing
    # ชุมพร 1 missing

    export(deaths, "deaths")

    if not date_prov_types.empty:
        symptoms = date_prov_types[["Cases Symptomatic", "Cases Asymptomatic"]]  # todo could keep province breakdown
        symptoms = symptoms.groupby(['Date']).sum()
        types = types.combine_first(symptoms)
        date_prov_types = date_prov_types[["Case Type", "Cases"]]
        # we often have multiple walkin events
        date_prov_types = date_prov_types.groupby(['Date', 'Province', 'Case Type']).sum()
        date_prov_types = date_prov_types.reset_index().pivot(index=["Date", "Province"], columns=['Case Type'])
        date_prov_types.columns = [f"Cases {c}" for c in date_prov_types.columns.get_level_values(1)]
        date_prov = date_prov.combine_first(date_prov_types)

    return date_prov, types


def get_cases_by_area_type():
    dfprov, twcases = get_cases_by_prov_tweets()
    briefings, cases = get_cases_by_prov_briefings()
    cases = cases.combine_first(twcases)
    dfprov = briefings.combine_first(dfprov)  # TODO: check they aggree
    # df2.index = df2.index.map(lambda x: difflib.get_close_matches(x, df1.index)[0])
    # dfprov = dfprov.join(PROVINCES['Health District Number'], on="Province")
    dfprov = join_provinces(dfprov, on="Province")
    # Now we can save raw table of provice numbers
    export(dfprov, "cases_by_province")

    # Reduce down to health areas
    dfprov_grouped = dfprov.groupby(["Date", "Health District Number"]).sum(min_count=1).reset_index()
    dfprov_grouped = dfprov_grouped.pivot(index="Date", columns=['Health District Number'])
    dfprov_grouped = dfprov_grouped.rename(columns=dict((i, f"Area {i}") for i in DISTRICT_RANGE))
    # cols = dict((f"Area {i}", f"Cases Area {i}") for i in DISTRICT_RANGE)
    # by_area = dfprov_grouped["Cases"].groupby(['Health District Number'],axis=1).sum(min_count=1).rename(columns=cols)
    # cols = dict((f"Area {i}", f"Cases Proactive Area {i}") for i in DISTRICT_RANGE)
    by_type = dfprov_grouped.groupby(level=0, axis=1).sum(min_count=1)
    # Collapse columns to "Cases Proactive Area 13" etc
    dfprov_grouped.columns = dfprov_grouped.columns.map(' '.join).str.strip()
    by_area = dfprov_grouped.combine_first(by_type)
    by_area = by_area.combine_first(cases)  # imported, proactive total etc

    # Ensure we have all areas
    for i in DISTRICT_RANGE:
        col = f"Cases Walkin Area {i}"
        if col not in by_area:
            by_area[col] = by_area.get(col, pd.Series(index=by_area.index, name=col))
        col = f"Cases Proactive Area {i}"
        if col not in by_area:
            by_area[col] = by_area.get(col, pd.Series(index=by_area.index, name=col))
    return by_area


def get_cases_by_area_api():
    cases = get_case_details_csv().reset_index()
    cases["province_of_onset"] = cases["province_of_onset"].str.strip(".")
    cases = join_provinces(cases, "province_of_onset")
    case_areas = pd.crosstab(cases['Date'], cases['Health District Number'])
    case_areas = case_areas.rename(columns=dict((i, f"Cases Area {i}") for i in DISTRICT_RANGE))
    return case_areas


def get_cases_by_area():
    # we will add in the tweet data for the export
    case_briefings_tweets = get_cases_by_area_type()
    case_api = get_cases_by_area_api()  # can be very wrong for the last days

    case_areas = case_briefings_tweets.combine_first(case_api)

    export(case_areas, "cases_by_area")
    return case_areas


##########################################
# Testing data
##########################################

def test_dav_files(url="http://nextcloud.dmsc.moph.go.th/public.php/webdav",
                   username="wbioWZAQfManokc",
                   password="null",
                   ext=".pdf .pptx",
                   dir="testing_moph"):
    return dav_files(url, username, password, ext, dir)


def get_tests_by_day():
    print("========Tests by Day==========")

    file = next(test_dav_files(ext="xlsx"))
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
    # print(tests["Pos"].sum(), pos + all_pos)
    # print(tests["Total"].sum(), total + all_total)
    # fix datetime
    tests.reset_index(drop=False, inplace=True)
    tests["Date"] = pd.to_datetime(tests["Date"])
    tests.set_index("Date", inplace=True)

    tests.rename(columns=dict(Pos="Pos XLS", Total="Tests XLS"), inplace=True)
    print(file, len(tests))

    return tests


def get_tests_by_area_chart_pptx(file, title, series, data, raw):
    start, end = find_date_range(title)
    if start is None or "เริ่มเปิดบริการ" in title or not any_in(title, "เขตสุขภาพ", "เขตสุขภำพ"):
        return data, raw

    # the graph for X period split by health area.
    # Need both pptx and pdf as one pdf is missing
    pos = list(series["จำนวนผลบวก"])
    tests = list(series["จำนวนตรวจ"])
    row = pos + tests + [sum(pos), sum(tests)]
    results = spread_date_range(start, end, row, ["Date"] + POS_COLS + TEST_COLS + ["Pos Area", "Tests Area"])
    # print(results)
    data = data.combine_first(results)
    raw = raw.combine_first(pd.DataFrame(
        [[start, end, ] + pos + tests],
        columns=["Start", "End", ] + POS_COLS + TEST_COLS
    ).set_index("Start"))
    print("Tests by Area", start.date(), "-", end.date(), file)
    return data, raw


def get_tests_by_area_pdf(file, page, data, raw):
    start, end = find_date_range(page)
    if start is None or "เริ่มเปิดบริการ" in page or not any_in(page, "เขตสุขภาพ", "เขตสุขภำพ"):
        return data, raw
    # Can't parse '35_21_12_2020_COVID19_(ถึง_18_ธันวาคม_2563)(powerpoint).pptx' because data is a graph
    # no pdf available so data missing
    # Also missing 14-20 Nov 2020 (no pptx or pdf)

    if "349585" in page:
        page = page.replace("349585", "349 585")
    # First line can be like จดัท ำโดย เพญ็พชิชำ ถำวงศ ์กรมวิทยำศำสตณก์ำรแพทย์ วันที่ท ำรำยงำน 15/02/2564 เวลำ 09.30 น.
    first, rest = page.split("\n", 1)
    page = (
        rest if "เพญ็พชิชำ" in first or "/" in first else page
    )  # get rid of first line that sometimes as date and time in it
    numbers, _ = get_next_numbers(page, "", debug=True)  # "ภาคเอกชน",
    # ภาครัฐ
    # ภาคเอกชน
    # จดัท ำโดย เพญ็พชิชำ ถำวงศ ์กรมวิทยำศำสตณก์ำรแพทย์
    # print(numbers)
    # TODO: should really find and parse X axis labels which contains 'เขต' and count
    tests_start = 13 if "total" not in page else 14
    pos = numbers[0:13]
    tests = numbers[tests_start:tests_start + 13]
    row = pos + tests + [sum(pos), sum(tests)]
    results = spread_date_range(start, end, row, ["Date"] + POS_COLS + TEST_COLS + ["Pos Area", "Tests Area"])
    data = data.combine_first(results)
    raw = raw.combine_first(pd.DataFrame(
        [[start, end, ] + pos + tests],
        columns=["Start", "End", ] + POS_COLS + TEST_COLS
    ).set_index("Start"))
    print("Tests by Area", start.date(), "-", end.date(), file)
    return data, raw


def get_tests_private_public_pptx(file, title, series, data):
    start, end = find_date_range(title)
    if start is None:
        return data
    elif "เริ่มเปิดบริการ" not in title and any_in(title, "เขตสุขภาพ", "เขตสุขภำพ"):
        # It's a by area chart
        return data
    elif not ("และอัตราการตรวจพบ" in title and "รายสัปดาห์" not in title and "จำนวนตรวจ" in series):
        return data

    # The graphs at the end with all testing numbers private vs public
    private = " Private" if "ภาคเอกชน" in title else ""

    # pos = series["Pos"]
    tests = series["จำนวนตรวจ"]
    positivity = series["% Detection"]
    dates = list(daterange(start, end, 1))
    df = pd.DataFrame(
        {
            "Date": dates,
            f"Tests{private}": tests,
            f"% Detection{private}": positivity,
        }
    ).set_index("Date")
    df[f"Pos{private}"] = (
        df[f"Tests{private}"] * df[f"% Detection{private}"] / 100.0
    )
    print(f"Tests {private}", start.date(), "-", end.date(), file)
    return data.combine_first(df)


def get_test_reports():
    data = pd.DataFrame()
    raw = pd.DataFrame()
    pubpriv = pd.DataFrame()

    for file in test_dav_files(ext=".pptx"):
        for chart, title, series, pagenum in pptx2chartdata(file):
            data, raw = get_tests_by_area_chart_pptx(file, title, series, data, raw)
            if not all_in(pubpriv.columns, 'Tests', 'Tests Private'):
                # Latest file as all the data we need
                pubpriv = get_tests_private_public_pptx(file, title, series, pubpriv)
    # Also need pdf copies because of missing pptx
    for file in test_dav_files(ext=".pdf"):
        pages = parse_file(file, html=False, paged=True)
        for page in pages:
            data, raw = get_tests_by_area_pdf(file, page, data, raw)
    export(raw, "tests_by_area")

    pubpriv['Pos Public'] = pubpriv['Pos'] - pubpriv['Pos Private']
    pubpriv['Tests Public'] = pubpriv['Tests'] - pubpriv['Tests Private']
    export(pubpriv, "tests_pubpriv")
    data = data.combine_first(pubpriv)

    return data


################################
# Vaccination reports
################################

def get_vaccination_coldtraindata(request_json):
    df_codes = pd.read_html("https://en.wikipedia.org/wiki/ISO_3166-2:TH")[0]
    codes = [code for code, prov, ptype in df_codes.itertuples(index=False) if "special" not in ptype]
    provinces = [prov.split("(")[0] for code, prov, ptype in df_codes.itertuples(index=False) if "special" not in ptype]

    url = "https://datastudio.google.com/batchedDataV2?appVersion=20210506_00020034"
    with open(request_json) as fp:
        post = json.load(fp)
    specs = post['dataRequest']
    post['dataRequest'] = []

    def set_filter(filters, field, value):
        for filter in filters:
            if filter['filterDefinition']['filterExpression']['queryTimeTransformation']['dataTransformation']['sourceFieldName'] == field:
                filter['filterDefinition']['filterExpression']['stringValues'] = value
        return filters

    def make_request(post, codes):
        for code in codes:
            for spec  in specs:
                pspec = copy.deepcopy(spec)
                set_filter(pspec['datasetSpec']['filters'], "_hospital_province_code_", [code])
                post['dataRequest'].append(pspec)
        r = requests.post(url, json=post)
        _, _, data = r.text.split("\n")
        data = json.loads(data)
        for resp in data['dataResponse']:
            yield resp
    all_prov = pd.DataFrame(columns=["Date", "Province", "Vaccine"]).set_index(["Date", "Province", "Vaccine"])
    for prov_spec, data in zip([(p, s) for p in provinces for s in specs], make_request(post, codes)):
        prov, spec = prov_spec
        prov = get_province(prov)
        fields = [(f['name'], f['dataTransformation']['sourceFieldName']) for f in spec['datasetSpec']['queryFields']]
        for datasubset in data['dataSubset']:
            colmuns = datasubset['dataset']['tableDataset']['column']
            df_cols = {}
            is_today = True
            for field, column in zip(fields, colmuns):
                if 'dateColumn' in column:
                    values = [d(date) for date in column['dateColumn']['values']]
                    is_today = False
                elif 'longColumn' in column:
                    values = [int(i) for i in column['longColumn']['values']]
                elif 'stringColumn' in column:
                    values = column['stringColumn']['values']
                else:
                    raise Exception("Unknown column type", column.keys())
                fieldname = dict(_vaccinated_on_='Date',
                                 _manuf_name_='Vaccine',
                                 datastudio_record_count_system_field_id_98323387='Vac Given').get(field[1], field[1])
                # datastudio_record_count_system_field_id_98323387 = supply?

                df_cols[fieldname] = values
            df = pd.DataFrame(df_cols)
            df['Province'] = prov
            if is_today:
                df['Date'] = today()
            all_prov = all_prov.combine_first(df.set_index(["Date", "Province", "Vaccine"]))
    return all_prov.reset_index().set_index("Date").loc['2021-02-28':].reset_index().set_index(['Date', 'Province'])


def vac_problem(daily, date, file, page):
    if "Anaphylaxis" not in page:
        return daily
    prob_a, rest = get_next_number(page, "Anaphylaxis")
    prob_p, rest = get_next_number(page, "Polyneuropathy")
    row = [date, prob_a, prob_p]
    assert not any_in(['None'], row)
    df = pd.DataFrame([row], columns=[
        "Date",
        "Vac Problem Anaphylaxis",
        "Vac Problem Polyneuropathy",
    ]).set_index("Date")
    return daily.combine_first(df)


def vaccination_daily(daily, date, file, page):
    if not re.search("(ให้หน่วยบริกำร|ใหห้นว่ยบริกำร|สรปุกำรจดัสรรวคัซนีโควดิ 19|ริการวัคซีนโควิด 19)", page):
        return daily
    # dose1_total, rest1 = get_next_number(page, "ได้รับวัคซีนเข็มที่ 1", until="โดส")
    # dose2_total, rest2 = get_next_number(page, "ได้รับวัคซีน 2 เข็ม", until="โดส")

    alloc_sv, rest = get_next_number(page, "Sinovac", until="โดส")
    alloc_az, rest = get_next_number(page, "AstraZeneca", until="โดส")
    #alloc_total, rest = get_next_number(page, "รวมกำรจัดสรรวัคซีนทั้งหมด", "รวมกำรจดัสรรวคัซนีทัง้หมด", until="โดส")
    #assert alloc_total == alloc_sv + alloc_az
    row = [date, alloc_sv, alloc_az]
    assert not any_in(['None'], row)
    df = pd.DataFrame([row], columns=[
        "Date",
        "Vac Allocated Sinovac",
        "Vac Allocated AstraZeneca",
    ]).set_index("Date")
    daily = daily.combine_first(df)

    d1_num, rest1 = get_next_numbers(page, "ได้รับวัคซีนเข็มที่ 1", "รับวัคซีนเข็มท่ี 1 จํานวน", until="2 เข็ม")
    d2_num, rest2 = get_next_numbers(page, "ได้รับวัคซีน 2 เข็ม", "ไดรับวัคซีน 2 เข็ม", until="รำย ดังรูป")

    # get_next_numbers(page, "ได้รับวัคซีนเข็มที่ 1", until="ได้รับวัคซีน 2 เข็ม")
    # medical, _ = get_next_number(text, "เป็นบุคลำกรทำงกำรแพทย์", "คลำกรทำงกำรแพทย์", until="รำย")
    # frontline, _ = get_next_number(text, "เจ้ำหน้ำที่ที่มีโอกำสสัมผัส", "โอกำสสัมผัสผู้ป่วย", until="รำย")
    # over60, _ = get_next_number(text, "ผู้ที่มีอำยุตั้งแต่ 60 ปีขึ้นไป", "ผู้ที่มีอำยุตั้งแต่ 60", until="รำย")
    # chronic, _ = get_next_number(text, "บุคคลที่มีโรคประจ", until="รำย")
    # area, _ = get_next_number(text, "ในพ้ืนที่เสี่ยง", "และประชำชนในพื้นท่ีเสี่ยง", until="รำย")

    for dose, numbers in enumerate([d1_num, d2_num], 1):
        if len(numbers) != 6 or not re.search("(บุคคลที่มีโรคประจ|บุคคลท่ีมีโรคประจําตัว)", rest):
            total, *_ = numbers
            df = pd.DataFrame([[date, total]], columns=[
                "Date",
                f"Vac Given {dose} Cum",
            ]).set_index("Date")
            daily = daily.combine_first(df)
            continue

        total, medical, frontline, sixty, over60, chronic, area, *_ = numbers
        assert sixty == 60
        row = [medical, frontline, over60, chronic, area]
        assert not any_in(row, None)
        assert 0.99 <= (sum(row) / total) <= 1.0
        df = pd.DataFrame([[date, total] + row],
                          columns=[
                              "Date",
                              f"Vac Given {dose} Cum",
                              f"Vac Group Medical Staff {dose} Cum",
                              f"Vac Group Other Frontline Staff {dose} Cum",
                              f"Vac Group Over 60 {dose} Cum",
                              f"Vac Group Risk: Disease {dose} Cum",
                              f"Vac Group Risk: Location {dose} Cum", ]
                          ).set_index("Date")
        daily = daily.combine_first(df)
    print(date.date(), "Vac Sum", daily.loc[date:date].to_string(header=False, index=False), file)
    return daily


def vaccination_tables(vaccinations, allocations, vacnew, date, page, file):
    def assert_no_repeat(d, prov, thaiprov, numbers):
        prev = d.get((date, prov))
        msg = f"Vac {date} {prov}|{thaiprov} repeated: {numbers} != {prev}"
        assert prev in [None, numbers], msg

    shots = re.compile(r"(เข็ม(?:ที|ที่|ท่ี)\s.?(?:1|2)\s*)")
    oldhead = re.compile("(เข็มที่ 1 วัคซีน|เข็มท่ี 1 และ|เข็มที ่1 และ)")
    lines = [line.strip() for line in page.split('\n') if line.strip()]
    preamble, *rest = split(lines, lambda x: shots.search(x) or oldhead.search(x))
    for headings, lines in pairwise(rest):
        shot_count = max(len(shots.findall(h)) for h in headings)
        oh_count = max(len(oldhead.findall(h)) for h in headings)
        table = {12: "new_given", 10: "given", 6: "alloc"}.get(shot_count, "old_given" if oh_count else None)
        if not table:
            continue
        added = 0
        for line in lines:
            # fix some number broken in the middle
            line = re.sub(r"(\d+ ,\d+)", lambda x: x.group(0).replace(" ", ""), line)
            area, *rest = line.split(' ', 1)
            if area == "รวม" or not rest:
                break
            if area in ["เข็มที่", "และ"]:  # Extra heading
                continue
            cols = [c.strip() for c in NUM_OR_DASH.split(rest[0]) if c.strip()]
            if len(cols) < 5:
                break
            if NUM_OR_DASH.match(area):
                thaiprov, *cols = cols
            else:
                thaiprov = area
            prov = get_province(thaiprov)
            numbers = parse_numbers(cols)
            added += 1
            if table == "alloc":
                allocations[(date, prov)] = numbers[3:7]
            elif table == "given":
                if len(numbers) == 16:
                    alloc_sino, alloc_az, *numbers = numbers
                assert len(numbers) == 14
                assert_no_repeat(vaccinations, prov, thaiprov, numbers)
                vaccinations[(date, prov)] = numbers
            elif table == "new_given":
                assert len(numbers) == 12  # some extra "-" throwing it out. have to use camelot
                assert_no_repeat(vacnew, prov, thaiprov, numbers)
                vacnew[(date, prov)] = numbers
            elif table == "old_given":
                alloc, target_num, given, perc, *rest = numbers
                medical, frontline, disease, elders, riskarea, *rest = rest
                # TODO: #อยู่ระหว่ำง ระบุ กลุ่มเป้ำหมำย - In the process of specifying the target group
                # unknown = sum(rest)
                vaccinations[(date, prov)] = [given, perc, 0, 0] + \
                    [medical, 0, frontline, 0, disease, 0, elders, 0, riskarea, 0]
                allocations[(date, prov)] = [alloc, 0, 0, 0]
        assert added > 7
        print(f"{date.date()}: {table} Vaccinations: {added}", file)
    return vaccinations, allocations, vacnew


def get_vaccinations():
    vacct = get_vaccination_coldtraindata("vac_request.json")
    vacct = vacct.reset_index().pivot(index=["Date", "Province"], columns=["Vaccine"]).fillna(0)
    vacct.columns = [" ".join(c).replace("Sinovac Life Sciences", "Sinovac") for c in vacct.columns]
    vacct['Vac Given'] = vacct.sum(axis=1, skipna=False)
    vaccum = vacct.groupby(level="Province", as_index=False).apply(lambda pdf: daily2cum(pdf))
    vacct = vacct.combine_first(vaccum.droplevel(0))

    folders = web_links("https://ddc.moph.go.th/dcd/pagecontent.php?page=643&dept=dcd",
                        ext=None, match=re.compile("2564"))
    links = (link for f in folders for link in web_links(f, ext=".pdf"))
    url = "https://ddc.moph.go.th/uploads/ckeditor2//files/Daily report "
    gen_links = (f"{url}{f.year}-{f.month:02}-{f.day:02}.pdf"
                 for f in reversed(list(daterange(d("2021-05-20"), today(), 1))))
    links = unique_values(chain(gen_links, links))
    links = sorted(links, key=lambda f: date if (date := file2date(f)) is not None else d("2020-01-01"), reverse=True)
    # add in newer https://ddc.moph.go.th/uploads/ckeditor2//files/Daily%20report%202021-06-04.pdf
    # Just need the latest
    pages = ((page, file2date(f), f) for f, _ in web_files(
        *links, dir="vaccinations") for page in parse_file(f) if file2date(f))
    vaccinations = {}
    allocations = {}
    vacnew = {}
    vac_daily = import_csv("vac_timeline", ["Date"]) if USE_CACHE_DATA else pd.DataFrame(
        columns=["Date"]).set_index(["Date"])
    all_vac = import_csv("vaccinations", ["Date", "Province"]) if USE_CACHE_DATA else pd.DataFrame(
        columns=["Date", "Province"]).set_index(["Date", "Province"])
    for page, date, file in pages:  # TODO: vaccinations are the day before I think
        if not date or date <= d("2021-01-01"):  # TODO: make go back later
            continue
        date = date - datetime.timedelta(days=1)  # TODO: get actual date from titles. maybe not always be 1 day delay
        vaccinations, allocations, vacnew = vaccination_tables(vaccinations, allocations, vacnew, date, page, file)
        vac_daily = vaccination_daily(vac_daily, date, file, page)
        vac_daily = vac_problem(vac_daily, date, file, page)
    df = pd.DataFrame((list(key) + value for key, value in vaccinations.items()), columns=[
        "Date",
        "Province",
        "Vac Given 1 Cum",
        "Vac Given 1 %",
        "Vac Given 2 Cum",
        "Vac Given 2 %",
        "Vac Group Medical Staff 1 Cum",
        "Vac Group Medical Staff 2 Cum",
        "Vac Group Other Frontline Staff 1 Cum",
        "Vac Group Other Frontline Staff 2 Cum",
        "Vac Group Over 60 1 Cum",
        "Vac Group Over 60 2 Cum",
        "Vac Group Risk: Disease 1 Cum",
        "Vac Group Risk: Disease 2 Cum",
        "Vac Group Risk: Location 1 Cum",
        "Vac Group Risk: Location 2 Cum",
    ]).set_index(["Date", "Province"])
    df = df.combine_first(vacct)
    # df_new = pd.DataFrame((list(key)+value for key,value in vacnew.items()), columns=[
    #     "Date",
    #     "Province",
    #     "Vac Given 1",
    #     "Vac Given 2",
    #     "Vac Group Medical Staff 1",
    #     "Vac Group Medical Staff 2",
    #     "Vac Group Other Frontline Staff 1",
    #     "Vac Group Other Frontline Staff 2",
    #     "Vac Group Over 60 1",
    #     "Vac Group Over 60 2",
    #     "Vac Group Risk: Disease 1",
    #     "Vac Group Risk: Disease 2",
    #     "Vac Group Risk: Location 1",
    #     "Vac Group Risk: Location 2",
    # ]).set_index(["Date", "Province"])
    alloc = pd.DataFrame((list(key) + value for key, value in allocations.items()), columns=[
        "Date",
        "Province",
        "Vac Allocated Sinovac 1",
        "Vac Allocated Sinovac 2",
        "Vac Allocated AstraZeneca 1",
        "Vac Allocated AstraZeneca 2",
    ]).set_index(["Date", "Province"])
    all_vac = all_vac.combine_first(df)
    all_vac = all_vac.combine_first(alloc)

    # Do cross check we got the same number of allocations to vaccination
    counts = all_vac.groupby("Date").count()
    missing_data = counts[counts['Vac Allocated AstraZeneca 1'] > counts['Vac Group Risk: Location 2 Cum']]
    # 2021-04-08 2021-04-06 2021-04-05- 03-02 just not enough given yet
    missing_data = missing_data["2021-04-09": "2021-05-03"]
    # 2021-05-02 2021-05-01 - use images for just one table??
    # We will just remove this days
    all_vac = all_vac.drop(index=missing_data.index)
    # After 2021-05-08 they stopped using allocation table. But cum should now always have 77 provinces
    # TODO: only have 76 prov? something going on
    missing_data = counts[counts['Vac Group Risk: Location 2 Cum'] < 76]["2021-05-04":]
    all_vac = all_vac.drop(index=missing_data.index)
    # TODO: parse the daily vaccinations to make up for missing data in cum tables

    # Fix holes in cumulative using any dailys
    # TODO: below is wrong approach. should add daily to cum -1
    # df_daily = df.reset_index().set_index("Date").groupby("Province").apply(cum2daily)
    # df_daily.combine_first(df_new)
    # df_cum = df_daily.groupby("Province").cumsum()
    # df_cum.columns = [f"{c} Cum" for c in df_cum.columns]
    # all_vac = all_vac.combine_first(df_cum)

    export(all_vac, "vaccinations", csv_only=True)

    thaivac = all_vac.groupby("Date").sum()
    thaivac = thaivac.combine_first(vac_daily)
    thaivac.drop(columns=["Vac Given 1 %", "Vac Given 1 %"], inplace=True)

    # Get vaccinations by district
    all_vac = join_provinces(all_vac, "Province")
    given_by_area_1 = area_crosstab(all_vac, 'Vac Given 1', ' Cum')
    given_by_area_2 = area_crosstab(all_vac, 'Vac Given 2', ' Cum')
    given_by_area_both = area_crosstab(all_vac, 'Vac Given', ' Cum')
    thaivac = thaivac.combine_first(given_by_area_1).combine_first(given_by_area_2).combine_first(given_by_area_both)
    export(thaivac, "vac_timeline")

    # TODO: can get todays from - https://ddc.moph.go.th/vaccine-covid19/ or briefings

    # Need to drop any dates that are incomplete.
    # TODO: could keep allocations?
    # thaivac = thaivac.drop(index=missing_data.index)

    # thaivac = thaivac.combine_first(cum2daily(thaivac))
    # thaivac = thaivac.drop([c for c in thaivac.columns if " Cum" in c], axis=1)
    # TODO: remove cumlutive and other stats we don't want

    # TODO: only return some daily summary stats
    return thaivac

################################
# Misc
################################


def get_ifr():
    url = "http://statbbi.nso.go.th/staticreport/Page/sector/EN/report/sector_01_11101_EN_.xlsx"
    file, _ = next(web_files(url, dir="json", check=False))
    pop = pd.read_excel(file, header=3, index_col=1)

    def year_cols(start, end):
        return [f"{i} year" for i in range(start, end)]

    pop['At 0'] = pop[year_cols(1, 10) + ["under 1"]].sum(axis=1)
    pop["At 10"] = pop[year_cols(10, 25)].sum(axis=1)
    pop["At 25"] = pop[year_cols(25, 46) + ["47 year"] + year_cols(47, 54)].sum(axis=1)
    pop["At 55"] = pop[year_cols(55, 65)].sum(axis=1)
    pop["At 65"] = pop[year_cols(65, 73) + ["74 year", "74 year"]].sum(axis=1)
    pop["At 75"] = pop[year_cols(75, 85)].sum(axis=1)
    pop["At 85"] = pop[year_cols(85, 101) + ["101 and over"]].sum(axis=1)
    # from http://epimonitor.net/Covid-IFR-Analysis.htm. Not sure why pd.read_html doesn't work in this case.
    ifr = pd.DataFrame([[.002, .002, .01, .04, 1.4, 4.6, 15]],
                       columns=["At 0", "At 10", "At 25",
                                "At 55", "At 65", "At 75", "At 85"],
                       ).transpose().rename(columns={0: "risk"})
    pop = pop[ifr.index]
    pop = pop.reset_index().dropna().set_index("Province").transpose()
    unpop = pop.reset_index().melt(
        id_vars=['index'],
        var_name='Province',
        value_name='Population'
    ).rename(columns=dict(index="Age"))
    total_pop = unpop.groupby("Province").sum().rename(
        columns=dict(Population="total_pop"))
    unpop = unpop.join(total_pop, on="Province").join(ifr["risk"], on="Age")
    unpop['ifr'] = unpop['Population'] / unpop['total_pop'] * unpop['risk']
    provifr = unpop.groupby("Province").sum()
    provifr = provifr.drop([p for p in provifr.index if "Region" in p] + ['Whole Kingdom'])

    # now normalise the province names
    provifr = join_provinces(provifr, "Province")
    return provifr


def get_hospital_resources():
    print("========ArcGIS==========")

    # PUI + confirmed, recovered etc stats
    fields = [
        'OBJECTID', 'ID', 'agency_code', 'label', 'agency_status', 'status',
        'address', 'province', 'amphoe', 'tambol', 'latitude', 'longitude',
        'level_performance', 'ministryname', 'depart', 'ShareRoom_Total',
        'ShareRoom_Available', 'ShareRoom_Used', 'Private_AIIR_Total',
        'Private_AIIR_Available', 'Private_AIIR_Used',
        'Private_Modified_AIIR_Total', 'Private_Modified_AIIR_Available',
        'Private_Modified_AIIR_Used', 'Private_Isolation_room_Total',
        'Private_Isolation_room_Availabl', 'Private_Isolation_room_Used',
        'Private_Cohort_ward_Total', 'Private_Cohort_ward_Available',
        'Private_Cohort_ward_Used', 'Private_High_Flow_Total',
        'Private_High_Flow_Available', 'Private_High_Flow_Used',
        'Private_OR_negative_pressure_To', 'Private_OR_negative_pressure_Av',
        'Private_OR_negative_pressure_Us', 'Private_ICU_Total',
        'Private_ICU_Available', 'Private_ICU_Used',
        'Private_ARI_clinic_Total', 'Private_ARI_clinic_Available',
        'Private_ARI_clinic_Used', 'Volume_control_Total',
        'Volume_control_Available', 'Volume_control_Used',
        'Pressure_control_Total', 'Pressure_control_Available',
        'Pressure_control_Used', 'Volumecontrol_Child_Total',
        'Volumecontrol_Child_Available', 'Volumecontrol_Child_Used',
        'Ambulance_Total', 'Ambulance_Availble', 'Ambulance_Used',
        'Pills_Favipiravir_Total', 'Pills_Favipiravir_Available',
        'Pills_Favipiravir_Used', 'Pills_Oseltamivir_Total',
        'Pills_Oseltamivir_Available', 'Pills_Oseltamivir_Used',
        'Pills_ChloroquinePhosphate_Tota', 'Pills_ChloroquinePhosphate_Avai',
        'Pills_ChloroquinePhosphate_Used', 'Pills_LopinavirRitonavir_Total',
        'Pills_LopinavirRitonavir_Availa', 'Pills_LopinavirRitonavir_Used',
        'Pills_Darunavir_Total', 'Pills_Darunavir_Available',
        'Pills_Darunavir_Used', 'Lab_PCRTest_Total', 'Lab_PCRTest_Available',
        'Lab_PCRTest_Used', 'Lab_RapidTest_Total', 'Lab_RapidTest_Available',
        'Lab_RapidTest_Used', 'Face_shield_Total', 'Face_shield_Available',
        'Face_shield_Used', 'Cover_all_Total', 'Cover_all_Available',
        'Cover_all_Used', 'ถุงมือไนไตรล์ชนิดใช้', 'ถุงมือไนไตรล์ชนิดใช้_1',
        'ถุงมือไนไตรล์ชนิดใช้_2', 'ถุงมือไนไตรล์ชนิดใช้_3',
        'ถุงมือไนไตรล์ชนิดใช้_4', 'ถุงมือไนไตรล์ชนิดใช้_5',
        'ถุงมือยางชนิดใช้แล้ว', 'ถุงมือยางชนิดใช้แล้ว_1',
        'ถุงมือยางชนิดใช้แล้ว_2', 'ถุงสวมขา_Leg_cover_Total',
        'ถุงสวมขา_Leg_cover_Available', 'ถุงสวมขา_Leg_cover_Used',
        'พลาสติกหุ้มคอ_HOOD_Total', 'พลาสติกหุ้มคอ_HOOD_Available',
        'พลาสติกหุ้มคอ_HOOD_Used', 'พลาสติกหุ้มรองเท้า_Total',
        'พลาสติกหุ้มรองเท้า_Availab', 'พลาสติกหุ้มรองเท้า_Used',
        'แว่นครอบตาแบบใส_Goggles_Total', 'แว่นครอบตาแบบใส_Goggles_Availab',
        'แว่นครอบตาแบบใส_Goggles_Used', 'เสื้อกาวน์ชนิดกันน้ำ_T',
        'เสื้อกาวน์ชนิดกันน้ำ_A', 'เสื้อกาวน์ชนิดกันน้ำ_U',
        'หมวกคลุมผมชนิดใช้แล้', 'หมวกคลุมผมชนิดใช้แล้_1',
        'หมวกคลุมผมชนิดใช้แล้_2', 'เอี๊ยมพลาสติกใส_Apron_Total',
        'เอี๊ยมพลาสติกใส_Apron_Available', 'เอี๊ยมพลาสติกใส_Apron_Used',
        'UTM_Total', 'UTM_Available', 'UTM_Used', 'VTM_Total', 'VTM_Available',
        'VTM_Used', 'Throat_Swab_Total', 'Throat_Swab_Available',
        'Throat_Swab_Used', 'NS_Swab_Total', 'NS_Swab_Available',
        'NS_Swab_Used', 'Surgicalmask_Total', 'Surgicalmask_Available',
        'Surgicalmask_Used', 'N95_Total', 'N95_Available', 'N95_Used',
        'Dr_ChestMedicine_Total', 'Dr_ChestMedicine_Available',
        'Dr_ChestMedicine_Used', 'Dr_ID_Medicine_Total',
        'Dr_ID_Medicine_Availble', 'Dr_ID_Medicine_Used', 'Dr_Medical_Total',
        'Dr_Medical_Available', 'Dr_Medical_Used', 'Nurse_ICN_Total',
        'Nurse_ICN_Available', 'Nurse_ICN_Used', 'Nurse_RN_Total',
        'Nurse_RN_Available', 'Nurse_RN_Used', 'Pharmacist_Total',
        'Pharmacist_Available', 'Pharmacist_Used', 'MedTechnologist_Total',
        'MedTechnologist_Available', 'MedTechnologist_Used', 'Screen_POE',
        'Screen_Walk_in', 'PUI', 'Confirm_mild', 'Confirm_moderate',
        'Confirm_severe', 'Confirm_Recovered', 'Confirm_Death', 'GlobalID',
        'region_health', 'CoverAll_capacity', 'ICU_Covid_capacity',
        'N95_capacity', 'AIIR_room_capacity', 'CoverAll_status',
        'Asymptomatic', 'ICUforCovidTotal', 'ICUforCovidAvailable',
        'ICUforCovidUsed'
    ]
    #    pui =  "https://services8.arcgis.com/241MQ9HtPclWYOzM/arcgis/rest/services/Corona_Date/FeatureServer/0/query?f=json&where=1%3D1&returnGeometry=false&spatialRel=esriSpatialRelIntersects&outFields=*&orderByFields=Date%20asc&resultOffset=0&resultRecordCount=32000&resultType=standard&cacheHint=true" # noqa: E501

    #    icu = "https://services8.arcgis.com/241MQ9HtPclWYOzM/arcgis/rest/services/Hospital_Data_Dashboard/FeatureServer/0/query?f=json&where=1%3D1&returnGeometry=false&spatialRel=esriSpatialRelIntersects&outFields=*&outStatistics=%5B%7B%22statisticType%22%3A%22sum%22%2C%22onStatisticField%22%3A%22Private_ICU_Total%22%2C%22outStatisticFieldName%22%3A%22value%22%7D%5D&resultType=standard&cacheHint=true" # noqa: E501

    rows = []
    for page in range(0, 2000, 1000):
        every_district = f"https://services8.arcgis.com/241MQ9HtPclWYOzM/arcgis/rest/services/Hospital_Data_Dashboard/FeatureServer/0/query?f=json&where=1%3D1&returnGeometry=false&spatialRel=esriSpatialRelIntersects&outFields=*&resultOffset={page}&resultRecordCount=1000&cacheHint=true"  # noqa: E501
        file, content = next(web_files(every_district, dir="json", check=True))
        jcontent = json.loads(content)
        rows.extend([x['attributes'] for x in jcontent['features']])

    data = pd.DataFrame(rows).groupby("province").sum()
    data['Date'] = today().date()
    data['Date'] = pd.to_datetime(data['Date'])
    data = data.reset_index().set_index(["Date", "province"])
    old = import_csv("hospital_resources")
    if old is not None:
        old = old.set_index(["Date", "province"])
        # TODO: seems to be dropping old data. Need to test
        data = add_data(old, data)
    export(data, "hospital_resources", csv_only=True)
    return data


def scrape_and_combine():
    quick = USE_CACHE_DATA and os.path.exists(os.path.join('api', 'combined.csv'))

    print(f'\n\nUSE_CACHE_DATA = {quick}\nCHECK_NEWER = {CHECK_NEWER}\n\n')

    if quick:
        # Comment out what you don't need to run
        vac = get_vaccinations()
        cases_by_area = get_cases_by_area()
        situation = get_situation()
        tests = get_tests_by_day()
        tests_reports = get_test_reports()
        cases = get_cases()
        # slow due to fuzzy join TODO: append to local copy thats already joined or add extra spellings
        cases_demo = get_cases_by_demographics_api()
        pass
    else:
        cases_by_area = get_cases_by_area()
        situation = get_situation()
        cases_demo = get_cases_by_demographics_api()
        # hospital = get_hospital_resources()
        vac = get_vaccinations()
        tests = get_tests_by_day()
        tests_reports = get_test_reports()
        cases = get_cases()

    print("========Combine all data sources==========")
    df = pd.DataFrame(columns=["Date"]).set_index("Date")
    for f in ['cases_by_area', 'cases', 'situation', 'tests_reports', 'tests', 'cases_demo', 'vac']:
        if f in locals():
            df = df.combine_first(locals()[f])
    print(df)

    if quick:
        old = import_csv("combined")
        old = old.set_index("Date")
        df = df.combine_first(old)

        return df
    else:
        export(df, "combined", csv_only=True)
        export(get_fuzzy_provinces(), "fuzzy_provinces", csv_only=True)
        return df


if __name__ == "__main__":

    # does exports
    scrape_and_combine()
