import datetime
import functools
import dateutil
from dateutil.parser import parse as d
from dateutil.relativedelta import relativedelta
from itertools import chain, islice
import json
import os
import re
import copy
import codecs
import shutil
import time

from bs4 import BeautifulSoup
import camelot
import numpy as np
import pandas as pd
import requests
from requests.exceptions import ConnectionError

from utils_pandas import add_data, check_cum, cum2daily, daily2cum, daterange, export, fuzzy_join, import_csv, \
    spread_date_range, cut_ages
from utils_scraping import CHECK_NEWER, MAX_DAYS, USE_CACHE_DATA, any_in, dav_files, fix_timeouts, get_next_number, get_next_numbers, \
    get_tweets_from, pairwise, parse_file, parse_numbers, pptx2chartdata, remove_suffix, replace_matcher, seperate, split, \
    strip, toint, unique_values,\
    web_files, web_links, all_in, NUM_OR_DASH, s, workbooks, worksheet2df
from utils_thai import DISTRICT_RANGE, area_crosstab, file2date, find_date_range, \
    find_thai_date, get_province, join_provinces, parse_gender, to_thaiyear, today,  \
    get_fuzzy_provinces, POS_COLS, TEST_COLS


##########################################
# Situation reports/PUI
##########################################

def situation_cases_cum(parsed_pdf, date):
    prison = None
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
        #if active is not None:
        #    active += prison

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
        [(date, cases, local, imported, quarantine, outside_quarantine, active, prison)],
        columns=["Date", "Cases Cum", "Cases Local Transmission Cum", "Cases Imported Cum",
                 "Cases In Quarantine Cum", "Cases Outside Quarantine Cum", "Cases Proactive Cum", "Cases Area Prison Cum"]
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
            # TODO latest reports have not_pui in same place as active_finding
        else:
            raise Exception(numbers)

        pui = {309371: 313813}.get(pui, pui)  # 2020-07-01
        # TODO: find 1529045 below and see which is correct 20201-04-26
        pui2 = pui if pui2 in [96989, 433807, 3891136, 385860, 326073, 1529045, 2159780, 278178, 2774962] else pui2
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
    assert pui is None or pui > 0, f"Invalid pui situation_en {date}"

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
    for file, _, _ in web_files(*web_links(url, ext=".pdf", dir="situation_en"), dir="situation_en"):
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


def situation_pui_th_death(dfsit, parsed_pdf, date, file):
    if "40 – 59" not in parsed_pdf:
        return dfsit
    numbers, rest = get_next_numbers(parsed_pdf, "15 – 39", until="40 – 59", ints=False)
    if len(numbers) == 3:
        a1_w1, a1_w2, a1_w3 = numbers
        a2_w1, a2_w2, a2_w3 = get_next_numbers(rest, "40 – 59", until=" 60 ", ints=False, return_rest=False)
        a3_w1, a3_w2, a3_w3, *_ = get_next_numbers(rest, " 60 ", ints=False, return_rest=False)
    else:
        # Actually uses 20-39 here but we will assume no one 15-20 died before this changed
        _, rest = get_next_numbers(parsed_pdf, "40 – 59", until=" 60 ", ints=False)
        numbers = get_next_numbers(rest, "2 เดือน 1", " 60 ปีขึ้นไป", ints=False, return_rest=False)
        a1_w1, a2_w1, a3_w1, a1_w2, a2_w2, a3_w2, a1_w3, a2_w3, a3_w3, *_ = numbers
    # else:
    # a3_w3, a2_w3, a1_w3, *_ = get_next_numbers(text,
    #                                            "วหรือภำวะเส่ียง",
    #                                            "หรือสูงอำยุ",
    #                                            "มีโรคประจ",
    #                                            "หรือภำวะเส่ียง",
    #                                            before=True,
    #                                            ints=False,
    #                                            return_rest=False)
    assert 0 <= a3_w3 <= 25
    assert 0 <= a2_w3 <= 25
    assert 0 <= a1_w3 <= 25

    # time to treatment
    numbers = get_next_numbers(
        parsed_pdf,
        "ระยะเวลำเฉล่ียระหว่ำงวันเร่ิมป่วย",
        "ระยะเวลำเฉล่ียระหว่ำงวันเร่ิม",
        "ถึงวันได้รับรักษา",
        ints=False,
        return_rest=False)
    if numbers:
        w1_avg, w1_min, w1_max, w2_avg, w2_min, w2_max, w3_avg, w3_min, w3_max, *_ = numbers
    else:
        # 'situation_th/situation-no598-230864.pdf'
        w3_avg, w3_min, w3_max = [np.nan] * 3
    columns = [
        "Date", "W3 CFR 15-39", "W3 CFR 40-59", "W3 CFR 60-", "W3 Time To Treatment Avg", "W3 Time To Treatment Min",
        "W3 Time To Treatment Max"
    ]
    df = pd.DataFrame([[date, a1_w3, a2_w3, a3_w3, w3_avg, w3_min, w3_max]], columns=columns).set_index("Date")
    print(date.date(), "Death Ages", df.to_string(header=False, index=False))
    return dfsit.combine_first(df)


def situation_pui_th(dfpui, parsed_pdf, date, file):
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
        return dfpui
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
    assert pui is None or pui > 0, f"Invalid pui situation_th {date}"

    cols = ["Tested Cum",
            "Tested PUI Cum",
            "Tested Proactive Cum",
            "Tested Quarantine Cum",
            "Tested PUI Walkin Private Cum",
            "Tested PUI Walkin Public Cum",
            "Tested PUI Walkin Cum"]
    df = pd.DataFrame(
        [(date,) + row],
        columns=["Date"] + cols
    ).set_index("Date")
    assert check_cum(df, dfpui, cols)
    dfpui = dfpui.combine_first(df)
    print(date.date(), file, df.to_string(header=False, index=False))
    return dfpui


def get_thai_situation():
    results = pd.DataFrame(columns=["Date"]).set_index("Date")
    links = web_links(
        "https://ddc.moph.go.th/viralpneumonia/situation.php",
        "https://ddc.moph.go.th/viralpneumonia/situation_more.php",
        ext=".pdf",
        dir="situation_th"
    )
    for file, _, _ in web_files(*links, dir="situation_th"):
        parsed_pdf = parse_file(file, html=False, paged=False)
        if "situation" not in os.path.basename(file):
            continue
        if "Situation Total number of PUI" in parsed_pdf:
            # english report mixed up? - situation-no171-220663.pdf
            continue
        date = file2date(file)
        results = situation_pui_th(results, parsed_pdf, date, file)
        results = situation_pui_th_death(results, parsed_pdf, date, file)

    return results


def get_situation_today():
    _, page, _ = next(web_files("https://ddc.moph.go.th/viralpneumonia/index.php", dir="situation_th", check=True))
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
    assert not any_in(row, None)
    assert pui > 0
    return pd.DataFrame(
        [[date, ] + row],
        columns=["Date", "Cases Cum", "Cases", "Tested PUI Cum", "Tested PUI", "Cases Imported Cum", "Cases Imported"]
    ).set_index("Date")


def get_situation():
    print("========Situation Reports==========")

    today_situation = get_situation_today()
    th_situation = get_thai_situation()
    en_situation = get_en_situation()
    situation = import_csv("situation_reports", ["Date"],
                           not USE_CACHE_DATA).combine_first(th_situation).combine_first(en_situation)

    cum = cum2daily(situation)

    situation = situation.combine_first(cum)  # any direct non-cum are trusted more

    # TODO: Not sure but 5 days have 0 PUI. Take them out for now
    # Date
    # 2020-02-12    0.0
    # 2020-02-14    0.0
    # 2020-10-13    0.0
    # 2020-12-29    0.0
    # 2021-05-02    0.0
    situation['Tested PUI'] = situation['Tested PUI'].replace(0, np.nan)

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
        file, text, url = next(
            web_files("https://covid19.th-stat.com/json/covid19v2/getTimeline.json", dir="json", check=True))
    except ConnectionError:
        # I think we have all this data covered by other sources. It's a little unreliable.
        return pd.DataFrame()
    data = pd.DataFrame(json.loads(text)['Data'])
    data['Date'] = pd.to_datetime(data['Date'])
    data = data.set_index("Date")
    cases = data[["NewConfirmed", "NewDeaths", "NewRecovered", "Hospitalized"]]
    cases = cases.rename(columns=dict(NewConfirmed="Cases", NewDeaths="Deaths", NewRecovered="Recovered"))
    cases["Source Cases"] = url
    return cases


@functools.lru_cache(maxsize=100, typed=False)
def get_case_details_csv():
    if False:
        return get_case_details_api()
    url = "https://data.go.th/dataset/covid-19-daily"
    file, text, _ = next(web_files(url, dir="json", check=True))
    data = re.search(r"packageApp\.value\('meta',([^;]+)\);", text.decode("utf8")).group(1)
    apis = json.loads(data)
    links = [api['url'] for api in apis if "รายงานจำนวนผู้ติดเชื้อ COVID-19 ประจำวัน" in api['name']]
    # get earlier one first
    links = sorted([link for link in links if '.php' not in link and '.xlsx' not in link], reverse=True)
    # 'https://data.go.th/dataset/8a956917-436d-4afd-a2d4-59e4dd8e906e/resource/be19a8ad-ab48-4081-b04a-8035b5b2b8d6/download/confirmed-cases.csv'
    cases = pd.DataFrame()
    for file, _, _ in web_files(*links, dir="json", check=True, strip_version=True, appending=True):
        if file.endswith(".xlsx"):
            continue
            #cases = pd.read_excel(file)
        elif file.endswith(".csv"):
            confirmedcases = pd.read_csv(file)
            if '�' in confirmedcases.loc[0]['risk']:
                # bad encoding
                with codecs.open(file, encoding="tis-620") as fp:
                    confirmedcases = pd.read_csv(fp)
            cases = cases.combine_first(confirmedcases.set_index("No."))
        else:
            raise Exception(f"Unknown filetype for covid19daily {file}")
    cases = cases.reset_index("No.")
    cases['announce_date'] = pd.to_datetime(cases['announce_date'], dayfirst=True)
    cases['Notified date'] = pd.to_datetime(cases['Notified date'], dayfirst=True, errors="coerce")
    cases = cases.rename(columns=dict(announce_date="Date")).set_index("Date")
    cases['age'] = pd.to_numeric(cases['age'], downcast="integer", errors="coerce")
    #assert cases.index.max() <
    print("Covid19daily: ", file, cases.index.max())
    return cases


def get_case_details_api():
    rid = "67d43695-8626-45ad-9094-dabc374925ab"
    chunk = 10000
    url = f"https://data.go.th/api/3/action/datastore_search?resource_id={rid}&limit={chunk}&q=&offset="
    records = []

    cases = import_csv("covid-19", ["_id"], dir="json")
    lastid = cases.last_valid_index() if cases.last_valid_index() else 0
    data = None
    while data is None or len(data) == chunk:
        r = s.get(f"{url}{lastid}")
        data = json.loads(r.content)['result']['records']
        df = pd.DataFrame(data)
        df['announce_date'] = pd.to_datetime(df['announce_date'], dayfirst=True)
        df['Notified date'] = pd.to_datetime(df['Notified date'], dayfirst=True, errors="coerce")
        df = df.rename(columns=dict(announce_date="Date"))
        # df['age'] = pd.to_numeric(df['age'], downcast="integer", errors="coerce")
        cases = cases.combine_first(df.set_index("_id"))
        lastid += chunk - 1
    export(cases, "covid-19", csv_only=True, dir="json")
    cases = cases.set_index("Date")
    print("Covid19daily: ", "covid-19", cases.last_valid_index())

    # # they screwed up the date conversion. d and m switched sometimes
    # # TODO: bit slow. is there way to do this in pandas?
    # for record in records:
    #     record['announce_date'] = to_switching_date(record['announce_date'])
    #     record['Notified date'] = to_switching_date(record['Notified date'])
    # cases = pd.DataFrame(records)
    return cases


@functools.lru_cache(maxsize=100, typed=False)
def get_cases_by_demographics_api():
    print("========Covid19Daily Demographics==========")

    cases = get_case_details_csv().reset_index()
    age_groups = cut_ages(cases, ages=[10, 20, 30, 40, 50, 60, 70], age_col="age", group_col="Age Group")
    case_ages = pd.crosstab(age_groups['Date'], age_groups['Age Group'])
    case_ages.columns = [f"Cases Age {a}" for a in case_ages.columns.tolist()]

    #labels2 = ["Age 0-14", "Age 15-39", "Age 40-59", "Age 60-"]
    #age_groups2 = pd.cut(cases['age'], bins=[0, 14, 39, 59, np.inf], right=True, labels=labels2)
    age_groups2 = cut_ages(cases, ages=[15, 40, 60], age_col="age", group_col="Age Group")
    case_ages2 = pd.crosstab(age_groups2['Date'], age_groups2['Age Group'])
    case_ages2.columns = [f"Cases Age {a}" for a in case_ages2.columns.tolist()]

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
        45: 'Cluster ตราด :Community',  # Trat?
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
        20210531.6: 'Cluster กรุงเทพมหานคร. คลองเตย:Community',
        20210622.0: 'อยู่ระหว่างการสอบสวน\n:Investigating',
        20210622.1: 'Cluster ตราด:Community',
        20210622.2: "สัมผัสผู้ป่วยยืนยัน \n อยู่ระหว่างสอบสวน:Contact",
        20210622.3: "ผู้เดินทางมาจากพื้นที่เสี่ยง กรุงเทพมหานคร.:Community",
        20210622.4: "อาศัย/เดินทางไปในพื้นที่ที่มีการระบาด:Community",
        20210622.5: "อยุ่ระหว่างสอบสวน:Unknown",
        20210622.6: "สัมผัสผู้ป่วยยืนยัน อยุ๋ระหว่างสอบสวน:Contact",
        20210622.7: "สัมผัสผู้ติดเชื้อยืนยัน\nอยู่ระหว่างสอบสวน:Contact",
        20210622.8: "ระหว่างการสอบสวนโรค:Investigating",
        20210622.9: "ปอดอักเสบ Pneumonia:Pneumonia",
        20210622.01: "Cluster ตลาดบางแค:Community",
        20210622.11: "คนไทยเดินทางมาจากต่างประเทศ:Imported",
        20210622.12: "คนไทยมาจากพื้นที่เสี่ยง:Community",
        20210622.13: "cluster ชลบุรี\n(อยู่ระหว่างการสอบสวน):Investigating",
        20210622.14: "Cluster โรงงาน  Big Star:Work",
        20210622.15: "Cluster สมุทรปราการ ตลาดเคหะบางพลี:Work",
        20210622.16: "Cluster ระยอง วิริยะประกันภัย:Work",
        20210622.17: "Cluster ตลาดบางแค/คลองขวาง:Work",
        20210622.18: "เดินทางมาจากพื้นที่มีการระบาดของโรค:Community",
        20210622.19: "Cluster งานมอเตอร์ โชว์:Community",
        20210622.02: "ทัณฑสถาน/เรือนจำ:Prison",
        20210622.21: "สถานที่ทำงาน:Work",
        20210622.22: "รอประสาน:Unknown",
        20210622.23: "ผู้ติดเชื้อในประเทศ:Contact",
        20210622.24: "ค้นหาเชิงรุก:Proactive Search",
        20210622.25: "Cluster ทัณฑสถานโรงพยาบาลราชทัณฑ์:Prison",
        20210622.26: "2.สัมผัสผู้ติดเชื้อ:Contact",
        20210622.27: "Cluster ระยอง:Community",
        20210622.28: "ตรวจสุขภาพแรงงานต่างด้าว:Work",
        20210622.29: "สัมผัสในสถานพยาบาล:Work",  # contact in hospital
        20210622.03: "ไปเที่ยวสถานบันเทิงในอุบลที่พบการระบาดของโรค Ubar:Entertainment",
        20210622.31: "ไปสถานที่เสี่ยง เช่น ตลาด สถานที่ชุมชน:Community",
        20210622.32: "Cluster ทัณฑสถานหญิงกลาง:Prison",
        20210622.33: "ACF สนามกีฬาไทย-ญี่ปุ่น:Entertainment",
        20210622.34: "ACF สีลม:Entertainment",
        20210622.35: "ACF รองเมือง:Entertainment",
        20210622.36: "ACF สนามกีฬาธูปะเตมีย์:Entertainment",
        20210622.37: "Cluster ห้างแสงทอง (สายล่าง):Community",
        20210622.38: "Cluster ทันฑสถานบำบัดพิเศษกลาง:Community",
        20210714.01: "Sandbox:Imported",
        20210731.01: "Samui plus:Imported",
        20210731.02: "ACF เคหะหลักสี่:Work",
        20210731.03: "เดินทางมาจากพื้นที่เสี่ยงที่มีการระบาดของโรค:Community",
        20210806.01: "ท้ายบ้าน:Unknown",
        20210806.02: "อื่นๆ:Unknown",  # Other
    }
    for v in r.values():
        key, cat = v.split(":")
        risks[key] = cat
    risks = pd.DataFrame(risks.items(), columns=[
                         "risk", "risk_group"]).set_index("risk")
    cases_risks, unmatched = fuzzy_join(cases, risks, on="risk", return_unmatched=True)

    # dump mappings to file so can be inspected
    matched = cases_risks[["risk", "risk_group"]]
    export(matched.value_counts().to_frame("count"), "risk_groups", csv_only=True)
    export(unmatched, "risk_groups_unmatched", csv_only=True)

    case_risks_daily = pd.crosstab(cases_risks['Date'], cases_risks["risk_group"])
    case_risks_daily.columns = [f"Risk: {x}" for x in case_risks_daily.columns]

    cases_risks['Province'] = cases_risks['province_of_onset']
    risks_prov = join_provinces(cases_risks, 'Province')
    risks_prov = risks_prov.value_counts(['Date', "Province", "risk_group"]).to_frame("Cases")
    risks_prov = risks_prov.reset_index()
    risks_prov = pd.crosstab(index=[risks_prov['Date'], risks_prov['Province']],
                             columns=risks_prov["risk_group"],
                             values=risks_prov['Cases'],
                             aggfunc="sum")
    risks_prov.columns = [f"Cases Risk: {c}" for c in risks_prov.columns]

    return case_risks_daily.combine_first(case_ages).combine_first(case_ages2), risks_prov


########################
# Dashboards
########################


def moph_dashboard():

    def skip_func(df, allow_na={}):
        def is_done(idx_value):
            if type(idx_value) == tuple:
                date, prov = idx_value
                prov = get_province(prov)
                idx_value = (date, prov)
            else:
                date = idx_value
            # Assume index of df is in the same order as params
            if df.empty:
                return False
            # allow certain fields null if before set date
            nulls = [c for c in df.columns if pd.isna(df[c].get(idx_value)) and date >= allow_na.get(c, d("1975-1-1"))]
            if not nulls:
                return True
            else:
                print(date, "MOPH Dashboard", f"Retry Missing data at {idx_value} for {nulls}. Retry")
                return False
        return is_done

    def getDailyStats(df):

        # def workbooks(df, allow_na={}, **params):
        #     ts = TS()
        #     ts.loads(url)
        #     fix_timeouts(ts.session, timeout=15)
        #     workbook = ts.getWorkbook()
        #     updated = workbook.getWorksheet("D_UpdateTime").data['max_update_date-alias'][0]
        #     updated = pd.to_datetime(updated, dayfirst=False)
        #     yield workbook, updated
        #     start = d("2021-01-01")
        #     for date in reversed(list(daterange(start, updated))):
        #         if not df.empty:
        #             # allow certain fields null if before set date
        #             nulls = [c for c in df.columns if pd.isna(df[c].get(date)) and date >= allow_na.get(c, start)]
        #             if not nulls:
        #                 continue
        #             else:
        #                 print(date, "MOPH Dashboard", f"Retry Missing data for {nulls}. Retry")
        #         try:
        #             yield setParamater(workbook, "param_date", str(date.date())), date
        #         except requests.exceptions.ReadTimeout:
        #             print(date, "MOPH Dashboard", "Timeout Error. Continue another day")
        #             break

        allow_na = {
            "ATK": d("2021-07-31"),
            "Cases Area Prison": d("2021-05-12"),
            "Tests": d("2021-07-05"),
            'Hospitalized Field HICI': d("2021-08-08"),
            'Hospitalized Field Hospitel': d("2021-08-08"),
            'Hospitalized Field Other': d("2021-08-08"),
            'Vac Given 1 Cum': d("2021-01-11"),
            'Vac Given 2 Cum': d("2021-01-11"),
            "Vac Given 3 Cum": d("2021-06-01"),
            'Hospitalized Field': d('2021-04-01'),
            'Hospitalized Respirator': d("2021-03-25"),  # patchy before this
            'Hospitalized Severe': d("2021-03-25"),
            'Hospitalized Hospital': d("2021-01-23"),
        }
        url = "https://public.tableau.com/views/SATCOVIDDashboard/1-dash-tiles-w"
        # new day starts with new info comes in
        dates = reversed(pd.date_range("2021-06-01", today() - relativedelta(hours=7)).to_pydatetime())
        for wb, date in workbooks(url, skip_func(df, allow_na), dates=dates):
            row = worksheet2df(
                wb,
                date,
                D_New="Cases",
                D_Walkin="Cases Walkin",
                D_Proact="Cases Proactive",
                D_NonThai="Cases Imported",
                D_Prison="Cases Area Prison",
                D_Hospital="Hospitalized Hospital",
                D_Severe="Hospitalized Severe",
                D_SevereTube="Hospitalized Respirator",
                D_Medic="Hospitalized",
                D_Recov="Recovered",
                D_Death="Deaths",
                D_ATK="ATK",
                D_Lab2={
                    "SUM(cnt_ma)-value": "Tests",
                    "DAY(txn_date)-value": "Date"
                },
                D_NewTL={
                    "SUM(case_new)-value": "Cases",
                    "DAY(txn_date)-value": "Date"
                },
                D_DeathTL={
                    "SUM(death_new)-value": "Deaths",
                    "DAY(txn_date)-value": "Date"
                },
                D_Vac_Stack={
                    "DAY(txn_date)-value": "Date",
                    "vaccine_plan_group-alias": {
                        "1": "1 Cum",
                        "2": "2 Cum",
                        "3": "3 Cum",
                    },
                    "SUM(vaccine_total_acm)-value": "Vac Given",
                },
                D_HospitalField="Hospitalized Field",
                D_Hospitel="Hospitalized Field Hospitel",
                D_HICI="Hospitalized Field HICI",
                D_HFieldOth="Hospitalized Field Other",
                D_RecovL={
                    "DAY(txn_date)-value": "Date",
                    "SUM(recovered_new)-value": "Recovered"
                }

            )

            if row.empty:
                break
            row["Source Cases"] = "https://ddc.moph.go.th/covid19-dashboard/index.php?dashboard=main"
            df = row.combine_first(df)  # prefer any updated info that might come in. Only applies to backdated series though
            print(date, "MOPH Dashboard", row.loc[row.last_valid_index():].to_string(index=False, header=False))
        # We get negative valus for field hosoutal before april
        df.loc[:"2021-03-31", 'Hospitalized Field'] = np.nan
        return df

    def getTimelines(df):
        # Get deaths by prov, date. and other stats - timeline
        #
        # ['< 10 ปี', '10-19 ปี', '20-29 ปี', '30-39 ปี', '40-49 ปี', '50-59 ปี', '60-69 ปี', '>= 70 ปี', 'ไม่ระบุ']

        # D4_TREND
        # cases = AGG(stat_count)-alias
        # severe = AGG(ผู้ติดเชื้อรายใหม่เชิงรุก)-alias,
        # deaths = AGG(ผู้เสียชีวิต)-alias (and AGG(ผู้เสียชีวิต (รวมทุกกลุ่มผู้ป่วย))-value  all patient groups)
        # cum cases = AGG(stat_accum)-alias
        # date  = DAY(date)-alias, DAY(date)-value
        url = "https://ddc.moph.go.th/covid19-dashboard/index.php?dashboard=select-trend-line"
        url = "https://dvis3.ddc.moph.go.th/t/sat-covid/views/SATCOVIDDashboard/4-dash-trend-w"
        for wb, idx_value in workbooks(url, lambda idx: False, dates=[], D4_CHART="age_range"):
            row = worksheet2df(
                wb,
                None,
                D4_TREND={
                    "DAY(date)-value": "Date",
                    "AGG(ผู้เสียชีวิต (รวมทุกกลุ่มผู้ป่วย))-value": "Deaths",
                    "AGG(stat_count)-alias": "Cases",
                    "AGG(ผู้ติดเชื้อรายใหม่เชิงรุก)-alias": "Hospitalized Severe",
                },
            )
            _, age_group = idx_value
            if not age_group:
                # TODO: get rid of this first workbook when iterating selects
                continue
            age_group = age_group.replace(" ปี", "").replace('ไม่ระบุ', "Unknown")
            if row.empty:
                continue
            row['Age'] = age_group
            row = row.pivot(values=["Deaths", "Cases", "Hospitalized Severe"], columns="Age")
            row.columns = [f"{n} Age {v}" for n, v in row.columns]
            row = row.rename(columns={a: a.replace(">= 70", "70+").replace("< 10", "0-9") for a in row.columns})
            df = row.combine_first(df)
            print(row.last_valid_index(), "MOPH Ages", age_group,
                  row.loc[row.last_valid_index():].to_string(index=False, header=False))
        df = df.loc[:, ~df.columns.duplicated()]  # remove duplicate columns
        return df

    def by_province(df):
        url = "https://public.tableau.com/views/SATCOVIDDashboard/2-dash-tiles-province-w"

        #selectable items - D2_ProvinceBar
        #    province : ['แม่ฮ่องสอน', 'พังงา', 'กระบี่', 'ลำพูน', 'ตราด', 'สตูล', 'พัทลุง', 'พะเยา', 'พิษณุโลก', 'แพร่'] ...
        #    AGG(measure_analyze) : [1, 14, 17, 17, 21, 28, 32, 41, 44, 45] ...
        # parameters [{'column': 'param_acm', 'values': ['วันที่เลือก', 'ค่าสะสมถึงวันที่เลือก'], 'parameterName': '[Parameters].[Parameter 9]'}]
        allow_na = {
            "Tests": today(),  # TODO: because they are 2 days late so need to say allow after instead of before?
            "Vac Given 3 Cum": d("2021-06-01"),
        }

        dates = reversed(pd.date_range("2021-08-01", today() - relativedelta(hours=7)).to_pydatetime())
        for wb, idx_value in workbooks(url, skip_func(df, allow_na), dates=dates, D2_Province="province"):
            date, province = idx_value
            row = worksheet2df(
                wb,
                date,
                D2_Vac_Stack={
                    "DAY(txn_date)-value": "Date",
                    "vaccine_plan_group-alias": {
                        "1": "1 Cum",
                        "2": "2 Cum",
                        "3": "3 Cum",
                    },
                    "SUM(vaccine_total_acm)-value": "Vac Given",
                },
                D2_Walkin="Cases Walkin",
                D2_Proact="Cases Proactive",
                D2_Prison="Cases Area Prison",
                D2_NonThai="Cases Imported",
                D2_New="Cases",
                D2_NewTL={
                    "AGG(stat_count)-alias": "Cases",
                    "DAY(txn_date)-value": "Date"
                },
                D2_Lab2={
                    "SUM(cnt_ma)-value": "Tests",
                    "DAY(txn_date)-value": "Date"
                },
                D2_Death="Deaths",
                D2_DeathTL={
                    "AGG(num_death)-value": "Deaths",
                    "DAY(txn_date)-value": "Date"
                },
            )
            if province is not None:
                row['Province'] = get_province(province)
                df = row.reset_index("Date").set_index(["Date", "Province"]).combine_first(df)
                print(date.date(), "MOPH Dashboard", row.loc[row.last_valid_index():].to_string(index=False, header=False))

        return df

    # all province case numbers in one go
    url = "https://ddc.moph.go.th/covid19-dashboard/index.php?dashboard=scoreboard"

    # 5 kinds cases stats for last 30 days
    url = "https://ddc.moph.go.th/covid19-dashboard/index.php?dashboard=30-days"

    daily = import_csv("moph_dashboard", ["Date"], False, dir="json")  # so we cache it
    daily = getDailyStats(daily)
    export(daily, "moph_dashboard", csv_only=True, dir="json")
    shutil.copy(os.path.join("json", "moph_dashboard.csv"), "api")  # "json" for caching, api so it's downloadable

    ages = import_csv("moph_dashboard_ages", ["Date"], False, dir="json")  # so we cache it
    ages = getTimelines(ages)
    export(ages, "moph_dashboard_ages", csv_only=True, dir="json")
    shutil.copy(os.path.join("json", "moph_dashboard_ages.csv"), "api")  # "json" for caching, api so it's downloadable

    dfprov = import_csv("moph_dashboard_prov", ["Date", "Province"], False, dir="json")  # so we cache it
    dfprov = by_province(dfprov)
    export(dfprov, "moph_dashboard_prov", csv_only=True, dir="json")
    shutil.copy(os.path.join("json", "moph_dashboard_prov.csv"), "api")  # "json" for caching, api so it's downloadable

    daily = daily.combine_first(ages)
    return daily, dfprov


########################
# Excess Deaths
########################

def excess_deaths():
    url = "https://stat.bora.dopa.go.th/stat/statnew/connectSAPI/stat_forward.php?"
    url += "API=/api/stattranall/v1/statdeath/list?action=73"
    url += "&statType=-1&statSubType=999&subType=99"
    rows = []
    provinces = pd.read_csv('province_mapping.csv', header=0)
    index = ["Year", "Month", "Province", "Gender", "Age"]
    df = import_csv("deaths_all", index, date_cols=[], dir="json")
    counts = df.reset_index(["Gender", "Age"]).groupby(["Year", "Month"]).count()
    if df.empty:
        lyear, lmonth = 2015, 0
    else:
        lyear, lmonth, lprov, lage, lgender = df.last_valid_index()
    done = False
    changed = False
    for year in range(2012, 2025):
        for month in range(1, 13):
            if done:
                break
            if counts.Age.get((year, month), 0) >= 77 * 102 * 2:
                continue
            date = datetime.datetime(year=year, month=month, day=1)
            print("Excess Deaths:", f"missing {year}-{month}")
            for prov, iso in provinces[["Name", "ISO[7]"]].itertuples(index=False):
                if iso is None or type(iso) != str:
                    continue
                dateth = f"{to_thaiyear(year, short=True)}{month:02}"
                print(".", end="")
                apiurl = f"{url}&yymmBegin={dateth}&yymmEnd={dateth}&cc={iso[3:]}"
                res = s.get(apiurl)
                data = json.loads(res.content)
                if len(data) != 2:
                    # data not found
                    if date < today() - relativedelta(months=1):
                        # Error in specific past data
                        print("Excess Deaths:", f"Error getting {prov}", apiurl, str(data))
                        continue
                    else:
                        # This months data not yet available
                        print("Excess Deaths:", f"Error in {year}-{month}")
                        done = True
                        break
                changed = True
                for sex, numbers in zip(["male", "female"], data):
                    total = numbers.get("lsSumTotTot")
                    thisrows = [[year, month, prov, sex, age, numbers.get(f"lsAge{age}")] for age in range(0, 102)]
                    assert total == sum([r[-1] for r in thisrows])
                    assert numbers.get("lsAge102") is None
                    rows.extend(thisrows)
            print()
    df = df.combine_first(pd.DataFrame(rows, columns=index + ["Deaths"]).set_index(index))
    if changed:
        export(df, "deaths_all", csv_only=True, dir="json")
        shutil.copy(os.path.join("json", "deaths_all.csv"), "api")  # "json" for cachine, api so it's downloadable

    return df


##################################
# RB Tweet Parsing
##################################

UNOFFICIAL_TWEET = re.compile("Full details at 12:30pm.*#COVID19")
OFFICIAL_TWEET = re.compile("#COVID19 update")
MOPH_TWEET = re.compile("🇹🇭 ยอดผู้ติดเชื้อโควิด-19")


def parse_official_tweet(df, date, text, url):
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
        "Source Cases"
    ]
    row = [date, imported, local, cases, deaths]
    row2 = row + [hospitalised, recovered]
    if date <= d("2021-05-01").date():
        assert not any_in(row, None), f"{date} Missing data in Official Tweet {row}"
    else:
        assert not any_in(row2, None), f"{date} Missing data in Official Tweet {row}"
    row_opt = row2 + [serious, vent, url]
    tdf = pd.DataFrame([row_opt], columns=cols).set_index("Date")
    print(date, "Official:", tdf.to_string(index=False, header=False))
    return df.combine_first(tdf)


def parse_unofficial_tweet(df, date, text, url):
    if not UNOFFICIAL_TWEET.search(text):
        return df
    deaths, _ = get_next_number(text, "deaths", before=True)
    cases, _ = get_next_number(text, "cases", before=True)
    prisons, _ = get_next_number(text, "prisons", before=True)
    if any_in([None], deaths, cases):
        # raise Exception(f"Can't parse tweet {date} {text}")
        return df
    cols = ["Date", "Deaths", "Cases", "Cases Area Prison", "Source Cases"]
    row = [date, deaths, cases, prisons, url]
    tdf = pd.DataFrame([row], columns=cols).set_index("Date")
    print(date, "Breaking:", tdf.to_string(index=False, header=False))
    return df.combine_first(tdf)


def parse_moph_tweet(df, date, text, url):
    cases, _ = get_next_number(text, "รวม", "ติดเชื้อใหม่", until="ราย")
    prisons, _ = get_next_number(text, "ที่ต้องขัง", "ในเรือนจำ", until="ราย")
    recovered, _ = get_next_number(text, "หายป่วย", "หายป่วยกลับบ้าน", until="ราย")
    deaths, _ = get_next_number(text, "เสียชีวิต", "เสียชีวิต", until="ราย")

    if any_in([None], deaths, cases):
        raise Exception(f"Can't parse tweet {date} {text}")
    numbers, _ = get_next_numbers(text, "ราย", until="ตั้งแต่")  # TODO: test len to make sure we didn't miss something

    if any_in([None], prisons, recovered):
        pass
    cols = ["Date", "Deaths", "Cases", "Cases Area Prison", "Recovered", "Source Cases"]
    row = [date, deaths, cases, prisons, recovered, url]
    tdf = pd.DataFrame([row], columns=cols).set_index("Date")
    print(date, "Moph:", tdf.to_string(index=False, header=False))
    return df.combine_first(tdf)


def parse_case_prov_tweet(walkins, proactive, date, text, url):
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
    # new = get_tweets_from(531202184, d("2021-04-03"), None, OFFICIAL_TWEET, "📍")
    new = get_tweets_from(531202184, d("2021-06-06"), None, OFFICIAL_TWEET, "📍")
    # old = get_tweets_from(72888855, d("2021-01-14"), d("2021-04-02"), "Official #COVID19 update", "📍")
    # old = get_tweets_from(72888855, d("2021-02-21"), None, OFFICIAL_TWEET, "📍")
    old = get_tweets_from(72888855, d("2021-05-21"), None, OFFICIAL_TWEET, "📍")
    # unofficial = get_tweets_from(531202184, d("2021-04-03"), None, UNOFFICIAL_TWEET)
    unofficial = get_tweets_from(531202184, d("2021-06-06"), None, UNOFFICIAL_TWEET)
    thaimoph = get_tweets_from(2789900497, d("2021-06-18"), None, MOPH_TWEET)
    officials = {}
    provs = {}
    breaking = {}
    for date, tweets in list(new.items()) + list(old.items()):
        for tweet, url in tweets:
            if "RT @RichardBarrow" in tweet:
                continue
            if OFFICIAL_TWEET.search(tweet):
                officials[date] = tweet, url
            elif "👉" in tweet and "📍" in tweet:
                if tweet in provs.get(date, ""):
                    continue
                provs[date] = provs.get(date, "") + " " + tweet
    for date, tweets in unofficial.items():
        for tweet, url in tweets:
            if UNOFFICIAL_TWEET.search(tweet):
                breaking[date] = tweet, url

    # Get imported vs walkin totals
    df = pd.DataFrame()

    for date, tweets in sorted(thaimoph.items(), reverse=True):
        for tweet, url in tweets:
            df = df.pipe(parse_moph_tweet, date, tweet, url)

    for date, tweet in sorted(officials.items(), reverse=True):
        text, url = tweet
        df = df.pipe(parse_official_tweet, date, text, url)

    for date, tweet in sorted(breaking.items(), reverse=True):
        text, url = tweet
        if date in officials:
            # do unoffical tweets if no official tweet
            continue
        df = df.pipe(parse_unofficial_tweet, date, text, url)

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


def briefing_case_types(date, pages, url):
    rows = []
    vac_rows = []
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
            domestic = get_next_number(rest, "ในประเทศ", return_rest=False, until="ราย")
            if domestic:
                assert domestic <= cases
                assert domestic == walkins + proactive
            quarantine = quarantine[0] if quarantine else 0
            ports, _ = get_next_number(
                text,
                "ช่องเส้นทางธรรมชาติ",
                "รายผู้ที่เดินทางมาจากต่างประเทศ",
                before=True,
                default=0
            )
            imported = ports + quarantine
            prison, _ = get_next_number(text.split("รวม")[1], "ที่ต้องขัง", default=0, until="ราย")
            cases2 = get_next_number(rest, r"\+", return_rest=False, until="ราย")
            if cases2 is not None and cases2 != cases:
                # Total cases moved to the bottom
                # cases == domestic
                cases = cases2
                assert cases == domestic + imported + prison
        # proactive += prison  # not sure if they are going to add this category going forward?

        assert cases == walkins + proactive + imported + prison, f"{date}: briefing case types don't match"

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
            url,
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
        "Source Cases",
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
        if not re.search(r"ที่\s*จังหวัด", text):
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


def briefing_deaths_provinces(dtext, date, total_deaths, file):
    bullets_re = re.compile(r"(•[^\(]*?\( ?\d+ ?\)(?:[\n ]*\([^\)]+\))?)\n?")

    # get rid of extra words in brakets to make easier
    text = re.sub(r"\b(ละ|จังหวัด|จังหวัด|อย่างละ|ราย)\b", " ", dtext)

    # remove age breakdown of deaths per provice to make it easier
    # e.g "60+ปี 58 ราย (85%)" - from 2021-08-24
    text = re.sub(r"([\d-]+\+?\s?ปี? *\d+ *(ราย)? *\(\d+%\))", " ", text)
    # and '50+ (14)' 2021-08-26
    text = re.sub(r"([\d]+\+? *\(\d+\))", " ", text)

    # remove the table header and page title.
    *pre, table_content = re.split(r"(?:โควิด[ \n-]*19\n\n|รวม\s*\(\s+\))", text, 1)

    # Provinces are split between bullets with disease and risk. Normally bangkok first line above and rest below
    ptext1, b1, rest_bullets = bullets_re.split(table_content, 1)
    if "หญิง" in rest_bullets:  # new format on 2021-08-09 - no gender and prov no longer shoved in the middle.
        rest_bullets2, gender = re.split("• *(?:หญิง|ชาย)", b1 + rest_bullets, 1)
        *bullets, ptext2 = bullets_re.split(rest_bullets2)
        ptext2, *age_text = re.split("•", ptext2, 1)
    else:
        ptext2 = ""
    ptext = ptext1 + ptext2
    # Now we have text that just contains provinces and numbers
    # but could have subtotals. Get each word + number (or 2 number) combo
    pcells = pairwise(strip(re.split(r"(\(?\d+\)?\s*\d*)", ptext)))

    province_count = {}
    last_provs = None

    def add_deaths(provinces, num):
        provs_thai = [p.strip("() ") for p in provinces.split() if len(p) > 1 and p.strip("() ")]
        provs = [pr for p in provs_thai for pr in get_province(p, ignore_error=True, cutoff=0.76, split=True)]

        # TODO: unknown from another cell get in there. Work out how to remove it a better way
        provs = [p for p in provs if p and p != "Unknown"]
        for p in provs:
            province_count[p] = province_count.get(p, 0) + num

    for provinces, num_text in pcells:
        # len() < 2 because some stray modifier?
        text_num, rest = get_next_number(provinces, remove=True)
        num, _ = get_next_number(num_text)
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
    msg = f"in {file} only found {dfprov['Deaths'].sum()}/{total_deaths} from {dtext}\n{pcells}"
    assert total_deaths == dfprov['Deaths'].sum() or date in [d("2021-07-20")], msg
    return dfprov


def briefing_deaths_summary(text, date, file):
    title_re = re.compile(r"(ผูป่้วยโรคโควดิ-19|ผู้ป่วยโรคโควิด-19) (เสยีชวีติ|เสียชีวิต) (ของประเทศไทย|ของประเทศไทย) (รายงานวันที่|รายงานวนัที่)")  # noqa
    if not title_re.search(text):
        return pd.DataFrame(), pd.DataFrame()
    # Summary of locations, reasons, medium age, etc

    # Congenital disease / risk factor The severity of the disease
    # congenital_disease = df[2][0]  # TODO: parse?
    # Risk factors for COVID-19 infection
    # risk_factors = df[3][0]
    numbers, *_ = get_next_numbers(text,
                                   "ค่ามัธยฐานของอา",
                                   "ค่ากลางขอ(?:งอ)?ายุ",
                                   "ามัธยฐานอายุ",
                                   "• ค่ากลาง",
                                   "ค่ากลางอาย ุ",
                                   ints=False)
    med_age, min_age, max_age, *_ = numbers

    genders = get_next_numbers(text, "(หญิง|ชาย)", return_rest=False)
    if genders and date == d("2021-08-09"):
        male, female, *_ = genders
        if get_next_numbers(text, "ชาย", return_rest=False)[0] == female:
            # They sometimes reorder them
            male, female = female, male
        assert male + female == deaths_title
    else:
        male, female = None, None

    numbers, *_ = get_next_numbers(text, "ค่ากลางระยะเวลา")
    if numbers:
        period_death_med, period_death_max, *_ = numbers

    title_num, _ = get_next_numbers(text, title_re)
    day, year, deaths_title, *_ = title_num

    text = re.sub(r"([\d]+wk)", "", text)  # remove 20wk pregnant
    diseases = {
        "Hypertension": ["ความดันโลหิตสูง", "HT", "ความดันโลหิตสงู"],
        "Diabetes": ["เบาหวาน", "DM"],
        "Hyperlipidemia": ["ไขมันในเลือดสูง", "HPL"],
        "Lung disease": ["โรคปอด"],
        "Obesity": ["โรคอ้วน", "อ้วน"],
        "Cerebrovascular": ["หลอดเลือดสมอง"],
        "Kidney disease": ["โรคไต"],
        "Heart disease": ["โรคหัวใจ"],
        "Bedridden": ["ติดเตียง"],
        "Pregnant": ["ตั้งครรภ์"],
        "None": ["ไม่มีโรคประจ", "ปฏิเสธโรคประจ าตัว", "ไม่มีโรคประจ าตัว"],
    }
    comorbidity = {
        disease: get_next_number(text, *thdiseases, default=0, return_rest=False)
        for disease, thdiseases in diseases.items()
    }
    assert sum(comorbidity.values()) >= deaths_title or date in [d("2021-8-10")], f"Missing comorbidity {comorbidity}\n{text}"

    risks = {
        "Family": ["คนในครอบครัว", "ครอบครัว", "สัมผัสญาติติดเชื้อมาเยี่ยม"],
        "Others": ["คนอื่นๆ", "คนอ่ืนๆ"],
        "Location": [
            "อาศัย/ไปพื้นที่ระบาด", "อาศัย/ไปพ้ืนที่ระบาด", "อาศัย/ไปพื้นทีร่ะบาด", "อาศัย/เข้าพ้ืนที่ระบาด",
            "อาศัย/เดินทางเข้าไปในพื้นที่ระบาด"
        ],  # Live/go to an epidemic area
        "Crowds": [
            "ไปที่แออัด", "ไปท่ีแออัด", "ไปสถานที่แออัดพลุกพลา่น", "เข้าไปในสถานที่แออัดพลุกพลา่น",
            "ไปสถานที่แออัดพลุกพล่าน"
        ],  # Go to crowded places
        "Work": ["อาชีพเสี่ยง"],  # Risky occupations
        "HCW": ["HCW", "บุคลากรทางการแพทย์"],
        "Unknown": ["ระบุได้ไม่ชัดเจน", "ระบุไม่ชัดเจน"],
    }
    risk = {
        en_risk: get_next_number(text, *th_risks, default=0, return_rest=False)
        for en_risk, th_risks in risks.items()
    }
    # TODO: Get all bullets and fuzzy match them to categories
    #assert sum(risk.values()) >= deaths_title, f"Missing risks {risk}\n{text}"

    #    risk_family, _ = get_next_number(text, "คนในครอบครัว", "ครอบครัว", "สัมผัสญาติติดเชื้อมาเยี่ยม", default=0)

    # TODO: <= 2021-04-30. there is duration med, max and 7-21 days, 1-4 days, <1
    # "ค่ากลางระยะเวลา (วันที่ทราบผลติดเชื้อ – เสียชีวิต) 9 วัน (นานสุด 85 วัน)"

    # TODO: "เป็นผู้ที่ได้วัคซีน AZ 1 เข็ม 7 ราย และไม่ระบุชนิด 1 เข็ม 1 ราย" <- vaccinated deaths
    # TODO: deaths at home - "เสียชีวิตที่บ้าน 1 ราย จ.เพชรบุรี พบเชื้อหลังเสียชีวิต"

    # TODO: what if they have more than one page?
    risk_cols = [f"Deaths Risk {r}" for r in risk.keys()]
    cm_cols = [f"Deaths Comorbidity {cm}" for cm in comorbidity.keys()]
    row = pd.DataFrame(
        [[date, deaths_title, med_age, min_age, max_age, male, female] + list(risk.values())
         + list(comorbidity.values())],
        columns=[
            "Date", "Deaths", "Deaths Age Median", "Deaths Age Min", "Deaths Age Max", "Deaths Male", "Deaths Female"
        ] + risk_cols + cm_cols).set_index("Date")
    dfprov = briefing_deaths_provinces(text, date, deaths_title, file)
    print(f"{date.date()} Deaths:", len(dfprov), "|", row.to_string(header=False, index=False), file)
    return row, dfprov


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
                # raise Exception(f"no province found for death in: {cell}")
                province = "Unknown"
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
        sum, dfprov = briefing_deaths_summary(text, date, file)
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
    # deaths = import_csv("deaths", ["Date", "Province"], not USE_CACHE_DATA)
    deaths = pd.DataFrame(columns=["Date", "Province"]).set_index(['Date', 'Province'])
    vac_prov = pd.DataFrame(columns=["Date", "Province"]).set_index(['Date', 'Province'])
    url = "http://media.thaigov.go.th/uploads/public_img/source/"
    start = d("2021-01-13")  # 12th gets a bit messy but could be fixed
    end = today()
    links = [f"{url}{f.day:02}{f.month:02}{f.year-1957}.pdf" for f in daterange(start, end, 1)]
    links += [f"{url}249764.pdf"]  # named incorrectly
    for file, text, briefing_url in web_files(*reversed(list(links)), dir="briefings"):
        pages = parse_file(file, html=True, paged=True)
        pages = [BeautifulSoup(page, 'html.parser') for page in pages]
        date = file2date(file) if "249764.pdf" not in file else d("2021-07-24")

        today_types = briefing_case_types(date, pages, briefing_url)
        types = types.combine_first(today_types)

        case_detail = briefing_case_detail(date, pages)
        date_prov_types = date_prov_types.combine_first(case_detail)

        prov = briefing_province_cases(date, pages)

        each_death, death_sum, death_by_prov = briefing_deaths(file, date, pages)
        for i, page in enumerate(pages):
            text = page.get_text()
            # Might throw out totals since doesn't include all prov
            # vac_prov = vac_briefing_provs(vac_prov, date, file, page, text)
            types = vac_briefing_totals(types, date, file, page, text)

        if not today_types.empty:
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
            ideaths, ddeaths = today_types.loc[today_types.last_valid_index()]['Deaths'], death_sum.loc[
                death_sum.last_valid_index()]['Deaths']
            assert wrong_deaths_report or (ddeaths == ideaths), f"Death details {ddeaths} didn't match total {ideaths}"

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


def prov_to_districts(dfprov):
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

    file, dl = next(test_dav_files(ext="xlsx"))
    dl()
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
    if "จำนวนผลบวก" not in series:
        # 2021-08-24 they added another graph with %
        return data, raw
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
    raw = import_csv("tests_by_area", ["Start"], not USE_CACHE_DATA, date_cols=["Start", "End"])
    pubpriv = import_csv("tests_pubpriv", ["Date"], not USE_CACHE_DATA)

    for file, dl in test_dav_files(ext=".pptx"):
        dl()
        for chart, title, series, pagenum in pptx2chartdata(file):
            data, raw = get_tests_by_area_chart_pptx(file, title, series, data, raw)
            if not all_in(pubpriv.columns, 'Tests', 'Tests Private'):
                # Latest file as all the data we need
                pubpriv = get_tests_private_public_pptx(file, title, series, pubpriv)
        assert not data.empty
        # TODO: assert for pubpriv too. but disappearerd after certain date
    # Also need pdf copies because of missing pptx
    for file, dl in test_dav_files(ext=".pdf"):
        dl()
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

def get_vaccination_coldchain(request_json, join_prov=False):
    print("Requesting coldchain:", request_json)
    if join_prov:
        df_codes = pd.read_html("https://en.wikipedia.org/wiki/ISO_3166-2:TH")[0]
        codes = [code for code, prov, ptype in df_codes.itertuples(index=False) if "special" not in ptype]
        provinces = [
            prov.split("(")[0] for code, prov, ptype in df_codes.itertuples(index=False) if "special" not in ptype
        ]
        provinces = [get_province(prov) for prov in provinces]
    else:
        provinces = codes = [None]

    url = "https://datastudio.google.com/batchedDataV2?appVersion=20210506_00020034"
    with open(request_json) as fp:
        post = json.load(fp)
    specs = post['dataRequest']
    post['dataRequest'] = []

    def set_filter(filters, field, value):
        for filter in filters:
            if filter['filterDefinition']['filterExpression']['queryTimeTransformation']['dataTransformation'][
                    'sourceFieldName'] == field:
                filter['filterDefinition']['filterExpression']['stringValues'] = value
        return filters

    def make_request(post, codes):
        for code in codes:
            for spec in specs:
                pspec = copy.deepcopy(spec)
                if code:
                    set_filter(pspec['datasetSpec']['filters'], "_hospital_province_code_", [code])
                post['dataRequest'].append(pspec)
        try:
            r = requests.post(url, json=post, timeout=120)
        except requests.exceptions.ReadTimeout:
            print("Timeout so using cached", request_json)
            with open(os.path.join("json", request_json), ) as fp:
                data = fp.read()
        else:
            _, _, data = r.text.split("\n")
        data = json.loads(data)
        if any(resp for resp in data['dataResponse'] if 'errorStatus' in resp):
            # raise Exception(resp['errorStatus']['reasonStr'])
            # read from cache if possible
            with open(os.path.join("json", request_json)) as fp:
                data = json.load(fp)
        else:
            with open(os.path.join("json", request_json), "w") as fp:
                fp.write(data)
        for resp in (resp for resp in data['dataResponse'] if 'errorStatus' in resp):
            # raise Exception(resp['errorStatus']['reasonStr'])
            pass
        for resp in (resp for resp in data['dataResponse'] if 'errorStatus' not in resp):
            yield resp
    if join_prov:
        dfall = pd.DataFrame(columns=["Date", "Province", "Vaccine"]).set_index(["Date", "Province", "Vaccine"])
    else:
        dfall = pd.DataFrame(columns=["Date"]).set_index(["Date"])

    for prov_spec, data in zip([(p, s) for p in provinces for s in specs], make_request(post, codes)):
        prov, spec = prov_spec
        fields = [(f['name'], f['dataTransformation']['sourceFieldName']) for f in spec['datasetSpec']['queryFields']]
        for datasubset in data['dataSubset']:
            colmuns = datasubset['dataset']['tableDataset']['column']
            df_cols = {}
            date_col = None
            for field, column in zip(fields, colmuns):
                fieldname = dict(_vaccinated_on_='Date',
                                 _manuf_name_='Vaccine',
                                 datastudio_record_count_system_field_id_98323387='Vac Given').get(field[1], field[1])
                nullIndex = column['nullIndex']
                del column['nullIndex']
                if column:
                    field_type = next(iter(column.keys()))
                    conv = dict(dateColumn=d, datetimeColumn=d, longColumn=int, doubleColumn=float,
                                stringColumn=str)[field_type]
                    values = [conv(i) for i in column[field_type]['values']]
                    if conv == d:
                        date_col = fieldname
                else:
                    values = []
                # datastudio_record_count_system_field_id_98323387 = supply?
                for i in nullIndex:  # TODO check we are doing this right
                    values.insert(i, None)
                df_cols[fieldname] = values
            df = pd.DataFrame(df_cols)
            if not date_col:
                df['Date'] = today()
            else:
                df['Date'] = df[date_col]
            if prov:
                df['Province'] = prov
                df = df.set_index(["Date", "Province", "Vaccine"])
            else:
                df = df.set_index(['Date'])
            dfall = dfall.combine_first(df)
    return dfall

# vac given table
# <p>สรุปการฉดีวัคซีนโควิด 19 ตัง้แตว่ันที่ 7 มิถุนายน 2564
# ผลการใหบ้ริการ ณ วนัที ่23 มิถุนายน 2564 เวลา 18.00 น.


def vac_briefing_totals(df, date, file, page, text):
    if not re.search("(รายงานสถานการณ์|ระลอกใหม่ เมษายน ประเทศไทย ตั้งแต่วันที่)", text):
        return df
    if not re.search("(ผู้รับวัคซีน|ผูรั้บวัคซีน)", text):
        return df
    # Vaccines
    numbers, rest = get_next_numbers(text, "ผู้รับวัคซีน", "ผูรั้บวัคซีน")
    if not numbers:
        return df
    rest, *_ = rest.split("หายป่วยแล้ว")
    total, _ = get_next_number(rest, "ฉีดแล้ว", "ฉีดแลว้", until="โดส")
    cums = [int(d.replace(",", "")) for d in re.findall(r"สะสม *([\d,]+) *ราย", rest)]
    daily = [int(d.replace(",", "")) for d in re.findall(r"\+([\d,]+) *ราย", rest)]
    if total:
        assert 0.99 <= sum(cums) / total <= 1.01
    else:
        total = sum(cums)
    assert len(cums) == len(daily)
    assert len(cums) < 4

    # We need given totals to ensure we use these over other api given totals
    row = [date - datetime.timedelta(days=1), sum(daily), total] + daily + cums + [file]
    columns = ["Date", "Vac Given", "Vac Given Cum"]
    columns += [f"Vac Given {d}" for d in range(1, len(cums) + 1)]
    columns += [f"Vac Given {d} Cum" for d in range(1, len(cums) + 1)]
    columns += ["Source Vac Given"]
    vac = pd.DataFrame([row], columns=columns).set_index("Date")
    if not vac.empty:
        print(f"{date.date()} Vac:", vac.to_string(header=False, index=False))
    df = df.combine_first(vac)

    return df


def vac_briefing_provs(df, date, file, page, text):
    if "ความครอบคลุมการรับบริการวัคซีนโควิด 19" not in text:
        return df

    lines = re.split(r"([\u0E00-\u0E7F \(\)\*]+(?:[0-9,\. ]+)+)", text)
    lines = [li.strip() for li in lines if li.strip()]
    *pre, table = split(lines, re.compile("ความครอบคลุม").search)
    rows = []
    for line in table:
        prov = re.search(r"[\u0E00-\u0E7F]+", line)
        numbers, _ = get_next_numbers(line, "", ints=False)
        if prov:
            prov = get_province(prov.group(0), ignore_error=True)
        if not prov or len(numbers) != 5:
            continue
        total, dose1, perc1, dose2, perc2 = numbers
        rows.append([date, prov, total, dose1, dose2])

    return df.combine_first(
        pd.DataFrame(rows, columns=["Date", "Province", "Vac Given Cum", "Vac Given 1 Cum",
                                    "Vac Given 2 Cum"]).set_index(["Date", "Province"]))


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
    if not re.search(r"(ให้หน่วยบริกำร|ใหห้นว่ยบริกำร|สรปุกำรจดัสรรวคัซนีโควดิ 19|ริการวัคซีนโควิด 19|ผู้ได้รับวัคซีนเข็มที่ 1)", page):  # noqa
        return daily
    date = find_thai_date(page)
    # fix numbers with spaces in them
    page = re.sub(r"(\d) (,\d)", r"\1\2", page)
    if date == d("2021-05-06"):
        page = re.sub(r",5 9 รำย", "", page)  # glitch on 2021-05-6
    # dose1_total, rest1 = get_next_number(page, "ได้รับวัคซีนเข็มที่ 1", until="โดส")
    # dose2_total, rest2 = get_next_number(page, "ได้รับวัคซีน 2 เข็ม", until="โดส")

    alloc_sv, rest = get_next_number(page, "Sinovac", until="โดส")
    alloc_az, rest = get_next_number(page, "AstraZeneca", until="โดส")

    # numbers, _ = get_next_numbers(page, "2 (รำย) รวม (โดส)")
    # if numbers:
    #     given1, given2, given_total, *_ = numbers

    # alloc_total, rest = get_next_number(page, "รวมกำรจัดสรรวัคซีนทั้งหมด", "รวมกำรจดัสรรวคัซนีทัง้หมด", until="โดส")
    # assert alloc_total == alloc_sv + alloc_az
    row = [date, alloc_sv, alloc_az]
    # assert not any_in(row, None)
    df = pd.DataFrame([row], columns=[
        "Date",
        "Vac Allocated Sinovac",
        "Vac Allocated AstraZeneca",
    ]).set_index("Date").fillna(value=np.nan)
    # TODO: until make more specific to only reports for allocations
    daily = daily.combine_first(df)

    if not re.search(r"(ากรทางการแพท|บุคคลที่มีโรคประจ|ากรทางการแพทย|กรทำงกำรแพทย์)", page):
        print(date.date(), "Vac Sum (Missing groups)", df.to_string(header=False, index=False), file)
        assert date < d("2021-07-12")
        return daily

    def clean_num(numbers):
        return [n for n in numbers if n not in [60, 7]]

    page = re.sub("ผัสผู้ป่วย 1,022", "", page)  # 2021-05-06

    d1_num, rest1 = get_next_numbers(page,
                                     r"เข็ม(?:ท่ี|ที่) 1 จํานวน",
                                     r"ซีนเข็มที่ 1 จ",
                                     r"1\s*จํานวน",
                                     r"1 จำนวน",
                                     until=r"(?:2 เข็ม)")
    d2_num, rest2 = get_next_numbers(page,
                                     r"ได้รับวัคซีน 2 เข็ม",
                                     r"ไดรับวัคซีน 2 เข็ม",
                                     until=r"(?:ดังรูป|โควิด 19|จังหวัดที่|3 \(Booster dose\))")
    d3_num, rest3 = get_next_numbers(page, r"3 \(Booster dose\)", until="ดังรูป")
    if not len(clean_num(d1_num)) == len(clean_num(d2_num)):
        if date > d("2021-04-24"):
            assert False
        else:
            print(date.date(), "Vac Sum (Error groups)", df.to_string(header=False, index=False), file)
            return daily
    # assert len(d3_num) == 0 or len(d3_num) == len(d2_num)

    is_risks = re.compile("(บุคคลที่มีโรคประจ|บุคคลท่ีมีโรคประจําตัว|ผู้ที่มีอายุตั้งแต่ 60|จำนวน|ได้รับวัคซีน 2)")

    for dose, numbers, rest in [(1, d1_num, rest1), (2, d2_num, rest2), (3, d3_num, rest3)]:
        cols = [
            "Date",
            f"Vac Given {dose} Cum",
            f"Vac Group Medical Staff {dose} Cum",
            f"Vac Group Health Volunteer {dose} Cum",
            f"Vac Group Other Frontline Staff {dose} Cum",
            f"Vac Group Over 60 {dose} Cum",
            f"Vac Group Risk: Disease {dose} Cum",
            f"Vac Group Risk: Pregnant {dose} Cum",
            f"Vac Group Risk: Location {dose} Cum",
        ]
        numbers = clean_num(numbers)  # remove 7 chronic diseases and over 60 from numbers
        if len(numbers) in [6, 8] and is_risks.search(rest):
            if len(numbers) == 8:
                total, medical, volunteer, frontline, over60, chronic, pregnant, area = numbers
            else:
                total, medical, frontline, over60, chronic, area = numbers
                pregnant = volunteer = 0
            row = [medical, volunteer, frontline, over60, chronic, pregnant, area]
            assert not any_in(row, None)
            assert 0.945 <= (sum([i for i in row if i]) / total) <= 1.01
            df = pd.DataFrame([[date, total] + row], columns=cols).set_index("Date")
        elif dose == 3:
            if len(numbers) == 2:
                numbers = numbers + [0] * 6
            else:
                numbers = [0] * 8
            df = pd.DataFrame([[date] + numbers], columns=cols).set_index("Date")
        elif numbers:
            assert date < d("2021-07-12")  # Should be getting all the numbers every day now
            total, *_ = numbers
            df = pd.DataFrame([[date, total]], columns=[
                "Date",
                f"Vac Given {dose} Cum",
            ]).set_index("Date")
        else:
            assert date < d("2021-07-12")  # Should be getting all the numbers every day now
            continue
        daily = daily.combine_first(df)
    daily = daily.fillna(value=np.nan)
    print(date.date(), "Vac Sum", daily.loc[date:date].to_string(header=False, index=False), file)
    return daily


def vaccination_tables(df, date, page, file):
    date = find_thai_date(page)
    givencols = [
        "Date",
        "Province",
        "Vac Given 1 Cum",
        "Vac Given 1 %",
        "Vac Given 2 Cum",
        "Vac Given 2 %",
    ]
    givencols3 = givencols + [
        "Vac Given 3 Cum",
        "Vac Given 3 %",
    ]
    vaccols7x3 = givencols3 + [
        f"Vac Group {g} {d} Cum" for g in [
            "Medical Staff", "Health Volunteer", "Other Frontline Staff", "Over 60", "Risk: Disease", "Risk: Pregnant",
            "Risk: Location"
        ] for d in range(1, 4)
    ]
    vaccols6x2 = [col for col in vaccols7x3 if " 3 " not in col and "Pregnant" not in col]
    vaccols5x2 = [col for col in vaccols6x2 if "Volunteer" not in col]

    alloc2_doses = [
        "Date",
        "Province",
        "Vac Allocated Sinovac 1",
        "Vac Allocated Sinovac 2",
        "Vac Allocated AstraZeneca 1",
        "Vac Allocated AstraZeneca 2",
        "Vac Allocated Sinovac",
        "Vac Allocated AstraZeneca",
    ]
    alloc2 = [
        "Date",
        "Province",
        "Vac Allocated Sinovac",
        "Vac Allocated AstraZeneca",
    ]
    alloc4 = alloc2 + ["Vac Allocated Sinopharm", "Vac Allocated Pfizer"]

    # def add(df, prov, numbers, cols):
    #     if not df.empty:
    #         try:
    #             prev = df[cols].loc[[date, prov]]
    #         except KeyError:
    #             prev = None
    #         msg = f"Vac {date} {prov} repeated: {numbers} != {prev}"
    #         assert prev in [None, numbers], msg
    #     row = [date, prov] + numbers
    #     df = df.combine_first(pd.DataFrame([row], columns=cols).set_index(["Date", "Province"]))
    #     return df

    rows = {}

    def add(prov, numbers, cols):
        assert rows.get((date, prov), None) is None or rows.get((date, prov), None).keys() != cols
        rows[(date, prov)] = {c: n for c, n in zip(cols, [date, prov] + numbers)}

    shots = re.compile(r"(เข็ม(?:ที|ที่|ท่ี)\s.?(?:1|2)\s*)")
    july = re.compile(r"\( *(?:ร้อยละ|รอ้ยละ) *\)", re.DOTALL)
    oldhead = re.compile(r"(เข็มที่ 1 วัคซีน|เข็มท่ี 1 และ|เข็มที ่1 และ)")
    lines = [line.strip() for line in page.split('\n') if line.strip()]
    _, *rest = split(lines, lambda x: (july.search(x) or shots.search(x) or oldhead.search(x)) and '2564' not in x)
    for headings, lines in pairwise(rest):
        shot_count = max(len(shots.findall(h)) for h in headings)
        table = {12: "new_given", 10: "given", 6: "alloc", 14: "july"}.get(shot_count)
        if not table and max(len(oldhead.findall(h)) for h in headings):
            table = "old_given"
        elif not table and max(len(july.findall(h)) for h in headings):
            table = "july"
        elif not table:
            continue
        added = None
        for line in lines:
            # fix some number broken in the middle
            line = re.sub(r"(\d+ ,\d+)", lambda x: x.group(0).replace(" ", ""), line)
            area, *rest = line.split(' ', 1)
            if area in ["เข็มที่", "และ", "จ", "ควำมครอบคลุม", 'ตั้งแต่วันที่', 'หมายเหตุ']:  # Extra heading
                continue
            if area == "รวม" or not rest:
                continue  # previously meant end of table. Now can be part of header. 2021-08-14
            cols = [c.strip() for c in NUM_OR_DASH.split(rest[0]) if c.strip()]
            if len(cols) < 5:
                break
            if added is None:
                added = 0
            if NUM_OR_DASH.match(area):
                thaiprov, *cols = cols
            else:
                thaiprov = area
            prov = get_province(thaiprov)
            numbers = parse_numbers(cols)
            added += 1
            if table == "alloc":
                sv1, sv2, az1, az2 = numbers[3:7]
                add(prov, [sv1, sv2, az1, az2, sv1 + sv2, az1 + az2], alloc2_doses)
            elif table == "given":
                if len(numbers) == 16:
                    alloc_sv, alloc_az, *numbers = numbers
                    add(prov, [alloc_sv, alloc_az], alloc2)
                assert len(numbers) == 14
                add(prov, numbers, vaccols5x2)
            elif table == "old_given":
                alloc, target_num, given, perc, *rest = numbers
                medical, frontline, disease, elders, riskarea, *rest = rest
                # TODO: #อยู่ระหว่ำง ระบุ กลุ่มเป้ำหมำย - In the process of specifying the target group
                # unknown = sum(rest)
                row = [given, perc, 0, 0] + [medical, 0, frontline, 0, disease, 0, elders, 0, riskarea, 0]
                add(prov, row, vaccols5x2)
                add(prov, [alloc, 0, 0, 0, alloc, 0], alloc2_doses)
            elif table == "new_given" and len(numbers) == 12:  # e.g. vaccinations/Daily report 2021-05-11.pdf
                dose1, dose2, *groups = numbers
                add(prov, [dose1, None, dose2, None] + groups, vaccols5x2)
            elif table == "new_given" and len(numbers) == 21:  # from 2021-07-20
                # Actually cumulative totals
                pop, alloc, givens, groups = numbers[0], numbers[1:4], numbers[4:8], numbers[9:21]
                sv, az, total_alloc = alloc
                add(prov, givens + groups + [pop], vaccols6x2 + ["Vac Population"])
                add(prov, [sv, az], alloc2)
            elif table == "july" and len(numbers) == 5:
                pop, given1, perc1, given2, perc2, = numbers
                row = [given1, perc1, given2, perc2]
                add(prov, row, givencols)
            elif table == "july" and len(numbers) in [33, 27, 21, 22, 17]:  # from 2021-08-05
                # Actually cumulative totals
                if len(numbers) == 21:
                    # Givens is a single total only 2021-08-16
                    pop, alloc, givens, groups = numbers[0], numbers[1:5], numbers[5:6], numbers[6:]
                    givens = [None] * 6  # We don't use the total
                elif len(numbers) == 22:
                    # Givens has sinopharm in it too. 2021-08-15
                    pop, alloc, givens, groups = numbers[0], numbers[1:6], numbers[6:7], numbers[7:]
                    givens = [None] * 6  # We don't use the total
                elif len(numbers) == 17:
                    # No allocations or givens 2021-08-10
                    pop, givens, groups = numbers[0], numbers[1:2], numbers[2:]
                    givens = [None] * 6
                    alloc = [None] * 4
                else:
                    pop, alloc, givens, groups = numbers[0], numbers[1:5], numbers[5:11], numbers[12:]
                if len(alloc) == 4:
                    sv, az, pf, total_alloc = alloc
                    sp = None
                else:
                    sv, az, sp, pf, total_alloc = alloc
                assert total_alloc is None or sum([m for m in [sv, az, pf, sp] if m]) == total_alloc
                if len(groups) == 15:
                    # medical has 3 doses, rest 2, so insert some Nones
                    for i in range(5, len(groups) + 6, 3):
                        groups.insert(i, None)
                add(prov, givens + groups + [pop], vaccols7x3 + ["Vac Population"])
                add(prov, [sv, az, sp, pf], alloc4)
            elif table == "july" and len(numbers) in [13]:
                # extra table with %  per population for over 60s and totals
                pop, d1, d1p, d2, d2p, d3, d3p, total, pop60, d60_1, d60_1p, d60_2, d60_2p = numbers
                add(prov, [d1, d1p, d2, d2p, d3, d3p], givencols3)
            else:
                assert False
        assert added is None or added > 7
    rows = pd.DataFrame.from_dict(rows, orient='index')
    rows = rows.set_index(["Date", "Province"]).fillna(np.nan) if not rows.empty else rows
    if 'Vac Given 1 Cum' in rows.columns:
        rows['Vac Given Cum'] = rows['Vac Given 1 Cum'] + rows['Vac Given 2 Cum']
    return df.combine_first(rows) if not rows.empty else df


def vaccination_reports_files():
    # also from https://ddc.moph.go.th/vaccine-covid19/diaryReportMonth/08/9/2021
    folders = web_links("https://ddc.moph.go.th/dcd/pagecontent.php?page=643&dept=dcd",
                        ext=None, match=re.compile("2564"))
    links = (link for f in folders for link in web_links(f, ext=".pdf"))
    url = "https://ddc.moph.go.th/uploads/ckeditor2/files/Daily report "
    gen_links = (f"{url}{f.year}-{f.month:02}-{f.day:02}.pdf"
                 for f in reversed(list(daterange(d("2021-05-20"), today(), 1))))
    links = unique_values(chain(links, gen_links))  # Some were not listed on the site so we guess
    links = sorted(links, key=lambda f: date if (date := file2date(f)) is not None else d("2020-01-01"), reverse=True)
    for link in links:
        date = file2date(link)
        if not date or date <= d("2021-02-27"):
            continue
        date = date - datetime.timedelta(days=1)  # TODO: get actual date from titles. maybe not always be 1 day delay
        if USE_CACHE_DATA and date < today() - datetime.timedelta(days=MAX_DAYS - 1):
            break

        def get_file(link=link):
            try:
                file, _, _ = next(iter(web_files(link, dir="vaccinations")))
            except StopIteration:
                return None
            return file

        yield link, date, get_file


def vaccination_reports_files2():
    # also from https://ddc.moph.go.th/vaccine-covid19/diaryReportMonth/08/9/2021
    folders = [f"https://ddc.moph.go.th/vaccine-covid19/diaryReportMonth/{m:02}/9/2021" for m in range(3, 13)]
    links = (link for f in folders for link in web_links(f, ext=".pdf"))
    links = sorted(links, reverse=True)
    count = 0
    for link in links:

        def get_file(link=link):
            try:
                file, _, _ = next(iter(web_files(link, dir="vaccinations")))
            except StopIteration:
                return None
            return file
        count += 1
        if USE_CACHE_DATA and count > MAX_DAYS:
            break
        yield link, None, get_file


def vaccination_reports():
    vac_daily = pd.DataFrame(columns=['Date']).set_index("Date")
    vac_prov_reports = pd.DataFrame(columns=['Date', 'Province']).set_index(["Date", "Province"])

    # add in newer https://ddc.moph.go.th/uploads/ckeditor2//files/Daily%20report%202021-06-04.pdf
    # Just need the latest

    for link, date, dl in vaccination_reports_files2():
        if (file := dl()) is None:
            continue
        table = pd.DataFrame(columns=["Date", "Province"]).set_index(["Date", "Province"])
        for page in parse_file(file):
            found_date = find_thai_date(page)
            if date is None:
                date = found_date
            table = vaccination_tables(table, date, page, file)

            vac_daily = vaccination_daily(vac_daily, date, file, page)
            vac_daily = vac_problem(vac_daily, date, file, page)
        print(date, "Vac Tables", len(table), "Provinces parsed", file)
        # TODO: move this into vaccination_tables so can be tested
        if d("2021-05-04") <= date <= d("2021-08-01") and len(table) < 77:
            print(date, "Dropping table: too few provinces")
            continue
        elif d("2021-04-09") <= date <= d("2021-05-03") and table.groupby("Date").count().iloc[0]['Vac Group Risk: Location 1 Cum'] != 77:
            #counts = table.groupby("Date").count()
            #missing_data = counts[counts['Vac Allocated AstraZeneca'] > counts['Vac Group Risk: Location 2 Cum']]
            # if not missing_data.empty:
            print(date, "Dropping table: alloc doesn't match prov")
            continue
        else:
            assert len(table) == 77 or date < d("2021-08-01")
        vac_prov_reports = vac_prov_reports.combine_first(table)

    # Do cross check we got the same number of allocations to vaccination
    # if not vac_prov_reports.empty:
    #     # counts = vac_prov_reports.groupby("Date").count()
    #     # missing_data = counts[counts['Vac Allocated AstraZeneca'] > counts['Vac Group Risk: Location 2 Cum']]
    #     # # 2021-04-08 2021-04-06 2021-04-05- 03-02 just not enough given yet
    #     # missing_data = missing_data["2021-04-09": "2021-05-03"]
    #     # # 2021-05-02 2021-05-01 - use images for just one table??
    #     # # We will just remove this days
    #     # vac_prov_reports = vac_prov_reports.drop(index=missing_data.index)
    #     # # After 2021-05-08 they stopped using allocation table. But cum should now always have 77 provinces
    #     # # TODO: only have 76 prov? something going on
    #     # missing_data = counts[counts['Vac Given 1 Cum'] < 77]["2021-05-04":]
    #     # vac_prov_reports = vac_prov_reports.drop(index=missing_data.index)

    #     # Just in case coldchain data not working
    

    return vac_daily, vac_prov_reports


def get_vac_coldchain():
    vac_import = get_vaccination_coldchain("vac_request_imports.json", join_prov=False)
    if not vac_import.empty:
        vac_import["_vaccine_name_"] = vac_import["_vaccine_name_"].apply(replace_matcher(["Astrazeneca", "Sinovac"]))
        vac_import = vac_import.drop(columns=['_arrive_at_transporter_']).pivot(columns="_vaccine_name_",
                                                                                values="_quantity_")
        vac_import.columns = [f"Vac Imported {c}" for c in vac_import.columns]
        vac_import = vac_import.fillna(0)
        vac_import['Vac Imported'] = vac_import.sum(axis=1)
        vac_import = vac_import.combine_first(daily2cum(vac_import))

    # Delivered Vac data from coldchain
    vac_delivered = get_vaccination_coldchain("vac_request_delivery.json", join_prov=False)
    vac_delivered = join_provinces(vac_delivered, '_hospital_province_')
    # TODO: save delivered by prov somewhere. note some hospitals unknown prov
    vac_delivered = vac_delivered.reset_index()
    vac_delivered['Date'] = vac_delivered['Date'].dt.floor('d')
    vac_delivered = vac_delivered[['Date', '_quantity_']].groupby('Date').sum()
    vac_delivered = vac_delivered.rename(columns=dict(_quantity_='Vac Delivered'))
    vac_delivered['Vac Delivered Cum'] = vac_delivered['Vac Delivered'].fillna(0).cumsum()

    # per prov given from coldchain
    vacct = get_vaccination_coldchain("vac_request_givenprov.json", join_prov=True)
    vacct = vacct.reset_index().set_index("Date").loc['2021-02-28':].reset_index().set_index(['Date', 'Province'])
    vacct = vacct.reset_index().pivot(index=["Date", "Province"], columns=["Vaccine"]).fillna(0)
    vacct.columns = [" ".join(c).replace("Sinovac Life Sciences", "Sinovac") for c in vacct.columns]
    vacct['Vac Given'] = vacct.sum(axis=1, skipna=False)
    vacct = vacct.loc[:today() - datetime.timedelta(days=1)]  # Todays data is incomplete
    vacct = vacct.fillna(0)
    vaccum = vacct.groupby(level="Province", as_index=False, group_keys=False).apply(daily2cum)
    vacct = vacct.combine_first(vaccum)

    # Their data can have some prov on the last day missing data
    # Need the last day we have a full set of data since some provinces can come in late in vac tracker data
    # TODO: could add unknowns
    counts1 = vacct['Vac Given'].groupby("Date").count()
    counts2 = vacct['Vac Given Cum'].groupby("Date").count()
    last_valid = max([counts2[counts1 > 76].last_valid_index(), counts2[counts2 > 76].last_valid_index()])
    vacct = vacct.loc[:last_valid]

    return vac_import, vac_delivered, vacct


def get_vaccinations():
    # TODO: replace the vacct per prov data with the dashboard data
    # TODO: replace the import/delivered data with?
    # vac_import, vac_delivered, vacct = get_vac_coldchain()

    vac_reports, vac_reports_prov = vaccination_reports()
    vac_slides_data = vac_slides()
    # vac_reports_prov.drop(columns=["Vac Given 1 %", "Vac Given 1 %"], inplace=True)

    vac_prov_sum = vac_reports_prov.groupby("Date").sum()

    vac_prov = import_csv("vaccinations", ["Date", "Province"], not USE_CACHE_DATA)
    vac_prov = vac_prov.combine_first(vac_reports_prov)  # .combine_first(vacct)
    if not USE_CACHE_DATA:
        export(vac_prov, "vaccinations", csv_only=True)

    # vac_prov = vac_prov.combine_first(vacct)

    # Add totals if they are missing
    # given = vac_prov[[f"Vac Given {d}" for d in range(1, 4)]].sum(axis=1).to_frame("Vac Given")
    # vac_prov = vac_prov.combine_first(given)
    given_cum = vac_prov[[f"Vac Given {d} Cum" for d in range(1, 4)]].sum(axis=1).to_frame("Vac Given Cum")
    vac_prov = vac_prov.combine_first(given_cum)

    # Get vaccinations by district # TODO: move this to plot
    vac_prov = join_provinces(vac_prov, "Province")
    given_by_area_1 = area_crosstab(vac_prov, 'Vac Given 1', ' Cum')
    given_by_area_2 = area_crosstab(vac_prov, 'Vac Given 2', ' Cum')
    given_by_area_both = area_crosstab(vac_prov, 'Vac Given', ' Cum')

    vac_timeline = import_csv("vac_timeline", ["Date"], not USE_CACHE_DATA)

    vac_timeline = vac_timeline.combine_first(
        vac_reports).combine_first(
        vac_slides_data).combine_first(
        # vac_delivered).combine_first(
        # vac_import).combine_first(
        given_by_area_1).combine_first(
        given_by_area_2).combine_first(
        given_by_area_both).combine_first(
        vac_prov_sum)
    if not USE_CACHE_DATA:
        export(vac_timeline, "vac_timeline")

    return vac_timeline


def vac_manuf_given(df, page, file, page_num):
    if not re.search(r"(ผลการฉีดวคัซีนสะสมจ|ผลการฉีดวัคซีนสะสมจ|านวนผู้ได้รับวัคซีน|านวนการได้รับวัคซีนสะสม|านวนผูไ้ดร้บัวคัซนี)", page):  # noqa
        return df
    if "AstraZeneca" not in page or file <= "vaccinations/1620104912165.pdf":  # 2021-03-21
        return df
    table = camelot.read_pdf(file, pages=str(page_num), process_background=True)[0].df
    title1, daily, title2, doses, *rest = [cell for cell in table[0] if cell.strip()]  # + title3, totals + extras
    date = find_thai_date(title1)
    # Sometimes header and cell are split into different rows 'vaccinations/1629345010875.pdf'
    if len(rest) == 3:
        doses = rest[0]  # Assumes header is doses cell

    # Sometimes there is an extra date thrown in inside brackets on the subheadings
    # e.g. vaccinations/1624968183817.pdf
    _, doses = find_thai_date(doses, remove=True)

    numbers = get_next_numbers(doses, return_rest=False)
    numbers = [n for n in numbers if n not in [1, 2, 3]]  # these are in subtitles and seem to switch positions
    sp1, sp2, sp3 = [0] * 3
    pf1, pf2, pf3 = [0] * 3
    az3, sv3, sp3 = [0] * 3
    total3 = 0
    if "pfizer" in doses.lower():
        total1, sv1, az1, sp1, pf1, total2, sv2, az2, sp2, pf2, total3, *dose3 = numbers
        if len(dose3) == 2:
            az3, pf3 = dose3
        else:
            sv3, az3, sp3, pf3 = dose3
    elif "Sinopharm" in doses:
        total1, sv1, az1, sp1, total2, sv2, az2, sp2 = numbers
    else:
        if len(numbers) == 6:
            total1, sv1, az1, total2, sv2, az2 = numbers
        else:
            # vaccinations/1620456296431.pdf # somehow ends up inside brackets
            total1, sv1, az1, sv2, az2 = numbers
            total2 = sv2 + az2
    assert total1 == sv1 + az1 + sp1 + pf1
    #assert total2 == sv2 + az2 + sp2 + pf2
    assert total3 == sv3 + az3 + sp3 + pf3 or date in [d("2021-08-15")]
    row = [date, sv1, az1, sp1, pf1, sv2, az2, sp2, pf2, sv3, az3, sp3, pf3]
    cols = [f"Vac Given {m} {d} Cum" for d in [1, 2, 3] for m in ["Sinovac", "AstraZeneca", "Sinopharm", "Pfizer"]]
    row = pd.DataFrame([row], columns=['Date'] + cols)
    print(date.date(), "Vac slides", file, row.to_string(header=False, index=False))
    return df.combine_first(row.set_index("Date"))

def vac_slides_groups(df, page, file, page_num):
    if "กลุ่มเปา้หมาย" not in page:
        return
    # does faily good job
    table = camelot.read_pdf(file, pages=str(page_num), process_background=False)[0].df
    table = table[2:]
    for i in range(1, 7):
        table[i] = pd.to_numeric(table[i].str.replace(",", "").replace("-", "0"))
    table.columns = ["group", "1 Cum", "1", "2 Cum", "2", "3 Cum", "3"]
    table.loc[:,"group"] = [
        "Vac Group Medical Staff",
        "Vac Group Health Volunteer",
        "Vac Group Other Frontline Staff",
        "Vac Group Over 60",
        "Vac Group Risk: Disease",
        "Vac Group Risk: Pregnant",
        "Vac Group Risk: Location",
        "Total"
    ]
    table.pivot(columns="group", values=["1 Cum", "2 Cum", "3 Cum"])


    # medical, rest = get_next_numbers(page, "บคุลากรทางการแพ", until="\n")
    # village, rest = get_next_numbers(rest, "เจา้หน้าทีด่", until="\n")
    # disease, rest = get_next_numbers(rest, "ผู้มีโรคเรือ้รัง 7", until="\n")
    # public, rest = get_next_numbers(rest, "ประชาชนทัว่ไป", until="\n")
    # over60, rest = get_next_numbers(rest, "ผู้มีอาย ุ60", until="\n")
    # pregnant, rest = get_next_numbers(rest, "หญิงตัง้ครรภ์", until="\n")
    # total, rest = get_next_numbers(rest, "รวม", until="\n")

# จ านวนการได้รับวัคซีนโควิด 19 ของประเทศไทย แยกตามกลุ่มเป้าหมาย
# สะสมตั้งแต่วันที่ 28 กุมภาพันธ์ – 9 สิงหาคม 2564


# ที่มา : ฐานข้อมูลกระทรวงสาธารณสุข (MOPH  Immunization Center) ข้อมูล ณ วันที่ 9 สิงหาคม 2564 เวลา 18.00 น.


# เขม็ที่ 1 (คน)  เพ่ิมขึน้ (คน) เขม็ที่ 2 (คน)  เพ่ิมขึน้ (คน) เขม็ที่ 3 (คน)  เพ่ิมขึน้ (คน)


# บคุลากรทางการแพทยแ์ละสาธารณสุข 832,908           5,042              718,384           4,308              268,022           46,457
# เจา้หน้าทีด่า่นหน้า 945,171           8,475              560,922           4,676              -                  -
# อาสาสมัครสาธารณสุขประจ าหมูบ่า้น 530,994           7,943              234,800           3,197              -                  -
# ผู้มีโรคเรือ้รัง 7 กลุ่มโรค 1,795,485        44,910            306,421           8,472              -                  -
# ประชาชนทัว่ไป 8,811,064        204,868           2,514,032        72,179            -                  -
# ผู้มีอาย ุ60 ปขีึน้ไป 3,414,683        78,160            231,332           11,514            -                  -
# หญิงตัง้ครรภ์ 6,438              991                 454                 138                 -                  -


# รวม 16,336,743      350,389           4,566,345        104,484           268,022           46,457


# กลุ่มเปา้หมาย
# จ านวนผู้ที่ไดร้ับวคัซีน

def vac_slides():
    folders = [f"https://ddc.moph.go.th/vaccine-covid19/diaryPresentMonth/{m}/10/2021" for m in range(1, 12)]
    links = sorted((link for f in folders for link in web_links(f, ext=".pdf")), reverse=True)
    files = (f for f, _, _ in web_files(*links, dir="vaccinations"))
    df = pd.DataFrame(columns=['Date']).set_index("Date")
    for file in files:
        for i, page in enumerate(parse_file(file), 1):
            # pass
            df = vac_manuf_given(df, page, file, i)
            #df = vac_slides_groups(df, page, file, i)
    return df


################################
# Misc
################################


def get_ifr():
    # replace with https://stat.bora.dopa.go.th/new_stat/webPage/statByAgeMonth.php
    url = "http://statbbi.nso.go.th/staticreport/Page/sector/EN/report/sector_01_11101_EN_.xlsx"
    file, _, _ = next(web_files(url, dir="json", check=False))
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
        file, content, _ = next(web_files(every_district, dir="json", check=True))
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


# TODO: Additional data sources
# - new moph apis
#    - https://covid19.ddc.moph.go.th/
# - medical supplies (tableux)
#    - https://public.tableau.com/app/profile/karon5500/viz/moph_covid_v3/Story1
#    - is it accurate?
#    - no timeseries
# - offocial moph dashboard (tableux)
#   - https://ddc.moph.go.th/covid19-dashboard/index.php?dashboard=select-trend-line
# - vaccine imports (unofficial) (getting out of date?)
#    - https://docs.google.com/spreadsheets/u/1/d/1BaCh5Tbm1EXwh4SeRM9dv-yemK2J5RpO-dz28UVtX3s/htmlview?fbclid=IwAR36L3itMKFv6fq7q-7_CF4WpxtI-QGQAcJ1f62BLen6N6IHc1iq-u-wWNI/export?gid=0&format=csv  # noqa
# - vaccine dashboard (power BI)
#   - https://dashboard-vaccine.moph.go.th/dashboard.html
#   - groups, ages, manuf per prov. ages per group all thailand
#   - no timeseries
# - Vaccine total numbers in at risk groups
#   - https://hdcservice.moph.go.th/hdc/main/index.php
# - vaccine slides
#   - has complications list but in graphic
# - briefings
#   - clusters per day
#   - nationality of deaths
#   - time to death?
#   - deaths at home
# - test reports
#   - top labs over time
# Public transport usage to determine mobility?
#   - https://datagov.mot.go.th/dataset/covid-19/resource/71a552d0-0fea-4e05-b78c-42d58aa88db6
#   - doesn't have pre 2020 dailies though



def scrape_and_combine():
    os.makedirs("api", exist_ok=True)
    quick = USE_CACHE_DATA and os.path.exists(os.path.join('api', 'combined.csv'))
    MAX_DAYS = int(os.environ.get("MAX_DAYS", 1 if USE_CACHE_DATA else 0))

    print(f'\n\nUSE_CACHE_DATA = {quick}\nCHECK_NEWER = {CHECK_NEWER}\nMAX_DAYS = {MAX_DAYS}\n\n')

    # TODO: replace with cli --data=situation,briefings --start=2021-06-01 --end=2021-07-01
    # "--data=" to plot only
    if USE_CACHE_DATA and MAX_DAYS == 0:
        old = import_csv("combined")
        old = old.set_index("Date")
        return old

    briefings_prov, cases_briefings = get_cases_by_prov_briefings()
    vac = get_vaccinations()
    dashboard, dash_prov = moph_dashboard()
    tests_reports = get_test_reports()
    cases_demo, risks_prov = get_cases_by_demographics_api()

    tweets_prov, twcases = get_cases_by_prov_tweets()
    timelineapi = get_cases()
    situation = get_situation()

    tests = get_tests_by_day()
    excess_deaths()
    case_api_by_area = get_cases_by_area_api()  # can be very wrong for the last days

    # Export briefings
    briefings = import_csv("cases_briefings", ["Date"], not USE_CACHE_DATA)
    briefings = briefings.combine_first(cases_briefings).combine_first(twcases).combine_first(timelineapi)
    export(briefings, "cases_briefings")

    # Export per province
    dfprov = import_csv("cases_by_province", ["Date", "Province"], not USE_CACHE_DATA)
    dfprov = dfprov.combine_first(
        dash_prov).combine_first(
        briefings_prov).combine_first(
        tweets_prov).combine_first(
        risks_prov)  # TODO: check they aggree
    dfprov = join_provinces(dfprov, on="Province")
    export(dfprov, "cases_by_province")

    # Export per district
    by_area = prov_to_districts(dfprov)

    cases_by_area = import_csv("cases_by_area", ["Date"], not USE_CACHE_DATA)
    cases_by_area = cases_by_area.combine_first(by_area).combine_first(case_api_by_area)
    export(cases_by_area, "cases_by_area")

    print("========Combine all data sources==========")
    df = pd.DataFrame(columns=["Date"]).set_index("Date")
    for f in [tests_reports, tests, dashboard, cases_briefings, twcases, timelineapi, cases_demo, cases_by_area, situation, vac]:
        df = df.combine_first(f)
    print(df)

    if quick:
        old = import_csv("combined", index=["Date"])
        df = df.combine_first(old)

        return df
    else:
        export(df, "combined", csv_only=True)
        export(get_fuzzy_provinces(), "fuzzy_provinces", csv_only=True)
        return df



if __name__ == "__main__":

    # does exports
    scrape_and_combine()
