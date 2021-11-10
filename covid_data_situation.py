import datetime
import dateutil
from dateutil.parser import parse as d
import os
import re

from bs4 import BeautifulSoup
import numpy as np
import pandas as pd

from utils_pandas import check_cum, cum2daily, export, import_csv
from utils_scraping import MAX_DAYS, USE_CACHE_DATA, any_in, get_next_number, get_next_numbers, \
    parse_file, web_files, web_links, logger
from utils_thai import file2date, find_thai_date


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
    if date >= d("2021-10-24"):
        # no point. its all duplicate info now
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


def situation_pui_en(parsed_pdf, date):
    parsed_pdf = parsed_pdf.replace("DDC Thailand 1", "")  # 2021-10-04
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
        if date == d("2021-09-26"):
            pui = pui2 = 3_112_896  # use thai report for this date
        # TODO: find 1529045 below and see which is correct 20201-04-26
        pui2 = pui if pui2 in [96989, 433807, 3891136, 385860, 326073, 1529045, 2159780, 278178, 2774962] else pui2
        pui2 = pui if date in [d("2021-10-04")] else pui2
        assert pui == pui2
    else:
        numbers, _ = get_next_numbers(
            parsed_pdf, "Total number of people who met the criteria of patients", debug=False,
        )
        if d("2020-01-30") < date < d("2021-10-06") and not numbers:
            raise Exception(f"Problem parsing {date}")
        elif not numbers:
            # They dropped PUI in oct
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
    if date == d("2021-09-26"):
        pui_walkin = 3_106_624  # use thai report for this date
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


def get_english_situation_files(check=False):
    dir = "inputs/situation_en"
    links = web_links(
        "https://ddc.moph.go.th/viralpneumonia/eng/situation.php",
        "https://ddc.moph.go.th/viralpneumonia/eng/situation_more.php",
        ext=".pdf",
        dir=dir,
        check=True,
    )

    for count, link in enumerate(links):
        if USE_CACHE_DATA and count > MAX_DAYS:
            break

        def dl_file(link=link):
            for file, _, _ in web_files(link, dir=dir, check=check):
                return file  # Just want first
            # Missing file
            return None

        date = file2date(link)
        yield link, date, dl_file


def get_en_situation():
    results = pd.DataFrame(columns=["Date"]).set_index("Date")
    for link, date, dl_file in get_english_situation_files():
        if (file := dl_file()) is None:
            continue

        if "situation" not in os.path.basename(file):
            continue
        if date <= dateutil.parser.parse("2020-01-30"):
            continue  # TODO: can manually put in numbers before this
        parsed_pdf = parse_file(file, html=False, paged=False).replace("\u200b", "")
        parsed_pdf = parsed_pdf.replace("DDC Thailand 1", "")  # footer put in the wrong place

        pui = situation_pui_en(parsed_pdf, date)
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
        logger.info('{} {} {}', date.date(), file, row.to_string(header=False, index=False))
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
    logger.info('{} {} {}', date.date(), "Death Ages", df.to_string(header=False, index=False))
    return dfsit.combine_first(df)


def situation_pui_th(dfpui, parsed_pdf, date, file):
    tests_total, active_finding, asq, not_pui = [None] * 4
    numbers, content = get_next_numbers(
        parsed_pdf,
        r"ด่านโรคติดต่อระหว่างประเทศ",
        r"ด่านโรคติดต่อระหวา่งประเทศ",  # 'situation-no346-141263n.pdf'
        r"นวนการตรวจทาง\S+องปฏิบัติการ",
        "ด่านควบคุมโรคติดต่อระหว่างประเทศ",
        until=r"(?:โรงพยาบาลด้วยตนเอง|ารับการรักษาท่ีโรงพยาบาลด|โรงพยาบาลเอกชน)",
        require_until=True
    )
    # cases = None

    if len(numbers) == 7:  # numbers and numbers[2] < 30000:
        tests_total, pui, active_finding, asq, not_pui, *rest = numbers
        if pui == 4534137:
            pui = 453413  # situation-no273-021063n.pdf
    elif len(numbers) > 8 and date < d("2021-10-06"):
        _, _, tests_total, pui, active_finding, asq, not_pui, *rest = numbers
    elif len(numbers) == 8:
        # 2021 - removed not_pui
        _, _, tests_total, pui, asq, active_finding, pui2, *rest = numbers
        assert pui == pui2
        not_pui = None
    elif len(numbers) == 6:  # > 2021-05-10
        tests_total, pui, asq, active_finding, pui2, screened = numbers
        if date == d("2021-09-26"):
            pui = pui2  # 3,142,338 != 3,138,544
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
    if d("2020-03-26") < date < d("2021-10-06") and not numbers:
        raise Exception(f"Problem finding PUI numbers for date {date}")
    elif not numbers or date > d("2021-10-06"):
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
    if date < d("2021-10-05"):
        # stopped publishing most data
        assert check_cum(df, dfpui, cols)
    dfpui = dfpui.combine_first(df)
    logger.info('{} {} {}', date.date(), file, df.to_string(header=False, index=False))
    return dfpui


def get_thai_situation_files(check=False):
    links = web_links(
        "https://ddc.moph.go.th/viralpneumonia/situation.php",
        "https://ddc.moph.go.th/viralpneumonia/situation_more.php",
        ext=".pdf",
        dir="inputs/situation_th",
        check=True,
    )
    count = 0
    for link in links:
        if USE_CACHE_DATA and count > MAX_DAYS:
            break
        count += 1

        def dl_file(link=link):
            for file, _, _ in web_files(link, dir="inputs/situation_th", check=check):
                return file  # Just want first
            # Missing file
            return None

        date = file2date(link)
        yield link, date, dl_file


def get_thai_situation():
    results = pd.DataFrame(columns=["Date"]).set_index("Date")
    for link, date, dl_file in get_thai_situation_files():
        if (file := dl_file()) is None:
            continue

        parsed_pdf = parse_file(file, html=False, paged=False)
        if "situation" not in os.path.basename(file):
            continue
        if "Situation Total number of PUI" in parsed_pdf:
            # english report mixed up? - situation-no171-220663.pdf
            continue
        results = situation_pui_th(results, parsed_pdf, date, file)
        results = situation_pui_th_death(results, parsed_pdf, date, file)

    return results


def get_situation_today():
    try:
        _, page, _ = next(web_files("https://ddc.moph.go.th/viralpneumonia/index.php", dir="inputs/situation_th", check=True))
    except StopIteration:
        return pd.DataFrame()
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
    logger.info("========Situation Reports==========")

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
