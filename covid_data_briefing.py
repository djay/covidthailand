import datetime
import re
from itertools import islice

import numpy as np
import pandas as pd
from bs4 import BeautifulSoup
from dateutil.parser import parse as d

from utils_pandas import daterange
from utils_pandas import export
from utils_scraping import any_in
from utils_scraping import camelot_cache
from utils_scraping import get_next_number
from utils_scraping import get_next_numbers
from utils_scraping import logger
from utils_scraping import MAX_DAYS
from utils_scraping import NUM_OR_DASH
from utils_scraping import pairwise
from utils_scraping import parse_file
from utils_scraping import parse_numbers
from utils_scraping import seperate
from utils_scraping import split
from utils_scraping import strip
from utils_scraping import USE_CACHE_DATA
from utils_scraping import web_files
from utils_thai import file2date
from utils_thai import find_thai_date
from utils_thai import get_province
from utils_thai import join_provinces
from utils_thai import parse_gender
from utils_thai import today


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
        lines = pairwise(islice(is_pcell.split("\n".join(cells)), 1, None))  # because can be split over <p>
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
                # TODO: sometimes cells/data separated by "-" 2021-01-03

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
            if domestic and date not in [d("2021-11-22"), d("2021-12-02"), d("2021-12-29")]:
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
        if date not in [d("2021-11-01")]:
            assert cases == walkins + proactive + imported + prison, f"{date}: briefing case types don't match"

        # hospitalisations
        hospital, field, severe, respirator, hospitalised = [np.nan] * 5
        numbers, rest = get_next_numbers(text, "อาการหนัก")
        if numbers:
            severe, respirator, *_ = numbers
            hospital, _ = get_next_number(text, "ใน รพ.")
            field, _ = get_next_number(text, "รพ.สนาม")
            num, _ = get_next_numbers(text, "ใน รพ.", before=True)
            hospitalised = num[0]
            assert hospital + field == hospitalised or date in [d("2021-09-04")]
        elif "ผู้ป่วยรักษาอยู่" in text:
            hospitalised, *_ = get_next_numbers(text, "ผู้ป่วยรักษาอยู่", return_rest=False, before=True)
            if date > d("2021-03-31"):  # don't seem to add up before this
                hospital, *_ = get_next_numbers(text, "ใน รพ.", return_rest=False, until="ราย")
                field, *_ = get_next_numbers(text, "รพ.สนาม", return_rest=False, until="ราย")
                assert hospital + field == hospitalised

        if date < d("2021-05-18"):
            recovered, _ = get_next_number(text, "(เพ่ิมขึ้น|เพิ่มขึ้น)", until="ราย")
        else:
            # 2021-05-18 Using single infographic with 3rd wave numbers?
            numbers, _ = get_next_numbers(text, "หายป่วยแล้ว", "หายป่วยแลว้")
            cum_recovered_3rd, recovered, *_ = numbers
            if cum_recovered_3rd < recovered:
                recovered = cum_recovered_3rd

        assert not pd.isna(recovered)

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
        logger.info("{} Briefing Cases: {}", date.date(), df.to_string(header=False, index=False))
    return df


def briefing_province_cases(file, date, pages):
    # TODO: also can be got from https://ddc.moph.go.th/viralpneumonia/file/scoreboard/scoreboard_02062564.pdf
    # Seems updated around 3pm so perhaps not better than briefing
    if date < d("2021-01-13"):
        pages = []
    rows = {}
    for i, soup in enumerate(pages):
        text = str(soup)
        if "รวมท ัง้ประเทศ" in text:
            continue
        if not re.search(r"(?:ที่|ที)#?\s*(?:จังหวัด|จงัหวดั)", text):  # 'ที# จงัหวดั' 2021-10-17
            continue
        if not re.search(r"(นวนผู้ติดเชื้อโควิดในประเทศรำยใหม่|อโควิดในประเทศรายให)", text):
            continue
        parts = [p.get_text() for p in soup.find_all("p")]
        parts = [line for line in parts if line]
        preamble, *tables = split(parts, re.compile(r"รวม\s*\((?:ราย|รำย)\)").search)
        if len(tables) <= 1:
            continue  # Additional top 10 report. #TODO: better detection of right report
        else:
            title, parts = tables
        while parts and "รวม" in parts[0]:
            # get rid of totals line at the top
            totals, *parts = parts
            # First line might be several
            totals, *more_lines = totals.split("\n", 1)
            parts = more_lines + parts
        parts = [c.strip() for c in NUM_OR_DASH.split("\n".join(parts)) if c.strip()]
        while True:
            if len(parts) < 9:
                # TODO: can be number unknown cases - e.g. หมายเหตุ : รอสอบสวนโรค จานวน 337 ราย
                break
            if NUM_OR_DASH.search(parts[0]):
                linenum, prov, *parts = parts
            else:
                # for some reason the line number doesn't show up? but it's there in the pdf...
                break
            numbers, parts = parts[:9], parts[9:]
            thai = prov.strip().strip(" ี").strip(" ์").strip(" ิ")
            if thai in ['กทม. และปรมิ ณฑล', 'รวมจงัหวดัอนื่ๆ(']:
                # bangkok + suburbs, rest of thailand
                break
            prov = get_province(thai)
            numbers = parse_numbers(numbers)
            numbers = numbers[1:-1]  # last is total. first is previous days
            assert len(numbers) == 7
            for i, cases in enumerate(reversed(numbers)):
                if i > 4:  # 2021-01-11 they use earlier cols for date ranges
                    break
                olddate = date - datetime.timedelta(days=i)
                if (olddate, prov) not in rows:
                    rows[(olddate, prov)] = cases
                else:
                    # TODO: apparently 2021-05-13 had to merge two lines but why?
                    # assert (olddate, prov) not in rows, f"{prov} twice in prov table line {linenum}"
                    pass  # if duplicate we will catch it below

                # if False and olddate == date:
                #     if cases > 0:
                #         print(date, linenum, thai, PROVINCES["ProvinceEn"].loc[prov], cases)
                #     else:
                #         print("no cases", linenum, thai, *numbers)
    data = ((d, p, c) for (d, p), c in rows.items())
    df = pd.DataFrame(data, columns=["Date", "Province", "Cases"]).set_index(["Date", "Province"])
    assert date >= d(
        "2021-01-13") and not df.empty, f"Briefing on {date} failed to parse cases per province"
    if date > d("2021-05-12") and date not in [d("2021-07-18")]:
        # TODO: 2021-07-18 has only 76 prov. not sure why yet. maybe doubled up or mispelled names?
        assert len(df.groupby("Province").count()) in [77, 78], f"Not enough provinces briefing {date}"
    return df


def briefing_deaths_provinces(dtext, date, file):
    if not deaths_title_re.search(dtext):
        return pd.DataFrame(columns=["Date", "Province"]).set_index(["Date", "Province"])

    bullets_re = re.compile(r"((?:•|� )[^\(]*?\( ?\d+ ?\)(?:[\n ]*\([^\)]+\))?)\n?")

    # get rid of extra words in brackets to make easier
    text = re.sub(r"\b(ละ|จังหวัด|จังหวัด|อย่างละ|ราย)\b", " ", dtext)

    # remove age breakdown of deaths per province to make it easier
    # e.g "60+ปี 58 ราย (85%)" - from 2021-08-24
    text = re.sub(r"([\d-]+\+?\s?(?:ปี)? *\d* *(?:ราย)? *\(\d+%?\))", " ", text)
    # and '50+ (14)' 2021-08-26
    text = re.sub(r"([\d]+\+?(?:ปี)? *\(\d+\))", " ", text)
    # (รายงานหลังเสียชีวิตเกิน 7 วัน 17  )  2021-09-07
    text = re.sub(r"\( *\S* *\d+ วัน *\d+ *\)", " ", text)

    # # 2021-10-17 get
    # text = re.sub(r"� ", "• ", text)

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

    title_num, _ = get_next_numbers(text, deaths_title_re)
    day, year, deaths_title, *_ = title_num

    if date in [d("2021-07-20"), d("2021-12-15")]:
        # 2021-12-15 - missing one from eastern
        pass
    else:
        msg = f"in {file} only found {dfprov['Deaths'].sum()}/{deaths_title} from {dtext}\n{pcells}"
        assert deaths_title == dfprov['Deaths'].sum(), msg
    return dfprov


deaths_title_re = re.compile(r"(ผูป่้วยโรคโควดิ-19|วยโรคโควิด-19) (เสยีชวีติ|เสียชีวิต) (ของประเทศไทย|ของประเทศไทย)")  # noqa
# ผู;ป=วยโรคโควิด-19 เสียชีวิต ของประเทศไทย รายงานวันท่ี 17 ต.ค. 64 (+68 ราย)


def briefing_deaths_summary(text, date, file):
    if not deaths_title_re.search(text):
        return pd.DataFrame()
    # Summary of locations, reasons, medium age, etc

    # Congenital disease / risk factor The severity of the disease
    # congenital_disease = df[2][0]  # TODO: parse?
    # Risk factors for COVID-19 infection
    # risk_factors = df[3][0]
    numbers, *_ = get_next_numbers(text,
                                   "ามัธยฐานของอา",
                                   "ค่ากลางขอ(?:งอ)?ายุ",
                                   "ามัธยฐานอายุ",
                                   "• ค่ากลาง",
                                   "ค่ากลางอาย ุ",
                                   ints=False)
    if numbers:
        med_age, min_age, max_age, *_ = numbers
    else:
        # 2021-09-15 no medium
        numbers = get_next_numbers(text, "อายุระหว่าง", until="ปี", return_rest=False)
        min_age, max_age = numbers
        med_age = None

    title_num, _ = get_next_numbers(text, deaths_title_re)
    day, year, deaths_title, *_ = title_num

    # deaths over 60
    if date > d("2021-08-02"):
        deaths_60 = get_next_number(text, r"60\s*(?:ปีขึ้นไป|ปีข้ึนไป|ป9ขึ้นไป|ปขึ้นไป)", return_rest=False)
        assert deaths_60 is not None
    else:
        deaths_60 = np.nan

    genders = get_next_numbers(text, "(หญิง|ชาย)", return_rest=False)
    if genders and date != d("2021-08-09"):
        male, female, *_ = genders
        if get_next_numbers(text, "ชาย", return_rest=False)[0] == female:
            # They sometimes reorder them
            male, female = female, male
        assert male + female == deaths_title or date in [d("2021-09-11")]
    else:
        male, female = None, None

    numbers, *_ = get_next_numbers(text, "ค่ากลางระยะเวลา")
    if numbers:
        period_death_med, period_death_max, *_ = numbers

    text = re.sub(r"([\d]+wk)", "", text)  # remove 20wk pregnant
    diseases = {
        "Hypertension": ["ความดันโลหิตสูง", "HT", "ความดันโลหิตสงู"],
        "Diabetes": ["เบาหวาน", "DM"],
        "Hyperlipidemia": ["ไขมันในเลือดสูง", "HPL"],
        "Lung disease": ["โรคปอด"],
        "Obesity": ["โรคอ้วน", "อ้วน", "อ1วน"],
        "Cerebrovascular": ["หลอดเลือดสมอง"],
        "Kidney disease": ["โรคไต"],
        "Heart disease": ["โรคหัวใจ"],
        "Bedridden": ["ติดเตียง"],
        "Pregnant": ["ตั้งครรภ์"],
        "None": ["ไม่มีโรคประจ", "ปฏิเสธโรคประจ าตัว", "ไม่มีโรคประจ าตัว", "ไม่มีประวัตโิรคเรือ้รงั"],
        # ไม่มีประวัตโิรคเรือ้รงั 3 ราย (2% - 2021-09-15 - only applies under 60 so not exactly the same number
    }
    comorbidity = {
        disease: get_next_number(text, *thdiseases, default=0, return_rest=False, until=r"\)", require_until=True)
        for disease, thdiseases in diseases.items()
    }
    if date not in [d("2021-8-10"), d("2021-09-23"), d("2021-11-22"), d("2021-12-10")]:
        assert sum(comorbidity.values()) >= deaths_title, f"Missing comorbidity {comorbidity}\n{text}"

    risks = {
        "Family": ["คนในครอบครัว", "ครอบครัว", "สัมผัสญาติติดเชื้อมาเยี่ยม"],
        "Others": ["คนอื่นๆ", "คนอ่ืนๆ", "คนรู้จัก", "คนรู1จัก"],
        "Residence": ["อาศัย"],
        "Location": [
            "อาศัย/ไปพื้นที่ระบาด", "อาศัย/ไปพ้ืนที่ระบาด", "อาศัย/ไปพื้นทีร่ะบาด", "อาศัย/เข้าพ้ืนที่ระบาด",
            "อาศัย/เดินทางเข้าไปในพื้นที่ระบาด", "ในพื้นท่ี",
        ],  # Live/go to an epidemic area
        "Crowds": [
            "ไปที่แออัด", "ไปท่ีแออัด", "ไปสถานที่แออัดพลุกพลา่น", "เข้าไปในสถานที่แออัดพลุกพลา่น",
            "ไปสถานที่แออัดพลุกพล่าน"
        ],  # Go to crowded places
        "Work": ["อาชีพเสี่ยง", "อาชีพเ"],  # Risky occupations
        "HCW": ["HCW", "บุคลากรทางการแพทย์"],
        "Unknown": ["ระบุได้ไม่ชัดเจน", "ระบุไม่ชัดเจน"],
    }
    risk = {
        en_risk: get_next_number(text, *th_risks, default=0, return_rest=False, dash_as_zero=True)
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
         + list(comorbidity.values()) + [deaths_60]],
        columns=[
            "Date", "Deaths", "Deaths Age Median", "Deaths Age Min", "Deaths Age Max", "Deaths Male", "Deaths Female"
        ] + risk_cols + cm_cols + ["Deaths 60 Plus"]).set_index("Date")
    logger.info("{} Deaths: {}", date.date(), row.to_string(header=False, index=False), file)
    return row


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
    df['gender'] = df['gender'].map(parse_gender)  # TODO: handle misspelling
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

        sum = briefing_deaths_summary(text, date, file)
        # Latest version of deaths. Only gives summary info
        dfprov = briefing_deaths_provinces(text, date, file)
        if not sum.empty:
            return all, sum, dfprov

        if "วิตของประเทศไทย" not in text:
            continue
        orig = None
        if date <= d("2021-04-19"):
            cells = [soup.get_text()]
        else:
            # Individual case detail for death
            orig = camelot_cache(file, i + 2, process_background=True)
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
        logger.info("{}: Deaths:  0", date.date())
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
        logger.info("{} Deaths: {}", date.date(), sum.to_string(header=False, index=False))
        dfprov = all[["Date", 'Province']].value_counts().to_frame("Deaths")

    # calculate per province counts
    return all, sum, dfprov


def briefing_atk(file, date, pages):
    df = pd.DataFrame()
    for i, soup in enumerate(pages):
        text = soup.get_text()
        if "ยอดตรวจ ATK" not in text:
            continue
        # remove all teh dates
        while True:
            found_date, text = find_thai_date(text, remove=True)
            if found_date is None:
                break
        atk_tests, _, atk_tests_cum, atk_pos, _, atk_pos_cum, *_ = get_next_numbers(text, "ยอดตรวจ ATK", return_rest=False)
        return pd.DataFrame([[date, atk_tests, atk_tests_cum, atk_pos, atk_pos_cum]],
                            columns=['Date', "Tests ATK Proactive", "Tests ATK Proactive Cum", "Pos ATK Proactive", "Pos ATK Proactive Cum"]).set_index("Date")
    return df


def briefing_documents(check=True):
    url = "http://media.thaigov.go.th/uploads/public_img/source/"
    start = d("2021-01-13")  # 12th gets a bit messy but could be fixed
    end = today()
    links = [f"{url}249764.pdf"]  # named incorrectly
    links += [f"{url}{f.day:02}{f.month:02}{f.year-1957}.pdf" for f in daterange(start, end, 1)]
    # for file, text, briefing_url in web_files(*), dir="briefings"):

    for link in reversed(list(links)):
        date = file2date(link) if "249764.pdf" not in link else d("2021-07-24")
        if USE_CACHE_DATA and date < today() - datetime.timedelta(days=MAX_DAYS):
            break

        def get_file(link=link):
            try:
                file, text, url = next(iter(web_files(link, dir="inputs/briefings")))
            except StopIteration:
                return None
            return file

        yield link, date, get_file


def get_cases_by_prov_briefings():
    logger.info("========Briefings==========")
    types = pd.DataFrame(columns=["Date", ]).set_index(['Date', ])
    date_prov = pd.DataFrame(columns=["Date", "Province"]).set_index(['Date', 'Province'])
    date_prov_types = pd.DataFrame(columns=["Date", "Province", "Case Type"]).set_index(['Date', 'Province'])
    # deaths = import_csv("deaths", ["Date", "Province"], not USE_CACHE_DATA)
    deaths = pd.DataFrame(columns=["Date", "Province"]).set_index(['Date', 'Province'])
    vac_prov = pd.DataFrame(columns=["Date", "Province"]).set_index(['Date', 'Province'])
    for briefing_url, date, get_file in briefing_documents():
        file = get_file()
        if file is None:
            continue
        pages = parse_file(file, html=True, paged=True)
        pages = [BeautifulSoup(page, 'html.parser') for page in pages]

        today_types = briefing_case_types(date, pages, briefing_url)
        types = types.combine_first(today_types)

        case_detail = briefing_case_detail(date, pages)
        date_prov_types = date_prov_types.combine_first(case_detail)

        prov = briefing_province_cases(file, date, pages)

        atk = briefing_atk(file, date, pages)

        each_death, death_sum, death_by_prov = briefing_deaths(file, date, pages)
        # TODO: This should be redundant now with dashboard having early info on vac progress.
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
                d("2021-02-19"),  # 02-19 death details is graphic (the doctor)
                d("2021-02-15"),  # no details of deaths (2)
                d("2021-02-10"),  # no details of deaths (1)
            ] or date < d("2021-02-01")  # TODO: check out why later
            ideaths, ddeaths = today_types.loc[today_types.last_valid_index()]['Deaths'], death_sum.loc[
                death_sum.last_valid_index()]['Deaths']
            assert wrong_deaths_report or (ddeaths == ideaths) or date in [d(
                "2021-08-27"), d("2021-09-10")], f"Death details {ddeaths} didn't match total {ideaths}"

        deaths = deaths.append(each_death, verify_integrity=True)
        date_prov = date_prov.combine_first(death_by_prov)
        types = types.combine_first(death_sum).combine_first(atk)

        date_prov = date_prov.combine_first(prov)

        # Do some checks across the data
        today_total = today_types[['Cases Proactive', "Cases Walkin"]].sum().sum()
        prov_total = prov.groupby("Date").sum()['Cases'].loc[date]
        warning = f"briefing provs={prov_total}, cases={today_total}"
        if today_total and prov_total:
            assert prov_total / today_total > 0.77, warning  # 2021-04-17 is very low but looks correct
        if today_total != prov_total:
            logger.info("{} WARNING: {}", date.date(), warning)
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

    # Since Deaths by province doesn't list all provinces, ensure missing are 0
    date_prov['Deaths'] = date_prov['Deaths'].unstack(fill_value=0).fillna(0).stack()

    return date_prov, types


def vac_briefing_totals(df, date, url, page, text):
    if not re.search("(รายงานสถานการณ์|ระลอกใหม่ เมษายน ประเทศไทย ตั้งแต่วันที่)", text):
        return df
    if not re.search("(ผู้รับวัคซีน|ผูรั้บวัคซีน)", text):
        return df
    # Vaccines
    numbers, rest = get_next_numbers(text, "ผู้รับวัคซีน", "ผูรั้บวัคซีน")
    if not numbers:
        return df
    rest, *_ = rest.split("หายป่วยแล้ว")
    # the reason there's no data for 2021-9-24 is that over 1 million doses were
    # given and they couldn't tabulate the data in time for briefing of 2021-9-25:
    # "ข้อมูลการให้บริการวัคซีนวันที่ 24 ก.ย. 64 อยู่ระหว่างตรวจสอบข้อมูล เนื่องจากมีผู้เข้ามารับวัคซีน มากกว่า 1 ล้านโดส"
    if date >= datetime.datetime(2021, 9, 25):
        # use numpy's Not a Number value to avoid breaking the plots with 0s
        total = np.nan
        cums = daily = [np.nan, np.nan, np.nan]
    else:
        total, _ = get_next_number(rest, "ฉีดแล้ว", "ฉีดแลว้", until="โดส")
        daily = [int(d.replace(",", "")) for d in re.findall(r"\+([\d,]+) *ราย", rest)]
        # on the first date that fourth doses were reported, 0 daily doses were
        # displayed despite there suddenly being 800 cumulative fourth doses:
        cums = [int(d.replace(",", "")) for d in re.findall(r"สะสม *([\d,]+) *ราย", rest)]
        if date in [d("2021-09-28")]:
            cums[0] = 31811342  # mistype. 31,8,310 - https://twitter.com/thaimoph/status/1442771132717797377
        if total:
            assert 0.99 <= sum(cums) / total <= 1.01
        else:
            total = sum(cums)
    assert len(cums) == len(daily)
    # data on fourth doses was added starting with briefing of the 26th
    assert len(cums) < 5

    # We need given totals to ensure we use these over other api given totals
    row = [date - datetime.timedelta(days=1), sum(daily), total] + daily + cums + [url]
    columns = ["Date", "Vac Given", "Vac Given Cum"]
    columns += [f"Vac Given {d}" for d in range(1, len(daily) + 1)]
    columns += [f"Vac Given {d} Cum" for d in range(1, len(cums) + 1)]
    columns += ["Source Vac Given"]
    vac = pd.DataFrame([row], columns=columns).set_index("Date")
    if not vac.empty:
        logger.info("{} Vac: {}", date.date(), vac.to_string(header=False, index=False))
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
