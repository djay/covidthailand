import datetime
import functools
from dateutil.relativedelta import relativedelta
import json
import os
import re
import codecs
from multiprocessing import Pool
import shutil

import pandas as pd
from requests.exceptions import ConnectionError

from utils_pandas import add_data, export, fuzzy_join, import_csv, cut_ages
from utils_scraping import CHECK_NEWER, USE_CACHE_DATA, web_files, s, logger
from utils_thai import DISTRICT_RANGE, join_provinces, to_thaiyear, today, get_fuzzy_provinces
import covid_data_dash
import covid_data_situation
import covid_data_briefing
import covid_data_vac
import covid_data_tweets
import covid_data_testing

#################################
# Cases Apis
#################################


def get_cases():
    logger.info("========Covid19 Timeline==========")
    try:
        file, text, url = next(
            web_files("https://covid19.th-stat.com/json/covid19v2/getTimeline.json", dir="inputs/json", check=True))
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
    file, text, _ = next(web_files(url, dir="inputs/json", check=True))
    data = re.search(r"packageApp\.value\('meta',([^;]+)\);", text.decode("utf8")).group(1)
    apis = json.loads(data)
    links = [api['url'] for api in apis if "รายงานจำนวนผู้ติดเชื้อ COVID-19 ประจำวัน" in api['name']]
    # get earlier one first
    links = sorted([link for link in links if '.php' not in link and '.xlsx' not in link], reverse=True)
    # 'https://data.go.th/dataset/8a956917-436d-4afd-a2d4-59e4dd8e906e/resource/be19a8ad-ab48-4081-b04a-8035b5b2b8d6/download/confirmed-cases.csv'
    cases = pd.DataFrame()
    for file, _, _ in web_files(*links, dir="inputs/json", check=True, strip_version=True, appending=True):
        if file.endswith(".xlsx"):
            continue
            #cases = pd.read_excel(file)
        elif file.endswith(".csv"):
            confirmedcases = pd.read_csv(file)
            if '�' in confirmedcases.loc[0]['risk']:
                # bad encoding
                with codecs.open(file, encoding="tis-620") as fp:
                    confirmedcases = pd.read_csv(fp)
            first, last, ldate = confirmedcases["No."].iloc[0], confirmedcases["No."].iloc[-1], confirmedcases["announce_date"].iloc[-1]
            logger.info("Covid19daily: rows={} {}={} {} {}", len(confirmedcases), last - first, last - first, ldate, file)
            cases = cases.combine_first(confirmedcases.set_index("No."))
        else:
            raise Exception(f"Unknown filetype for covid19daily {file}")
    cases = cases.reset_index("No.")
    cases['announce_date'] = pd.to_datetime(cases['announce_date'], dayfirst=True)
    cases['Notified date'] = pd.to_datetime(cases['Notified date'], dayfirst=True, errors="coerce")
    cases = cases.rename(columns=dict(announce_date="Date")).set_index("Date")
    cases['age'] = pd.to_numeric(cases['age'], downcast="integer", errors="coerce")
    #assert cases.index.max() <
    return cases


def get_case_details_api():
    rid = "67d43695-8626-45ad-9094-dabc374925ab"
    chunk = 10000
    url = f"https://data.go.th/api/3/action/datastore_search?resource_id={rid}&limit={chunk}&q=&offset="
    records = []

    cases = import_csv("covid-19", ["_id"], dir="inputs/json")
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
    export(cases, "covid-19", csv_only=True, dir="inputs/json")
    cases = cases.set_index("Date")
    logger.info("Covid19daily: covid-19 {}", cases.last_valid_index())

    # # they screwed up the date conversion. d and m switched sometimes
    # # TODO: bit slow. is there way to do this in pandas?
    # for record in records:
    #     record['announce_date'] = to_switching_date(record['announce_date'])
    #     record['Notified date'] = to_switching_date(record['Notified date'])
    # cases = pd.DataFrame(records)
    return cases


@functools.lru_cache(maxsize=100, typed=False)
def get_cases_by_demographics_api():
    logger.info("========Covid19Daily Demographics==========")

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
    risks['อยู่ระหว่างการสอบสวน'] = "Investigating"  # Under investigation
    risks['การค้นหาผู้ป่วยเชิงรุกและค้นหาผู้ติดเชื้อในชุมชน'] = "Proactive Search"
    risks['State Quarantine'] = 'Imported'
    risks['ไปสถานที่ชุมชน เช่น ตลาดนัด สถานที่ท่องเที่ยว'] = "Community"
    risks['Cluster ผับ Thonglor'] = "Entertainment"
    risks['ผู้ที่เดินทางมาจากต่างประเทศ และเข้า ASQ/ALQ'] = 'Imported'
    risks['Cluster บางแค'] = "Community"  # bangkhee
    risks['Cluster ตลาดพรพัฒน์'] = "Community"  # market
    risks['Cluster ระยอง'] = "Entertainment"  # Rayong
    # work with foreigners
    risks['อาชีพเสี่ยง เช่น ทำงานในสถานที่แออัด หรือทำงานใกล้ชิดสัมผัสชาวต่างชาติ เป็นต้น'] = "Work"
    risks['ศูนย์กักกัน ผู้ต้องกัก'] = "Prison"  # detention
    risks['คนไทยเดินทางกลับจากต่างประเทศ'] = "Imported"
    risks['สนามมวย'] = "Entertainment"  # Boxing
    risks['ไปสถานที่แออัด เช่น งานแฟร์ คอนเสิร์ต'] = "Community"  # fair/market
    risks['คนต่างชาติเดินทางมาจากต่างประเทศ'] = "Imported"
    risks['บุคลากรด้านการแพทย์และสาธารณสุข'] = "Work"
    risks['ระบุไม่ได้'] = "Unknown"
    risks['อื่นๆ'] = "Unknown"
    risks['พิธีกรรมทางศาสนา'] = "Community"  # Religious
    risks['Cluster บ่อนพัทยา/ชลบุรี'] = "Entertainment"  # gambling rayong
    risks['ผู้ที่เดินทางมาจากต่างประเทศ และเข้า HQ/AHQ'] = "Imported"
    risks['Cluster บ่อนไก่อ่างทอง'] = "Entertainment"  # cockfighting
    risks['Cluster จันทบุรี'] = "Entertainment"  # Chanthaburi - gambling?
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
        20210510.5: 'ร้านอาหาร:Entertainment',  # restaurant
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
        # African gem merchants dining after Ramadan
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
# Excess Deaths
########################

def excess_deaths():
    url = "https://stat.bora.dopa.go.th/stat/statnew/connectSAPI/stat_forward.php?"
    url += "API=/api/stattranall/v1/statdeath/list?action=73"
    url += "&statType=-1&statSubType=999&subType=99"
    rows = []
    provinces = pd.read_csv('province_mapping.csv', header=0)
    index = ["Year", "Month", "Province", "Gender", "Age"]
    df = import_csv("deaths_all", index, date_cols=[], dir="inputs/json")
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
            logger.info("Excess Deaths: missing {}-{}", year, month)
            for prov, iso in provinces[["Name", "ISO[7]"]].itertuples(index=False):
                if iso is None or type(iso) != str:
                    continue
                dateth = f"{to_thaiyear(year, short=True)}{month:02}"
                logger.bind(end="").opt(raw=True).info(".")
                apiurl = f"{url}&yymmBegin={dateth}&yymmEnd={dateth}&cc={iso[3:]}"
                res = s.get(apiurl, timeout=30)
                data = json.loads(res.content)
                if len(data) != 2:
                    # data not found
                    if date < today() - relativedelta(months=1):
                        # Error in specific past data
                        logger.info("Excess Deaths: Error getting {} {} {}", prov, apiurl, str(data))
                        continue
                    else:
                        # This months data not yet available
                        logger.info("Excess Deaths: Error in {}-{}", year, month)
                        done = True
                        break
                changed = True
                for sex, numbers in zip(["male", "female"], data):
                    total = numbers.get("lsSumTotTot")
                    thisrows = [[year, month, prov, sex, age, numbers.get(f"lsAge{age}")] for age in range(0, 102)]
                    assert total == sum([r[-1] for r in thisrows])
                    assert numbers.get("lsAge102") is None
                    rows.extend(thisrows)
            logger.opt(raw=True).info("\n")
    df = df.combine_first(pd.DataFrame(rows, columns=index + ["Deaths"]).set_index(index))
    if changed:
        export(df, "deaths_all", csv_only=True, dir="inputs/json")
        shutil.copy(os.path.join("inputs", "json", "deaths_all.csv"), "api")  # "json" for caching, api so it's downloadable

    return df


def prov_to_districts(dfprov):
    # Reduce down to health areas
    dfprov_grouped = dfprov.groupby(["Date", "Health District Number"]).sum(min_count=1).reset_index()
    dfprov_grouped = dfprov_grouped.pivot(index="Date", columns=['Health District Number'])
    dfprov_grouped = dfprov_grouped.rename(columns=dict((i, f"Area {i}") for i in DISTRICT_RANGE))

    # Can cause problems sum across all provinces. might be missing data.
    # by_type = dfprov_grouped.groupby(level=0, axis=1).sum(min_count=1)

    # Collapse columns to "Cases Proactive Area 13" etc
    dfprov_grouped.columns = dfprov_grouped.columns.map(' '.join).str.strip()
    by_area = dfprov_grouped  # .combine_first(by_type)

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
    logger.info("========Covid-19 case details - get_cases_by_area_api==========")
    cases = get_case_details_csv().reset_index()
    cases["province_of_onset"] = cases["province_of_onset"].str.strip(".")
    cases = join_provinces(cases, "province_of_onset")
    case_areas = pd.crosstab(cases['Date'], cases['Health District Number'])
    case_areas = case_areas.rename(columns=dict((i, f"Cases Area {i}") for i in DISTRICT_RANGE))
    return case_areas


################################
# Misc
################################


def get_ifr():
    # replace with https://stat.bora.dopa.go.th/new_stat/webPage/statByAgeMonth.php
    url = "http://statbbi.nso.go.th/staticreport/Page/sector/EN/report/sector_01_11101_EN_.xlsx"
    file, _, _ = next(web_files(url, dir="inputs/json", check=False))
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
    logger.info("========ArcGIS==========")

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
        file, content, _ = next(web_files(every_district, dir="inputs/json", check=True))
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
# - medical supplies (tableau)
#    - https://public.tableau.com/app/profile/karon5500/viz/moph_covid_v3/Story1
#    - is it accurate?
#    - no timeseries
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

    logger.info('\n\nUSE_CACHE_DATA = {}\nCHECK_NEWER = {}\nMAX_DAYS = {}\n\n', quick, CHECK_NEWER, MAX_DAYS)

    # TODO: replace with cli --data=situation,briefings --start=2021-06-01 --end=2021-07-01
    # "--data=" to plot only
    if USE_CACHE_DATA and MAX_DAYS == 0:
        old = import_csv("combined")
        old = old.set_index("Date")
        return old

    with Pool(1 if MAX_DAYS > 0 else None) as pool:

        dash_daily = pool.apply_async(covid_data_dash.dash_daily)
        # These 3 are slowest so should go first
        dash_by_province = pool.apply_async(covid_data_dash.dash_by_province)
        dash_trends_prov = pool.apply_async(covid_data_dash.dash_trends_prov)
        vac = pool.apply_async(covid_data_vac.get_vaccinations)
        # TODO: split vac slides as that's the slowest

        briefings_prov__cases_briefings = pool.apply_async(covid_data_briefing.get_cases_by_prov_briefings)
    
        dash_ages = pool.apply_async(covid_data_dash.dash_ages)

        situation = pool.apply_async(covid_data_situation.get_situation)

        cases_demo__risks_prov = pool.apply_async(get_cases_by_demographics_api)

        tweets_prov__twcases = pool.apply_async(covid_data_tweets.get_cases_by_prov_tweets)
        timelineapi = pool.apply_async(get_cases)

        tests = pool.apply_async(covid_data_testing.get_tests_by_day)
        tests_reports = pool.apply_async(covid_data_testing.get_test_reports)

        xcess_deaths = pool.apply_async(excess_deaths)
        case_api_by_area = pool.apply_async(get_cases_by_area_api)  # can be very wrong for the last days

        # Now block getting until we get each of the data
        situation = situation.get()

        dash_daily = dash_daily.get()
        dash_ages = dash_ages.get()
        dash_by_province = dash_by_province.get()
        dash_trends_prov = dash_trends_prov.get()

        vac = vac.get()
        briefings_prov, cases_briefings = briefings_prov__cases_briefings.get()
        cases_demo, risks_prov = cases_demo__risks_prov.get()

        tweets_prov, twcases = tweets_prov__twcases.get()
        timelineapi = timelineapi.get()

        tests = tests.get()
        tests_reports = tests_reports.get()

        xcess_deaths.get()
        case_api_by_area = case_api_by_area.get()  # can be very wrong for the last days

    # Combine dashboard data
    dash_by_province = dash_trends_prov.combine_first(dash_by_province)
    export(dash_by_province, "moph_dashboard_prov", csv_only=True, dir="inputs/json")
    # "json" for caching, api so it's downloadable
    shutil.copy(os.path.join("inputs", "json", "moph_dashboard_prov.csv"), "api")
    shutil.copy(os.path.join("inputs", "json", "moph_dashboard.csv"), "api")
    shutil.copy(os.path.join("inputs", "json", "moph_dashboard_ages.csv"), "api")

    # Export briefings
    briefings = import_csv("cases_briefings", ["Date"], not USE_CACHE_DATA)
    briefings = briefings.combine_first(cases_briefings).combine_first(twcases).combine_first(timelineapi)
    export(briefings, "cases_briefings")

    # Export per province
    dfprov = import_csv("cases_by_province", ["Date", "Province"], not USE_CACHE_DATA)
    dfprov = dfprov.combine_first(
        briefings_prov).combine_first(
        dash_by_province).combine_first(
        tweets_prov).combine_first(
        risks_prov)  # TODO: check they agree
    dfprov = join_provinces(dfprov, on="Province")
    export(dfprov, "cases_by_province")

    # Export per district (except tests which are dodgy?)
    by_area = prov_to_districts(dfprov[[c for c in dfprov.columns if "Tests" not in c]])

    cases_by_area = import_csv("cases_by_area", ["Date"], not USE_CACHE_DATA)
    cases_by_area = cases_by_area.combine_first(by_area).combine_first(case_api_by_area)
    export(cases_by_area, "cases_by_area")

    logger.info("========Combine all data sources==========")
    df = pd.DataFrame(columns=["Date"]).set_index("Date")
    for f in [tests_reports, tests, cases_briefings, twcases, timelineapi, cases_demo, cases_by_area, situation, vac, dash_ages, dash_daily]:
        df = df.combine_first(f)
    logger.info(df)

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
