import datetime
import functools
import json
import os
import shutil

import numpy as np
import pandas as pd
import requests
from dateutil.parser import parse as d
from dateutil.relativedelta import relativedelta

from utils_pandas import add_data
from utils_pandas import cum2daily
from utils_pandas import cut_ages
from utils_pandas import export
from utils_pandas import fuzzy_join
from utils_pandas import import_csv
from utils_pandas import weekly2daily
from utils_pandas import weeks_to_end_date
from utils_scraping import logger
from utils_scraping import s
from utils_scraping import url2filename
from utils_scraping import web_files
from utils_scraping import web_links
from utils_thai import DISTRICT_RANGE
from utils_thai import join_provinces
from utils_thai import to_thaiyear
from utils_thai import today


#################################
# Cases Apis
#################################


def get_cases_old():
    # https://covid19.th-stat.com/json/covid19v2/getTimeline.json
    # https://covid19.ddc.moph.go.th/api/Cases/round-1to2-all
    # https://covid19.ddc.moph.go.th/api/Cases/timeline-cases-all
    # {"Date":"01\/01\/2020","NewConfirmed":0,"NewRecovered":0,"NewHospitalized":0,"NewDeaths":0,"Confirmed":0,"Recovered":0,"Hospitalized":0,"Deaths":0}
    # {"txn_date":"2021-03-31","new_case":42,"total_case":28863,"new_case_excludeabroad":24,"total_case_excludeabroad":25779,"new_death":0,"total_death":94,"new_recovered":47,"total_recovered":27645}
    # "txn_date":"2021-04-01","new_case":26,"total_case":28889,"new_case_excludeabroad":21,"total_case_excludeabroad":25800,"new_death":0,"total_death":94,"new_recovered":122,"total_recovered":27767,"update_date":"2021-09-01 07:40:49"}
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


def get_cases_timelineapi():
    # https://covid19.th-stat.com/json/covid19v2/getTimeline.json
    # https://covid19.ddc.moph.go.th/api/Cases/round-1to2-all
    # https://covid19.ddc.moph.go.th/api/Cases/timeline-cases-all
    # {"Date":"01\/01\/2020","NewConfirmed":0,"NewRecovered":0,"NewHospitalized":0,"NewDeaths":0,"Confirmed":0,"Recovered":0,"Hospitalized":0,"Deaths":0}
    # {"txn_date":"2021-03-31","new_case":42,"total_case":28863,"new_case_excludeabroad":24,"total_case_excludeabroad":25779,"new_death":0,"total_death":94,"new_recovered":47,"total_recovered":27645}
    # "txn_date":"2021-04-01","new_case":26,"total_case":28889,"new_case_excludeabroad":21,"total_case_excludeabroad":25800,"new_death":0,"total_death":94,"new_recovered":122,"total_recovered":27767,"update_date":"2021-09-01 07:40:49"}
    url1 = "https://covid19.ddc.moph.go.th/api/Cases/round-1to2-all"
    url2 = "https://covid19.ddc.moph.go.th/api/Cases/timeline-cases-all"
    try:
        json1, _, url = next(web_files(url1, dir="inputs/json", check=False), None)
        json2, _, url = next(web_files(url2, dir="inputs/json", check=False), None)
    except requests.exceptions.RequestException:
        # I think we have all this data covered by other sources. It's a little unreliable.
        return pd.DataFrame()
    data = pd.concat([pd.read_json(json1), pd.read_json(json2)])
    data['Date'] = pd.to_datetime(data['txn_date'])
    data = data.set_index("Date")
    data = data.rename(columns=dict(new_case="Cases", new_death="Deaths", new_recovered="Recovered"))
    cases = data[["Cases", "Deaths", "Recovered"]]
    # 2021-12-28 had duplicate because cases went up 4610 from 2305. Why? Google says 4610
    cases = cases[~cases.index.duplicated(keep='first')]
    cases["Source Cases"] = url
    return cases


def get_cases_timelineapi_weekly():
    try:
        json1, _, url = next(web_files("https://covid19.ddc.moph.go.th/api/Cases/round-1to2-all",
                             dir="inputs/json/weekly", check=False), None)
    except requests.exceptions.RequestException:
        # I think we have all this data covered by other sources. It's a little unreliable.
        return pd.DataFrame()
    df2 = pd.read_json(json1)
    df3 = load_paged_json("https://covid19.ddc.moph.go.th/api/Cases/report-round-3-y21-line-lists",
                          ["year", "weeknum"], dir="inputs/json/weekly")
    df4 = load_paged_json("https://covid19.ddc.moph.go.th/api/Cases/timeline-cases-all",
                          ["year", "weeknum"], dir="inputs/json/weekly")
    # df = pd.concat([df2, df3, df4])
    df = df4  # there is overlap and it has different values. Just use this year?

    # week 44 2022 (29 oct) has wrong value of 25146? 10x before or after it? Earlier dates too? 2022-09-03, 2022-07-30

    df = weeks_to_end_date(df, year_col="year", week_col="weeknum", offset=0)
    if df.empty:
        return df
    df = df.drop(columns=['update_date', "index"])
    df = df.rename(columns=dict(new_case="Cases", total_case="Cases Cum",
                   new_case_excludeabroad="Cases Local", total_case_excludeabroad="Cases Local Cum",
                   new_death="Deaths", total_death="Deaths Cum",
                   case_walkin="Cases Walkin", case_foriegn="Cases Imported", case_prison="Cases Prison",
                   new_recovered="Recovered", total_recovered="Recovered Cum",
                                ))
    df = df.drop(columns=[col for col in df.columns if "_" in col])
    daily = weekly2daily(df[[col for col in df.columns if " Cum" not in col]])
    cum = df[[col for col in df.columns if "Cum" in col]].reindex(
        pd.date_range(df.index.min(), df.index.max(), name="Date")).interpolate()
    df = cum2daily(cum).combine_first(daily)

    # daily = [col for col in df.columns if "Cum" not in col]
    # df[daily] = (df[daily] / 7)

    df["Source Cases"] = url
    return df


# def get_case_details_csv():
#     cols = "No.,announce_date,Notified date,sex,age,Unit,nationality,province_of_isolation,risk,province_of_onset,district_of_onset".split(
#         ",")
#     url = "https://data.go.th/dataset/covid-19-daily"
#     file, text, _ = next(web_files(url, dir="inputs/json", check=True))
#     data = re.search(r"packageApp\.value\('meta',([^;]+)\);", text.decode("utf8")).group(1)
#     apis = json.loads(data)
#     links = [api['url'] for api in apis if "รายงานจำนวนผู้ติดเชื้อ COVID-19 ประจำวัน" in api['name']]
#     # get earlier one first
#     links = sorted([link for link in links if any_in(link, "csv", "271064")], reverse=True)
#     # 'https://data.go.th/dataset/8a956917-436d-4afd-a2d4-59e4dd8e906e/resource/be19a8ad-ab48-4081-b04a-8035b5b2b8d6/download/confirmed-cases.csv'
#     cases = pd.DataFrame()
#     done = []
#     for link, check in zip(links, ([False] * len(links))[:-1] + [True]):
#         file = url2filename(link)
#         if file in done:
#             continue
#         done.append(file)
#         for file, _, _ in web_files(link, dir="inputs/json", check=check, strip_version=True, appending=True):
#             if file.endswith(".xlsx"):
#                 confirmedcases = utils_excel.read(file)  # takes a long time
#                 confirmedcases.columns = cols
#                 # confirmedcases = fread(file).to_pandas()
#             elif file.endswith(".csv"):
#                 confirmedcases = pd.read_csv(file)
#                 if "risk" not in confirmedcases.columns:
#                     confirmedcases.columns = cols
#                 if '�' in confirmedcases.loc[0]['risk']:
#                     # bad encoding
#                     with codecs.open(file, encoding="tis-620") as fp:
#                         confirmedcases = pd.read_csv(fp)
#             else:
#                 raise Exception(f"Unknown filetype for covid19daily {file}")
#             first, last, ldate = confirmedcases["No."].iloc[0], confirmedcases["No."].iloc[-1], confirmedcases["announce_date"].iloc[-1]
#             logger.info("Covid19daily: rows={} {}={} {} {}", len(confirmedcases), last - first, last - first, ldate, file)
#             cases = cases.combine_first(confirmedcases.set_index("No."))
#     cases = cases.reset_index("No.")
#     cases['announce_date'] = pd.to_datetime(cases['announce_date'], dayfirst=True)
#     cases['Notified date'] = pd.to_datetime(cases['Notified date'], dayfirst=True, errors="coerce")
#     cases = cases.rename(columns=dict(announce_date="Date"))
#     cases['age'] = pd.to_numeric(cases['age'], downcast="integer", errors="coerce")
#     #assert cases.index.max() <
#     return cases

def cleanup_cases(cases):

    cases["province_of_onset"] = cases["province_of_onset"].str.strip(".")
    cases = join_provinces(cases, "province_of_onset")

    # Classify Jobs and patient types

    # Fix typos in Nationality columns
    # This won't include every possible misspellings and need some further improvement
    if "nationality" in cases.columns:
        cases = fuzzy_join(cases, import_csv("mapping_nationality", 'Nat Alt', date_cols=[], dir="."), 'nationality')
        cases['nationality'] = cases['Nat Main'].fillna(cases['nationality'])

    cases = fuzzy_join(cases, import_csv("mapping_patient_type", 'alt', date_cols=[], dir="."), 'patient_type')
    # TODO: reduce down to smaller list or just show top 5?
    cases, unmatched_jobs = fuzzy_join(cases, import_csv(
        "mapping_jobs", 'alt', date_cols=[], dir="."), 'job', return_unmatched=True)
    if "job" in unmatched_jobs.columns:
        unmatched_jobs = unmatched_jobs.groupby(["job", "Job Type"], dropna=False).sum().sort_values(["count"], ascending=False)
        export(unmatched_jobs, "unmatched_jobs", csv_only=True)
    cases['Job Type'] = cases['Job Type'].fillna("Unknown")

    # Clean up Risks
    # TODO: move this to mapping file
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
        20211113.01: "Phuket Sandbox:Imported",
        20211113.02: "Chonburi Sandbox:Imported",
        20211113.03: "Test and Go:Imported",
        20211113.04: "ผู้ที่เดินทางมาจากต่างประเทศ และเข้า AQ:Imported",
        20211113.05: "สถานศึกษา:Work",  # educational institutions
        20211113.06: "สัมผัสผู้ป่วยยืนยัน ภายในครอบครัว/ชุมชน/เพื่อน:Contact",
        20211113.07: "10.อื่นๆ:Unknown",
        20220114.01: "BKK Sandbox:Imported",
        20220114.02: "กระบี่:Community",  # Krabi
        20220114.03: "กรุงเทพมหานคร:Community",  # Bangkok
        20220114.04: "ขอนแก่น:Community",  # Khonkaen
        20220412.01: "Cluster Memory 90's กทม.:Entertainment",
        20220412.02: "Cluster New Jazz กทม.:Entertainment",
        20220412.03: "ไม่ระบุ:Unknown",
    }
    for v in r.values():
        key, cat = v.split(":")
        risks[key] = cat
    risks = pd.DataFrame(risks.items(), columns=[
                         "risk", "risk_group"]).set_index("risk")
    cases, unmatched = fuzzy_join(cases, risks, on="risk", return_unmatched=True)

    # dump mappings to file so can be inspected
    matched = cases[["risk", "risk_group"]]
    export(matched.value_counts().to_frame("count"), "risk_groups", csv_only=True)
    export(unmatched, "risk_groups_unmatched", csv_only=True)

    return cases


def get_case_details_api_weekly():

    # df3 = load_paged_json("https://covid19.ddc.moph.go.th/api/Deaths/round-3-line-list", ["year", "weeknum"], target_date, dir="inputs/json/weekly")
    # df1 = load_paged_json("https://covid19.ddc.moph.go.th/api/Cases/round-1to2-line-lists", ["year", "weeknum"], target_date, dir="inputs/json/weekly")
    df = load_paged_json("https://covid19.ddc.moph.go.th/api/Cases/round-4-line-lists",
                         ["year", "weeknum"], None, dir="inputs/json/weekly/cases", timeout=40)
    df['age'] = pd.to_numeric(df['age_number'], errors="coerce")
    df = df.rename(columns=dict(province="province_of_onset"))
    df = weeks_to_end_date(df, year_col="year", week_col="weeknum", offset=0).reset_index()
    df = df.drop(columns=['update_date', "index", 'age_number'])

    cases = cleanup_cases(df)
    # assert total == len(cases) - init_cases_len
    cases = cases.astype(dict(gender=str, risk=str, job=str, province_of_onset=str))

    logger.info("Covid19weekly: covid-19 {}", len(cases))
    return cases


def get_case_details_api():

    cases = import_csv("covid-19", dir="inputs/json",
                       date_cols=["Date", "update_date", "txn_date", "update_date2"],
                       str_cols=["Health District Number", "Job Type", "Nat Main", "Patient Type", "age_range", "gender",
                                 "nationality", "patient_type", "patient_type2", "risk", "risk_group", "translation", "job"],
                       # int_cols=["No.", ] # "age", "index", "int", ]
                       )
    return cases  # after 2022-10-01 switched same url to have weekly numbers
    # if "risk_group" not in cases.columns or cases["risk_group"].count() < 40000:
    #     cases = cleanup_cases(cases)
    if not cases.empty and cases["Date"].min() > d("2020-02-01"):
        url = "https://covid19.ddc.moph.go.th/api/Cases/round-1to2-line-lists"
        file, _, _ = next(iter(web_files(url, dir="inputs/json", check=False, appending=False)))
        init_cases = pd.read_csv(file).reset_index()
        init_cases.columns = ['Date', "No.", "gender", "age", "age_range", "nationality", "job",
                              "risk", "patient_type", "province_of_onset", "update_date", "update_date2", "patient_type2"]
        init_cases['Date'] = pd.to_datetime(init_cases['Date'])
        init_cases['update_date'] = pd.to_datetime(init_cases['update_date'], errors="coerce")
        init_cases = cleanup_cases(init_cases)
        assert len(init_cases) == 28863
        cases = pd.concat([init_cases, cases], ignore_index=True)
    # init_cases_len = 28863
    # lastid = cases.last_valid_index() if cases.last_valid_index() else 0
    target_date = cases["Date"].max()
    url = "https://covid19.ddc.moph.go.th/api/Cases/round-3-line-lists"
    df = load_paged_json(url, "Date", target_date, dir="inputs/json")
    df['Date'] = pd.to_datetime(df['txn_date'])
    df['update_date'] = pd.to_datetime(df['update_date'], errors="coerce")
    df['age'] = pd.to_numeric(df['age_number'])
    df = df.rename(columns=dict(province="province_of_onset"))

    # Get rid of last partial day from cases and from the new data
    cases = cases[cases['Date'] < target_date]
    df = df[df['Date'] >= target_date]

    assert df.iloc[0]['Date'] >= cases.iloc[-1]["Date"]
    assert df.iloc[0]['update_date'] >= cases.iloc[-1]["update_date"]
    # assert total == len(cases) - init_cases_len + len(df)
    df = cleanup_cases(df)
    cases = pd.concat([cases, df], ignore_index=True)  # TODO: this is slow. faster way?
    # assert total == len(cases) - init_cases_len
    # cases = cases.astype(dict(gender=str, risk=str, job=str, province_of_onset=str))
    export(cases, "covid-19", csv_only=True, dir="inputs/json/weekly")

    # cases = cases.set_index("Date")
    logger.info("Covid19daily: covid-19 {}", len(cases))

    # # they screwed up the date conversion. d and m switched sometimes
    # # TODO: bit slow. is there way to do this in pandas?
    # for record in records:
    #     record['announce_date'] = to_switching_date(record['announce_date'])
    #     record['Notified date'] = to_switching_date(record['Notified date'])
    # cases = pd.DataFrame(records)
    return cases


def load_paged_json(url, index=["year", "weeknum"], target_index=None, dir="inputs/json/weekly", check=True, timeout=80):
    basename = url2filename(url)
    if not target_index:
        # Then we will cache it ourselves and return the data
        cached = import_csv(basename, dir=dir, date_cols=[], return_empty=False)
        target_index = cached[index].iloc[-1] if not cached.empty else None
    else:
        cached = None

    data = []
    # First check api is working ok
    file, content, _ = next(iter(web_files(url, dir=None, check=check, appending=False, timeout=timeout, threads=1)), None)
    pagedata = json.loads(content) if content is not None else {}
    if "data" not in pagedata:
        return pd.DataFrame(pagedata) if cached is None else cached
    page = pagedata['data']
    assert page
    last_page = pagedata['meta']['last_page']
    total = pagedata['meta']['total']
    chunk = pagedata['meta']['per_page']
    if cached is not None:
        if len(cached) == total:
            return cached
        togo = (total - len(cached)) / chunk
        logger.info("getting {} more pages".format(togo))

    df = pd.DataFrame()
    page = []
    # Because there is no unique case number to match up we will work backwards
    # until we get to the start of the last date we have, or where update date is before our last
    # update date
    # TODO: Unless the cache is not up to date enough. In that case we go forward and assume the
    # data is so old that it won't change so continuing based on page numbers is ok. This allows us to
    # build up the cache over time even if we get failures making us stop
    # if today().date() == d("2023-01-30").date():
    #     cached = pd.DataFrame()  # Fix mistake where first page was doubled
    backwards = cached is None or len(cached) / total > 0.9
    if backwards:
        pagenum = last_page
        pages = range(last_page, 1, -1)
    else:
        pagenum = int(len(cached) / chunk) + 1  # Assumes we didn't get a partial page before? but we shouldn't?
        target_index = None
        df = cached
        pages = range(pagenum, last_page, 1)
    pages_got = 0
    is_first = False
    urls = [f"{url}?page={p}" for p in pages]
    for file, content, _ in web_files(*urls, dir=None, check=check, appending=False, timeout=timeout, threads=3):
        if file is None:
            if backwards:
                df = pd.Dataframe()  # Can't join it. have eto give up
            break
        pagedata = json.loads(content)
        data = pagedata['data']
        if not pagedata['data']:
            break
        pages_got += 1
        dfpage = pd.DataFrame(data)
        df = pd.concat([dfpage, df] if backwards else [df, dfpage])
        if pagenum == 1 and backwards or not backwards and pagenum == last_page:
            break
        elif target_index is not None and backwards:
            # we want the page with our target on but not at the top
            on_page = (dfpage[index] == target_index).all(axis=1).any()
            if not on_page and is_first:
                # Join at page boundries
                df = pd.concat([cached, df])
                assert len(df) == total
                break
            is_first = (dfpage[index].iloc[0] == target_index).all()
            if on_page and not is_first:
                # Assume that last couple of pages might change so join where the nearest change in index happened
                # first place we get our target
                # get rid of additional data in case it changed
                cache_before = cached[(cached[index] == target_index).all(axis=1)].index[0]
                # get last part of latest pages
                df_after = df[(df[index] == target_index).all(axis=1)].index[0]
                cached = cached.iloc[:cache_before]
                df = df[df_after:]
                # stick togeather
                df = pd.concat([cached, df])
                assert len(df) == total
                break
        elif not backwards and pages_got == 150:
            # Cut our loses here so we don't take so much time. Get more later
            break
        pagenum += -1 if backwards else +1

    if not df.empty:
        export(df.set_index(index), basename, csv_only=True, dir=dir)  # Ensure we don't include default index in the export
    return df


@functools.lru_cache(maxsize=100, typed=False)
def get_cases_by_demographics_api():

    def process(cases):
        # Age groups
        age_groups = cut_ages(cases, ages=[10, 20, 30, 40, 50, 60, 70], age_col="age", group_col="Age Group")
        case_ages = pd.crosstab(age_groups['Date'], age_groups['Age Group'])
        case_ages.columns = [f"Cases Age {a}" for a in case_ages.columns.tolist()]

        #labels2 = ["Age 0-14", "Age 15-39", "Age 40-59", "Age 60-"]
        #age_groups2 = pd.cut(cases['age'], bins=[0, 14, 39, 59, np.inf], right=True, labels=labels2)
        age_groups2 = cut_ages(cases, ages=[15, 40, 60], age_col="age", group_col="Age Group")
        case_ages2 = pd.crosstab(age_groups2['Date'], age_groups2['Age Group'])
        case_ages2.columns = [f"Cases Age {a}" for a in case_ages2.columns.tolist()]

        case_risks_daily = pd.crosstab(cases['Date'], cases["risk_group"])
        case_risks_daily.columns = [f"Risk: {x}" for x in case_risks_daily.columns]

        # Prov data based on this api file
        cases['Province'] = cases['province_of_onset']
        # risks_prov = join_provinces(cases, 'Province')
        risks_prov = cases.value_counts(['Date', "Province", "risk_group"]).to_frame("Cases")
        risks_prov = risks_prov.reset_index()
        risks_prov = pd.crosstab(index=[risks_prov['Date'], risks_prov['Province']],
                                 columns=risks_prov["risk_group"],
                                 values=risks_prov['Cases'],
                                 aggfunc="sum")
        risks_prov.columns = [f"Cases Risk: {c}" for c in risks_prov.columns]

        cases = cases.reset_index(drop=True)
        case_areas = pd.crosstab(cases['Date'], cases['Health District Number'])
        case_areas = case_areas.rename(columns=dict((i, f"Cases Area {i}") for i in DISTRICT_RANGE))

        cases_daily = case_risks_daily.combine_first(case_ages).combine_first(case_ages2)
        return cases_daily, risks_prov, case_areas

    cases = get_case_details_api()  # until oct 2022
    # TODO: use latest weekly data
    cases_weekly = get_case_details_api_weekly()  # 2022 onwards
    # cases = cases.combine_first(cases_weekly)

    cases_daily, risks_prov, case_areas = process(cases)
    cases_daily_w, risks_prov_w, case_areas_w = process(cases_weekly)
    risks_prov_w = risks_prov_w.reset_index("Province").groupby("Province", group_keys=True).apply(weekly2daily)

    return (
        cases_daily.combine_first(weekly2daily(cases_daily_w)),
        risks_prov.combine_first(risks_prov_w),
        case_areas.combine_first(weekly2daily(case_areas_w))
    )


def timeline_by_province():
    url = "https://covid19.ddc.moph.go.th/api/Cases/timeline-cases-by-provinces"
    file, _, _ = next(iter(web_files(url, dir="inputs/json", check=False, appending=False, timeout=40)), None)
    df = pd.read_json(file)
    df = df.rename(columns={"txn_date": "Date", "province": "Province", "new_case": "Cases", "total_case": "Cases Cum",
                   "new_case_excludeabroad": "Cases Local", "total_case_excludeabroad": "Case Local Cum", "new_death": "Deaths", "total_death": "Deaths Cum"})
    df = join_provinces(df, "Province")
    df['Date'] = pd.to_datetime(df['Date'])
    df = df.drop(columns=['update_date'])

    return df.set_index(["Date", "Province"])


def timeline_by_province_weekly():
    # url = "https://covid19.ddc.moph.go.th/api/Cases/round-1to2-by-provinces"
    # df = load_paged_json(url, ["year", "weeknum"], [2020, 1])
    url = "https://covid19.ddc.moph.go.th/api/Cases/timeline-cases-by-provinces"
    file, _, _ = next(iter(web_files(url, dir="inputs/json/weekly", check=True, appending=False, timeout=80)), None)
    df = pd.read_json(file)

    df = df.rename(columns={"province": "Province", "new_case": "Cases", "total_case": "Cases Cum",
                   "new_case_excludeabroad": "Cases Local", "total_case_excludeabroad": "Case Local Cum", "new_death": "Deaths", "total_death": "Deaths Cum"})
    df = df[df['Province'] != 'ทั้งประเทศ']  # Get rid of whole country rows for now
    df = join_provinces(df, "Province", extra=[])
    df = weeks_to_end_date(df, year_col="year", week_col="weeknum", offset=0)
    df = df.drop(columns=['update_date', "index"])
    df = df.groupby("Province").apply(lambda x: x.drop(columns="Province").reindex(
        pd.date_range(x.index.min(), x.index.max(), name="Date")).interpolate())

    # daily = [col for col in df.columns if "Cum" not in col]
    # df[daily] = (df[daily] / 7).round().astype(int)

    df = cum2daily(df[[col for col in df.columns if "Cum" in col]]).combine_first(df)

    df = df.reset_index().set_index(["Date", "Province"])
    return df


def deaths_by_province_weekly():
    # https://covid19.ddc.moph.go.th/api/Deaths/weekly-deaths-line-lists - current week only
    years = [
        "https://covid19.ddc.moph.go.th/api/Deaths/round-1to2-line-list",  # - 2020-2021
        "https://covid19.ddc.moph.go.th/api/Deaths/round-3-line-list",  # = 2021-2021
        "https://covid19.ddc.moph.go.th/api/Deaths/round-4-line-list",  # - 2022-2022 - includes type and cluster?
    ]
    data = [load_paged_json(url, dir="inputs/json/weekly/deaths") for url in years]
    csv_2023 = "https://covid19.ddc.moph.go.th/api/CSV/Deaths/round-4-line-list"  # isn't that supposed to be round 5?
    file, _, _ = next(web_files(csv_2023, dir="inputs/csv/weekly", check=True, appending=True), None)
    data += [pd.read_csv(file)]
    df = pd.concat(data)
    # "age":"57","age_range":"50-59 \u0e1b\u0e35","occupation":"\u0e44\u0e21\u0e48\u0e23\u0e30\u0e1a\u0e38","type":"\u0e1c\u0e39\u0e49\u0e1b\u0e48\u0e27\u0e22\u0e22\u0e37\u0e19\u0e22\u0e31\u0e19","death_cluster":null
    # TODO: counts per province per age range, total deaths,
    # TODO classify occupation or type? is type reason for death?
    df = df.rename(columns={"province": "Province", })
    df = df[df['Province'] != 'ทั้งประเทศ']  # Get rid of whole country rows for now
    df = join_provinces(df, "Province", extra=[])
    df = weeks_to_end_date(df, year_col="year", week_col="weeknum", offset=0)
    df = df.drop(columns=['update_date', "index"])
    # Get the deaths
    deaths_by_province = df.reset_index().groupby(["Date", "Province"]).size().to_frame("Deaths")
    # Ensure we have all days and all provinces
    dindex = deaths_by_province.reset_index("Province").index.unique()
    pindex = deaths_by_province.reset_index("Date").index.unique()
    deaths_by_province = deaths_by_province.reindex(pd.MultiIndex.from_product([dindex, pindex])).replace(np.nan, 0)
    # TODO: turn into daily averages
    deaths_daily = deaths_by_province.reset_index("Province").groupby("Province", group_keys=True).apply(weekly2daily)
    # deaths_daily = deaths_by_province.reset_index("Province").groupby("Province", group_keys=False, as_index=True).resample('d').bfill().reset_index().set_index(["Date", "Province"]).div(7)

    # TODO: get min, max, mean ages per day (per provnince and combined)
    df['age'] = pd.to_numeric(df['age'])
    timeline = df.reset_index().groupby("Date")['age'].max().to_frame("Deaths Age Max")
    timeline["Deaths Age Min"] = df.reset_index().groupby("Date")['age'].min()
    timeline["Deaths Age Median"] = df.reset_index().groupby("Date")['age'].median()
    age_groups = cut_ages(df, ages=[10, 20, 30, 40, 50, 60, 70], age_col="age", group_col="Age Group").reset_index()
    ages = pd.crosstab(age_groups['Date'], age_groups['Age Group'])
    ages.columns = [f"Deaths Age {a}" for a in ages.columns.tolist()]
    ages = weekly2daily(ages)
    timeline = timeline.combine_first(ages)

    # type is either 'confirmed patient', 'probable patient'
    # dealth_cluster can say if family, friend etc
    # df['Deaths Risk Family']
    # occupation

    return timeline, deaths_daily


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
                try:
                    res = s.get(apiurl, timeout=30)
                    data = json.loads(res.content)
                except Exception:
                    data = []
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


# Get IHME dataset


def ihme_dataset(check=True):
    data = pd.DataFrame()
    # listing out urls not very elegant, but this only need yearly update
    # TODO: get links directly from https://www.healthdata.org/covid/data-downloads so new year updates
    # urls = ['https://ihmecovid19storage.blob.core.windows.net/latest/data_download_file_reference_2022.csv',
    #         'https://ihmecovid19storage.blob.core.windows.net/latest/data_download_file_reference_2021.csv',
    #         'https://ihmecovid19storage.blob.core.windows.net/latest/data_download_file_reference_2020.csv']
    # IHME seems to have problem with their latest section and have pointed main site back to archives
    scenario = "file_best_masks"  # "file_reference" doesn't seem to fit mask use here. they assume its dropped
    urls = [u for u in web_links("https://www.healthdata.org/covid/data-downloads", ext="csv") if scenario in u]
    for file, _, _ in web_files(*reversed(urls), dir="inputs/IHME", check=check, appending=False):
        data_in_file = pd.read_csv(file)
        data_in_file = data_in_file.loc[(data_in_file['location_name'] == "Thailand")]
        data = add_data(data, data_in_file)
    # already filtered for just Thailand data above
    data.drop(['location_id', 'location_name'], axis=1, inplace=True)
    data.rename(columns={'date': 'Date', 'mobility_mean': 'Mobility Index'}, inplace=True)
    data["Date"] = pd.to_datetime(data["Date"])
    data = data.sort_values(by="Date")
    data = data.set_index("Date")

    return(data)


def get_ifr():
    # replace with https://stat.bora.dopa.go.th/new_stat/webPage/statByAgeMonth.php
    url = "http://statbbi.nso.go.th/staticreport/Page/sector/EN/report/sector_01_11101_EN_.xlsx"
    file, _, _ = next(web_files(url, dir="inputs/json", check=False), None)
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
    total_pop = unpop.groupby("Province").sum(numeric_only=True).rename(
        columns=dict(Population="total_pop"))
    unpop = unpop.join(total_pop, on="Province").join(ifr["risk"], on="Age")
    unpop['ifr'] = unpop['Population'] / unpop['total_pop'] * unpop['risk']
    provifr = unpop.groupby("Province").sum(numeric_only=True)
    provifr = provifr.drop([p for p in provifr.index if "Region" in p] + ['Whole Kingdom'])

    # now normalise the province names
    provifr = join_provinces(provifr, "Province")
    return provifr


if __name__ == '__main__':
    timeline_weekly = get_cases_timelineapi_weekly()
    cases_demo, risks_prov, case_api_by_area = get_cases_by_demographics_api()
    deaths_weekly, deaths_prov_weekly = deaths_by_province_weekly()
    timeline = get_cases_timelineapi()
    timeline = timeline.combine_first(timeline_weekly)

    timeline_prov = timeline_by_province()
    timeline_prov_weekly = timeline_by_province_weekly()
    timeline_prov = timeline_prov.combine_first(timeline_prov_weekly)

    ihme_dataset()

    dfprov = import_csv("cases_by_province", ["Date", "Province"], False)
    dfprov = dfprov.combine_first(timeline_prov).combine_first(risks_prov).combine_first(deaths_prov_weekly)

    dfprov = join_provinces(dfprov, on="Province")
    export(dfprov, "cases_by_province")

    old = import_csv("combined", index=["Date"])
    df = timeline.combine_first(cases_demo).combine_first(deaths_weekly).combine_first(old)
    export(df, "combined", csv_only=True)

    excess_deaths()

    import covid_plot_cases
    import covid_plot_deaths
    covid_plot_deaths.save_deaths_plots(df)
    covid_plot_cases.save_caseprov_plots(df)
    covid_plot_cases.save_cases_plots(df)
    # covid_plot_cases.save_infections_estimate(df)
