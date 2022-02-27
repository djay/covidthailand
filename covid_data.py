import json
import os
import shutil
from multiprocessing import Pool

import pandas as pd

import covid_data_api
import covid_data_bed
import covid_data_briefing
import covid_data_dash
import covid_data_situation
import covid_data_testing
import covid_data_tweets
import covid_data_vac
from utils_pandas import add_data
from utils_pandas import export
from utils_pandas import import_csv
from utils_scraping import CHECK_NEWER
from utils_scraping import logger
from utils_scraping import USE_CACHE_DATA
from utils_scraping import web_files
from utils_thai import DISTRICT_RANGE
from utils_thai import get_fuzzy_provinces
from utils_thai import join_provinces
from utils_thai import today


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
# health district 8 data - https://r8way.moph.go.th/r8way/covid-19


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
        api_provs = pool.apply_async(covid_data_api.timeline_by_province)

        dash_daily = pool.apply_async(covid_data_dash.dash_daily)
        # These 3 are slowest so should go first
        dash_by_province = pool.apply_async(covid_data_dash.dash_by_province)
        # This doesn't add any more info since severe cases was a mistake
        # dash_trends_prov = pool.apply_async(covid_data_dash.dash_trends_prov)
        vac_slides = pool.apply_async(covid_data_vac.vac_slides)
        vac_reports_and_prov = pool.apply_async(covid_data_vac.vaccination_reports)
        beds = pool.apply_async(covid_data_bed.get_df)

        # TODO: split vac slides as that's the slowest

        briefings_prov__cases_briefings = pool.apply_async(covid_data_briefing.get_cases_by_prov_briefings)

        dash_ages = pool.apply_async(covid_data_dash.dash_ages)

        # today_situation = pool.apply_async(covid_data_situation.get_situation_today)
        th_situation = pool.apply_async(covid_data_situation.get_thai_situation)
        en_situation = pool.apply_async(covid_data_situation.get_en_situation)

        cases_demo__risks_prov = pool.apply_async(covid_data_api.get_cases_by_demographics_api)

        tweets_prov__twcases = pool.apply_async(covid_data_tweets.get_cases_by_prov_tweets)
        timelineapi = pool.apply_async(covid_data_api.get_cases)

        tests = pool.apply_async(covid_data_testing.get_tests_by_day)
        tests_reports = pool.apply_async(covid_data_testing.get_test_reports)

        xcess_deaths = pool.apply_async(covid_data_api.excess_deaths)
        case_api_by_area = pool.apply_async(covid_data_api.get_cases_by_area_api)  # can be very wrong for the last days

        ihme_dataset = pool.apply_async(covid_data_api.ihme_dataset)

        # Now block getting until we get each of the data
        # today_situation = today_situation.get()
        th_situation = th_situation.get()
        en_situation = en_situation.get()

        dash_daily = dash_daily.get()
        dash_ages = dash_ages.get()
        dash_by_province = dash_by_province.get()
        # dash_trends_prov = dash_trends_prov.get()

        vac_reports, vac_reports_prov = vac_reports_and_prov.get()
        vac_slides = vac_slides.get()
        ihme_dataset = ihme_dataset.get()
        briefings_prov, cases_briefings = briefings_prov__cases_briefings.get()
        cases_demo, risks_prov = cases_demo__risks_prov.get()

        tweets_prov, twcases = tweets_prov__twcases.get()
        timelineapi = timelineapi.get()

        tests = tests.get()
        tests_reports = tests_reports.get()

        xcess_deaths.get()
        case_api_by_area = case_api_by_area.get()  # can be very wrong for the last days

        beds = beds.get()

        api_provs = api_provs.get()

    # Combine dashboard data
    # dash_by_province = dash_trends_prov.combine_first(dash_by_province)
    export(dash_by_province, "moph_dashboard_prov", csv_only=True, dir="inputs/json")
    # "json" for caching, api so it's downloadable
    shutil.copy(os.path.join("inputs", "json", "moph_dashboard_prov.csv"), "api")
    shutil.copy(os.path.join("inputs", "json", "moph_dashboard.csv"), "api")
    shutil.copy(os.path.join("inputs", "json", "moph_dashboard_ages.csv"), "api")
    shutil.copy(os.path.join("inputs", "json", "moph_bed.csv"), "api")

    # Export briefings
    briefings = import_csv("cases_briefings", ["Date"], not USE_CACHE_DATA)
    briefings = briefings.combine_first(cases_briefings).combine_first(twcases)
    export(briefings, "cases_briefings")

    # Export per province
    dfprov = import_csv("cases_by_province", ["Date", "Province"], not USE_CACHE_DATA)
    dfprov = dfprov.combine_first(
        api_provs).combine_first(
        briefings_prov).combine_first(
        dash_by_province).combine_first(
        tweets_prov).combine_first(
        risks_prov)  # TODO: check they agree
    dfprov = join_provinces(dfprov, on="Province")
    if "Hospitalized Severe" in dfprov.columns:
        # Made a mistake. This is really Cases Proactive
        dfprov["Cases Proactve"] = dfprov["Hospitalized Severe"]
        dfprov = dfprov.drop(columns=["Hospitalized Severe"])
    export(dfprov, "cases_by_province")

    # Export per district (except tests which are dodgy?)
    by_area = prov_to_districts(dfprov[[c for c in dfprov.columns if "Tests" not in c]])

    cases_by_area = import_csv("cases_by_area", ["Date"], not USE_CACHE_DATA)
    cases_by_area = cases_by_area.combine_first(by_area).combine_first(case_api_by_area)
    export(cases_by_area, "cases_by_area")

    # Export IHME dataset
    export(ihme_dataset, "ihme")

    # Export situation
    situation = covid_data_situation.export_situation(th_situation, en_situation)

    vac = covid_data_vac.export_vaccinations(vac_reports, vac_reports_prov, vac_slides)

    logger.info("========Combine all data sources==========")
    df = pd.DataFrame(columns=["Date"]).set_index("Date")
    for f in [tests_reports, tests, cases_briefings, timelineapi, twcases, cases_demo, cases_by_area, situation, vac, dash_ages, dash_daily]:
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
