import datetime
import sys
import time
from lib2to3.pgen2.pgen import DFAState

import numpy as np
import pandas as pd
import tableauscraper
from dateutil.parser import parse as d
from dateutil.relativedelta import relativedelta

from utils_pandas import export
from utils_pandas import import_csv
from utils_scraping import any_in
from utils_scraping import logger
from utils_scraping import USE_CACHE_DATA
from utils_scraping_tableau import workbook_explore
from utils_scraping_tableau import workbook_iterate
from utils_scraping_tableau import workbook_series
from utils_scraping_tableau import workbook_value
from utils_thai import get_province
from utils_thai import today


########################
# Dashboards
########################


# # all province case numbers in one go but we already have this data
# url = "https://ddc.moph.go.th/covid19-dashboard/index.php?dashboard=scoreboard"

# # 5 kinds cases stats for last 30 days but we already have I think
# url = "https://ddc.moph.go.th/covid19-dashboard/index.php?dashboard=30-days"


def todays_data():
    url = "https://public.tableau.com/views/SATCOVIDDashboard/1-dash-tiles"
    # new day starts with new info comes in
    while True:
        # dates = reversed(pd.date_range("2021-01-24", today() - relativedelta(hours=7.5)).to_pydatetime())
        # get_wb, date = next(workbook_iterate(url, param_date=dates))
        # date = next(iter(date))
        # if (wb := get_wb()) is None:
        #     continue
        ts = tableauscraper.TableauScraper(verify=False)
        ts.loads(url)
        wb = ts.getWorkbook()

        last_update = wb.getWorksheet("D_UpdateTime (2)").data
        if last_update.empty:
            continue
        last_update = pd.to_datetime(last_update['max_update_date-alias'], dayfirst=False).iloc[0]
        if last_update >= today().date():
            return True
        # We got todays data too early
        print("z", end="")
        time.sleep(60)


def dash_daily():
    df = import_csv("moph_dashboard", ["Date"], False, dir="inputs/json")  # so we cache it

    # remove crap from bad pivot
    df = df.drop(columns=[c for c in df.columns if "Vac Given" in c and not any_in(c, "Cum",)])
    # somehow we got some dodgy rows. should be no neg cases 2021
    if 'Cases' in df.columns:
        df = df.drop(df[df['Cases'] == 0.0].index)
    # Fix spelling mistake
    if 'Postitive Rate Dash' in df.columns:
        df = df.drop(columns=['Postitive Rate Dash'])

    all_atk_reg = pd.DataFrame()

    allow_na = {
        "ATK": [[d("2021-07-31"), d("2022-07-05")], [d("2021-04-01"), d("2021-07-30"), 0.0, 0.0]],
        "Cases Area Prison": d("2021-05-12"),
        "Positive Rate Dash": (d("2021-07-01"), today() - relativedelta(days=14)),
        "Tests": today(),  # it's no longer there
        'Hospitalized Field HICI': d("2021-08-08"),
        'Hospitalized Field Hospitel': [[d("2021-08-04"), today(), 0.0], [d("2021-04-01"), d("2021-08-03"), 0.0, 0.0]],
        'Hospitalized Field Other': d("2021-08-08"),
        'Vac Given 1 Cum': (d("2021-08-01"), today() - relativedelta(days=4)),
        'Vac Given 2 Cum': (d("2021-08-01"), today() - relativedelta(days=4)),
        "Vac Given 3 Cum": (d("2021-08-01"), today() - relativedelta(days=4)),
        'Hospitalized Field': (d('2021-04-20'), today(), 100),
        'Hospitalized Respirator': (d("2021-03-25"), today(), 1),  # patchy before this
        'Hospitalized Severe': (d("2021-04-01"), today(), 10),  # try and fix bad values
        'Hospitalized Hospital': (d("2021-01-27"), today(), 1),
        'Recovered': (d('2021-01-01'), today(), 1),
        'Cases Walkin': (d('2021-01-01'), today(), 1),
        # 'Infections Non-Hospital Cum': (d("2022-04-08"), d("2022-06-12"), 800000),  # Redo older rows because cumsum cal was off
    }
    url = "https://public.tableau.com/views/SATCOVIDDashboard/1-dash-tiles"
    # new day starts with new info comes in
    dates = reversed(pd.date_range("2021-01-24", today() - relativedelta(hours=7.5)).to_pydatetime())
    # for get_wb, date in workbook_iterate(url, D_NewTL="DAY(txn_date)"):
    for get_wb, date in workbook_iterate(url, inc_no_param=True, param_date=dates):
        if date is None:
            # initial one which is today
            date = today()
        else:
            date = next(iter(date), None)
        if type(date) == str:
            date = d(date)
        if skip_valid(df, date, allow_na):
            continue
        if (wb := get_wb()) is None:
            continue

        row = pd.DataFrame()
        row = row.combine_first(workbook_series(wb, "D_NewTL", {
            "SUM(case_new)-value": "Cases",
            "DAY(txn_date)-value": "Date"
        }))

        # last_update = workbook_value(wb, None, "D_UpdateTime (2)", "Date", is_date=True)
        last_update = wb.getWorksheet("D_UpdateTime (2)").data
        if not last_update.empty:
            last_update = pd.to_datetime(
                last_update['max_update_date-alias'].str.replace("2565", "2022"), dayfirst=False).iloc[0]
            if last_update.normalize() < row.index.max() or date.date() > row.index.max().date():
                # We got todays data too early
                continue
        else:
            last_update = None

        row = row.combine_first(workbook_value(wb, date, "D_New", "Cases"))
        row = row.combine_first(workbook_value(wb, date, "D_Walkin", "Cases Walkin"))
        row = row.combine_first(workbook_value(wb, date, "D_Proact", "Cases Proactive"))
        row = row.combine_first(workbook_value(wb, date, "D_NonThai", "Cases Imported"))
        row = row.combine_first(workbook_value(wb, date, "D_Prison", "Cases Area Prison"))
        row = row.combine_first(workbook_value(wb, date, "D_Hospital", "Hospitalized Hospital"))
        row = row.combine_first(workbook_value(wb, date, "D_Severe", "Hospitalized Severe", np.nan))
        row = row.combine_first(workbook_value(wb, date, "D_SevereTube", "Hospitalized Respirator", np.nan))
        row = row.combine_first(workbook_value(wb, date, "D_Medic", "Hospitalized"))
        row = row.combine_first(workbook_value(wb, date, "D_Recov", "Recovered"))
        row = row.combine_first(workbook_value(wb, date, "D_Death", "Deaths"))
        row = row.combine_first(workbook_value(wb, date, "D_ATK", "ATK", np.nan))
        row = row.combine_first(workbook_value(wb, date, "D_HospitalField", "Hospitalized Field"))
        row = row.combine_first(workbook_value(wb, date, "D_Hospitel", "Hospitalized Field Hospitel"))
        row = row.combine_first(workbook_value(wb, date, "D_HICI", "Hospitalized Field HICI"))
        row = row.combine_first(workbook_value(wb, date, "D_HFieldOth", "Hospitalized Field Other"))
        row = row.combine_first(workbook_series(wb, "D_Lab2", {
            "AGG(% ติดเฉลี่ย)-value": "Positive Rate Dash",
            "DAY(txn_date)-value": "Date",
        }, np.nan))
        row = row.combine_first(workbook_series(wb, "D_Lab", {
            "AGG(% ติดเฉลี่ย)-alias": "Positive Rate Dash",
            "ATTR(txn_date)-alias": "Date",
        }, np.nan))

        row = row.combine_first(workbook_series(wb, "D_DeathTL", {
            "SUM(death_new)-value": "Deaths",
            "DAY(txn_date)-value": "Date"
        }))
        row = row.combine_first(workbook_series(wb, "D_Vac_Stack", {
            "DAY(txn_date)-value": "Date",
            "vaccine_plan_group-alias": {
                "1": "1 Cum",
                "2": "2 Cum",
                "3": "3 Cum",
            },
            "SUM(vaccine_total_acm)-value": "Vac Given",
        }))
        row = row.combine_first(workbook_series(wb, "D_RecovL", {
            "DAY(txn_date)-value": "Date",
            "SUM(recovered_new)-value": "Recovered"
        }))

        if row.empty:
            break

        # wb.getWorksheet("D_UpdateTime").data.iloc[0]
        assert date >= row.index.max()  # might be something broken with setParam for date
        row["Source Cases"] = "https://ddc.moph.go.th/covid19-dashboard/index.php?dashboard=main"
        if date < today() - relativedelta(days=30):  # TODO: should use skip_valid rules to work which are delayed rather than 0?
            row.loc[date] = row.loc[date].fillna(0.0)  # ATK and HICI etc are null to mean 0.0

        # Not date indexed as it's weekly
        atk_reg = wb.getWorksheet("ATK+WEEK_line_Total (1)").data
        if not atk_reg.empty:
            # It's the same value for all dates so only need on first iteration
            col = "Infections Non-Hospital Cum"  # ATK+?  no real explanation for this number
            atk_reg = atk_reg.rename(columns={"Week-value": "Week", 'SUM(#SETDATE_WEEK_CNT)-value': col})[["Week", col]]
            atk_reg['Date'] = (pd.to_numeric(atk_reg['Week']) * 7).apply(lambda x: pd.DateOffset(x) + d("2022-01-01"))
            atk_reg = atk_reg.set_index("Date")[[col]]
            # atk_reg = atk_reg.cumsum()
            all_atk_reg = all_atk_reg.combine_first(atk_reg)

        df = row.combine_first(df)  # prefer any updated info that might come in. Only applies to backdated series though
        logger.info("{} MOPH Dashboard {}", date, row.loc[row.last_valid_index():].to_string(index=False, header=False))
    # We get negative values for field hospital before April
    assert df[df['Recovered'] == 0.0].loc["2021-03-05":].empty
    df.loc[:"2021-03-31", 'Hospitalized Field'] = np.nan
    # 2022-05-07 and 03 got 0.0 by mistake
    df['Hospitalized Respirator'] = df['Hospitalized Respirator'].replace(0.0, np.nan)
    df["Hospitalized Severe"] = df["Hospitalized Severe"].replace(0.0, np.nan)
    df = all_atk_reg.cumsum().combine_first(df)
    export(df, "moph_dashboard", csv_only=True, dir="inputs/json")
    return df


def dash_ages():
    df = import_csv("moph_dashboard_ages", ["Date"], False, dir="inputs/json")  # so we cache it

    # Fix mistake in column name
    df.columns = [c.replace("Hospitalized Severe", "Cases Proactive") for c in df.columns]

    # Get deaths by prov, date. and other stats - timeline
    #
    # ['< 10 ปี', '10-19 ปี', '20-29 ปี', '30-39 ปี', '40-49 ปี', '50-59 ปี', '60-69 ปี', '>= 70 ปี', 'ไม่ระบุ']

    # D4_TREND
    # cases = AGG(stat_count)-alias
    # proactive = AGG(ผู้ติดเชื้อรายใหม่เชิงรุก)-alias,
    # deaths = AGG(ผู้เสียชีวิต)-alias (and AGG(ผู้เสียชีวิต (รวมทุกกลุ่มผู้ป่วย))-value  all patient groups)
    # cum cases = AGG(stat_accum)-alias
    # date  = DAY(date)-alias, DAY(date)-value
    url = "https://ddc.moph.go.th/covid19-dashboard/index.php?dashboard=select-trend-line"
    url = "https://dvis3.ddc.moph.go.th/t/sat-covid/views/SATCOVIDDashboard/4-dash-trend-w"
    url = "https://dvis3.ddc.moph.go.th/t/sat-covid/views/SATCOVIDDashboard/4-dash-trend?:isGuestRedirectFromVizportal=y&:embed=y"  # noqa

    def range2eng(range):
        return range.replace(" ปี", "").replace('ไม่ระบุ', "Unknown").replace(">= 70", "70+").replace("< 10", "0-9")

    for get_wb, idx_value in workbook_iterate(url, D4_CHART="age_range", verify=False):
        age_group = next(iter(idx_value))
        age_group = range2eng(age_group)
        skip = not pd.isna(df[f"Cases Age {age_group}"].get(str(today().date())))
        if skip or (wb := get_wb()) is None:
            continue
        row = workbook_series(
            wb,
            "D4_TREND", {
                "DAY(date)-value": "Date",
                "AGG(ผู้เสียชีวิต (รวมทุกกลุ่มผู้ป่วย))-value": "Deaths",
                "AGG(stat_count)-alias": "Cases",
                "AGG(ผู้ติดเชื้อรายใหม่เชิงรุก)-alias": "Cases Proactive",
            },
            index_col="Date"
        )
        if row.empty:
            continue
        row['Age'] = age_group
        row = row.pivot(values=["Deaths", "Cases", "Cases Proactive"], columns="Age")
        row.columns = [f"{n} Age {v}" for n, v in row.columns]
        df = row.combine_first(df)
        logger.info("{} MOPH Ages {} {}", row.last_valid_index(), range2eng(age_group),
                    row.loc[row.last_valid_index():].to_string(index=False, header=False))
    df = df.loc[:, ~df.columns.duplicated()]  # remove duplicate columns
    export(df, "moph_dashboard_ages", csv_only=True, dir="inputs/json")
    return df


def dash_trends_prov():
    df = import_csv("moph_dashboard_prov_trends", ["Date", "Province"], False, dir="inputs/json")  # so we cache it

    url = "https://dvis3.ddc.moph.go.th/t/sat-covid/views/SATCOVIDDashboard/4-dash-trend"

    for get_wb, idx_value in workbook_iterate(url, D4_CHART="province", verify=False):
        province = get_province(next(iter(idx_value)))
        date = str(today().date())
        try:
            df.loc[(date, province)]
            continue
        except KeyError:
            pass
        if (wb := get_wb()) is None:
            continue

        row = workbook_series(
            wb,
            None,
            'D4_TREND',
            {
                "DAY(date)-value": "Date",
                "AGG(ผู้เสียชีวิต (รวมทุกกลุ่มผู้ป่วย))-value": "Deaths",
                "AGG(stat_count)-alias": "Cases",
                "AGG(ผู้ติดเชื้อรายใหม่เชิงรุก)-alias": "Cases Proactive",
            },
        )

        if row.empty or province is None:
            continue
        row['Province'] = province
        df = row.reset_index("Date").set_index(["Date", "Province"]).combine_first(df)
        logger.info("{} DASH Trend prov {}", row.last_valid_index(),
                    row.loc[row.last_valid_index():].to_string(index=False, header=False))
    df = df.loc[:, ~df.columns.duplicated()]  # remove duplicate columns
    export(df, "moph_dashboard_prov_trends", csv_only=True, dir="inputs/json")  # Speeds up things locally
    return df


def dash_by_province():
    df = import_csv("moph_dashboard_prov", ["Date", "Province"], False, dir="inputs/json")  # so we cache it

    url = "https://public.tableau.com/views/SATCOVIDDashboard/2-dash-tiles-province-w"
    url = "https://public.tableau.com/views/SATCOVIDDashboard/2-dash-tiles-province-w?:size=1200,1050&:embed=y&:showVizHome=n&:bootstrapWhenNotified=y&:tabs=n&:toolbar=n&:apiID=host0"

    # Fix spelling mistake
    if 'Postitive Rate Dash' in df.columns:
        df = df.drop(columns=['Postitive Rate Dash'])
    if "Hospitalized Severe" in df.columns:
        df = df.drop(columns=["Hospitalized Severe"])  # was actually proactive

    last_pos_rate = max(df["Positive Rate Dash"].last_valid_index()[0], today() - relativedelta(days=31))
    valid = {
        # shouldn't be 0 pos rate. Maybe set to min 0.001 again later?
        "Positive Rate Dash": (d("2021-05-20"), last_pos_rate, 0.0, 2),  # Might have to remove it completely.
        "Tests": today(),  # It's no longer there
        "Vac Given 1 Cum": (d("2021-08-01"), today() - relativedelta(days=2), 1),
        "Vac Given 2 Cum": (d("2021-08-01"), today() - relativedelta(days=2)),
        "Vac Given 3 Cum": (d("2021-08-01"), today() - relativedelta(days=2)),
        # all the non-series will take too long to get historically
        "Cases Walkin": (d("2021-07-01"), today() - relativedelta(days=5)),
        "Cases Proactive": (d("2021-08-01"), today() - relativedelta(days=5)),
        "Cases Area Prison": (d("2021-08-01"), today() - relativedelta(days=5)),
        "Cases Imported": (d("2021-08-01"), today() - relativedelta(days=5)),
        "Deaths": d("2021-07-12"),  # Not sure why but Lamphun seems to be missing death data before here?
        "Cases": d("2021-06-28"),  # Only Lampang?
    }
    # Dates with no data and doesn't seem to change
    skip = [
        (d("2021-10-12"), "Mae Hong Son"),
        (d('2021-10-09'), 'Phayao'),
        (d('2021-10-09'), 'Nan'),
        (d('2021-10-07'), 'Nan'),
        (d("2021-10-04"), "Amnat Charoen"),
        (d('2021-10-04'), 'Nan'),
        (d('2021-10-03'), 'Nan'),
        (d("2021-10-02"), "Phrae"),
        (d("2021-10-02"), "Phayao"),
        (d('2021-10-02'), 'Nan'),
        (d("2021-10-01"), "Amnat Charoen"),
        (d('2021-10-01'), 'Chai Nat'),
        (d("2021-09-28"), "Phayao"),
        (d('2021-09-28'), 'Nan'),
        (d("2021-09-27"), "Phayao"),
        (d("2021-09-26"), "Mukdahan"),
        (d('2021-09-26'), 'Bueng Kan'),
        (d('2021-09-26'), 'Nan'),
        (d("2021-09-23"), 'Phayao'),
        (d("2021-09-22"), 'Nakhon Phanom'),
        (d('2021-09-21'), 'Nan'),
    ]

    dates = reversed(pd.date_range("2021-02-01", today() - relativedelta(hours=7.5)).to_pydatetime())
    #dates = [d.strftime('%m/%d/%Y') for d in dates]
    # Add in None for today as selecting today doesn't give us new data anymore. TODO: fix TableauScraper to remember the last data it had
    # prov_values = ['อุบลราชธานี', 'อุทัยธานี', 'อุตรดิตถ์', 'อุดรธานี', 'อำนาจเจริญ', 'อ่างทอง', 'หนองคาย', 'สุรินทร์', 'สุราษฎร์ธานี', 'สุพรรณบุรี', 'สระบุรี', 'สระแก้ว', 'สมุทรสาคร', 'สมุทรสงคราม', 'สมุทรปราการ', 'สตูล', 'สงขลา', 'สกลนคร', 'ศรีสะเกษ', 'เลย', 'ลำพูน', 'ลำปาง', 'ลพบุรี', 'ราชบุรี', 'ระยอง', 'ร้อยเอ็ด', 'ยะลา', 'ยโสธร', 'แม่ฮ่องสอน', 'มหาสารคาม', 'ภูเก็ต', 'แพร่',
    #                'เพชรบูรณ์', 'เพชรบุรี', 'พิษณุโลก', 'พิจิตร', 'ปัตตานี', 'ปราจีนบุรี', 'ประจวบคีรีขันธ์', 'ปทุมธานี', 'บุรีรัมย์', 'นนทบุรี', 'นครสวรรค์', 'นครศรีธรรมราช', 'นครราชสีมา', 'นครพนม', 'นครปฐม', 'นครนายก', 'ตาก', 'ตราด', 'ตรัง', 'เชียงใหม่', 'เชียงราย', 'ชุมพร', 'ชัยภูมิ', 'ชลบุรี', 'ฉะเชิงเทรา', 'จันทบุรี', 'ขอนแก่น', 'กาฬสินธุ์', 'กาญจนบุรี', 'กรุงเทพมหานคร', 'กระบี่']
    for get_wb, idx_value in workbook_iterate(url, inc_no_param=False, param_date=[None] + list(dates), D2_Province="province", verify=False):
        # for get_wb, idx_value in workbook_iterate(url, inc_no_param=False, param_date=[None] + list(dates), param_province=prov_values, verify=False):
        date, province = idx_value
        #date = d(date, dayfirst=False)
        if date is None:
            date = (today() - relativedelta(hours=7.5))
        if province is None:
            continue
        province = get_province(province)
        if (date, province) in skip or skip_valid(df, (date, province), valid):
            print("s", end="")
            continue
        if (wb := get_wb("D2_NewTL (2)")) is None:
            continue
        row = pd.DataFrame()
        row = row.combine_first(workbook_series(wb, "D2_NewTL", {
            "AGG(stat_count)-alias": "Cases",
            "DAY(txn_date)-value": "Date"
        }))
        last_update = row.last_valid_index()
        # TODO: ensure we are looking at the right provice. can't seem to get cur selection from wb.getWorksheet("D2_Province")
        # Need to work if the data has been updated yet. If it has last deaths should be today.
        if date.date() > last_update.date():
            # the date we are trying to get isn't the last deaths we know about. No new data yet
            logger.warning("{} MOPH Dashboard {}", date.date(), f"Skipping {province} as data update={last_update}")
            continue

        row = row.combine_first(workbook_value(wb, date, "D2_Walkin", "Cases Walkin"))
        row = row.combine_first(workbook_value(wb, date, "D2_Proact", "Cases Proactive"))
        row = row.combine_first(workbook_value(wb, date, "D2_Prison", "Cases Area Prison"))
        row = row.combine_first(workbook_value(wb, date, "D2_NonThai", "Cases Imported"))
        row = row.combine_first(workbook_value(wb, date, "D2_New", "Cases"))
        row = row.combine_first(workbook_value(wb, date, "D2_Death", "Deaths"))
        row = row.combine_first(workbook_series(wb, "D2_DeathTL", {
            "AGG(num_death)-value": "Deaths",
            "DAY(txn_date)-value": "Date"
        }, end=date))
        row = row.combine_first(workbook_series(wb, "D_Lab2", {
            "AGG(% ติดเฉลี่ย)-value": "Positive Rate Dash",
            "DAY(txn_date)-value": "Date",
        }, np.nan))
        row = row.combine_first(workbook_series(wb, "D_Lab", {
            "AGG(% ติดเฉลี่ย)-value": "Positive Rate Dash",
            "DAY(txn_date)-value": "Date",
        }, np.nan))
        row = row.combine_first(workbook_series(wb, "D2_Vac_Stack", {
            "DAY(txn_date)-value": "Date",
            "vaccine_plan_group-alias": {
                "1": "1 Cum",
                "2": "2 Cum",
                "3": "3 Cum",
            },
            "SUM(vaccine_total_acm)-value": "Vac Given",
        }))

        row['Province'] = province
        df = row.reset_index("Date").set_index(["Date", "Province"]).combine_first(df)
        logger.info("{} MOPH Dashboard {}", date.date(),
                    row.loc[row.last_valid_index():].to_string(index=False, header=False))
        if USE_CACHE_DATA:
            # Save as we go to help debugging
            export(df, "moph_dashboard_prov", csv_only=True, dir="inputs/json")
    export(df, "moph_dashboard_prov", csv_only=True, dir="inputs/json")  # Speeds up things locally

    return df


def skip_valid(df, idx_value, allow_na={}):
    "return true if we have a value already for this index or the value isn't in range"

    if type(idx_value) == tuple:
        date, prov = idx_value
        idx_value = (str(date.date()) if date else None, prov)
    else:
        date = idx_value
        idx_value = str(date.date()) if date else None
    # Assume index of df is in the same order as params
    if df.empty:
        return False

    def is_valid(column, date, idx_value, limits=None):
        if limits is None:
            limits = allow_na.get(column, None)
            if limits is None:
                return True
            elif type(limits) in (list, tuple) and type(limits[0]) in (list, tuple):
                # multiple rules. recurse
                return all([is_valid(column, date, idx_value, limits=rule) for rule in limits])
            else:
                pass
        maxdate = today()
        mins = []
        if type(limits) in [tuple, list]:
            mindate, *limits = limits
            if limits:
                maxdate, *mins = limits
        elif limits is None:
            mindate = d("1975-1-1")
        else:
            mindate = limits
        if not(date is None or mindate <= date <= maxdate):
            return True
        try:
            val = df.loc[idx_value][column]
        except KeyError:
            return False
        if pd.isna(val):
            return False
        if not mins:
            return True

        min_val, *max_val = mins
        if type(val) == str:
            return False
        elif min_val > val:
            return False
        elif max_val and val > max_val[0]:
            return False
        else:
            return True

    # allow certain fields null if before set date
    nulls = [c for c in df.columns if not is_valid(c, date, idx_value)]
    if not nulls:
        return True
    else:
        logger.info("{} MOPH Dashboard Retry Missing data at {} for {}. Retry", date, idx_value, nulls)
        return False


def check_dash_ready():
    print("Waiting for today's dashboard update ")
    gottoday = todays_data()
    print(gottoday)
    sys.exit(0 if gottoday else 1)


if __name__ == '__main__':
    # check_dash_ready()

    dash_ages_df = dash_ages()
    dash_by_province_df = dash_by_province()
    dash_daily_df = dash_daily()

    # This doesn't add any more info since severe cases was a mistake
#    dash_trends_prov_df = dash_trends_prov()
