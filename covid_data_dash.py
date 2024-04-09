import datetime
import sys
import time
from lib2to3.pgen2.pgen import DFAState

import numpy as np
import pandas as pd
import tableauscraper
from bs4 import BeautifulSoup
from dateutil.parser import parse as d
from dateutil.relativedelta import relativedelta

import covid_plot_cases
import covid_plot_deaths
import covid_plot_vacs
from utils_pandas import cum2daily
from utils_pandas import export
from utils_pandas import import_csv
from utils_pandas import weeks_to_end_date
from utils_scraping import any_in
from utils_scraping import logger
from utils_scraping import USE_CACHE_DATA
from utils_scraping import web_files
from utils_scraping_tableau import force_setParameter
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
    return df

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

    atk_days = set([pd.DateOffset(week * 7) + d("2022-01-01") for week in range(15, 40)])

    def valid_atk(df, date):
        if date not in atk_days:
            return True
        # Get rid of cumulative values. Just store weekly now. # TODO: change the col name
        if 800000 > df.loc[date]['Infections Non-Hospital Cum'] > 50000:
            return True
        return False

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
        if skip_valid(df, date, allow_na) and valid_atk(df, date):
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
    df = all_atk_reg.combine_first(df)
    export(df, "moph_dashboard", csv_only=True, dir="inputs/json")
    return df


def dash_weekly(file="moph_dash_weekly"):
    df = import_csv(file, ["Date"], False, dir="inputs/json")  # so we cache it

    allow_na = {
        'Vac Given 1 Cum': (d("2021-08-01"), d("2023-05-23")),
        'Vac Given 2 Cum': (d("2021-08-01"), d("2023-05-23")),
        "Vac Given 3 Cum": (d("2021-08-01"), d("2023-05-23")),
        'Hospitalized Respirator': (d("2021-03-25"), today(), 1),  # patchy before this
        'Hospitalized Severe': (d("2021-04-01"), today(), 10),  # try and fix bad values
        'Cases Cum': (d("2022-09-17"), today(), 4625384),
        'Deaths Male': (d("2024-01-01"), today()),
    }

    url = "https://public.tableau.com/views/SATCOVIDDashboard_WEEK/1-dash-week"
    # aggregated for week ending on sat
    dates = reversed(pd.date_range("2022-09-25", today() - relativedelta(days=1, hours=7.5), freq='W-SAT').to_pydatetime())

    latest = next(dates, None)
    for get_wb, this_index in workbook_iterate(url, inc_no_param=False, param_date_weekend=[None] + list(dates)):
        # date, wave = this_index
        date = this_index[0]
        date = date if date is not None else latest
        if skip_valid(df, date, allow_na):
            continue
        if (wb := get_wb()) is None:
            continue

        # end_date = workbook_value(wb, None, "D_UpdateTime (2)", "Date", is_date=True)
        # if end_date.date() != date.date():
        #     # we have a problem. not setting the date right
        #     continue

        # TODO: should be part of workbook_iterate so its done once.
        row_since2023 = row = extract_basics(wb, date)
        if row_since2023.empty:
            logger.warning("{} MOPH Dashboard: wrong date: skip", date)
            continue

        wb = force_setParameter(wb, "param_wave", "ตั้งแต่เริ่มระบาด")
        # We miss data not effected by wave
        row_update = extract_basics(wb, date, check_date=False)
        assert not row_update.empty
        row = row_update.combine_first(row_since2023)

        df = row.combine_first(df)  # prefer any updated info that might come in. Only applies to backdated series though
        logger.info("{} MOPH Dashboard {}", date, row.loc[row.last_valid_index():].to_string(index=False, header=False))
    export(df, file, csv_only=True, dir="inputs/json")
    return df


def closest(sub, repl):
    def get_name(wb, name):
        return {s.name.replace(sub, repl): s.name for s in wb.worksheets}.get(name)
    return get_name


def dash_province_weekly(file="moph_province_weekly"):
    df = import_csv(file, ["Date", "Province"], False, dir="inputs/json")  # so we cache it

    # Remove any dips in cumualtive values. can be caused by getting daily instead#
    # lambda mydf: mydf.loc[mydf['Cases Cum'].ffill() < mydf['Cases Cum'].cummax().ffill(), 'Cases Cum'] = np.nan
    # decresed = df[(df[[c for c in df.columns if " Cum" in c]].groupby("Province").diff() < 0).any(axis=1)]
    contiguous = df[["Cases Cum", "Deaths Cum"]].dropna()
    dec1 = contiguous[(contiguous.groupby("Province").diff() < 0).any(axis=1)]  # in case its the drop thats wrong
    dec2 = contiguous[(contiguous.groupby("Province").diff(-1) > 0).any(axis=1)]  # In case its a spike thats wrong
    contiguous = df[["Vac Given 1 Cum", "Vac Given 2 Cum", "Vac Given 3 Cum"]].dropna()
    dec3 = contiguous[(contiguous.groupby("Province").diff() < 0).any(axis=1)]
    dec4 = contiguous[(contiguous.groupby("Province").diff(-1) > 0).any(axis=1)]
    decreased = dec1.combine_first(dec2).combine_first(dec3).combine_first(dec4)
    # Just remove bad rows
    df = df.drop(index=decreased.index)

    valid = {
        # "Deaths Cum": (d("2022-12-11"), today(), 1),
        "Cases Cum": (today() - relativedelta(days=22), today(), 150),  # TODO: need better way to reject this year cum values
        # 'Vac Given 1 Cum': (today() - relativedelta(days=22), today() - relativedelta(days=4)),
        # 'Vac Given 1 Cum': (d("2021-08-01"), d("2023-05-23")),

    }
    url = "https://public.tableau.com/views/SATCOVIDDashboard_WEEK/2-dash-week-province"
    dates = reversed(pd.date_range("2022-01-01", today() - relativedelta(hours=7.5), freq='W-SAT').to_pydatetime())
    # dates = iter([d.strftime("%m/%d/%Y") for d in dates])
    latest = next(dates, None)
    # ts = tableauscraper.TableauScraper()
    # try:
    #     ts.loads(url)
    # except AttributeError as e:
    #     # Somethign is messed up about the page returned to scrape
    #     logger.error("{} MOPH Dashboard: Can't scrape {}", e)
    #     return df
    # provs = ts.getWorkbook().getWorksheet("D2_Province (2)").getSelectableValues("province")
    _, content, _ = next(web_files("https://ddc.moph.go.th/covid19-dashboard/?dashboard=province", proxy=False, check=True))
    if content is None:
        return df
    soup = BeautifulSoup(content, 'html.parser')
    # soup = parse_file(file, html=True, paged=False)
    provs = [p.get("value") for p in soup.select("#sel-province")[0].find_all("option") if p.get("value")]

    for get_wb, idx_value in workbook_iterate(url, inc_no_param=False, param_date_weekend=[None] + list(dates), filters=dict(province=provs), verify=False):
        # for get_wb, idx_value in workbook_iterate(url, inc_no_param=False, param_date=list(dates), D2_Province="province", verify=False):
        date, province = idx_value
        if date is None:
            date = latest
        # date = d(date, dayfirst=False)
        if province is None:
            continue
        province = get_province(province)
        # TODO: make invalid not inc Cum values
        if skip_valid(df, (date, province), valid) and (date, province) not in decreased.index:
            print("s", end="")
            continue
        if (wb := get_wb()) is None:
            continue
        "D2_Update (2)"
        row = extract_basics(wb, date)
        if row.empty:
            logger.warning("{} MOPH Dashboard: wrong date: skip {}", date, province)
            continue  # Not getting latest data yet

        wb = force_setParameter(wb, "param_wave", "ตั้งแต่เริ่มระบาด")
        # We miss data not effected by wave
        row_update = extract_basics(wb, date, check_date=False, base_df=row)
        assert not row_update.empty
        row = row_update.combine_first(row)

        row['Province'] = province
        row = row.reset_index().set_index(["Date", "Province"])

        combined = row.combine_first(df)

        # Test if this creates a dip
        contiguous = combined[["Cases Cum", "Deaths Cum"]]
        dec1 = contiguous[(contiguous.groupby("Province").diff() < 0).any(axis=1)]  # in case its the drop thats wrong
        bad_rows = dec1.index.intersection(row.index)
        if len(bad_rows) > 0:
            logger.info("{} MOPH dash, dropping invalid row. cum value not inc. {}", bad_rows,
                        row.loc[bad_rows].to_string(index=False, header=False))
            df = combined.drop(bad_rows)
            # TODO: Some rows don't seem to show a drop
            # TODO: We should drop the row before as well as that might be the source of the bad data.
        else:
            df = combined
            logger.info("{} MOPH Dashboard {}", row.index.max(),
                        row.loc[row.last_valid_index():].to_string(index=False, header=False))
    export(df, file, csv_only=True, dir="inputs/json")

    # Vac Given 3 Cum seems to be 3+4+5+6 which is wrong
    df = df.drop(columns=["Vac Given 3 Cum"])
    return df


def extract_basics(wb, date, check_date=True, base_df=None):

    row = pd.DataFrame()
    # D_CaseNew_/7 - daily avg cases
    # D_DeathNew_/7 - daily avg deaths
    # D_Death (2)

    def to_cum(cum, periodic, name):
        """ take a single cum value and daily changes and return a cum series going backwards """
        cum_name = name + " Cum"
        if periodic.empty or not cum[cum_name].last_valid_index():
            return pd.DataFrame()
        periodic = periodic[name]
        cum = cum[cum_name]

        periodic = periodic.replace(np.nan, 0)
        assert cum.last_valid_index()
        combined = periodic.combine_first(cum)  # TODO: this might go back too far. should really be min of periodic?
        df = cum.reindex(combined.index).bfill().subtract(periodic.reindex(
            combined.index)[::-1].cumsum()[::-1].shift(-1), fill_value=0)
        assert (df > 0).any()
        return df.to_frame(cum_name).ffill()

    vacs = workbook_series(wb, ["D_Vac2Table"], {
        "vaccine_plan_group-alias": {
            "1": "1 Cum",
            "2": "2 Cum",
            "3": "3 Cum",
        },
        "SUM(vaccine_total_acm)-alias": "Vac Given",
    }, index_date=False, index_col="Date", index_value=date)

    vacs_dates = workbook_series(wb, ["D_Vac_Stack (2)", "D2_Vac_Stack (2)"], {
        "DAY(txn_date)-value": "Date",
        "vaccine_plan_group-alias": {
            "1": "1 Cum",
            "2": "2 Cum",
            "3": "3 Cum",
        },
        "SUM(vaccine_total_acm)-value": "Vac Given",
        # "SUM(vaccine_total_acm)-alias": "Vac Given",
    }, index_date=True)

    cases = workbook_series(wb, ["D_NewTL (2)", "D2_NewTL (2)"], {
        "SUM(case_new)-value": "Cases",
        "AGG(stat_count)-value": "Cases",
        "AGG(STAT_COUNT)-value": "Cases",
        "ATTR(week)-alias": "Week"
    }, index_col="Week", index_date=False)
    cases = weeks_to_end_date(cases, year_col="Year", week_col="Week", offset=0, date=date)
    if cases.empty and base_df is not None and 'Cases' in base_df.columns:
        cases = base_df[['Cases']]

    deaths = workbook_series(wb, ["D_DeathTL (2)", "D2_DeathTL (2)"], {
        "SUM(death_new)-value": "Deaths",
        "AGG(# NUM_DEATH)-value": "Deaths",
        "ATTR(week)-alias": "Week"
    }, index_col="Week", index_date=False)
    deaths = weeks_to_end_date(deaths, year_col="Year", week_col="Week", offset=0, date=date)
    if deaths.empty and base_df is not None and 'deaths' in base_df.columns:
        deaths = base_df[['Deaths']]

    # There is no date in the data to tell us that its returning the correct data except for the
    # the deaths and cases. lets just look if we got latest instead.
    if check_date and ((not deaths.empty and date < deaths.index.max()) or (not cases.empty and date < cases.index.max())):
        return row
    # date = cases.index.max()  # We can't get update date always so use lastest cases date
    cases_cum = workbook_value(wb, date, ["D_NewACM (2)", "D2_NewACM (2)"], "Cases Cum", default=np.nan)
    deaths_cum = workbook_value(wb, date, ["D_DeathACM (2)", "D2_DeathACM (2)"], "Deaths Cum", default=np.nan)
    row = row.combine_first(cases_cum)
    row = row.combine_first(to_cum(row, cases, "Cases")).combine_first(cases)
    row = row.combine_first(deaths_cum)
    if not deaths.empty:
        row = row.combine_first(to_cum(row, deaths, "Deaths")).combine_first(deaths)

    row = row.combine_first(workbook_value(wb, date, "D_Severe (2)", "Hospitalized Severe", None))
    row = row.combine_first(workbook_value(wb, date, "D_SevereTube (2)", "Hospitalized Respirator", None))

    ages = workbook_series(wb, 'cvd_agegroup', {'Measure Values-value': 'Deaths',
                           "Measure Names-alias": "Age Group"}, index_col="Age Group", index_date=False)
    gender = workbook_series(wb, 'cvd_gender', {'Measure Values-value': 'Deaths',
                             "Measure Names-alias": "Gender"}, index_col="Gender", index_date=False)

    if not ages.empty:
        ages['Date'] = deaths.index.max()
        ages = ages.reset_index().pivot(columns=['Age Group'], values=["Deaths"], index=["Date"])
        ages.columns = [f"Deaths Age {a}" for a in ["0-4", "10-19", "20-49", "5-9", "50-59", "60-69", "70+"]]
        gender['Date'] = deaths.index.max()
        gender = gender.reset_index().pivot(columns=['Gender'], values=["Deaths"], index=["Date"])
        gender.columns = [f"Deaths {g}" for g in ["Male", "Female"]]
        row = row.combine_first(gender).combine_first(ages)

    # TODO: should switch from weekly?
    row = row.combine_first(vacs).combine_first(vacs_dates)
    return row


def dash_ages():
    df = import_csv("moph_dashboard_ages", ["Date"], False, dir="inputs/json")  # so we cache it
    return df  # no longer there

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
        age_group = next(iter(idx_value), None)
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
        province = get_province(next(iter(idx_value), None))
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
    # Vac Given 3 Cum seems to be 3+4+5+6 which is wrong
    df = df.drop(columns=["Vac Given 3 Cum"])
    return df

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

    dash_daily_df = dash_weekly()
    dash_by_province_df = dash_province_weekly()
    dash_by_province_daily = dash_by_province()
    # dash_ages_df = dash_ages()

    # This doesn't add any more info since severe cases was a mistake
#    dash_trends_prov_df = dash_trends_prov()

    df = import_csv("combined", index=["Date"], date_cols=["Date"])
    prov = import_csv("cases_by_province", index=["Date", "Province"], date_cols=["Date"])

    # df = dash.combine(df, lambda s1, s2: s1)
    # df = briefings.combine(df, lambda s1, s2: s1)
    vaccols = [f"Vac Given {d} Cum" for d in range(1, 5)]
    daily_prov = cum2daily(dash_by_province_df, exclude=vaccols)
    prov = dash_by_province_daily.combine_first(daily_prov).combine_first(prov)
    # Write this one as it's imported
    export(prov, "cases_by_province", csv_only=True)

    daily = cum2daily(dash_daily_df, exclude=vaccols)
    df = daily.combine_first(df)
    export(df, "combined", csv_only=True)

    covid_plot_vacs.save_vacs_prov_plots(df)
    covid_plot_vacs.save_vacs_plots(df)
    covid_plot_deaths.save_deaths_plots(df)
    covid_plot_cases.save_cases_plots(df)
