import os
import re

import matplotlib.cm
import numpy as np
import pandas as pd

import utils_thai
from covid_plot_utils import plot_area
from covid_plot_utils import source
from utils_pandas import cum2daily
from utils_pandas import fix_gaps
from utils_pandas import get_cycle
from utils_pandas import import_csv
from utils_pandas import normalise_to_total
from utils_pandas import perc_format
from utils_pandas import pred_vac
from utils_pandas import rearrange
from utils_pandas import topprov
from utils_scraping import any_in
from utils_scraping import logger
from utils_thai import AREA_LEGEND_SIMPLE
from utils_thai import DISTRICT_RANGE_SIMPLE
from utils_thai import FIRST_AREAS
from utils_thai import get_provinces
from utils_thai import thaipop
from utils_thai import trend_table


def save_vacs_plots(df: pd.DataFrame) -> None:
    logger.info('======== Generating Vaccinations Plots ==========')

    ####################
    # Vaccines
    ####################
    manuf = ["Sinovac", "AstraZeneca", "Sinopharm", "Pfizer", "Moderna"]
    man_cols = pd.DataFrame()
    for m in manuf:
        man_cols[m] = df[[c for c in df.columns if f"Given {m}" in str(c)]].sum(axis=1)
    man_cols = man_cols.replace(0.0, np.nan).interpolate().diff().replace(0.0, np.nan)
    plot_area(df=man_cols,
              title='Covid Vaccinations by Manufacturer - Thailand',
              cols_subset=list(man_cols.columns),
              png_prefix='vac_manuf',
              periods_to_plot=["3", "all"],
              ma_days=7,
              kind='line', stacked=False, percent_fig=False,
              footnote_left=f'{source}Data Source: DDC Daily Vaccination Reports')

    def clean_vac_leg(label, first="1st Jab", second="2nd Jab"):
        c = label
        c = re.sub(r"(?:Vac )?(?:Group )?(.*) (?:1|Only 1)(?: Cum)?", fr"{first} - \1", c)
        c = re.sub(r"(?:Vac )?(?:Group )?(.*) 2(?: Cum)?", fr"{second} - \1", c)
        c = re.sub(r"(?:Vac )?(?:Group )?(.*) 3(?: Cum)?", r"3rd Booster/Other \1", c)
        c = re.sub(r"(.*) (?:Only|Given)", r"\1", c)
        c = c.replace(
            'General Population', 'General Population (0-59)').replace(
            'Risk: Location', 'General Population (0-59)').replace(
            'Student', 'Students 12-17').replace(
            'Medical All', 'Medical Staff & Volunteers').replace(
            'Risk: Disease', 'Risk from 7 Diseases',).replace(
            'Risk: Pregnant', 'Pregnant',
        )
        return c

    groups = [c for c in df.columns if str(c).startswith('Vac Group')]
    df_vac_groups = df['2021-02-28':][groups]
    # Too many groups. Combine some for now
    # for dose in range(1, 4):
    #     df_vac_groups[f"Vac Group Risk: Location {dose} Cum"] = df_vac_groups[
    #         f"Vac Group Risk: Location {dose} Cum"].add(df_vac_groups[f'Vac Group Risk: Pregnant {dose} Cum'],
    #                                                     fill_value=0)
    #     df_vac_groups[f"Vac Group Medical Staff {dose} Cum"] = df_vac_groups[f"Vac Group Medical Staff {dose} Cum"].add(
    #         df_vac_groups[f'Vac Group Health Volunteer {dose} Cum'], fill_value=0)
    # groups = [c for c in groups if "Pregnant" not in c and "Volunteer" not in c and " 3 " not in c]
    df_vac_groups = df_vac_groups[groups]

    # go backwards to get rid of "dips". ie take later value as correct. e.g. 2021-06-21
    df_vac_groups = df_vac_groups.reindex(index=df_vac_groups.index[::-1])
    df_vac_groups = df_vac_groups.cummin()  # if later corrected down, take that number into past
    df_vac_groups = df_vac_groups.reindex(index=df_vac_groups.index[::-1])
    # We have some missing days so interpolate e.g. 2021-05-04
    df_vac_groups = df_vac_groups.interpolate(method="time", limit_area="inside")

    # TODO: should we use actual Given?
    df_vac_groups['Vac Given Cum'] = df[[f'Vac Given {d} Cum' for d in range(1, 4)]].sum(axis=1, skipna=False)
    df_vac_groups['Vac Given'] = df[[f'Vac Given {d}' for d in range(1, 4)]].sum(axis=1, skipna=False)
    df_vac_groups['Vac Given 1 Cum'] = df['Vac Given 1 Cum']
    df_vac_groups['Vac Given 2 Cum'] = df['Vac Given 2 Cum']
    df_vac_groups['Vac Given 3 Cum'] = df['Vac Given 3 Cum']
    # risk location is really general population and that now includes frontline and pregnant
    gen_groups = ['Risk: Pregnant', 'Other Frontline Staff', 'Risk: Location']
    for d in range(1, 4):
        df_vac_groups[f'Vac Group General Population {d} Cum'] = df_vac_groups[[
            f'Vac Group {g} {d} Cum' for g in gen_groups]].sum(axis=1, skipna=True, min_count=1)
        df_vac_groups = df_vac_groups.drop(columns=[f'Vac Group {g} {d} Cum' for g in gen_groups])
    groups = [c for c in df_vac_groups.columns if str(c).startswith('Vac Group')]

    df_vac_groups['Vac Imported Cum'] = df_vac_groups[[
        c for c in df_vac_groups.columns if "Vac Imported" in c]].sum(axis=1, skipna=False)

    # now convert to daily and interpolate and then normalise to real daily total.
    vac_daily = cum2daily(df_vac_groups)
    # bring in any daily figures we might have collected first
    vac_daily = df[['Vac Given', 'Vac Given 1', 'Vac Given 2', 'Vac Given 3']].combine_first(vac_daily)
    daily_cols = [c for c in vac_daily.columns if c.startswith(
        'Vac Group') and ' 3' not in c] + ['Vac Given 3']  # Keep for unknown
    # We have "Medical All" instead
    daily_cols = [c for c in daily_cols if not any_in(c, "Medical Staff", "Volunteer")]
    # interpolate to fill gaps and get some values for each group
    vac_daily[daily_cols] = vac_daily[daily_cols].interpolate(method="time", limit_area="inside")
    # now normalise the filled in days so they add to their real total
    vac_daily = vac_daily.pipe(normalise_to_total, daily_cols, 'Vac Given')

    # vac_daily['7d Runway Rate'] = (df['Vac Imported Cum'].fillna(method="ffill") - df_vac_groups['Vac Given Cum']) / 7
    days_to_target = (pd.Timestamp('2022-01-01') - vac_daily.index.to_series()).dt.days
    vac_daily['Target Rate 1'] = (50000000 - df_vac_groups['Vac Given 1 Cum']) / days_to_target
    vac_daily['Target Rate 2'] = (50000000 * 2 - df_vac_groups['Vac Given 2 Cum']) / days_to_target

    #daily_cols = rearrange(daily_cols, 2, 1, 4, 3, 10, 9, 8, 7, 6, 5)
    daily_cols = [c for c in daily_cols if "2" in c] + [c for c in daily_cols if "1" in c] + [c for c in daily_cols if "3" in c]

    plot_area(df=vac_daily,
              title='Daily Covid Vaccinations by Priority Groups - Thailand',
              legends=[
                  # 'Doses per day needed to run out in a week',
                  # 'Rate for 70% 1st Jab in 2021',
                  # 'Rate for 70% 2nd Jab in 2021'
              ] + [clean_vac_leg(c) for c in daily_cols],  # bar puts the line first?
              legend_cols=2,
              png_prefix='vac_groups_daily', cols_subset=daily_cols,
              between=[
                  # '7d Runway Rate',
                  # 'Target Rate 1',
                  # 'Target Rate 2'
              ],
              periods_to_plot=["30d", "3"],  # too slow to do all
              ma_days=None,
              kind='bar', stacked=True, percent_fig=False,
              cmap=get_cycle('tab20', len(daily_cols) - 1, extras=["grey"], unpair=True),
              footnote_left=f'{source}Data Source: DDC Daily Vaccination Reports')

    # # Now turn daily back to cumulative since we now have estimates for every day without dips
    # vac_cum = vac_daily.cumsum().combine_first(vac_daily[daily_cols].fillna(0).cumsum())
    # vac_cum.columns = [f"{c} Cum" for c in vac_cum.columns]
    # # Not sure why but we end up with large cumulative than originally so normalise
    # for c in groups:
    #     vac_cum[c] = vac_cum[c] / vac_cum[groups].sum(axis=1) * df_vac_groups['Vac Given Cum']

    vac_cum = df_vac_groups

    # TODO: adjust allocated for double dose group
    # second_dose = [c for c in groups if "2 Cum" in c]
    # first_dose = [c for c in groups if "1 Cum" in c]
    # vac_cum['Available Vaccines Cum'] = df['Vac Imported Cum'].fillna(method="ffill") - vac_cum[second_dose].sum(axis=1)

    cols = []
    # We want people vaccinated not total doses
    for c in groups:
        if "1" in c:
            vac_cum[c.replace(" 1 Cum", " Only 1 Cum")] = vac_cum[c].sub(vac_cum[c.replace(" 1 Cum", " 2 Cum")])
            cols.extend([c.replace(" 1 Cum", " 2 Cum"), c.replace(" 1 Cum", " Only 1 Cum")])

    #cols_cum = rearrange(cols, 1, 2, 3, 4, 9, 10, 7, 8, )
    #cols_cum = cols_cum  # + ['Available Vaccines Cum']
    cols_cum = [c for c in cols if " 2 Cum" in c] + [c for c in cols if " 1 Cum" in c]
    # We have "Medical All" instead
    cols_cum = [c for c in cols_cum if not any_in(c, "Medical Staff", "Volunteer")]

    # TODO: get paired colour map and use do 5 + 5 pairs
    legends = [clean_vac_leg(c) for c in cols_cum]

    plot_area(df=vac_cum,
              title='Population Vaccinated against Covid by Priority Groups - Thailand',
              legends=legends,
              png_prefix='vac_groups', cols_subset=cols_cum,
              ma_days=None,
              kind='area', stacked=True, percent_fig=True,
              cmap=get_cycle('tab20', len(cols_cum), unpair=True),
              # between=['Available Vaccines Cum'],
              y_formatter=thaipop,
              footnote_left=f'{source}Data Source: DDC Daily Vaccination Reports')

    # Targets for groups
    # https://www.facebook.com/informationcovid19/photos/a.106455480972785/342985323986465/

    # 712,000 for medical staff
    # 1,900,000 for frontline staffs
    # 1,000,000 for village health volunteer
    # 5,350,000 for risk: disease
    # 12,500,000 for risk: over 60
    # 28,538,000 for general population

    # medical staff  712,000
    # village health volunteers 1,000,000
    # frontline workers 1,900,000
    # underlying diseases 6,347,125
    # general public  28,634,733 - 46,169,508
    # elderly over 60 10,906,142
    # pregnant 500,000
    # Students 12-17 4,500,000
    # Target total 50,000,000
    # Total was 73,833,176?
    goals = [
        ('Medical All', 1000000 + 712000),
        # ('Health Volunteer', 1000000),
        # ('Medical Staff', 712000),
        # ('Other Frontline Staff', 1900000),
        ['Over 60', 12704543],  # Was 10906142
        ('Risk: Disease', 6347125),
        ('General Population', 41621025),  # was 48569508
        # ('Risk: Pregnant', 500000),
        ('Student', 4500000),
        ('Kids', 5150082),
    ]
    for d in [3, 2, 1]:
        for group, goal in goals:
            vac_cum[f'Vac Group {group} {d} Cum % ({goal/1000000:.1f}M)'] = vac_cum[
                f'Vac Group {group} {d} Cum'] / goal * 100

    dose1 = vac_cum[[f'Vac Group {group} 1 Cum % ({goal/1000000:.1f}M)' for group, goal in goals]]
    dose2 = vac_cum[[f'Vac Group {group} 2 Cum % ({goal/1000000:.1f}M)' for group, goal in goals]]
    dose3 = vac_cum[[f'Vac Group {group} 3 Cum % ({goal/1000000:.1f}M)' for group, goal in goals]]
    pred1, pred2 = pred_vac(dose1, dose2, lag=40)
    _, pred3 = pred_vac(dose2, dose3, lag=150)
    pred1 = pred1.clip(upper=pred1.iloc[0].clip(100), axis=1)  # no more than 100% unless already over
    pred2 = pred2.clip(upper=pred2.iloc[0].clip(100), axis=1)  # no more than 100% unless already over
    pred3 = pred3.clip(upper=pred2.iloc[0].clip(100), axis=1)  # no more than 100% unless already over
    # vac_cum = vac_cum.combine_first(pred1).combine_first(pred2).combine_first(pred3)

    cols2 = [c for c in vac_cum.columns if " 2 Cum %" in c and "Vac Group " in c and "Pred" not in c]
    legends = [clean_vac_leg(c) for c in cols2]
    plot_area(df=vac_cum.combine_first(pred2),
              title='Vaccination by group - 2nd Dose - Thailand',
              legends=legends,
              png_prefix='vac_groups_goals_full', cols_subset=cols2,
              kind='line',
              actuals=list(pred2.columns),
              ma_days=None,
              stacked=False, percent_fig=False,
              y_formatter=perc_format,
              cmap=get_cycle('tab20', len(cols2) * 2, unpair=True, start=len(cols2)),
              footnote_left=f'{source}Data Source: DDC Daily Vaccination Reports',
              footnote='Assumes avg 40day gap between doses')

    cols2 = [c for c in vac_cum.columns if " 1 Cum %" in c and "Vac Group " in c and "Pred" not in c]
    # actuals = [c for c in vac_cum.columns if " 1 Pred" in c]
    legends = [clean_vac_leg(c) for c in cols2]
    plot_area(df=vac_cum.combine_first(pred1),
              title='Vaccination by group - 1st Dose - Thailand',
              legends=legends,
              png_prefix='vac_groups_goals_half', cols_subset=cols2,
              actuals=list(pred1.columns),
              ma_days=None,
              kind='line', stacked=False, percent_fig=False,
              y_formatter=perc_format,
              cmap=get_cycle('tab20', len(cols2) * 2, unpair=True, start=len(cols2)),  # TODO: seems to be getting wrong colors
              footnote_left=f'{source}Data Source: DDC Daily Vaccination Reports')

    cols2 = [c for c in vac_cum.columns if " 3 Cum %" in c and "Vac Group " in c and "Pred" not in c]
    legends = [clean_vac_leg(c) for c in cols2]
    plot_area(df=vac_cum.combine_first(pred3),
              title='Vaccination by group - 3rd Dose - Thailand',
              legends=legends,
              png_prefix='vac_groups_goals_3', cols_subset=cols2,
              kind='line',
              actuals=list(pred3.columns),
              ma_days=None,
              stacked=False, percent_fig=False,
              y_formatter=perc_format,
              cmap=get_cycle('tab20', len(cols2) * 2, unpair=True, start=len(cols2)),
              footnote_left=f'{source}Data Source: DDC Daily Vaccination Reports',
              footnote='Assumes avg 150d to booster')

    cols = rearrange([f'Vac Given Area {area} Cum' for area in DISTRICT_RANGE_SIMPLE], *FIRST_AREAS)
    df_vac_areas_s1 = df['2021-02-28':][cols].interpolate(limit_area="inside")
    plot_area(df=df_vac_areas_s1,
              title='Covid Vaccination Doses by Health District - Thailand',
              legends=AREA_LEGEND_SIMPLE,
              png_prefix='vac_areas', cols_subset=cols,
              ma_days=None,
              kind='area', stacked=True, percent_fig=False,
              cmap='tab20',
              footnote_left=f'{source}Data Sources: MOPH Covid-19 Dashboard\n  DDC Daily Vaccination Reports')

    # Do a % of peak chart for death vs cases
    cols = ['Cases', 'Deaths', 'ATK', ]
    peaks = df[cols] / df[cols].rolling(7, 3, center=True).mean().max(axis=0) * 100
    peaks["Vaccinated"] = df['Vac Given 2 Cum'] / 72034815.0 * 100  # pops['Vac Population'].sum() * 100  # pops.sum() is
    # pops['Vac Population'].sum() * 100  # pops.sum() is 72034815.0
    peaks["Boosted"] = df['Vac Given 3 Cum'] / 72034815.0 * 100
    peaks["Positive Rate"] = (df["Pos XLS"] / df["Tests XLS"] * 100)
    cols = [
        'Cases',
        #        'ATK',
        'Vaccinated',
        "Boosted",
        'Deaths',
        'Positive Rate',
    ]
    legend = [
        "Confirmed Cases (% of peak)",
        #        "Reg. ATK - Probable Case (% of peak)",
        "Vaccinated - 2nd dose (% of Thai Pop.)",
        "Vaccinated - 3rd dose (% of Thai Pop.)",
        "Reported Covid Deaths (% of peak)",
        "PCR +ve per PCR Test (Positive Rate)",
    ]
    plot_area(df=peaks,
              title='Covid 19 Trends - Thailand',
              png_prefix='cases_peak', cols_subset=cols, legends=legend,
              ma_days=7,
              kind='line', stacked=False, percent_fig=False, clean_end=True,
              cmap='tab10',
              y_formatter=perc_format,
              footnote_left=f'{source}Data Source: MOPH Covid-19 Dashboard,  CCSA Daily Briefing',
              footnote="% of peak (except vaccinated).\nVaccinated is % of population with 2 jabs.")

    # kind of dodgy since ATK is subset of positives but we don't know total ATK
    cols = [
        'Cases',
        'Tests XLS',
        'ATK',
    ]
    peaks = df[cols] / df.rolling(7).mean().max(axis=0) * 100
    legends = [
        'Cases from PCR Tests',
        'PCR Tests',
        'Home Isolation from Positive ATK Tests',
    ]
    plot_area(df=peaks,
              title='Tests as % of Peak - Thailand',
              legends=legends,
              png_prefix='tests_peak', cols_subset=cols,
              ma_days=7,
              kind='line', stacked=False, percent_fig=False, clean_end=True,
              cmap='tab20_r',
              y_formatter=perc_format,
              footnote='ATK: Covid-19 Rapid Antigen Self Test Kit\n'
              + 'PCR: Polymerase Chain Reaction',
              footnote_left=f'{source}Data Source: MOPH Covid-19 Dashboard,  CCSA Daily Briefing')


def save_vacs_prov_plots(df, df_prov=None):
    # Top 5 vaccine rollouts
    vac = import_csv("vaccinations", ['Date', 'Province'])
    # vac = vac.groupby("Province", group_keys=False)
    if df_prov is None:
        df_prov = import_csv("cases_by_province", ['Date', 'Province'])
        # df_prov = df_prov.groupby("Province", group_keys=False)
    vac = vac.combine_first(df_prov[[c for c in df_prov.columns if "Vac" in c]])
    vac = vac.groupby("Province", group_keys=False).apply(fix_gaps)

    # Let's trust the dashboard more but they could both be different
    # TODO: dash gives different higher values. Also glitches cause problems
    # vac = dash_prov.combine_first(vac)
    #vac = vac.combine_first(vac_dash[[f"Vac Given {d} Cum" for d in range(1, 4)]])

    # Add them all up
    vac = vac.combine_first(vac[[f"Vac Given {d} Cum" for d in range(1, 4)]].sum(
        axis=1, skipna=False).to_frame("Vac Given Cum"))
    vac = vac.join(get_provinces()[['Population', 'region']], on='Province')

    # Reset populations to the latest since they changed definitions over time
    # Bring in vac populations
    pops = vac["Vac Population"].groupby("Province").last().to_frame("Vac Population")  # It's not on all data
    # vac = vac.join(pops, rsuffix="2")
    for pop_col in ["Vac Population Risk: Disease", 'Vac Population Over 60s', 'Vac Population']:
        vac = vac.join(vac[pop_col].groupby("Province").last().to_frame(pop_col), lsuffix="1")
    vac["Vac Population2"] = vac["Vac Population"]

    # top5 = vac.pipe(topprov, lambda df: df['Vac Given Cum'] / df['Vac Population2'] * 100)
    # cols = top5.columns.to_list()
    # pred = pred_vac(top5)
    # plot_area(df=top5,
    #           title='Covid Vaccination Doses - Top Provinces - Thailand',
    #           png_prefix='vac_top5_doses', cols_subset=cols,
    #           ma_days=None,
    #           kind='line', stacked=False, percent_fig=False,
    #           cmap='tab10',
    #           actuals=pred,
    #           y_formatter=perc_format,
    #           footnote_left=f'{source}Data Sources: MOPH Covid-19 Dashboard\n  DDC Daily Vaccination Reports')

    by_region = vac.reset_index()
    pop_region = by_region.pivot_table('Vac Population2', 'Date', 'region', "sum").replace(0, np.nan)
    by_region_1 = by_region.pivot_table('Vac Given 1 Cum', 'Date', 'region', "sum").replace(0, np.nan) / pop_region * 100
    by_region_2 = by_region.pivot_table('Vac Given 2 Cum', 'Date', 'region', "sum").replace(0, np.nan) / pop_region * 100
    by_region_3 = by_region.pivot_table('Vac Given 3 Cum', 'Date', 'region', "sum").replace(0, np.nan) / pop_region * 100
    pred_1, pred_2 = pred_vac(by_region_1, by_region_2)
    pred_2 = pred_2.clip(upper=pred_2.iloc[0].clip(90), axis=1)  # no more than 100% unless already over
    pred_1 = pred_1.clip(upper=pred_1.iloc[0].clip(90), axis=1)  # no more than 100% unless already over

    plot_area(df=by_region_2.combine_first(pred_2),
              title='Vacccinated - 2nd Dose - by Region - Thailand',
              png_prefix='vac_region_2', cols_subset=utils_thai.REG_COLS, legends=utils_thai.REG_LEG,
              ma_days=7,
              kind='line', stacked=False, percent_fig=False, mini_map=True,
              cmap=utils_thai.REG_COLOURS,
              actuals=list(pred_2.columns),
              table=trend_table(vac['Vac Given 2 Cum'] / vac['Vac Population2'] * 100, sensitivity=30, style="rank_up"),
              y_formatter=perc_format,
              footnote='Table of % vaccinated and 7 day trend in change in rank',
              footnote_left=f'{source}Data Sources: DDC Daily Vaccination Reports',
              )
    plot_area(df=by_region_1.combine_first(pred_1),
              title='Vacccinatated - 1st Dose - by Region - Thailand',
              png_prefix='vac_region_1', cols_subset=utils_thai.REG_COLS, legends=utils_thai.REG_LEG,
              ma_days=7,
              kind='line', stacked=False, percent_fig=False, mini_map=True,
              cmap=utils_thai.REG_COLOURS,
              actuals=list(pred_1.columns),
              table=trend_table(vac['Vac Given 1 Cum'] / vac['Vac Population2'] * 100, sensitivity=30, style="rank_up"),
              y_formatter=perc_format,
              footnote='Table of % vaccinated and 7 day trend in change in rank',
              footnote_left=f'{source}Data Sources: DDC Daily Vaccination Reports',
              )

    pred_2, pred_3 = pred_vac(by_region_2, by_region_3, ahead=90, lag=150)
    pred_3 = pred_3.clip(upper=pred_2.iloc[0].clip(90), axis=1)  # no more than 100% unless already over

    plot_area(df=by_region_3.combine_first(pred_3),
              title='Vacccinated - 3rd Dose - by Region - Thailand',
              png_prefix='vac_region_3', cols_subset=utils_thai.REG_COLS, legends=utils_thai.REG_LEG,
              ma_days=7,
              kind='line', stacked=False, percent_fig=False, mini_map=True,
              cmap=utils_thai.REG_COLOURS,
              actuals=list(pred_3.columns),
              table=trend_table(vac['Vac Given 3 Cum'] / vac['Vac Population2'] * 100, sensitivity=30, style="rank_up"),
              y_formatter=perc_format,
              footnote='Assumes 5 month booster avg. Table shows rank change',
              footnote_left=f'{source}Data Sources: DDC Daily Vaccination Reports',
              )

    # for over 60s
    pop_region = by_region.pivot_table("Vac Population Over 60s", 'Date', 'region', "sum").replace(0, np.nan)
    by_region_1 = by_region.pivot_table('Vac Group Over 60 1 Cum', 'Date', 'region',
                                        "sum").replace(0, np.nan) / pop_region * 100
    by_region_2 = by_region.pivot_table('Vac Group Over 60 2 Cum', 'Date', 'region',
                                        "sum").replace(0, np.nan) / pop_region * 100
    by_region_3 = by_region.pivot_table('Vac Group Over 60 3 Cum', 'Date', 'region',
                                        "sum").replace(0, np.nan) / pop_region * 100
    pred_1, pred_2 = pred_vac(by_region_1, by_region_2)
    pred_2 = pred_2.clip(upper=pred_2.iloc[0].clip(100), axis=1)  # no more than 100% unless already over
    pred_1 = pred_1.clip(upper=pred_1.iloc[0].clip(100), axis=1)  # no more than 100% unless already over

    plot_area(df=by_region_2.combine_first(pred_2),
              title='Vacccinated Over 60s - 2nd Dose - by Region - Thailand',
              png_prefix='vac_region_60s_2', cols_subset=utils_thai.REG_COLS, legends=utils_thai.REG_LEG,
              ma_days=7,
              kind='line', stacked=False, percent_fig=False, mini_map=True,
              cmap=utils_thai.REG_COLOURS,
              actuals=list(pred_2.columns),
              table=trend_table(vac['Vac Group Over 60 2 Cum'] / vac["Vac Population Over 60s"]
                                * 100, sensitivity=30, style="rank_up"),
              y_formatter=perc_format,
              footnote='Table of % vaccinated and 7 day trend in change in rank',
              footnote_left=f'{source}Data Sources: DDC Daily Vaccination Reports',
              )

    # for risk disease
    pop_region = by_region.pivot_table("Vac Population Risk: Disease", 'Date', 'region', "sum").replace(0, np.nan)
    by_region_1 = by_region.pivot_table('Vac Group Risk: Disease 1 Cum', 'Date', 'region',
                                        "sum").replace(0, np.nan) / pop_region * 100
    by_region_2 = by_region.pivot_table('Vac Group Risk: Disease 2 Cum', 'Date', 'region',
                                        "sum").replace(0, np.nan) / pop_region * 100
    by_region_3 = by_region.pivot_table('Vac Group Risk: Disease 3 Cum', 'Date', 'region',
                                        "sum").replace(0, np.nan) / pop_region * 100

    pred_1, pred_2 = pred_vac(by_region_1, by_region_2)
    pred_2 = pred_2.clip(upper=pred_2.iloc[0].clip(100), axis=1)  # no more than 100% unless already over
    pred_1 = pred_1.clip(upper=pred_1.iloc[0].clip(100), axis=1)  # no more than 100% unless already over

    plot_area(df=by_region_2.combine_first(pred_2),
              title='Vacccinated Risk of 7 Diseases - 2nd Dose - by Region - Thailand',
              png_prefix='vac_region_disease_2', cols_subset=utils_thai.REG_COLS, legends=utils_thai.REG_LEG,
              ma_days=7,
              kind='line', stacked=False, percent_fig=False, mini_map=True,
              cmap=utils_thai.REG_COLOURS,
              actuals=list(pred_2.columns),
              table=trend_table(vac['Vac Group Risk: Disease 2 Cum'] / vac["Vac Population Risk: Disease"]
                                * 100, sensitivity=30, style="rank_up"),
              y_formatter=perc_format,
              footnote='Table of % vaccinated and 7 day trend in change in rank',
              footnote_left=f'{source}Data Sources: DDC Daily Vaccination Reports',
              )

    vac_prov_daily = cum2daily(vac)
    # vac_prov_daily = vac_prov_daily.join(get_provinces()[['Population', 'region']], on='Province')
    vac_prov_daily = vac_prov_daily.join(pops, rsuffix="2")

    by_region = vac_prov_daily.reset_index()
    pop_region = by_region.pivot_table("Vac Population", 'Date', 'region', "sum").replace(0, np.nan)
    by_region_1 = by_region.pivot_table('Vac Given 1', 'Date', 'region', "sum").replace(0, np.nan)
    by_region_2 = by_region.pivot_table('Vac Given 2', 'Date', 'region', "sum").replace(0, np.nan)
    plot_area(df=by_region_2 / pop_region * 100000,
              title='Vacccinatations/100k - 2nd Dose - by Region - Thailand',
              png_prefix='vac_region_daily_2', cols_subset=utils_thai.REG_COLS, legends=utils_thai.REG_LEG,
              ma_days=21,
              kind='line', stacked=False, percent_fig=False, mini_map=True,
              cmap=utils_thai.REG_COLOURS,
              #              table = trend_table(vac_prov_daily['Vac Given 2'], sensitivity=10, style="green_up"),
              footnote='Table of latest Vacciantions and 7 day trend per 100k',
              footnote_left=f'{source}Data Sources: DDC Daily Vaccination Reports',
              )
    plot_area(df=by_region_1 / pop_region * 100000,
              title='Vacccinatations/100k - 1st Dose - by Region - Thailand',
              png_prefix='vac_region_daily_1', cols_subset=utils_thai.REG_COLS, legends=utils_thai.REG_LEG,
              ma_days=21,
              kind='line', stacked=False, percent_fig=False, mini_map=True,
              cmap=utils_thai.REG_COLOURS,
              #              table = trend_table(vac_prov_daily['Vac Given 1'], sensitivity=10, style="green_up"),
              footnote='Table of latest Vacciantions and 7 day trend per 100k',
              footnote_left=f'{source}Data Sources: DDC Daily Vaccination Reports',
              )

    # TODO: to make this work have to fix negative values
    # plot_area(df=by_region,
    #           title='Covid Deaths - by Region - Thailand',
    #           png_prefix='vac_region_daily_stacked', cols_subset=utils_thai.REG_COLS, legends=utils_thai.REG_LEG,
    #           ma_days=14,
    #           kind='area', stacked=True, percent_fig=True,
    #           cmap=utils_thai.REG_COLOURS,
    #           footnote_left=f'{source}Data Source: MOPH Covid-19 Dashboard')

    top5 = vac.pipe(topprov, lambda df: df['Vac Given 1 Cum'] / df['Vac Population2'] * 100)
    pred = pred_vac(top5)
    pred = pred.clip(upper=pred.iloc[0].clip(100), axis=1)  # no more than 100% unless already over
    cols = top5.columns.to_list()
    plot_area(df=top5.combine_first(pred),
              title='Covid Vaccinations 1st Dose - Top Provinces - Thailand',
              png_prefix='vac_top5_doses_1', cols_subset=cols,
              ma_days=None,
              actuals=list(pred.columns),
              kind='line', stacked=False, percent_fig=False,
              cmap='tab10',
              y_formatter=perc_format,
              footnote_left=f'{source}Data Sources: MOPH Covid-19 Dashboard\n  DDC Daily Vaccination Reports',
              footnote='Percentage include ages 0-18')

    top5 = vac.pipe(topprov, lambda df: df['Vac Given 2 Cum'] / df['Vac Population2'] * 100)
    # since top5 might be different need to recalculate
    top5_dose1 = vac.pipe(
        topprov,
        lambda df: df['Vac Given 2 Cum'] / df['Vac Population2'] * 100,
        lambda df: df['Vac Given 1 Cum'] / df['Vac Population2'] * 100,
    )
    _, pred = pred_vac(top5_dose1, top5)
    pred = pred.clip(upper=pred.iloc[0].clip(100), axis=1)  # no more than 100% unless already over
    cols = top5.columns.to_list()
    plot_area(df=top5.combine_first(pred),
              title='Covid Vaccinations 2nd Dose - Top Provinces - Thailand',
              png_prefix='vac_top5_doses_2', cols_subset=cols,
              actuals=list(pred.columns),
              ma_days=None,
              kind='line', stacked=False, percent_fig=False,
              cmap='tab10',
              y_formatter=perc_format,
              footnote_left=f'{source}Data Sources: MOPH Covid-19 Dashboard\n  DDC Daily Vaccination Reports',
              footnote="Percentage include ages 0-18")

    top5 = vac.pipe(topprov, lambda df: df['Vac Given 3 Cum'] / df['Vac Population2'] * 100)
    # since top5 might be different need to recalculate
    top5_dose2 = vac.pipe(
        topprov,
        lambda df: df['Vac Given 3 Cum'] / df['Vac Population2'] * 100,
        lambda df: df['Vac Given 2 Cum'] / df['Vac Population2'] * 100,
    )
    _, pred = pred_vac(top5_dose2, top5)
    pred = pred.clip(upper=pred.iloc[0].clip(100), axis=1)  # no more than 100% unless already over
    cols = top5.columns.to_list()
    plot_area(df=top5.combine_first(pred),
              title='Covid Vaccinations 3rd Dose - Top Provinces - Thailand',
              png_prefix='vac_top5_doses_3', cols_subset=cols,
              actuals=list(pred.columns),
              ma_days=None,
              kind='line', stacked=False, percent_fig=False,
              cmap='tab10',
              y_formatter=perc_format,
              footnote_left=f'{source}Data Sources: MOPH Covid-19 Dashboard\n  DDC Daily Vaccination Reports',
              footnote="Percentage include ages 0-18")

    top5 = vac.pipe(topprov, lambda df: -df['Vac Given 1 Cum'] / df['Vac Population2'] * 100,
                    lambda df: df['Vac Given 1 Cum'] / df['Vac Population2'] * 100,
                    other_name=None, num=7)
    cols = top5.columns.to_list()
    pred = pred_vac(top5)
    pred = pred.clip(upper=pred.iloc[0].clip(100), axis=1)  # no more than 100% unless already over
    plot_area(df=top5.combine_first(pred),
              title='Covid Vaccination 1st Dose - Lowest Provinces - Thailand',
              png_prefix='vac_low_doses_1', cols_subset=cols,
              actuals=list(pred.columns),
              ma_days=None,
              kind='line', stacked=False, percent_fig=False,
              cmap='tab10',
              y_formatter=perc_format,
              footnote_left=f'{source}Data Sources: MOPH Covid-19 Dashboard\n  DDC Daily Vaccination Reports',
              footnote='Percentage include ages 0-18')

    top5 = vac.pipe(topprov, lambda df: -df['Vac Given 2 Cum'] / df['Vac Population2'] * 100,
                    lambda df: df['Vac Given 2 Cum'] / df['Vac Population2'] * 100,
                    other_name=None, num=7)
    cols = top5.columns.to_list()
    top5_dose1 = vac.pipe(topprov, lambda df: -df['Vac Given 2 Cum'] / df['Vac Population2'] * 100,
                          lambda df: df['Vac Given 1 Cum'] / df['Vac Population2'] * 100,
                          other_name=None, num=7)
    _, pred = pred_vac(top5_dose1, top5)
    pred = pred.clip(upper=pred.iloc[0].clip(100), axis=1)  # no more than 100% unless already over
    plot_area(df=top5.combine_first(pred),
              title='Covid Vaccinations 2nd Dose - Lowest Provinces - Thailand',
              png_prefix='vac_low_doses_2', cols_subset=cols,
              actuals=list(pred.columns),
              ma_days=None,
              kind='line', stacked=False, percent_fig=False,
              cmap='tab10',
              y_formatter=perc_format,
              footnote_left=f'{source}Data Sources: MOPH Covid-19 Dashboard\n  DDC Daily Vaccination Reports',
              footnote='Percentage include ages 0-18')
    logger.info('======== Finish Vaccinations Plots ==========')


if __name__ == "__main__":

    df = import_csv("combined", index=["Date"], date_cols=["Date"])
    briefings = import_csv("cases_briefings", index=["Date"], date_cols=["Date"])
    dash = import_csv("moph_dashboard", ["Date"], False, dir="inputs/json")  # so we cache it
    dash_weekly = import_csv("moph_dash_weekly", ["Date"], False, dir="inputs/json")  # so we cache it
    # have vac in briefings and dashboard
    df = briefings.combine_first(dash).combine_first(cum2daily(dash_weekly, drop=False)).combine_first(df)
    vac = import_csv("vac_timeline", ['Date'])
    df = df.combine_first(vac)

    os.environ["MAX_DAYS"] = '0'
    os.environ['USE_CACHE_DATA'] = 'True'
    save_vacs_prov_plots(df)
    save_vacs_plots(df)
