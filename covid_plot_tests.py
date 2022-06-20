import io
import os

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from dateutil.parser import parse as d

import utils_thai
from covid_data_api import ihme_dataset
from covid_plot_utils import plot_area
from covid_plot_utils import source
from utils_pandas import import_csv
from utils_pandas import perc_format
from utils_pandas import rearrange
from utils_pandas import topprov
from utils_scraping import any_in
from utils_scraping import logger
from utils_thai import area_crosstab
from utils_thai import AREA_LEGEND
from utils_thai import AREA_LEGEND_ORDERED
from utils_thai import AREA_LEGEND_SIMPLE
from utils_thai import DISTRICT_RANGE
from utils_thai import DISTRICT_RANGE_SIMPLE
from utils_thai import FIRST_AREAS
from utils_thai import join_provinces
from utils_thai import trend_table

est_variants = """
week,BA.1 (Omicron BA.1),BA.2 (Omicron BA.2)
100, 100, 0
101, 100, 0
102, 100, 0
103, 100, 0
104, 100, 0
105, 97, 3
106, 97, 3
107, 95, 5
108, 92, 8
109, 90, 10
110, 80, 20
111, 75, 25
112, 70, 30
113, 45, 55
114, 40, 60
115, 16, 83
116, 15, 85
117, 14, 86
118, 8, 92
119, 4, 96
120, 3, 97
"""


def save_tests_plots(df: pd.DataFrame) -> None:
    logger.info('======== Generating Tests Plots ==========')

    # Vartiants
    # sequence data have less of but more detail
    seq = import_csv("variants_sequenced", index=["End"], date_cols=["End"])
    seq = seq.fillna(0)
    # Group into major categories, BA.2 vs BA.1
    seq["BA.2 (Omicron BA.2)"] = seq[(c for c in seq.columns if "BA.2" in c)].sum(axis=1)
    seq["BA.1 (Omicron BA.1)"] = seq[(c for c in seq.columns if "BA.1" in c)].sum(axis=1)
    seq["Other"] = seq[(c for c in seq.columns if not any_in(c, "BA.1", "BA.2"))].sum(axis=1)
    # TODO: others?
    seq = seq[(c for c in seq.columns if "(" in c)]
    seq = seq.apply(lambda x: x / x.sum(), axis=1)

    # add in manual values
    mseq = pd.read_csv(io.StringIO(est_variants))
    mseq['End'] = (mseq['week'] * 7).apply(lambda x: pd.DateOffset(x) + d("2019-12-27"))
    mseq = mseq.set_index("End").drop(columns=["week"])
    mseq = mseq / 100
    seq = seq.combine_first(mseq)

    variants = import_csv("variants", index=["End"], date_cols=["End"])
    variants = variants.fillna(0)
    variants = variants.rename(columns={'B.1.1.529 (Omicron': 'BA.1 (Omicron BA.1)'})
    variants = variants.apply(lambda x: x / x.sum(), axis=1)

    # seq is all omicron variants
    seq = seq.multiply(variants["BA.1 (Omicron BA.1)"], axis=0)

    # fill in leftover dates with SNP genotyping data (major varient types)
    variants = seq.combine_first(variants)

    # TODO: missing seq data results in all BA.1. so either need a other omicron or nan data after date we are sure its not all BA1
    variants["2021-12-24":]['BA.1 (Omicron BA.1)'] = np.nan

    cols = variants.columns.to_list()
    variants = variants.reindex(pd.date_range(variants.index.min(), variants.index.max(), freq='D')).interpolate()
    variants['Cases'] = df['Cases']
    variants = (variants[cols].multiply(variants['Cases'], axis=0))
    cols = sorted(variants.columns, key=lambda c: c.split("(")[1])
    plot_area(df=variants,
              title='Covid Cases by Variant - Estimated - Thailand',
              png_prefix='cases_by_variants', cols_subset=cols,
              ma_days=7,
              kind='area', stacked=True, percent_fig=True,
              cmap='tab10',
              # y_formatter=perc_format,
              footnote="% of variant estimated from random sample, not all cases",
              footnote_left=f'{source}Data Source: SARS-CoV-2 variants in Thailand Report')

    # # matplotlib global settings
    # matplotlib.use('AGG')
    # plt.style.use('dark_background')

    # # create directory if it does not exists
    # pathlib.Path('./outputs').mkdir(parents=True, exist_ok=True)

    dash_prov = import_csv("moph_dashboard_prov", ["Date", "Province"], dir="inputs/json")
    # TODO: 0 maybe because no test data on that day? Does median make sense?
    dash_prov["Positive Rate Dash"] = dash_prov["Positive Rate Dash"].replace({0.0: np.nan})

    # Computed data
    # TODO: has a problem if we have local transmission but no proactive
    # TODO: put somewhere else
    walkins = pd.DataFrame(df["Cases Local Transmission"] - df["Cases Proactive"], columns=['Cases Walkin'])
    # In case XLS is not updated before the pptx
    df = df.combine_first(walkins).combine_first(df[['Tests',
                                                     'Pos']].rename(columns=dict(Tests="Tests XLS", Pos="Pos XLS")))

    cols = [
        'Tests XLS',
        'Tests ATK',
        'Tests Public',
        'Tested PUI',
        'Tested PUI Walkin Public',
        'Tests ATK Proactive',
    ]
    legends = [
        'PCR Tests',
        'ATK Tests (DMSC)',
        'PCR Tests (Public Hospitals)',
        'Persons Under Investigation (PUI)',
        'Persons Under Investigation (Public Hospitals)',
        'ATK Tests (NHSO provided)',
    ]
    plot_area(df=df,
              title='PCR Tests and PUI - Thailand',
              legends=legends,
              png_prefix='tests', cols_subset=cols,
              ma_days=7,
              kind='line', stacked=False, percent_fig=False,
              cmap='tab10',
              # actuals=['Tests XLS'],
              footnote='Note: PCR tests likely higher than shown ( due to cases > PCR Positives)\n'
              'PCR: Polymerase Chain Reaction\n'
              'PUI: Person Under Investigation\n'
              'Proactive: Testing done at high risk locations, rather than random sampling.',
              footnote_left=f'{source}Data Sources: Daily Situation Reports\n  DMSC: Thailand Laboratory Testing Data')

    cols = [
        'Tested Cum',
        'Tested PUI Cum',
        'Tested Proactive Cum',
        'Tested Quarantine Cum',
        'Tested PUI Walkin Private Cum',
        'Tested PUI Walkin Public Cum',
    ]
    legends = [
        'People Checked',
        'Person Under Investigation (PUI)',
        'PUI Proactive',
        'PUI Quarantine',
        'PUI Walk-in (Private Hospital)',
        'PUI Walk-in (Public Hospital)',
    ]
    plot_area(df=df,
              title='People Under Investigation (PUI) - Cumulative - Thailand',
              legends=legends,
              png_prefix='tested_pui', cols_subset=cols,
              ma_days=7,
              kind='line', stacked=False, percent_fig=False,
              cmap='tab10',
              footnote='Note: Excludes some proactive tests.\n'
                       'PCR: Polymerase Chain Reaction\n'
                       'PUI: Person Under Investigation\n'
                       'Proactive: Testing done at high risk locations, rather than random sampling.',
              footnote_left=f'{source}Data Sources: Daily Situation Reports\n  DMSC: Thailand Laboratory Testing Data')

    # kind of dodgy since ATK is subset of positives but we don't know total ATK
    cols = [
        'Cases',
        'Cases Proactive',
        'Tests XLS',
        'Tests ATK Proactive',
    ]
    legends = [
        "Cases from PCR Tests",
        "Cases from Proactive PCR Tests",
        "PCR Tests",
        "ATK Tests (NHSO provided)",
    ]
    peaks = df[cols] / df.rolling(7).mean().max(axis=0) * 100
    plot_area(df=peaks,
              title='Tests as % of Peak - Thailand',
              png_prefix='tests_peak', cols_subset=cols, legends=legends,
              ma_days=7,
              kind='line', stacked=False, percent_fig=False, clean_end=True,
              cmap='tab10',
              y_formatter=perc_format,
              footnote='ATK: Covid-19 Rapid Antigen Self Test Kit\n'
                       'Proactive: Testing done at high risk locations, rather than random sampling.',
              footnote_left='Data Source: MOPH Covid-19 Dashboard,  CCSA Daily Briefing')

    ###############
    # Positive Rate
    ###############
    df["Positivity PUI"] = df["Cases"].divide(df["Tested PUI"]) * 100
    df["Positivity Public"] = df["Pos Public"] / df["Tests Public"] * 100
    df["Positivity Cases/Tests"] = df["Cases"] / df["Tests XLS"] * 100
    df["Positivity Public+Private"] = (df["Pos XLS"] / df["Tests XLS"] * 100)
    df['Positivity Walkins/PUI3'] = df['Cases Walkin'].divide(df['Tested PUI']) / 3.0 * 100
    df['Positive Rate Private'] = (df['Pos Private'] / df['Tests Private']) * 100
    df['Cases per PUI3'] = df['Cases'].divide(df['Tested PUI']) / 3.0 * 100
    df['Cases per Tests'] = df['Cases'] / df['Tests XLS'] * 100
    df['Positive Rate ATK Proactive'] = df['Pos ATK Proactive'] / df['Tests ATK Proactive'] * 100
    df['Positive Rate ATK'] = df['Pos ATK'] / df['Tests ATK'] * 100
    df['Positive Rate PCR + ATK'] = (df['Pos XLS'] + df['Pos ATK']) / \
        (df['Tests ATK Proactive'] + df['Tests ATK']) * 100
    df['Positive Rate Dash %'] = df['Positive Rate Dash'] * 100

    ihme = ihme_dataset(check=False)
    df['infection_detection'] = ihme['infection_detection'] * 100

    cols = [
        'Positivity Public+Private',
        'Positive Rate ATK',
        'Positivity Cases/Tests',
        # 'Cases per PUI3',
        # 'Positivity Walkins/PUI3',
        'Positive Rate ATK Proactive',
        'Positive Rate PCR + ATK',
        'Positive Rate Dash %',
        'infection_detection',
    ]
    legends = [
        'Positive Results per PCR Test (Positive Rate)',
        'Positive Results per ATK Test (Positive Rate)',
        'Confirmed Cases per PCR Test',
        # 'Confirmed Cases per PUI*3',
        # 'Walkin Cases per PUI*3',
        'Positive Results per ATK Test (NHSO provided)',
        'Positive Results per PCR/ATK Test (DMSc)',
        'Positive Rate from DDC Dashboard',
        'Estimated Cases per Infection (IHME detection rate)',
    ]
    plot_area(df=df,
              title='Positive Rate - Thailand',
              legends=legends,
              highlight=['Positivity Public+Private'],
              png_prefix='positivity', cols_subset=cols,
              ma_days=7,
              kind='line', stacked=False, percent_fig=False,
              cmap='tab10',
              y_formatter=perc_format,
              footnote='While PCR test data is missing, Cases per Test might be a better estimate of Positive Rate\n'
              'WHO recommends < 5% *assuming tests are > 7k per day over 2 weeks\n'
              'NHSO provided ATK go to "high risk" areas so should show higher than normal positive rate',
              footnote_left=f'\n{source}Data Sources: DMSC Test Reports, DDC Dashboard, IHME')

    df['PUI per Case'] = df['Tested PUI'].divide(df['Cases'])
    df['PUI3 per Case'] = df['Tested PUI'] * 3 / df['Cases']
    df['PUI3 per Walkin'] = df['Tested PUI'] * 3 / df['Cases Walkin']
    df['PUI per Walkin'] = df['Tested PUI'].divide(df['Cases Walkin'])
    df['Tests per case'] = df['Tests XLS'] / df['Cases']
    df['Tests per positive'] = df['Tests XLS'] / df['Pos XLS']

    cols = [
        'Tests per positive',
        'Tests per case',
        'PUI per Case',
        'PUI3 per Case',
        'PUI per Walkin',
    ]
    legends = [
        'PCR Tests per Positive Result',
        'PCR Tests per Case',
        'PUI per Case',
        'PUI*3 per Case',
        'PUI per Walk-in Case',
    ]
    plot_area(df=df,
              title='Tests per Confirmed Covid Cases - Thailand',
              legends=legends,
              png_prefix='tests_per_case', cols_subset=cols,
              ma_days=7,
              kind='line', stacked=False, percent_fig=False,
              cmap='tab10',
              footnote='\nPUI: Person Under Investigation\n'
                       'PCR: Polymerase Chain Reaction\n'
                       'Note: Walkin Cases/3xPUI seems to give an estimate of positive rate (when cases are high),\n'
                       'so it is included for when testing data is delayed. It is not the actual positive rate.',
              footnote_left=f'\n{source}Data Sources: Daily Situation Reports\n  DMSC: Thailand Laboratory Testing Data')

    cols = [
        'Positivity Cases/Tests',
        'Positivity Public',
        'Positivity PUI',
        'Positive Rate Private',
        'Positivity Public+Private',
    ]
    legends = [
        'Confirmed Cases per PCR Test (Public Hospital)',
        'Positive Results per PCR Test (Private Hospital)',
        'Confirmed Cases per PUI',
        'Positive Results per PCR Test (Private Hospital)',
        'Positive Results per PCR Test',
    ]
    plot_area(df=df,
              title='Positive Rate - Thailand',
              legends=legends,
              png_prefix='positivity_all', cols_subset=cols,
              ma_days=7,
              kind='line', stacked=False, percent_fig=False,
              cmap='tab10',
              footnote='\nPUI: Person Under Investigation\n'
              + 'Positivity Rate: The percentage of COVID-19 tests that come back positive.',
              footnote_left=f'\n{source}Data Sources: Daily Situation Reports\n  DMSC: Thailand Laboratory Testing Data')

    ########################
    # Public vs Private
    ########################
    df['Tests Private Ratio'] = (df['Tests Private'] / df['Tests Public']).rolling('7d').mean()
    df['Tests Positive Private Ratio'] = (df['Pos Private'] / df['Pos Public']).rolling('7d').mean()
    df['Positive Rate Private Ratio'] = (df['Pos Private'] / (df['Tests Private'])
                                         / (df['Pos Public'] / df['Tests Public'])).rolling('7d').mean()
    df['PUI Private Ratio'] = (df['Tested PUI Walkin Private'] / df['Tested PUI Walkin Public']).rolling('7d').mean()
    cols = [
        'Tests Private Ratio',
        'Tests Positive Private Ratio',
        'PUI Private Ratio',
        'Positive Rate Private Ratio',
    ]
    plot_area(df=df,
              title='Testing Private Ratio - Thailand',
              png_prefix='tests_private_ratio', cols_subset=cols,
              ma_days=7,
              kind='line', stacked=False, percent_fig=False,
              cmap='tab10',
              footnote='\nPUI: Person Under Investigation\n'
              + 'Positivity Rate: The percentage of COVID-19 tests that come back positive.',
              footnote_left=f'\n{source}Data Sources: Daily Situation Reports\n  DMSC: Thailand Laboratory Testing Data')

    ##################
    # Test Plots
    ##################
    df["Cases outside Prison"] = df["Cases Local Transmission"].sub(df["Cases Area Prison"], fill_value=0)

    cols = [
        'Cases',
        'Cases Walkin',
        'Pos XLS',
        'Pos ATK',
        # 'Pos Public',
        'ATK',
        'Pos ATK Proactive',
    ]
    legends = [
        'Confirmed Cases',
        'Confirmed Walk-in Cases',
        'Positive PCR Test Results',
        'Positive ATK Test Results (DMSC)',
        #    'Positive PCR Test Results (Public)',
        'Registered ATK Probable Case (Home Isolation)',
        'Positive Proactive ATK Test Results (NHSO provided)',
    ]
    plot_area(df=df,
              title='Positive Test Results vs. Confirmed Covid Cases - Thailand',
              legends=legends,
              png_prefix='cases', cols_subset=cols,
              #   actuals=["Cases", "Pos XLS"],
              ma_days=7,
              kind='line', stacked=False, percent_fig=False,
              cmap="tab10",
              footnote='ATK: Covid-19 Rapid Antigen Self Test Kit\n'
                       'Cases higher than PCR positive tests likely due to missing PCR test data',
              footnote_left=f'{source}Data Sources: Daily Situation Reports\n  DMSC: Thailand Laboratory Testing Data')

    cols = [
        'Cases',
        'Cases outside Prison',
        'Cases Walkin',
        'Pos XLS',
    ]
    legends = [
        'Confirmed Cases',
        'Confirmed Cases excl. Prison Cases',
        'Confirmed Cases excl. Proactive Cases',
        'Positive PCR Test Results',
    ]
    plot_area(df=df,
              title='Covid Cases vs. Positive Tests - Thailand',
              legends=legends,
              png_prefix='cases_tests', cols_subset=cols,
              ma_days=21,
              kind='line', stacked=False, percent_fig=False,
              cmap="tab10",
              footnote='Proactive: Testing done at high risk locations, rather than random sampling.\n'
              'Cases higher than PCR positive tests likely due to missing PCR test data',
              footnote_left=f'{source}Data Sources: Daily Situation Reports\n  DMSC: Thailand Laboratory Testing Data')

    df['Cases 3rd Cum'] = df['2021-04-01':]['Cases'].cumsum()
    df['Cases outside Prison 3rd Cum'] = df['2021-04-01':]['Cases outside Prison'].cumsum()
    df['Cases Walkin 3rd Cum'] = df['2021-04-01':]['Cases Walkin'].cumsum()
    df['Pos XLS 3rd Cum'] = df['2021-04-01':]['Pos XLS'].cumsum()
    cols = [
        'Cases 3rd Cum',
        'Cases outside Prison 3rd Cum',
        'Cases Walkin 3rd Cum',
        'Pos XLS 3rd Cum',
    ]
    legends = [
        'Confirmed Cases',
        'Confirmed Cases excl. Prison Cases',
        'Confirmed Cases excl. Proactive Cases',
        'Positive PCR Test Results',
    ]
    plot_area(df=df,
              title='3rd Wave Cumulative Covid Cases and Positive Tests - Thailand',
              legends=legends,
              png_prefix='cases_tests_cum3', cols_subset=cols,
              ma_days=None,
              kind='line', stacked=False, percent_fig=False,
              cmap="tab10",
              footnote='Proactive: Testing done at high risk locations, rather than random sampling.',
              footnote_left=f'{source}Data Sources: Daily Situation Reports\n  DMSC: Thailand Laboratory Testing Data')

    cols = [
        'Cases',
        'Pos Area',
        'Pos XLS',
        'Pos Public',
        'Pos Private',
        'Pos',
    ]
    legends = [
        'Cases',
        'Positive PCR Test Results (Health Districts Combined)',
        'Positive PCR Test Results',
        'Positive PCR Test Results (Public Hospitals)',
        'Positive PCR Test Results (Private Hospitals)',
        'Positive Test Results',
    ]
    plot_area(df=df,
              title='Positive Test Results vs. Confirmed Covid Cases - Thailand',
              legends=legends,
              png_prefix='cases_all', cols_subset=cols,
              ma_days=7,
              kind='line', stacked=False, percent_fig=False,
              cmap='tab20',
              footnote_left=f'{source}Data Sources: Daily Situation Reports\n  DMSC: Thailand Laboratory Testing Data')

    ##########################
    # Tests by area
    ##########################
    plt.rc('legend', **{'fontsize': 12})

    cols = rearrange([f'Tests Area {area}' for area in DISTRICT_RANGE], *FIRST_AREAS)
    plot_area(df=df,
              title='PCR Tests by Health District - Thailand',
              legends=AREA_LEGEND_SIMPLE,
              png_prefix='tests_area', cols_subset=cols[0],
              ma_days=None,
              kind='area', stacked=True, percent_fig=False,
              cmap='tab20',
              footnote='Note: Excludes some proactive and private tests (non-PCR) so actual tests is higher.\n'
              + 'Proactive: Testing done at high risk locations, rather than random sampling.\n'
              + 'PCR: Polymerase Chain Reaction',
              footnote_left=f'{source}Data Source: DMSC: Thailand Laboratory Testing Data')

    cols = rearrange([f'Pos Area {area}' for area in DISTRICT_RANGE_SIMPLE], *FIRST_AREAS)
    plot_area(df=df,
              title='PCR Positive Test Results by Health District - Thailand',
              legends=AREA_LEGEND_SIMPLE,
              png_prefix='pos_area', cols_subset=cols,
              ma_days=None,
              kind='area', stacked=True, percent_fig=False,
              cmap='tab20',
              footnote='Note: Excludes some proactive and private tests (non-PCR) so actual tests is higher.\n'
              + 'Proactive: Testing done at high risk locations, rather than random sampling.\n'
              + 'PCR: Polymerase Chain Reaction',
              footnote_left=f'{source}Data Source: DMSC: Thailand Laboratory Testing Data')

    for area in DISTRICT_RANGE_SIMPLE:
        df[f'Tests Area {area} (i)'] = df[f'Tests Area {area}'].interpolate(limit_area="inside")
    test_cols = [f'Tests Area {area} (i)' for area in DISTRICT_RANGE_SIMPLE]
    for area in DISTRICT_RANGE_SIMPLE:
        df[f'Tests Daily {area}'] = (df[f'Tests Area {area} (i)'] / df[test_cols].sum(axis=1) * df['Tests'])
    cols = rearrange([f'Tests Daily {area}' for area in DISTRICT_RANGE_SIMPLE], *FIRST_AREAS)
    plot_area(df=df,
              title='PCR Tests by Health District - Thailand',
              legends=AREA_LEGEND_SIMPLE,
              png_prefix='tests_area_daily', cols_subset=cols,
              ma_days=7,
              kind='line', stacked=False, percent_fig=False,
              cmap='tab20',
              footnote='Note: Excludes some proactive and private tests (non-PCR) so actual tests is higher.\n'
              + 'Proactive: Testing done at high risk locations, rather than random sampling.\n'
              + 'PCR: Polymerase Chain Reaction',
              footnote_left=f'{source}Data Source: DMSC: Thailand Laboratory Testing Data')

    for area in DISTRICT_RANGE_SIMPLE:
        df[f'Pos Area {area} (i)'] = df[f'Pos Area {area}'].interpolate(limit_area="inside")
    pos_cols = [f'Pos Area {area} (i)' for area in DISTRICT_RANGE_SIMPLE]
    for area in DISTRICT_RANGE_SIMPLE:
        df[f'Pos Daily {area}'] = (df[f'Pos Area {area} (i)'] / df[pos_cols].sum(axis=1) * df['Pos'])
    cols = rearrange([f'Pos Daily {area}' for area in DISTRICT_RANGE_SIMPLE], *FIRST_AREAS)

    plot_area(df=df,
              title='Positive PCR Tests by Health District - Thailand',
              legends=AREA_LEGEND_SIMPLE,
              png_prefix='pos_area_daily', cols_subset=cols,
              ma_days=7,
              kind='area', stacked=True, percent_fig=False,
              cmap='tab20',
              footnote='Note: Excludes some proactive and private tests (non-PCR) so actual tests is higher.\n'
              + 'Proactive: Testing done at high risk locations, rather than random sampling.\n'
              + 'PCR: Polymerase Chain Reaction',
              footnote_left=f'{source}Data Source: DMSC: Thailand Laboratory Testing Data')

    # Workout positivity for each area as proportion of positivity for that period
    for area in DISTRICT_RANGE_SIMPLE:
        df[f'Positivity {area}'] = (
            df[f'Pos Area {area} (i)'] / df[f'Tests Area {area} (i)'] * 100
        )
    cols = [f'Positivity {area}' for area in DISTRICT_RANGE_SIMPLE]
    df['Total Positivity Area'] = df[cols].sum(axis=1)
    for area in DISTRICT_RANGE_SIMPLE:
        df[f'Positivity {area}'] = (df[f'Positivity {area}'] / df['Total Positivity Area']
                                    * df['Positivity Public+Private'])
    plot_area(df=df,
              title='Positive Rate by Health District - Thailand',
              legends=AREA_LEGEND_SIMPLE,
              png_prefix='positivity_area', cols_subset=rearrange(cols, *FIRST_AREAS),
              ma_days=7,
              kind='area', stacked=True, percent_fig=False,
              cmap='tab20',
              y_formatter=perc_format,
              footnote='PCR: Polymerase Chain Reaction\n'
              + 'Positivity Rate: The percentage of COVID-19 tests that come back positive.\n'
              + 'Note: Excludes some proactive and private tests (non-PCR) so actual tests is higher.\n'
              + 'Proactive: Testing done at high risk locations, rather than random sampling.',
              footnote_left=f'{source}Data Source: DMSC: Thailand Laboratory Testing Data')

    # for area in DISTRICT_RANGE_SIMPLE:
    #     df[f'Positivity Daily {area}'] = df[f'Pos Daily {area}'] / df[f'Tests Daily {area}'] * 100
    # cols = [f'Positivity Daily {area}' for area in DISTRICT_RANGE_SIMPLE]
    pos_areas = join_provinces(dash_prov, "Province", ["Health District Number", "region"])
    pos_areas = area_crosstab(pos_areas, "Positive Rate Dash", aggfunc="mean") * 100
    cols = rearrange([f'Positive Rate Dash Area {area}' for area in DISTRICT_RANGE_SIMPLE], *FIRST_AREAS)
    topcols = df[cols].sort_values(by=df[cols].last_valid_index(), axis=1, ascending=False).columns[:5]
    legends = rearrange(AREA_LEGEND_ORDERED, *[cols.index(c) + 1 for c in topcols])[:5]
    plot_area(df=pos_areas,
              title='Average Positive Rate - by Health District - Thailand',
              legends=legends,
              png_prefix='positivity_area_unstacked', cols_subset=topcols,
              ma_days=7,
              kind='line', stacked=False, percent_fig=False,
              cmap='tab10',
              y_formatter=perc_format,
              footnote='Positivity Rate: The % of COVID-19 tests that come back positive.',
              footnote_left=f'{source}Data Source: MOPH Covid-19 Dashboard')

    pos_areas = join_provinces(dash_prov, "Province", ["Health District Number", "region"]).reset_index()
    pos_areas = pd.crosstab(pos_areas['Date'], pos_areas['region'],
                            values=pos_areas["Positive Rate Dash"], aggfunc="mean") * 100
    plot_area(df=pos_areas,
              title='PCR Positive Rate - Mean per Region - Thailand',
              png_prefix='positivity_region', cols_subset=utils_thai.REG_COLS, legends=utils_thai.REG_LEG,
              ma_days=21,
              kind='line', stacked=False, percent_fig=False, mini_map=True,
              cmap=utils_thai.REG_COLOURS,
              y_formatter=perc_format,
              # TODO: fix table when incomplete data
              # table=trend_table(dash_prov["Positive Rate Dash"].dropna() * 100, sensitivity=4, style="green_down", ma_days=21),
              footnote='Positivity Rate: The % of COVID-19 tests that come back positive.\nDashboard positive rate differs from testing reports',
              footnote_left=f'{source}Data Source: MOPH Covid-19 Dashboard')

    top5 = dash_prov.pipe(topprov,
                          lambda df: df["Positive Rate Dash"] * 100,
                          name="Province Positive Rate",
                          other_name=None,
                          num=5)
    cols = top5.columns.to_list()
    plot_area(df=top5,
              title='Positive Rate - Top Provinces - Thailand',
              png_prefix='positivity_prov_top', cols_subset=cols,
              ma_days=14,
              kind='line', stacked=False, percent_fig=False,
              cmap='tab10',
              y_formatter=perc_format,
              footnote='Positivity Rate: The percentage of COVID-19 tests that come back positive.',
              footnote_left=f'{source}Data Source: MOPH Covid-19 Dashboard')

    top5 = dash_prov.pipe(topprov,
                          lambda df: -df["Positive Rate Dash"] * 100,
                          lambda df: df["Positive Rate Dash"] * 100,
                          name="Province Positive Rate",
                          other_name=None,
                          num=5)
    cols = top5.columns.to_list()
    plot_area(df=top5,
              title='Positive Rate - Lowest Provinces - Thailand',
              png_prefix='positivity_prov_low', cols_subset=cols,
              ma_days=14,
              kind='line', stacked=False, percent_fig=False,
              cmap='tab10',
              y_formatter=perc_format,
              footnote='Positivity Rate: The percentage of COVID-19 tests that come back positive.',
              footnote_left=f'{source}Data Source: MOPH Covid-19 Dashboard')

    for area in DISTRICT_RANGE_SIMPLE:
        df[f'Cases/Tests {area}'] = (
            df[f'Cases Area {area}'] / df[f'Tests Area {area}'] * 100
        )
    cols = [f'Cases/Tests {area}' for area in DISTRICT_RANGE_SIMPLE]
    plot_area(df=df,
              title='Highest Covid Cases/Tests by Health District - Thailand',
              legends=AREA_LEGEND_SIMPLE,
              png_prefix='casestests_area_unstacked', cols_subset=rearrange(cols, *FIRST_AREAS),
              ma_days=None,
              kind='area', stacked=False, percent_fig=False,
              cmap='tab20',
              footnote='Note: Excludes some proactive and private tests (non-PCR) so actual tests is higher.\n'
              + 'Proactive: Testing done at high risk locations, rather than random sampling.\n'
              + 'PCR: Polymerase Chain Reaction',
              footnote_left=f'{source}Data Source: DMSC: Thailand Laboratory Testing Data')

    for area in DISTRICT_RANGE_SIMPLE:
        df[f'Case-Pos {area}'] = (
            df[f'Cases Area {area}'] - df[f'Pos Area {area}']
        )
    cols = [f'Case-Pos {area}' for area in DISTRICT_RANGE_SIMPLE]
    plot_area(df=df,
              title='Which Health Districts have more Covid Cases than Positive Results? - Thailand',
              legends=AREA_LEGEND_SIMPLE,
              png_prefix='cases_from_positives_area', cols_subset=rearrange(cols, *FIRST_AREAS),
              ma_days=None,
              kind='area', stacked=False, percent_fig=False, limit_to_zero=False,
              cmap='tab20',
              footnote_left=f'{source}Data Source: CCSA Daily Briefing')

    logger.info('======== Finish Tests Plots ==========')


if __name__ == "__main__":
    df = import_csv("combined", index=["Date"])
    os.environ["MAX_DAYS"] = '0'
    os.environ['USE_CACHE_DATA'] = 'True'
    save_tests_plots(df)
