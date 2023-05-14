import io
import os

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from dateutil.parser import parse as d

import utils_thai
from covid_data_api import ihme_dataset
from covid_data_testing import get_variant_api
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

# Eyeballed from the plots for sequenced varaints in the reports
est_variants = """
week,BA.1 (Omicron),BA.2 (Omicron)
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

# 'B.1.1.7 (Alpha)'
# 'B.1.351 (Beta)'
# 'B.1.617.2 (Delta)'
groups = {
    'B.1.1': 'B.1.1.7 (Alpha)',
    'B.1.351': 'B.1.351 (Beta)',
    'B.1.617.2': 'B.1.617.2 (Delta)',
    'AY.': 'B.1.617.2 (Delta)',
    "BA.1": "BA.1 (Omicron)",
    "BA.2": "BA.2 (Omicron)",
    "BA.4": "BA.4/BA.5 (Omicron)",
    "BA.5": "BA.4/BA.5 (Omicron)",
    "BA.2.75": "BN.1/BA.2.75 (Omicron)",
    "BA.2.76": "BN.1/BA.2.75 (Omicron)",
    "BN.1.": "BN.1/BA.2.75 (Omicron)",
    "XBB": "XBB (Omicron)",
    "BQ.X": "Other",
    "Other": "Other",
}


def group_seq(seq):
    def group(variant):
        label = next((label for match, label in reversed(groups.items()) if match in variant.upper()), "Other")
        return label
    unstacked = seq.unstack().reset_index(name="Detected").rename(columns=dict(level_0="Variant"))
    unstacked['Variant Group'] = unstacked['Variant'].apply(group)
    seq = pd.pivot_table(unstacked, columns="Variant Group", values="Detected", index="End", aggfunc="sum")
    seq = seq.apply(lambda x: x / x.sum(), axis=1)
    # Put them back in the order above
    # seq = seq[dict(zip(groups.values(), [1] * len(groups))).keys()]
    # seq.columns = [c + " (Omicron)" for c in seq.columns]
    return seq


def combined_variant_reports():
    # Vartiants
    # sequence data have less of but more detail
    seq = import_csv("variants_sequenced", index=["End"], date_cols=["End"])
    seq = seq.fillna(0)
    seq = seq[seq.sum(axis=1) > 20]  # If not enough samples we won't use it
    # Group into major categories, BA.2 vs BA.1
    seq = group_seq(seq)

    # add in manual values
    mseq = pd.read_csv(io.StringIO(est_variants))
    mseq['End'] = (mseq['week'] * 7).apply(lambda x: pd.DateOffset(x) + d("2019-12-27"))
    mseq = mseq.set_index("End").drop(columns=["week"])
    mseq = mseq / 100
    seq = seq.combine_first(mseq)
    # last_data = seq.index.max()  # Sequence data is behind genotyping. Lets not interpolate past best data we have

    variants = import_csv("variants", index=["End"], date_cols=["End"])
    variants = variants.fillna(0)
    variants = variants.rename(columns={'B.1.1.529 (Omicron)': 'BA.1 (Omicron)'})
    variants = variants.apply(lambda x: x / x.sum(), axis=1)

    # seq is all omicron variants
    allseq = seq.multiply(variants["BA.1 (Omicron)"], axis=0)
    seq = allseq.combine_first(seq.loc["2023-01-20":])
    seq = seq.rename(columns={'Other (Omicron)': 'Other'})  # Now includes BQ.X

    # TODO: missing seq data results in all BA.1. so either need a other omicron or nan data after date we are sure its not all BA1
    variants.loc["2021-12-24":, 'BA.1 (Omicron)'] = np.nan

    # fill in leftover dates with SNP genotyping data (major varient types)
    variants = seq.combine_first(variants)

    # This is the PCR based survalience. Less detailed but more samples and 1 week ahead of sequencing.
    area = import_csv("variants_by_area", index=["Start", "End"], date_cols=["Start", "End"])
    area = area.groupby(["Start", "End"]).sum()
    area = area.reset_index().drop(columns=["Health Area", "Start"]).set_index(
        "End").rename(columns={"B.1.1.529 (Omicron)": "Other", "BA.2.75 (Omicron)": "BN.1/BA.2.75 (Omicron)"})
    area = area.apply(lambda x: x / x.sum(), axis=1)
    # Omicron didn't get spit out until 2022-06-24 so get rid of the rest
    # TODO: should we prefer seq data or pcr data?
    variants = variants.combine_first(area["2022-06-24":])
    last_data = variants['BA.2 (Omicron)'].last_valid_index()
    variants = variants.reindex(pd.date_range(variants.index.min(), last_data, freq='D')).interpolate()
    return variants


def save_variant_plots(df: pd.DataFrame) -> None:
    variants = combined_variant_reports()
    api = get_variant_api()
    api = api.resample("7D", label='right', closed='right').mean()
    # api = api.rolling("7d").mean()
    # api = api[api.sum(axis=1) > 5]  # If not enough samples we won't use it
    if not api.empty:
        variants = group_seq(api)
        foot_source = f'{source}Data Source: GISAID'
    else:
        logger.warning("Using Variants from reports. GISAID problem")
        foot_source = f'{source}Data Source: SARS-CoV-2 variants in Thailand(DMSc)'

    cols = rearrange(variants.columns.to_list(), "BN.1/BA.2.75 (Omicron)", "XBB (Omicron)", "Other", first=False)
    variants['Cases'] = df['Cases']
    case_variants = (variants[cols].multiply(variants['Cases'], axis=0))
    # cols = sorted(variants.columns, key=lambda c: c.split("(")[1])
    plot_area(df=case_variants,
              title='Cases by Major Variant - Interpolated from Sampling - Thailand',
              png_prefix='cases_by_variants', cols_subset=cols,
              ma_days=7,
              kind='area', stacked=True, percent_fig=True,
              cmap='tab10',
              # y_formatter=perc_format,
              footnote="Estimate combines random sample data from SNP Genotyping by PCR and Genome Sequencing\nextraploated to cases. Not all cases are tested.",
              footnote_left=foot_source)

    ihme = ihme_dataset(check=False)
    today = df['Cases'].index.max()
    #est_cases = ihme["inf_mean"].loc[:today].to_frame("Estimated Total Infections (IHME)")
    inf_variants = (variants[cols].multiply(ihme['inf_mean'], axis=0))
    # cols = sorted(variants.columns, key=lambda c: c.split("(")[1])
    plot_area(df=inf_variants,
              title='Est. Infections by Major Variant - Interpolated from Sampling - Thailand',
              png_prefix='inf_by_variants', cols_subset=cols,
              ma_days=7,
              kind='area', stacked=True, percent_fig=True,
              cmap='tab10',
              # y_formatter=perc_format,
              footnote="Estimate combines random sample data from SNP Genotyping by PCR and Genome Sequencing\nextraploated to infections. Not all infections are tested. IHME infections is an estimate from modeling",
              footnote_left=foot_source)

    death_variants = (variants[cols].multiply(df['Deaths'], axis=0)).dropna(axis=0)
    plot_area(df=death_variants,
              title='Deaths by Major Variant - Interpolated from Sampling - Thailand',
              png_prefix='deaths_by_variants', cols_subset=cols,
              ma_days=7,
              kind='area', stacked=True, percent_fig=True,
              cmap='tab10',
              footnote="Cases are tests for variants not Deaths so this is an approximation. Estimate combines random sample data from SNP Genotyping by PCR and Genome Sequencing\nextraploated to infections.",
              footnote_left=foot_source)

    hosp_variants = (variants[cols].multiply(df['Hospitalized'], axis=0)).dropna(axis=0)
    plot_area(df=hosp_variants,
              title='Hospitalized by Major Variant - Interpolated from Sampling - Thailand',
              png_prefix='hosp_by_variants', cols_subset=cols,
              ma_days=7,
              kind='area', stacked=True, percent_fig=True,
              cmap='tab10',
              footnote="This is an approximation. Estimate combines random sample data from SNP Genotyping by PCR and Genome Sequencing\nextraploated.",
              footnote_left=foot_source)


def save_tests_plots(df: pd.DataFrame) -> None:

    # # matplotlib global settings
    # matplotlib.use('AGG')
    # plt.style.use('dark_background')

    # # create directory if it does not exists
    # pathlib.Path('./outputs').mkdir(parents=True, exist_ok=True)

    # Computed data
    # TODO: has a problem if we have local transmission but no proactive
    # TODO: put somewhere else
    walkins = pd.DataFrame(df["Cases Local Transmission"] - df["Cases Proactive"], columns=['Cases Walkin'])
    # In case XLS is not updated before the pptx
    df = df.combine_first(walkins).combine_first(df[['Tests',
                                                     'Pos']].rename(columns=dict(Tests="Tests XLS", Pos="Pos XLS")))
    dash = import_csv("moph_dashboard", ["Date"], False, dir="inputs/json")
    df['ATK+'] = dash['Infections Non-Hospital Cum'].cumsum().interpolate(limit_area="inside").diff()

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
    # there is some weird spikes in tests and pos that throw out this measure. seems like they dumped extra data on certain
    # days. can avg it to try and remove it but better to just remove the outliers
    # roll = df["Tests XLS"].rolling(7)
    # devs = (df["Tests XLS"] - roll.mean()) / roll.std()
    # tests_cleaned = df["Tests XLS"][devs < 1.9]

    # Fix spikes in cases that didn't use to be there
    cleaned_cases = df.loc[:, 'Cases']
    cleaned_cases.loc["2022-10-02":"2022-10-08"] = np.nan
    cleaned_cases.loc["2022-10-30":"2022-11-05"] = np.nan

    df["Positivity Cases/Tests"] = (cleaned_cases / df["Tests XLS"]) * 100
    df["Positivity Public+Private"] = (df["Pos XLS"] / df["Tests XLS"] * 100)
    df['Positivity Walkins/PUI3'] = df['Cases Walkin'].divide(df['Tested PUI']) / 3.0 * 100
    df['Positive Rate Private'] = (df['Pos Private'] / df['Tests Private']) * 100
    df['Cases per PUI3'] = df['Cases'].divide(df['Tested PUI']) / 3.0 * 100
    df['Cases per Tests'] = df['Cases'] / df['Tests XLS'] * 100
    df['Positive Rate ATK Proactive'] = df['Pos ATK Proactive'] / df['Tests ATK Proactive'] * 100
    df['Positive Rate ATK'] = df['Pos ATK'] / df['Tests ATK'] * 100
    df['Positive Rate PCR + ATK'] = (df['Pos XLS'] + df['Pos ATK']) / (df['Tests XLS'] + df['Tests ATK']) * 100
    df['Positive Rate Dash %'] = df['Positive Rate Dash'] * 100

    ihme = ihme_dataset(check=False)
    df['infection_detection'] = ihme['infection_detection'] * 100
    cols = [
        'Positivity Public+Private',
        'Positive Rate ATK',
        'Positive Rate PCR + ATK',
        'Positivity Cases/Tests',
        # 'Cases per PUI3',
        # 'Positivity Walkins/PUI3',
        'Positive Rate ATK Proactive',
        'Positive Rate Dash %',
        'infection_detection',
    ]
    legends = [
        'Positive Results per PCR Test (Positive Rate)',
        'Positive Results per ATK Test (Positive Rate)',
        'Positive Results per Test (PCR + ATK)',
        'Confirmed Cases per PCR Test',
        # 'Confirmed Cases per PUI*3',
        # 'Walkin Cases per PUI*3',
        'Positive Results per ATK Test (NHSO provided)',
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
    # # Gets too big. takes forever
    # plot_area(df=df,
    #           title='Tests per Confirmed Covid Cases - Thailand',
    #           legends=legends,
    #           png_prefix='tests_per_case', cols_subset=cols,
    #           ma_days=7,
    #           kind='line', stacked=False, percent_fig=False,
    #           cmap='tab10',
    #           footnote='\nPUI: Person Under Investigation\n'
    #                    'PCR: Polymerase Chain Reaction\n'
    #                    'Note: Walkin Cases/3xPUI seems to give an estimate of positive rate (when cases are high),\n'
    #                    'so it is included for when testing data is delayed. It is not the actual positive rate.',
    #           footnote_left=f'\n{source}Data Sources: Daily Situation Reports\n  DMSC: Thailand Laboratory Testing Data')

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
        # 'ATK+',
        # 'Pos Public',
        'ATK',
        'Pos ATK Proactive',
    ]
    legends = [
        'Confirmed Cases',
        'Confirmed Walk-in Cases',
        'Positive PCR Test Results',
        'Positive ATK Test Results (DMSC)',
        # 'ATK+ (DDC Dash)',
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
def save_test_area_plots(df):
    plt.rc('legend', **{'fontsize': 12})

    # by_area = import_csv("tests_by_area", index=["Start"], date_cols=["Start", "End"]).drop(columns=["End"])
    # # Works up until 2021-04-11. before this dates are offset?
    # by_area = by_area.reindex(pd.date_range(by_area.index.min(), by_area.index.max(), freq='W'))
    # # .interpolate(limit_area="inside")

    by_area = import_csv("tests_by_area", index=["End"], date_cols=["Start", "End"])
    by_area_d = by_area.drop(columns=["Start"]).div((by_area.index - by_area["Start"]).dt.days, axis=0)
    # TODO: since it's daily mean, should move to the center of teh week?
    by_area_d = by_area_d.reindex(pd.date_range(by_area_d.index.min(), by_area_d.index.max(),
                                  freq='D')).interpolate(limit_area="inside")

    cols = rearrange([f'Tests Area {area}' for area in DISTRICT_RANGE_SIMPLE], *FIRST_AREAS)
    plot_area(df=by_area_d,
              title='PCR Tests by Health District - Thailand',
              legends=AREA_LEGEND_SIMPLE,
              png_prefix='tests_area', cols_subset=cols,
              ma_days=None,
              kind='area', stacked=True, percent_fig=False,
              cmap='tab20',
              footnote='Note: Excludes some proactive and private tests (non-PCR) so actual tests is higher.\n'
              + 'Proactive: Testing done at high risk locations, rather than random sampling.\n'
              + 'PCR: Polymerase Chain Reaction',
              footnote_left=f'{source}Data Source: DMSC: Thailand Laboratory Testing Data')

    cols = rearrange([f'Pos Area {area}' for area in DISTRICT_RANGE_SIMPLE], *FIRST_AREAS)
    plot_area(df=by_area_d,
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

    # for area in DISTRICT_RANGE_SIMPLE:
    #     df[f'Tests Area {area} (i)'] = df[f'Tests Area {area}'].interpolate(limit_area="inside")
    test_cols = [f'Tests Area {area}' for area in DISTRICT_RANGE_SIMPLE]
    for area in DISTRICT_RANGE_SIMPLE:
        df[f'Tests Daily {area}'] = (by_area_d[f'Tests Area {area}'] / by_area_d[test_cols].sum(axis=1) * df['Tests XLS'])
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

    # for area in DISTRICT_RANGE_SIMPLE:
    #     df[f'Pos Area {area} (i)'] = df[f'Pos Area {area}'].interpolate(limit_area="inside")
    pos_cols = [f'Pos Area {area}' for area in DISTRICT_RANGE_SIMPLE]
    for area in DISTRICT_RANGE_SIMPLE:
        df[f'Pos Daily {area}'] = (by_area_d[f'Pos Area {area}'] / by_area_d[pos_cols].sum(axis=1) * df['Pos XLS'])
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
            by_area_d[f'Pos Area {area}'] / by_area_d[f'Tests Area {area}'] * 100
        )
    cols = [f'Positivity {area}' for area in DISTRICT_RANGE_SIMPLE]

    plot_area(df=df,
              title='Positive Rate by Health District - Thailand',
              legends=AREA_LEGEND_SIMPLE,
              png_prefix='positivity_area', cols_subset=rearrange(cols, *FIRST_AREAS),
              ma_days=7,
              kind='line', stacked=True, percent_fig=False,
              cmap='tab20',
              y_formatter=perc_format,
              footnote='PCR: Polymerase Chain Reaction\n'
              + 'Positivity Rate: The percentage of COVID-19 tests that come back positive.\n'
              + 'Note: Excludes some proactive and private tests (non-PCR) so actual tests is higher.\n'
              + 'Proactive: Testing done at high risk locations, rather than random sampling.',
              footnote_left=f'{source}Data Source: DMSC: Thailand Laboratory Testing Data')

    df['Total Positivity Area'] = df[cols].sum(axis=1)
    for area in DISTRICT_RANGE_SIMPLE:
        df[f'Positivity {area}'] = (df[f'Positivity {area}'] / df['Total Positivity Area']
                                    * (df["Pos XLS"] / df["Tests XLS"] * 100))
    plot_area(df=df,
              title='Positive Rate by Health District - Thailand',
              legends=AREA_LEGEND_SIMPLE,
              png_prefix='positivity_area_stacked', cols_subset=rearrange(cols, *FIRST_AREAS),
              ma_days=7,
              kind='area', stacked=True, percent_fig=False,
              cmap='tab20',
              y_formatter=perc_format,
              footnote='PCR: Polymerase Chain Reaction\n'
              + 'Positivity Rate: The percentage of COVID-19 tests that come back positive.\n'
              + 'Note: Excludes some proactive and private tests (non-PCR) so actual tests is higher.\n'
              + 'Proactive: Testing done at high risk locations, rather than random sampling.',
              footnote_left=f'{source}Data Source: DMSC: Thailand Laboratory Testing Data')

    dash_prov = import_csv("moph_dashboard_prov", ["Date", "Province"], dir="inputs/json")
    # TODO: 0 maybe because no test data on that day? Does median make sense?
    dash_prov["Positive Rate Dash"] = dash_prov["Positive Rate Dash"].replace({0.0: np.nan})

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
    tests_by_province = import_csv("tests_by_province", index=["Date", "Province"])
    pos_prov = tests_by_province[[c for c in tests_by_province.columns if ' Pos' in c]].sum(
        axis=1) / tests_by_province[[c for c in tests_by_province.columns if ' Tests' in c]].sum(axis=1)
    pos_prov = pos_prov.to_frame("Positive Rate")
    pos_prov = join_provinces(pos_prov, "Province", ["Health District Number", "region"]).reset_index()
    pos_prov = pd.crosstab(pos_prov['Date'], pos_prov['region'],
                           values=pos_prov["Positive Rate"], aggfunc="mean") * 100
    plot_area(df=pos_areas.combine_first(pos_prov),
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


if __name__ == "__main__":
    df = import_csv("combined", index=["Date"])
    os.environ["MAX_DAYS"] = '0'
    os.environ['USE_CACHE_DATA'] = 'True'
    save_variant_plots(df)
    save_test_area_plots(df)
    save_tests_plots(df)
