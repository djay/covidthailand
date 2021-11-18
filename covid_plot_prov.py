import matplotlib.cm
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import numpy as np
import re

from covid_data import get_ifr, scrape_and_combine
from utils_pandas import cum2daily, cut_ages, cut_ages_labels, decreasing, get_cycle, human_format, perc_format, \
    import_csv, increasing, normalise_to_total, rearrange, set_time_series_labels_2, topprov, pred_vac, fix_gaps
from utils_scraping import remove_prefix, remove_suffix, any_in, logger
from utils_thai import DISTRICT_RANGE, DISTRICT_RANGE_SIMPLE, AREA_LEGEND, AREA_LEGEND_SIMPLE, \
    AREA_LEGEND_ORDERED, FIRST_AREAS, area_crosstab, get_provinces, join_provinces, thaipop, thaipop2, trend_table

from covid_plot_utils import plot_area

reg_cols = ["Bangkok Metropolitan Region", "Central", "Eastern", "Western", "Northeastern", "Northern", "Southern"]
reg_leg = ["Bangkok Region", "Central", "Eastern", "Western", "Northeastern", "Northern", "Southern"]
reg_colours = "Set2"

def save_prov_plots(df: pd.DataFrame) -> None:
    logger.info('======== Generating Province Plots ==========')
    source = 'Source: https://djay.github.io/covidthailand - (CC BY)\n'

    # # matplotlib global settings
    # matplotlib.use('AGG')
    # plt.style.use('dark_background') 

    # # create directory if it does not exists
    # pathlib.Path('./outputs').mkdir(parents=True, exist_ok=True)

    dash_prov = import_csv("moph_dashboard_prov", ["Date", "Province"], dir="inputs/json")

    # Computed data
    # TODO: has a problem if we have local transmission but no proactive
    # TODO: put somewhere else
    walkins = pd.DataFrame(df["Cases Local Transmission"] - df["Cases Proactive"], columns=['Cases Walkin'])
    # In case XLS is not updated before the pptx
    df = df.combine_first(walkins).combine_first(df[['Tests',
                                                     'Pos']].rename(columns=dict(Tests="Tests XLS", Pos="Pos XLS")))

    cols = [
        'Tests XLS',
        'Tests Public',
        'Tested PUI',
        'Tested PUI Walkin Public',
        'Tests ATK Proactive'
    ]
    legends = [
        'PCR Tests',
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
    df['Postive Rate ATK Proactive'] = df['Pos ATK Proactive'] / df['Tests ATK Proactive'] * 100
    df['Postive Rate PCR + ATK'] = (df['Pos XLS'] + df['Pos ATK Proactive']) / \
        (df['Tests ATK Proactive'] + df['Tests ATK Proactive']) * 100
    df['Positive Rate Dash %'] = df['Positive Rate Dash'] * 100

    cols = [
        'Positivity Public+Private',
        'Positivity Cases/Tests',
        # 'Cases per PUI3',
        # 'Positivity Walkins/PUI3',
        'Postive Rate ATK Proactive',
        'Postive Rate PCR + ATK',
        'Positive Rate Dash %',
    ]
    legends = [
        'Positive Results per PCR Test (Positive Rate)',
        'Confirmed Cases per PCR Test',
        # 'Confirmed Cases per PUI*3',
        # 'Walkin Cases per PUI*3',
        'Positive Results per ATK Test (NHSO provided)',
        'Positive Results per PCR + ATK Test (NHSO provided)',
        'Positive Rate from DDC Dashboard',
    ]
    plot_area(df=df,
              title='Positive Rate (PCR + ATK Proactive) - Thailand',
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
              footnote_left=f'\n{source}Data Sources: DMSC Test Reports, MOPH Covid-19 Dashboard')

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
        # 'Pos Public',
        'ATK',
        'Pos ATK Proactive',
    ]
    legends = [
        'Confirmed Cases',
        'Confirmed Walk-in Cases',
        'Positive PCR Test Results',
        #    'Positive PCR Test Results (Public)',
        'Probable Cases from ATK Tests (home isolation)',
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

    # No longer include prisons in proactive number
    df['Cases Proactive Community'] = df['Cases Proactive']  # .sub(df['Cases Area Prison'], fill_value=0)
    #df['Cases inc ATK'] = df['Cases'].add(df['ATK'], fill_value=0)
    cols = [
        'Cases Imported',
        'Cases Walkin',
        'Cases Proactive Community',
        'Cases Area Prison',
    ]
    legends=[
        'Tests in Quarantine/Imported',
        'Walk-ins/Traced Tests in Hospital',
        'Mobile Proactive Tests in Community',
        'Proactive Tests in Prison',
        # "Rapid Testing (Antigen/ATK)"
    ]
    plot_area(df=df,
              title='Covid Cases by Where Tested - Thailand',
              legends=legends,
              png_prefix='cases_types', cols_subset=cols,
              unknown_name='Cases Unknown', unknown_total='Cases',
              ma_days=7,
              kind='area', stacked=True, percent_fig=True,
              actuals=["Cases"],
              cmap="tab10",
              footnote="Rapid test positives (ATK) aren't included in Confirmed Cases without PCR Test.\n"
                        + 'Contact tracing counts as a Walk-in.\n'
                        + 'PCR: Polymerase Chain Reaction\n'
                        + 'ATK: Covid-19 Rapid Antigen Self Test Kit\n'
                        + 'Walk-in: Testing done at hospital or test lab (PCR test).\n'
                        + 'Proactive: Testing done at high risk locations, rather than random sampling.',
              footnote_left=f'{source}Data Sources: CCSA Daily Briefing\n  MOPH Daily Situation Report')

    cols = [
        'Cases Symptomatic',
        'Cases Asymptomatic',
    ]
    legends = [
        'Symptomatic Cases',
        'Asymptomatic Cases',
    ]
    plot_area(df=df,
              title='Covid Cases by Symptoms - Thailand',
              legends=legends,
              png_prefix='cases_sym', cols_subset=cols,
              unknown_name='Cases Symptomatic Unknown', unknown_total='Cases',
              ma_days=None,
              kind='area', stacked=True, percent_fig=False, clean_end=True,
              cmap='tab10',
              footnote_left=f'{source}Data Sources: CCSA Daily Briefing\n  MOPH Daily Situation Report')

    # cols = ['Cases Imported','Cases Walkin', 'Cases Proactive', 'Cases Unknown']
    # plot_area(df=df, png_prefix='cases_types_all', cols_subset=cols, title='Thailand Covid Cases by Test Type',
    #           kind='area', stacked=True, percent_fig=False, ma_days=None, cmap='tab10')

    # Thailand Covid Cases by Age
    #cols = ["Age 0-9", "Age 20-29", "Age 30-39", "Age 40-49", "Age 50-65", "Age 66-"]
    cols = cut_ages_labels([10, 20, 30, 40, 50, 60, 70], "Cases Age")
    plot_area(df=df,
              title='Covid Cases by Age - Thailand',
              png_prefix='cases_ages', cols_subset=cols,
              unknown_name='Cases Unknown Age', unknown_total='Cases', unknown_percent=False,
              ma_days=7,
              kind='area', stacked=True, percent_fig=True, clean_end=True,
              cmap=get_cycle('summer_r', len(cols) + 1),
              footnote_left=f'{source}Data Source: API: Daily Reports of COVID-19 Infections')

    # Thailand Covid Cases by Risk
    cols = [c for c in df.columns if str(c).startswith("Risk: ")]
    cols = rearrange(cols, "Risk: Imported", "Risk: Pneumonia",
                     "Risk: Community", "Risk: Contact", "Risk: Work",
                     "Risk: Entertainment", "Risk: Proactive Search",
                     "Risk: Unknown")
    plot_area(df=df,
              title='Covid Cases by Risk - Thailand',
              png_prefix='cases_causes', cols_subset=cols,
              unknown_name='Risk: Investigating', unknown_total='Cases',
              ma_days=7,
              kind='area', stacked=True, percent_fig=True, clean_end=True,
              actuals=['Cases'],
              cmap='tab10',
              footnote='Grouped from original data which has over 70 risk categories.\n'
                        + 'Clusters have been grouped into either Work (factories),\n'
                        + 'Entertainment (bars/gambling...) or Community (markets) related.\n'
                        + 'Proactive: Testing done at high risk locations, rather than random sampling.',
              footnote_left=f'{source}Data Source: API: Daily Reports of COVID-19 Infections')

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
              kind='area', stacked=True, percent_fig=False,
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
    pos_areas = pd.crosstab(pos_areas['Date'], pos_areas['region'], values=pos_areas["Positive Rate Dash"], aggfunc="median") * 100
    plot_area(df=pos_areas,
              title='PCR Positive Rate - Median per Region - Thailand',
              png_prefix='positivity_region', cols_subset=reg_cols, legends=reg_leg,
              ma_days=21,
              kind='line', stacked=False, percent_fig=False,
              cmap=reg_colours,
              y_formatter=perc_format,
              table=trend_table(dash_prov["Positive Rate Dash"].dropna() * 100, sensitivity=4, style="green_down"),
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

    #######################
    # Hospital plots
    #######################
    cols_delayed = ["Hospitalized", "Recovered", "Hospitalized Severe", "Hospitalized Respirator", "Hospitalized Field"]

    # TODO: we are missing some severe, ventilator mid april. why?
    df[cols_delayed] = df[cols_delayed].interpolate(limit_area="inside")

    # TODO: use unknowns to show this plot earlier?

    # because severe includes those on respirators
    df["Hospitalized Severe excl vent"] = df["Hospitalized Severe"].sub(df["Hospitalized Respirator"], fill_value=None)
    non_split = df[["Hospitalized Severe excl vent", "Hospitalized Respirator", "Hospitalized Field"]].sum(skipna=True,
                                                                                                           axis=1)

    df["Hospitalized Hospital"] = df["Hospitalized"].sub(non_split, fill_value=None)
    cols = [
        'Hospitalized Respirator',
        'Hospitalized Severe excl vent',
        'Hospitalized Hospital',
        'Hospitalized Field',
    ]
    legends = [
        'In Serious Condition on Ventilator',
        'In Serious Condition without Ventilator',
        'In Isolation/Hospital',
        'In Field Hospital',
    ]
    # plot_area(df=df, png_prefix='cases_active', cols_subset=cols,
    #           title='Thailand Active Covid Cases\n(Severe, Field, and Respirator only available from '
    #                 '2021-04-24 onwards)',
    #           legends=legends,
    #           kind='area', stacked=True, percent_fig=False, ma_days=None, cmap='tab10')

    cols = [
        'Hospitalized Severe',
        'Hospitalized Severe excl vent',
        'Hospitalized Respirator',
    ]
    legends = [
        'In Serious Condition',
        'In Serious Condition without Ventilator',
        'In Serious Condition on Ventilator',
    ]
    plot_area(df=df,
              title='Active Covid Cases in Serious Condition - Thailand',
              legends=legends,
              png_prefix='active_severe', cols_subset=cols,
              actuals=False,
              ma_days=7,
              kind='line', stacked=False, percent_fig=False,
              cmap='tab10', 
              footnote_left=f'{source}Data Source: CCSA Daily Briefing')

    # show cumulative deaths, recoveries and hospitalisations (which should all add up to cases)
    df['Recovered since 2021-04-01'] = df['2021-04-14':]['Recovered'].cumsum()
    df['Died since 2021-04-01'] = df['2021-04-01':]['Deaths'].cumsum()
    df['Cases since 2021-04-01'] = df['2021-04-01':]['Cases'].cumsum()
    # This is those in hospital but we make this the catch all
    exits = df[['Recovered since 2021-04-01', 'Died since 2021-04-01']].sum(axis=1, skipna=True)
    df['Other Active Cases'] = \
        df['Cases since 2021-04-01'].sub(non_split, fill_value=0).sub(exits, fill_value=0).clip(0)

    cols = [
        'Died since 2021-04-01',
        'Hospitalized Respirator',
        'Hospitalized Severe',
        'Other Active Cases',
        'Hospitalized Field',
        'Recovered since 2021-04-01',
    ]
    legends = [
        'Deaths from Cases since 1st April',
        'In Serious Condition on Ventilator',
        'In Serious Condition without Ventilator',
        'In Hospital/Mild Condition',
        'In Field Hospital',
        'Recovered from Cases Confirmed since 1st April',
    ]
    plot_area(df=df,
              title='Covid Cases by Current Outcome since 1st April 2021 - Thailand',
              legends=legends,
              png_prefix='cases_cumulative', cols_subset=cols,
              ma_days=None,
              kind='area', stacked=True, percent_fig=False,
              cmap='tab10',
              footnote_left=f'{source}Data Source: CCSA Daily Briefing')

    # TODO: I think we can replace the recovered since april with plot showing just hospitalisations?
    df["Hospitalized Field Unknown"] = df["Hospitalized Field"].sub(df[["Hospitalized Field Hospitel", "Hospitalized Field HICI"]].sum(axis=1, skipna=True), fill_value=0)

    cols = [
        'Hospitalized Respirator',
        'Hospitalized Severe',
        'Hospitalized Field Unknown',
        'Hospitalized Field Hospitel',
        'Hospitalized Field HICI',
        ]
    df["Hospitalized Mild"] = df["Hospitalized"].sub(df[cols].sum(axis=1, skipna=True), fill_value=0)
    cols = [
        'Hospitalized Respirator',
        'Hospitalized Severe',
        'Hospitalized Mild', 
        'Hospitalized Field Unknown',
        'Hospitalized Field Hospitel',
        'Hospitalized Field HICI',
    ]
    legends = [
        'In Serious Condition on Ventilator',
        'In Serious Condition without Ventilator',
        'In Mild Condition in Hospital',
        'In Mild Condition in Field Hospital/Other',
        'In Mild Condition in Hotel Field Hospital (Hospitel)',
        'In Mild Condition in Home/Community Isolation (HICI)',
    ]
    plot_area(df=df,
              title='Active Cases by Condition - Thailand',
              png_prefix='active_hospital', cols_subset=cols, legends=legends,
              # unknown_name='Hospitalized Other', unknown_total='Hospitalized', unknown_percent=True,
              ma_days=7,
              kind='area', stacked=True, percent_fig=True, clean_end=True,
              cmap='tab10',
              footnote_left='Data Source: MOPH Covid-19 Dashboard,  CCSA Daily Briefing')

    df["Hospitalized All Mild"] = df["Hospitalized Mild"] + df["Hospitalized Field"]
    cols = [
        'Hospitalized Respirator',
        'Hospitalized Severe',
        'Hospitalized All Mild',
    ]
    legends = [
        'In Serious Condition on Ventilator',
        'In Serious Condition without Ventilator',
        'In Mild Condition',
    ]
    peaks = df[cols] / df.rolling(7).mean().max(axis=0) * 100
    plot_area(df=peaks,
              title='Active Covid Cases by Condition as % of Peak - Thailand',
              png_prefix='active_peak', cols_subset=cols,
              legends=legends,
              ma_days=7,
              kind='line', stacked=False, percent_fig=False, clean_end=True,
              cmap='tab10',
              y_formatter=perc_format,
              footnote_left='Data Source: MOPH Covid-19 Dashboard')

