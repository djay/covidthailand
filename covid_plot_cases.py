import matplotlib.cm
import numpy as np
import pandas as pd

import utils_thai
from covid_data import get_ifr
from covid_data import scrape_and_combine
from covid_data_api import get_case_details_csv
from covid_plot_utils import plot_area
from covid_plot_utils import source
from utils_pandas import cum2daily
from utils_pandas import cut_ages
from utils_pandas import cut_ages_labels
from utils_pandas import decreasing
from utils_pandas import fuzzy_join
from utils_pandas import get_cycle
from utils_pandas import import_csv
from utils_pandas import increasing
from utils_pandas import normalise_to_total
from utils_pandas import perc_format
from utils_pandas import rearrange
from utils_pandas import topprov
from utils_scraping import logger
from utils_scraping import remove_prefix
from utils_thai import area_crosstab
from utils_thai import AREA_LEGEND
from utils_thai import DISTRICT_RANGE
from utils_thai import DISTRICT_RANGE_SIMPLE
from utils_thai import FIRST_AREAS
from utils_thai import join_provinces
from utils_thai import trend_table


def save_cases_plots(df: pd.DataFrame) -> None:
    logger.info('======== Generating Cases Plots ==========')

    # No longer include prisons in proactive number
    df['Cases Proactive Community'] = df['Cases Proactive']  # .sub(df['Cases Area Prison'], fill_value=0)
    # df['Cases inc ATK'] = df['Cases'].add(df['ATK'], fill_value=0)
    cols = [
        'Cases Imported',
        'Cases Walkin',
        'Cases Proactive Community',
        'Cases Area Prison',
    ]
    legends = [
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
    # cols = ["Age 0-9", "Age 20-29", "Age 30-39", "Age 40-49", "Age 50-65", "Age 66-"]
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

    """ Thailand Covid Cases by Nationality """

    cases = get_case_details_csv()
    # List out all nationalities by number of occurrences, select only 5 largest nationalities excluding Thai and others(non-labled)
    nat_index = cases['nationality'].value_counts().index
    top5_list = nat_index[~nat_index.isin(['Thai', 'Others'])][:5]

    # List out all nationalities apart from Thai and top5
    others_list = nat_index[~nat_index.isin(np.concatenate((top5_list, ['Thai'])))]

    # Counts number of cases of each nationality by date
    counts_by_nation = pd.crosstab(cases['Date'], cases['nationality'])

    # Create another DataFrame containing top 5 and others (Others = Sum of every other nationality)
    by_nationality_top5 = counts_by_nation[top5_list]
    by_nationality_top5['Others'] = counts_by_nation[others_list].sum(axis=1)
    cols = [c for c in by_nationality_top5.columns]
    plot_area(df=by_nationality_top5,
              title='Non-Thai Covid Cases - by Nationality - Thailand',
              png_prefix='cases_nation', cols_subset=cols,
              ma_days=7,
              kind='line', stacked=False, percent_fig=False,
              cmap='tab10',
              footnote='\n*Thai cases are excluded',
              footnote_left=f'\n{source}Data Sources: API: Daily Reports of COVID-19 Infections')

    #######################
    # Cases by provinces
    #######################

    cases = import_csv("cases_by_province")
    # fill in missing provinces
    cases_pivot = cases.fillna(0).pivot_table(index="Date", columns="Province", values="Cases")
    # fill in missing days
    all_days = pd.date_range(cases_pivot.index.min(), cases_pivot.index.max(), name="Date")
    cases_pivot = cases_pivot.reindex(all_days).fillna(0)  # put in missing days with NaN
    cases = cases.set_index(["Date", "Province"]).combine_first(cases_pivot.unstack().to_frame("Cases"))
    cases = join_provinces(cases, "Province",
                           ["Health District Number", "region"])  # to fill in missing health districts
    # cases = cases.fillna(0)  # all the other values
    ifr = get_ifr()
    cases = cases.join(ifr[['ifr', 'Population', 'total_pop']], on="Province")

    cases_region = cases.reset_index()
    pop_region = pd.crosstab(cases_region['Date'], cases_region['region'], values=cases_region["Population"],
                             aggfunc="sum")
    cases_region = pd.crosstab(cases_region['Date'], cases_region['region'], values=cases_region["Cases"],
                               aggfunc="sum")
    plot_area(df=cases_region / pop_region * 100000,
              title='Cases/100k - by Region - Thailand',
              png_prefix='cases_region', cols_subset=utils_thai.REG_COLS, legends=utils_thai.REG_LEG,
              ma_days=7,
              kind='line', stacked=False, percent_fig=False, mini_map=True,
              cmap=utils_thai.REG_COLOURS,
              table=trend_table(cases['Cases'], sensitivity=25, style="green_down"),
              footnote='Table of latest Cases and 7 day trend per 100k',
              footnote_left=f'{source}Data Source: MOPH Covid-19 Dashboard')

    cases_region['Cases'] = df['Cases']
    plot_area(df=cases_region,
              title='Cases - by Region - Thailand',
              png_prefix='cases_region_stacked', cols_subset=utils_thai.REG_COLS, legends=utils_thai.REG_LEG,
              ma_days=7,
              kind='area', stacked=True, percent_fig=True, mini_map=True,
              unknown_name="Imported/Prisons", unknown_total="Cases",
              cmap=utils_thai.REG_COLOURS,
              footnote_left=f'{source}Data Source: MOPH Covid-19 Dashboard')

    # cols = rearrange([f'Cases Area {area}' for area in DISTRICT_RANGE] + ['Cases Imported'], *FIRST_AREAS)
    # plot_area(df=df,
    #           title='Covid Cases by Health District - Thailand',
    #           legends=AREA_LEGEND + ['Imported Cases'],
    #           png_prefix='cases_areas', cols_subset=cols,
    #           unknown_name="Unknown District", unknown_total="Cases",
    #           ma_days=7,
    #           kind='area', stacked=True, percent_fig=True,
    #           cmap='tab20',
    #           footnote_left=f'{source}Data Source: CCSA Daily Briefing')

    # cols = rearrange([f'Cases Walkin Area {area}' for area in DISTRICT_RANGE], *FIRST_AREAS)
    # plot_area(df=df,
    #           title='"Walk-in" Covid Cases by Health District - Thailand',
    #           legends=AREA_LEGEND,
    #           png_prefix='cases_areas_walkins', cols_subset=cols,
    #           ma_days=None,
    #           kind='area', stacked=True, percent_fig=False,
    #           cmap='tab20',
    #           footnote='Walk-in: Testing done at hospital or test lab (PCR test).\n'
    #                     + 'PCR: Polymerase Chain Reaction',
    #           footnote_left=f'{source}Data Source: CCSA Daily Briefing')

    # cols = rearrange([f'Cases Proactive Area {area}' for area in DISTRICT_RANGE], *FIRST_AREAS)
    # plot_area(df=df,
    #           title='"Proactive" Covid Cases by Health District - Thailand',
    #           legends=AREA_LEGEND,
    #           png_prefix='cases_areas_proactive', cols_subset=cols,
    #           ma_days=None,
    #           kind='area', stacked=True, percent_fig=False,
    #           cmap='tab20',
    #           footnote='Proactive: Testing done at high risk locations, rather than random sampling.',
    #           footnote_left=f'{source}Data Source: CCSA Daily Briefing')

    def cases_per_capita(col):
        def func(adf):
            return adf[col] / adf['Population'] * 100000

        return func

    top5 = cases.pipe(topprov,
                      increasing(cases_per_capita("Cases")),
                      cases_per_capita("Cases"),
                      name="Province Cases (3d MA)",
                      other_name="Other Provinces",
                      num=5)
    cols = top5.columns.to_list()
    plot_area(df=top5,
              title='Confirmed Covid Cases/100k - Trending Up Provinces - Thailand',
              png_prefix='cases_prov_increasing', cols_subset=cols,
              ma_days=14,
              kind='line', stacked=False, percent_fig=False,
              cmap='tab10',
              footnote='\nNote: Per 100,000 people.',
              footnote_left=f'\n{source}Data Sources: CCSA Daily Briefing\n  API: Daily Reports of COVID-19 Infections')

    top5 = cases.pipe(topprov,
                      decreasing(cases_per_capita("Cases")),
                      cases_per_capita("Cases"),
                      name="Province Cases (3d MA)",
                      other_name="Other Provinces",
                      num=5)
    cols = top5.columns.to_list()
    plot_area(df=top5,
              title='Confirmed Covid Cases/100k - Trending Down Provinces - Thailand',
              png_prefix='cases_prov_decreasing', cols_subset=cols,
              ma_days=14,
              kind='line', stacked=False, percent_fig=False,
              cmap='tab10',
              footnote='\nNote: Per 100,000 people.',
              footnote_left=f'\n{source}Data Sources: CCSA Daily Briefing\n  API: Daily Reports of COVID-19 Infections')

    top5 = cases.pipe(topprov,
                      cases_per_capita("Cases"),
                      name="Province Cases",
                      other_name="Other Provinces",
                      num=5)
    cols = top5.columns.to_list()

    plot_area(df=top5,
              title='Confirmed Covid Cases/100k - Top Provinces - Thailand',
              png_prefix='cases_prov_top', cols_subset=cols,
              ma_days=14,
              kind='line', stacked=False, percent_fig=False,
              cmap='tab10',
              footnote_left=f'{source}Data Sources: CCSA Daily Briefing\n  API: Daily Reports of COVID-19 Infections')

    top5 = cases.pipe(topprov,
                      increasing(cases_per_capita('Cases Walkin'), 14),
                      cases_per_capita('Cases Walkin'),
                      name="Province Cases Walkin (7d MA)",
                      other_name="Other Provinces",
                      num=5)
    cols = top5.columns.to_list()
    plot_area(df=top5,
              title='"Walk-in" Covid Cases/100k - Top Provinces - Thailand',
              png_prefix='cases_walkins_increasing', cols_subset=cols,
              ma_days=14,
              kind='line', stacked=False, percent_fig=False,
              cmap='tab10',
              footnote='\nNote: Per 100,000 people.\n'
                       + 'PCR: Polymerase Chain Reaction\n'
                       + 'Walk-in: Testing done at hospital or test lab (PCR test).',
              footnote_left=f'\n{source}Data Sources: CCSA Daily Briefing\n  API: Daily Reports of COVID-19 Infections')

    for risk in ['Contact', 'Proactive Search', 'Community', 'Work', 'Unknown']:
        top5 = cases.pipe(topprov,
                          increasing(cases_per_capita(f"Cases Risk: {risk}")),
                          cases_per_capita(f"Cases Risk: {risk}"),
                          name=f"Province Cases {risk} (7d MA)",
                          other_name="Other Provinces",
                          num=5)
        cols = top5.columns.to_list()
        plot_area(df=top5,
                  title=f'{risk} Related Covid Cases/100k - Trending Up Provinces - Thailand',
                  png_prefix=f'cases_{risk.lower().replace(" ", "_")}_increasing', cols_subset=cols,
                  ma_days=14,
                  kind='line', stacked=False, percent_fig=False,
                  cmap='tab10',
                  footnote='\nNote: Per 100,000 people.\n'
                           + 'Proactive: Testing done at high risk locations, rather than random sampling.',
                  footnote_left=f'\n{source}Data Sources: CCSA Daily Briefing\n  API: Daily Reports of COVID-19 Infections')

    def top(func, _):
        return func

    # sev_region = cases.reset_index()
    # sev_region = pd.crosstab(sev_region['Date'], sev_region['region'], values=sev_region['Hospitalized Severe'], aggfunc="sum")
    # plot_area(df=sev_region / pop_region,
    #           title='Severe Hospitalations/100k - by Region - Thailand',
    #           png_prefix='active_severe_region', cols_subset=utils_thai.REG_COLS,
    #           ma_days=7,
    #           kind='line', stacked=False, percent_fig=False,
    #           cmap='tab10',
    #           table = cases['Hospitalized Severe'],
    #           trend_sensitivity = 25,
    #           footnote='Table of latest Severe Cases and 7 day trend per 100k',
    #           footnote_left=f'{source}Data Source: MOPH Covid-19 Dashboard')

    # plot_area(df=sev_region,
    #           title='Severe Hospitalations/ - by Region - Thailand',
    #           png_prefix='active_severe_region_stacked', cols_subset=utils_thai.REG_COLS,
    #           ma_days=7,
    #           kind='area', stacked=True, percent_fig=True,
    #           cmap='tab10',
    #           footnote_left=f'{source}Data Source: MOPH Covid-19 Dashboard')

    # for direction, title in zip([increasing, decreasing, top], ["Trending Up ", "Trending Down ", ""]):
    #     top5 = cases.pipe(topprov,
    #                       direction(cases_per_capita('Hospitalized Severe')),
    #                       cases_per_capita('Hospitalized Severe'),
    #                       name="Province Active Cases Severe (7d MA)",
    #                       other_name="Other Provinces",
    #                       num=8)
    #     cols = top5.columns.to_list()
    #     plot_area(df=top5,
    #         title=f'Severe Active Covid Cases/100k - {title}Provinces - Thailand',
    #         png_prefix=f'active_severe_{direction.__name__}', cols_subset=cols,
    #         ma_days=14,
    #         kind='line', stacked=False, percent_fig=False,
    #         cmap='tab10',
    #         footnote='Note: Per 100,000 people.',
    #         footnote_left=f'{source}Data Source: CCSA Daily Briefing')

    # TODO: work out based on districts of deaths / IFR for that district
    cases['Deaths'] = cases['Deaths'].fillna(0)
    cases = cases.groupby("Province").apply(lambda df: df.assign(deaths_ma=df[
        "Deaths"].rolling(7, min_periods=1).mean()))

    cases["Infections Estimate"] = cases['Deaths'] / (cases['ifr'] / 100)
    cases["Infections Estimate (MA)"] = cases['deaths_ma'] / (cases['ifr'] / 100)
    cases_est = cases.groupby(["Date"]).sum()

    # TODO: work out unknown deaths and use whole thailand IFR for them
    # cases_est['Deaths Unknown'] = (df['Deaths'] - cases_est['Deaths']) / ifr['ifr']['Whole Kingdom'] * 100

    # 11 days was median days to death reported in situation reports I think
    cases_est["Infections Estimate"] = cases_est["Infections Estimate"].shift(-11)
    # cases_est["Infections Estimate (MA)"] = cases_est["Infections Estimate (MA)"].shift(-14)
    cases_est = cases_est.rename(columns=dict(Deaths="Deaths prov sum"))
    cases_est = cases_est.join(df['Deaths'], on="Date")
    # cases_est['Cases (MA)'] = cases_est['Cases'].rolling("7d").mean()
    cases_est["Infections Estimate Simple"] = cases_est["Deaths"].shift(-11) / 0.0054
    cols = [
        'Cases',
        'Infections Estimate',
    ]
    legends = [
        'Confirmed Cases',
        'Infections Estimate based on Deaths',
    ]
    plot_area(df=cases_est,
              title='Covid Infections (unofficial estimate) - Thailand',
              legends=legends,
              png_prefix='cases_infections_estimate', cols_subset=cols,
              actuals=True,
              ma_days=7,
              kind='line', stacked=False, percent_fig=False,
              cmap='tab10',
              footnote='Note: Based on Deaths/IFR.\n'
                       + 'IFR: Infection Fatality Rate\n'
                       + 'DISCLAIMER: See website for the assumptions of this simple estimate.',
              footnote_left=f'{source}Data Sources: CCSA Daily Briefing\n  Covid IFR Analysis, Thailand Population by Age')

    # Do CFR for all regions. show vaccine effectiveness
    # cfr_est = lambda df: df['Deaths'].shift(11) / df['Cases'] * 100
    def cfr_est(df): return df['Deaths'].rolling(14).mean().shift(11) / df['Cases'].rolling(14).mean() * 100
    by_region = cases[['Cases', 'Deaths', "region"]].groupby(["Date", "region"]).sum()
    # cfr_region = cases.reset_index()
    cfr_region = by_region.groupby("region", group_keys=False).apply(cfr_est).to_frame("CFR Est").reset_index()
    cfr_region = pd.crosstab(cfr_region['Date'], cfr_region['region'], values=cfr_region["CFR Est"], aggfunc="sum")
    plot_area(df=cfr_region,
              title='Case Fatality Rate Estimate - by Region - Thailand',
              png_prefix='cfr_region', cols_subset=utils_thai.REG_COLS, legends=utils_thai.REG_LEG,
              ma_days=7,
              kind='line', stacked=False, percent_fig=False, mini_map=True,
              cmap=utils_thai.REG_COLOURS,
              # table=trend_table(cases['Cases'], sensitivity=25, style="green_down"),
              footnote="CFR is not the Infection fatality rate so doesn't tell the chance of dying if infected\n"
              "CFR is affected by detection rate of cases/deaths. Deaths shifted by 11 days. 14 day avg used.",
              footnote_left=f'{source}Data Source: MOPH Covid-19 Dashboard')

    # Do a % of peak chart for cases vs. social distancingn (reduced mobility)
    cols = ['Cases']
    peaks = df[cols] / df[cols].rolling(7).mean().max(axis=0) * 100

    ihme = import_csv("ihme", ['Date'])
    col_list = ['Mobility Index', 'mobility_obs']
    mobility = ihme[col_list]
    # keep only observed mobility, removing forcasted part
    mobility = mobility.loc[mobility['mobility_obs'] == 1]
    # Calculate Reduced Mobility Index
    mobility_min = mobility['Mobility Index'].min()
    mobility_max = mobility['Mobility Index'].max()
    mobility['Reduced Mobility Index - IHME (% of peak)'] = (1 + (mobility_min -
                                                                  mobility['Mobility Index']) / (mobility_max - mobility_min)) * 100

    peaks = peaks.combine_first(mobility)
    cols += ['Reduced Mobility Index - IHME (% of peak)']
    legend = ["Confirmed Cases (% of peak)", "Reduced Mobility Index - IHME (% of peak)"]
    plot_area(df=peaks,
              title='Social Distancing - Reduced Mobility and Number of New Cases',
              png_prefix='mobility', cols_subset=cols, legends=legend,
              ma_days=7,
              kind='line', stacked=False, percent_fig=False, clean_end=True,
              periods_to_plot=["all", "3"],
              cmap='tab10',
              y_formatter=perc_format,
              footnote_left=f'{source}Data Source: Institute for Health Metrics and Evaluation')
