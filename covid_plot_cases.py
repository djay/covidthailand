import matplotlib.cm
import pandas as pd

from covid_data import get_ifr, scrape_and_combine
from utils_pandas import cum2daily, cut_ages, cut_ages_labels, decreasing, get_cycle, perc_format, \
    import_csv, increasing, normalise_to_total, rearrange, topprov
from utils_scraping import remove_prefix, logger
from utils_thai import DISTRICT_RANGE, DISTRICT_RANGE_SIMPLE, AREA_LEGEND, \
    FIRST_AREAS, area_crosstab, join_provinces, trend_table

from covid_plot_utils import plot_area

reg_cols = ["Bangkok Metropolitan Region", "Central", "Eastern", "Western", "Northeastern", "Northern", "Southern"]
reg_leg = ["Bangkok Region", "Central", "Eastern", "Western", "Northeastern", "Northern", "Southern"]
reg_colours = "Set2"

def save_cases_plots(df: pd.DataFrame) -> None:
    logger.info('======== Generating Cases Plots ==========')
    source = 'Source: https://djay.github.io/covidthailand - (CC BY)\n'

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
    cases = join_provinces(cases, "Province", ["Health District Number", "region"])  # to fill in missing health districts
    # cases = cases.fillna(0)  # all the other values
    ifr = get_ifr()
    cases = cases.join(ifr[['ifr', 'Population', 'total_pop']], on="Province")


    cases_region = cases.reset_index()
    pop_region = pd.crosstab(cases_region['Date'], cases_region['region'], values=cases_region["Population"], aggfunc="sum")
    cases_region = pd.crosstab(cases_region['Date'], cases_region['region'], values=cases_region["Cases"], aggfunc="sum")
    plot_area(df=cases_region / pop_region * 100000,
              title='Cases/100k - by Region - Thailand',
              png_prefix='cases_region', cols_subset=reg_cols, legends=reg_leg,
              ma_days=7,
              kind='line', stacked=False, percent_fig=False,
              cmap=reg_colours,
              table = trend_table(cases['Cases'], sensitivity=25, style="green_down"),
              footnote='Table of latest Cases and 7 day trend per 100k',
              footnote_left=f'{source}Data Source: MOPH Covid-19 Dashboard')

    plot_area(df=cases_region,
              title='Cases - by Region - Thailand',
              png_prefix='cases_region_stacked', cols_subset=reg_cols, legends=reg_leg,
              ma_days=7,
              kind='area', stacked=True, percent_fig=True,
              cmap=reg_colours,
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
                  png_prefix=f'cases_{risk.lower().replace(" ","_")}_increasing', cols_subset=cols,
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
    #           png_prefix='active_severe_region', cols_subset=reg_cols,
    #           ma_days=7,
    #           kind='line', stacked=False, percent_fig=False,
    #           cmap='tab10',
    #           table = cases['Hospitalized Severe'],
    #           trend_sensitivity = 25,
    #           footnote='Table of latest Severe Cases and 7 day trend per 100k',
    #           footnote_left=f'{source}Data Source: MOPH Covid-19 Dashboard')

    # plot_area(df=sev_region,
    #           title='Severe Hospitalations/ - by Region - Thailand',
    #           png_prefix='active_severe_region_stacked', cols_subset=reg_cols,
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

