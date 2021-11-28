import matplotlib.cm
import pandas as pd
from pandas.tseries.offsets import MonthEnd

from covid_data import get_ifr, scrape_and_combine
from utils_pandas import cum2daily, cut_ages, cut_ages_labels, decreasing, get_cycle, perc_format, \
    import_csv, increasing, normalise_to_total, rearrange, topprov
from utils_scraping import remove_prefix, logger
from utils_thai import DISTRICT_RANGE, DISTRICT_RANGE_SIMPLE, AREA_LEGEND, \
    FIRST_AREAS, area_crosstab, join_provinces, trend_table
import utils_thai

from covid_plot_utils import plot_area, source


def save_deaths_plots(df: pd.DataFrame) -> None:
    logger.info('======== Generating Deaths Plots ==========')

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

    def cases_per_capita(col):
        def func(adf):
            return adf[col] / adf['Population'] * 100000
        return func

    ####################
    # Deaths
    ####################

    # TODO: predict median age of death based on population demographics

    cols = [
        'Deaths',
        'Deaths Risk Family',
        'Deaths Comorbidity None',
    ]
    legends = [
        'Deaths',
        'Deaths Infected from Family',
        'Deaths without Underlying Diseases',
    ]
    plot_area(df=df,
              title='Covid Deaths - Thailand',
              legends=legends,
              png_prefix='deaths_reason', cols_subset=cols,
              actuals=['Deaths'],
              ma_days=7,
              kind='line', stacked=False, percent_fig=False,
              cmap='tab10',
              footnote_left=f'{source}Data Source: CCSA Daily Briefing')

    df['Deaths Age Median (MA)'] = df['Deaths Age Median'].rolling('7d').mean()
    cols = [
        'Deaths Age Median (MA)',
        'Deaths Age Max',
        'Deaths Age Min',
    ]
    legends = [
        'Median Age of Deaths',
        'Maximum Age of Deaths',
        'Minimum Age of Deaths',
    ]
    plot_area(df=df,
              title='Covid Deaths Age Range - Thailand',
              legends=legends,
              highlight=['Deaths Age Median (MA)'],
              between=['Deaths Age Max', 'Deaths Age Min'],
              png_prefix='deaths_age', cols_subset=cols,
              ma_days=None,
              kind='line', stacked=False, percent_fig=False,
              cmap='tab10',
              footnote_left=f'{source}Data Source: CCSA Daily Briefing')

    cols = rearrange([f'Deaths Area {area}' for area in DISTRICT_RANGE], *FIRST_AREAS)
    plot_area(df=df,
              title='Covid Deaths by Health District - Thailand',
              legends=AREA_LEGEND,
              png_prefix='deaths_by_area', cols_subset=cols,
              ma_days=7,
              kind='area', stacked=True, percent_fig=True,
              cmap='tab20',
              footnote_left=f'{source}Data Source: CCSA Daily Briefing')

    by_region = cases.reset_index()
    by_region = pd.crosstab(by_region['Date'], by_region['region'], values=by_region['Deaths'], aggfunc="sum")    
    plot_area(df=by_region / pop_region * 100000,
              title='Covid Deaths/100k - by Region - Thailand',
              png_prefix='deaths_region', cols_subset=utils_thai.REG_COLS, legends=utils_thai.REG_LEG,
              ma_days=21,
              kind='line', stacked=False, percent_fig=False, mini_map=True,
              cmap=utils_thai.REG_COLOURS,
              table = trend_table(cases['Deaths'], sensitivity=25, style="green_down"),
              footnote='Table of latest Deaths and 7 day trend per 100k',
              footnote_left=f'{source}Data Source: CCSA Daily Briefing')

    by_region['Deaths'] = df['Deaths']
    plot_area(df=by_region,
              title='Covid Deaths - by Region - Thailand',
              png_prefix='deaths_region_stacked', cols_subset=utils_thai.REG_COLS, legends=utils_thai.REG_LEG,
              ma_days=21,
              kind='area', stacked=True, percent_fig=True, mini_map=True,
    #          unknown_name="Imported/Prisons", unknown_total="Deaths",  # I don't think deaths get seperated
              cmap=utils_thai.REG_COLOURS,
              footnote_left=f'{source}Data Source: MOPH Covid-19 Dashboard')

    top5 = cases.pipe(topprov,
                      cases_per_capita("Deaths"),
                      name="Province Cases",
                      other_name="Other Provinces",
                      num=5)
    cols = top5.columns.to_list()

    plot_area(df=top5,
              title='Covid Deaths/100k - Top Provinces - Thailand',
              png_prefix='deaths_prov_top', cols_subset=cols,
              ma_days=21,
              kind='line', stacked=False, percent_fig=False,
              cmap='tab10',
              footnote_left=f'{source}Data Sources: CCSA Daily Briefing\n  API: Daily Reports of COVID-19 Infections')


    # Work out Death ages from CFR from situation reports
    age_ranges = ["15-39", "40-59", "60-"]

    cols = [f'W3 CFR {age}' for age in age_ranges]
    plot_area(df=df,
              title='Covid CFR since 2021-04-01 - Thailand',
              png_prefix='deaths_w3cfr', cols_subset=cols,
              ma_days=None,
              kind='line', stacked=False, percent_fig=False,
              cmap='tab10',
              footnote='CFR: Case Fatality Rate\nMeasures the severity of a disease by defining the total number\n of deaths as a proportion of reported cases at a specific time.',
              footnote_left=f'{source}Data Source: CCSA Daily Briefing')

    #ages = ["Age 0-14", "Age 15-39", "Age 40-59", "Age 60-"]
    ages = cut_ages_labels([15, 40, 60], "Cases Age")
    # Put unknowns into ages based on current ratios. But might not be valid for prison unknowns?
    # w3_cases = df[ages + ['Cases', 'Deaths']].pipe(normalise_to_total, ages, "Cases")
    w3_cases = df[ages + ['Cases', 'Deaths']]

    cols = ages
    plot_area(df=w3_cases,
              title='Covid Cases by Age - Thailand',
              png_prefix='cases_ages2', cols_subset=cols,
              unknown_name='Cases Unknown Age', unknown_total='Cases', unknown_percent=False,
              ma_days=7,
              kind='area', stacked=True, percent_fig=True,
              cmap=get_cycle('summer_r', len(cols), extras=["gainsboro"]),
              footnote_left=f'{source}Data Source: API: Daily Reports of COVID-19 Infections')

    case_ages_cum = w3_cases["2021-04-01":].cumsum()

    # work out ages of deaths from cfr
    # CFR * cases = deaths
    for ages_range in age_ranges:
        case_ages_cum[f"Deaths Age {ages_range} Cum"] = df[f"W3 CFR {ages_range}"].rolling(
            21, min_periods=13,
            center=True).mean() / 100 * case_ages_cum[f"Cases Age {ages_range.replace('60-', '60+')}"].rolling(
                7, min_periods=3, center=True).mean()
    deaths_by_age = cum2daily(case_ages_cum)
    death_cols = [f'Deaths Age {age}' for age in age_ranges]
    deaths_by_age['Deaths'] = df['Deaths']
    deaths_by_age['Deaths Ages Median'] = deaths_by_age['Deaths'].rolling(7, min_periods=3, center=True).mean()
    deaths_by_age['Deaths Ages Sum'] = deaths_by_age[death_cols].sum(axis=1)
    deaths_by_age = deaths_by_age.pipe(normalise_to_total, death_cols, 'Deaths Ages Median')
    cols = death_cols + ['Deaths Ages Median', 'Deaths Ages Sum']
    plot_area(df=deaths_by_age,
              title='Covid Deaths Age Range - Thailand',
              png_prefix='deaths_age_bins', cols_subset=cols,
              ma_days=None,
              kind='line', stacked=False, percent_fig=False,
              cmap='tab10',
              footnote_left=f'{source}Data Source: CCSA Daily Briefing')
    # don't use this chart anymore since we can get this data from the dashboard
    # plot_area(df=deaths_by_age,
    #           png_prefix='deaths_age_est',
    #           cols_subset=death_cols,
    #           title='Thailand Covid Death Age Distribution\nEstimation from smoothed CFR from daily situation reports',
    #           kind='area',
    #           stacked=True,
    #           percent_fig=True,
    #           ma_days=None,
    #           cmap=get_cycle('summer_r', len(cols) + 1))

    # Plot death ages from dashboard data
    death_cols = ['0-9', '10-19', '20-29', '30-39', '40-49', '50-59', '60-69', '70+']
    death_cols = [f"Deaths Age {age}" for age in death_cols]
    plot_area(df=df,
              title='Covid Deaths Age Distribution - Thailand',
              png_prefix='deaths_age_dash', cols_subset=death_cols,
              unknown_name='Deaths Unknown Age', unknown_total='Deaths', unknown_percent=False,
              ma_days=7,
              kind='area', stacked=True, percent_fig=True, clean_end=True,
              cmap=get_cycle('summer_r', len(death_cols), extras=["gainsboro"]),
              footnote_left=f'{source}Data Source: MOPH Covid-19 Dashboard')


    # Excess Deaths

    # TODO: look at causes of death
    # - https://data.humdata.org/dataset/who-data-for-thailand
    # - https://en.wikipedia.org/wiki/Health_in_Thailand
    # - pm2.5?
    # - change in reporting?
    # Just normal ageing population

    #  Take avg(2015-2019)/(2021) = p num. (can also correct for population changes?)
    def calc_pscore(adf):
        months = adf.groupby(["Year", "Month"], as_index=False).sum().pivot(columns="Year", values="Deaths", index="Month")
        death3_avg = months[years3].mean(axis=1)
        death3_min = months[years3].min(axis=1)
        death3_max = months[years3].max(axis=1)
        death5_avg = months[years5].mean(axis=1)
        death5_min = months[years5].min(axis=1)
        death5_max = months[years5].max(axis=1)
        result = pd.DataFrame()
        for year in [2020, 2021]:
            res = pd.DataFrame()
            res['Excess Deaths'] = (months[year] - death5_avg)
            res['P-Score'] = res['Excess Deaths'] / death5_avg * 100
            res['Pre Avg'], res['Pre Min'], res['Pre Max'] = death3_avg, death3_min, death3_max
            res['Pre 5 Avg'], res['Pre 5 Min'], res['Pre 5 Max'] = death5_avg, death5_min, death5_max
            res['Deaths All Month'] = months[year]
            for y in range(2012, 2022):
                res[f'Deaths {y}'] = months[y]
            res['Date'] = pd.to_datetime(f'{year}-' + res.index.astype(int).astype(str) + '-1',
                                         format='%Y-%m') + MonthEnd(0)
            result = result.combine_first(res.reset_index().set_index("Date"))
        result = result.dropna(subset=['P-Score'])
        return result.drop(columns=["Month"])

    excess = import_csv("deaths_all", dir="inputs/json", date_cols=[])
    excess = join_provinces(excess, 'Province', ['region', 'Health District Number'])
    years5 = list(range(2015, 2020))
    years3 = [2015, 2016, 2017, 2018]

    all = calc_pscore(excess)
    all['Deaths Covid'] = df['Deaths'].groupby(pd.Grouper(freq='M')).sum()
    all['Deaths (ex. Known Covid)'] = all['Deaths All Month'] - all['Deaths Covid']
    all['Deaths 2021 (ex. Known Covid)'] = all['Deaths 2021'] - all['Deaths Covid']
    all['Expected Deaths'] = all['Pre 5 Avg'] + all['Deaths Covid']
    all['Deviation from expected Deaths'] = (all['Excess Deaths'] - all['Deaths Covid']) / all['Pre Avg'] * 100
    legends = [
        'Deviation from Normal Deaths excl. Covid Deaths',
        'Deviation from Normal Deaths Average 2015-19',
    ]
    plot_area(df=all, png_prefix='deaths_pscore',
              title='Monthly Deaths above Normal - Thailand',
              legends=legends,
              cols_subset=['Deviation from expected Deaths', 'P-Score'],
              ma_days=None, 
              kind='line', stacked=False, percent_fig=False, limit_to_zero=False,
              cmap='tab10',
              y_formatter=perc_format,
              footnote="Note: There is some variability in comparison years 2015-19 so normal is a not a certain value.",
              footnote_left=f'{source}Data Source: MOPH Covid-19 Dashboard')

    cols = [f'Deaths {y}' for y in range(2012, 2021, 1)]
    by_month = pd.DataFrame(all)
    by_month['Month'] = by_month.index.strftime('%B')
    years2020 = by_month["2020-01-01":"2021-01-01"][cols + ['Month']].reset_index().set_index("Month")
    cols2021 = ['Deaths 2021', 'Deaths 2021 (ex. Known Covid)']
    years2021 = by_month["2021-01-01":"2022-01-01"][cols2021 + ['Month']].reset_index().set_index("Month")
    by_month = years2020.combine_first(years2021).sort_values("Date")
    cols = cols + cols2021

    plot_area(df=by_month, 
              title='Excess Deaths - Thailand',
              legend_pos="lower center", legend_cols=3,
              png_prefix='deaths_excess_years', cols_subset=cols,
              ma_days=None,
              kind='bar', stacked=False, percent_fig=False, limit_to_zero=False,
              cmap='tab10',
              footnote='\n\n\n\nNote: Number of deaths from all causes compared to previous years.',
              footnote_left=f'\n\n\n\n{source}Data Source: MOPH Covid-19 Dashboard')

    # Test to get box plots working
    # https://stackoverflow.com/questions/57466631/matplotlib-boxplot-and-bar-chart-shifted-when-overlaid-using-twinx
    # fig, ax = plt.subplots(figsize=(20, 10))
    # ax2 = ax.twinx()
    # boxes = pan_months[[f'Deaths {y}' for y in range(2015,2020)]].transpose()
    # boxes.plot.box(ax=ax2, grid=False)
    # pan_months[['Deaths (ex. Known Covid)', 'Deaths Covid', ]].plot.bar(ax=ax, stacked=True, align='center', alpha=0.3)
    # ax2.set_ylim(0)
    # ax.set_ylim(ax2.get_ylim())
    # plt.savefig("test.png")

    # TODO: Why the spikes in 2018 and 2019? Is there a way to correct? Change in reporting method?
    # death rates increase smoothly and so do total deaths here - https://knoema.com/atlas/Thailand/topics/Demographics/Mortality/Number-of-deaths
    # why different totals?
    # deaths by region 2019 - https://www.statista.com/statistics/1107886/thailand-number-of-male-deaths-by-region/
    # Age    Deaths
    # Year
    # 2002    514080   25896.0
    # 2003    514080   25968.0
    # 2004    514080   29951.0
    # 2005    514080   34385.0
    # 2006    514080   34098.0
    # 2007    514080   33573.0
    # 2008    514080   33743.0
    # 2009    514080   33718.0
    # 2010    514080   35049.0
    # 2011  10024560  322167.0
    # 2012  13194720  422776.0
    # 2013  12323640  413373.0
    # 2014  13194720  448601.0
    # 2015   9519048  456391.0
    # 2016   9519048  480434.0
    # 2017   9519048  468911.0
    # 2018   9519048  473541.0
    # 2019   9519048  506211.0
    # 2020   9519048  501438.0
    # 2021   4759524  263005.0
    # deaths causes over multiple years - https://data.worldbank.org/indicator/SH.DTH.NCOM.ZS?locations=TH
    #  - has suicides and road accidents also - as rate
    # pneumonia? - https://nucleuswealth.com/articles/is-thailand-hiding-covid-19-cases/
    # UN causes of death 2016 - https://www.who.int/nmh/countries/tha_en.pdf. total deaths - 539,000??
    # road deaths? http://rvpreport.rvpeservice.com/viewrsc.aspx?report=0486&session=16

    def group_deaths(excess, by, daily_covid):
        cols5y = [f'Deaths {y}' for y in years5]

        dfby = excess.groupby(by).apply(calc_pscore)
        covid_by = daily_covid.groupby([by, pd.Grouper(level=0, freq='M')])['Deaths'].sum()
        dfby['Deaths ex Covid'] = dfby['Deaths All Month'] - covid_by
        dfby['Covid Deaths'] = covid_by

        dfby = dfby.reset_index().pivot(values=["Deaths All Month", 'Deaths ex Covid', 'Covid Deaths'] + cols5y,
                                        index="Date",
                                        columns=by)
        dfby.columns = [' '.join(c) for c in dfby.columns]

        # Bar chart is not aligned right otherwise
        dfby = dfby.set_index(dfby.index - pd.offsets.MonthBegin(1))
        labels = list(excess[by].unique())

        # Need to adjust each prev year so stacked in the right place
        for i in range(1, len(labels)):
            prev_bars = dfby[[f'Deaths All Month {label}' for label in labels[:i]]].sum(axis=1)
            covid = dfby[f'Covid Deaths {labels[i]}']

            for year in cols5y:
                dfby[f'{year} {labels[i]}'] += prev_bars.add(covid, fill_value=0)
        return dfby, labels

    # Do comparison bar charts to historical distribution of years

    pan_months = pd.DataFrame(all)
    pan_months = pan_months.set_index(pan_months.index - pd.offsets.MonthBegin(1))
    # pan_months['Month'] = pan_months['Date'].dt.to_period('M')

    by_region, regions = group_deaths(excess, "region", cases)
    # # Get covid deaths by region
    # covid_by_region = cases.groupby([pd.Grouper(level=0, freq='M'), "region"])['Deaths'].sum()
    # # fix up dates to start on 1st (for bar graph)
    # covid_by_region = covid_by_region.reset_index("region")
    # covid_by_region = covid_by_region.set_index(covid_by_region.index - pd.offsets.MonthBegin(1))
    # by_region = by_region.combine_first(covid_by_region.pivot(values="Deaths", columns="region").add_prefix("Covid Deaths "))

    by_age = excess.pipe(cut_ages, [10, 20, 30, 40, 50, 60, 70])
    # by_age = excess.pipe(cut_ages, [15, 65, 75, 85])
    new_cols = dict({a: remove_prefix(a, "Deaths Age ") for a in death_cols}, **{"Deaths Age 60-": "60+"})
    # Get the deaths ages and unstack so can be matched with excess deaths
    covid_age = df[death_cols].rename(
        columns=new_cols).unstack().to_frame("Deaths").rename_axis(["Age Group", "Date"]).reset_index("Age Group")
    by_age, ages = group_deaths(by_age, "Age Group", covid_age)

    footnote = """
Expected Deaths = Min/Mean/Max of years before the pandemic ({year_span}) + Known Covid Deaths.
Note: Excess deaths can be changed by many factors other than Covid.
    """.strip()
    footnote3 = f"""{footnote}
2015-2018 was used to compare for the most stable death rates. For other comparisons see
https://djay.github.io/covidthailand/#excess-deaths
    """.strip()
    footnote5 = f"""{footnote}
For a comparison excluding 2019 (which had higher than expected deaths)
see https://djay.github.io/covidthailand/#excess-deaths
    """.strip()

    for years in [years5, years3]:
        year_span = f"{min(years)}-{max(years)}"
        cols_y = [f'Deaths {y}' for y in years]
        note = (footnote5 if len(years) > 4 else footnote3).format(year_span=year_span)
        suffix = "_5y" if len(years) > 4 else ""

        legends = [
            'Deaths excl. Covid Deaths',
            'Confirmed Covid Deaths',
        ]
        plot_area(df=pan_months,
                  title=f'Deaths from All Causes {year_span} - Thailand',
                  legends=legends,
                  legend_cols=2, legend_pos="lower center",
                  png_prefix=f'deaths_excess_covid{suffix}',
                  cols_subset=['Deaths (ex. Known Covid)', 'Deaths Covid'],
                  box_cols=cols_y,
                  periods_to_plot=['all'],
                  kind='bar', stacked=True,
                  cmap='tab10',
                  footnote=note,
                  footnote_left=f'{source}Data Sources: Office of Registration Administration\n  Department of Provincial Administration')

        plot_area(df=by_region,
                  title=f'Deaths from All Causes vs. Expected Deaths by Region ({year_span}) - Thailand',
                  legends=[f'{reg}' for reg in regions],
                  legend_cols=4, legend_pos="lower center",
                  png_prefix=f'deaths_excess_region{suffix}',
                  cols_subset=[f'Deaths All Month {reg}' for reg in regions],
                  periods_to_plot=['all'],
                  box_cols=[[f"{y} {reg}" for y in cols_y] for reg in regions],
                  kind='bar', stacked=True,
                  cmap='tab10',
                  footnote=note,
                  footnote_left=f'{source}Data Sources: Office of Registration Administration\n  Department of Provincial Administration')

        plot_area(df=by_age,
                  title=f'Deaths from All Causes by Age vs. Expected Deaths ({year_span}) - Thailand',
                  legends=[f'{age}' for age in ages],
                  legend_cols=2, legend_pos="center left",
                  png_prefix=f'deaths_excess_age_bar{suffix}',
                  cols_subset=[f'Deaths All Month {age}' for age in ages],
                  box_cols=[[f"{y} {age}" for y in cols_y] for age in ages],
                  periods_to_plot=['all'],
                  kind='bar', stacked=True,
                  cmap='tab10',
                  footnote=note,
                  footnote_left=f'{source}Data Sources: Office of Registration Administration\n  Department of Provincial Administration')

    by_province = excess.groupby(["Province"]).apply(calc_pscore)
    by_province['Deaths Covid'] = cases.groupby(["Province", pd.Grouper(level=0, freq='M')])['Deaths'].sum()
    top5 = by_province.pipe(topprov, lambda adf: (adf["Excess Deaths"] - adf['Deaths Covid']) / adf['Pre 5 Avg'] * 100, num=5)
    cols = top5.columns.to_list()
    plot_area(df=top5, 
              title='Deviation from Expected Monthly Deaths - Thailand',
              png_prefix='deaths_expected_prov', cols_subset=cols,
              periods_to_plot=['all'],
              ma_days=None,
              kind='line', stacked=False, percent_fig=False, limit_to_zero=False,
              cmap='tab10',
              footnote='Note: Average 2015-19 plus known Covid deaths.\n' + footnote5,
              footnote_left=f'{source}Data Sources: Office of Registration Administration\n  Department of Provincial Administration')

    top5 = by_province.pipe(topprov, lambda adf: adf["Excess Deaths"], num=7)
    cols = top5.columns.to_list()
    plot_area(df=top5,
              title='Excess Deaths - Highest Provinces - Thailand',
              png_prefix='deaths_excess_prov', cols_subset=cols,
              periods_to_plot=['all'],
              ma_days=None,
              kind='line', stacked=False, percent_fig=False, limit_to_zero=False,
              cmap='tab10',
              footnote_left=f'{source}Data Sources: Office of Registration Administration\n  Department of Provincial Administration')

    by_district = excess.groupby("Health District Number").apply(calc_pscore)
    by_district['Deaths Covid'] = cases.groupby(["Health District Number", pd.Grouper(level=0, freq='M')])['Deaths'].sum()
    by_district['Deviation from expected Deaths'] = (by_district['Excess Deaths'] - by_district['Deaths Covid']) / by_district['Pre 5 Avg'] * 100
    top5 = area_crosstab(by_district, "Deviation from expected Deaths", "")
    cols = rearrange([f'Deviation from expected Deaths Area {area}' for area in DISTRICT_RANGE_SIMPLE], *FIRST_AREAS)
    plot_area(df=top5, 
              title='Deviation from Expected Monthly Deaths - Thailand',
              legends=AREA_LEGEND,
              png_prefix='deaths_expected_area', cols_subset=cols,
              periods_to_plot=['all'],
              ma_days=None,
              kind='line', stacked=False, percent_fig=False, limit_to_zero=False,
              cmap='tab20',
              footnote='Note: Average 2015-2019 plus known Covid deaths.',
              footnote_left=f'{source}Data Sources: Office of Registration Administration\n  Department of Provincial Administration')

    by_age = excess.pipe(cut_ages, [15, 65, 75, 85]).groupby(["Age Group"]).apply(calc_pscore)
    by_age = by_age.reset_index().pivot(values=["P-Score"], index="Date", columns="Age Group")
    by_age.columns = [' '.join(c) for c in by_age.columns]

    plot_area(df=by_age,
              title='Excess Deaths (P-Score) by Age - Thailand',
              png_prefix='deaths_pscore_age',
              cols_subset=list(by_age.columns),
              periods_to_plot=['all'],
              kind='line', stacked=False, limit_to_zero=False,
              cmap='tab10',
              footnote='P-Test: A statistical method used to test one or more hypotheses within\n a population or a proportion within a population.',
              footnote_left=f'{source}Data Sources: Office of Registration Administration\n  Department of Provincial Administration')
