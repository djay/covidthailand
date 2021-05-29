import os
import pathlib
from typing import Sequence, Union, List, Callable

import matplotlib
import matplotlib.cm
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter
import pandas as pd

from covid_data import get_ifr, scrape_and_combine
from utils_pandas import custom_cm, get_cycle, human_format, import_csv, rearrange, topprov, trendline
from utils_scraping import remove_suffix
from utils_thai import DISTRICT_RANGE, DISTRICT_RANGE_SIMPLE, PROVINCES, AREA_LEGEND, AREA_LEGEND_SIMPLE, \
    AREA_LEGEND_ORDERED, FIRST_AREAS, thaipop, thaipop2


def plot_area(df: pd.DataFrame, png_prefix: str, cols_subset: Union[str, Sequence[str]], title: str,
              legends: List[str] = None, kind: str = 'line', stacked=False, percent_fig: bool = True,
              unknown_name: str = 'Unknown', unknown_total: str = None, unknown_percent=False,
              ma_days: int = None, cmap: str = 'tab20',
              reverse_cmap: bool = False, highlight: List[str] = [],
              y_formatter: Callable[[float, int], str] = human_format, clean_end=True,
              between: List[str] = []) -> None:
    """Creates one .png file for several time periods, showing data in absolute numbers and percentage terms.

    :param df: data frame containing all available data
    :param png_prefix: file prefix (file suffix is '.png')
    :param cols_subset: specify columns from the pandas DataFrame based on either a column name prefix or based on a
                        list of column names.
    :param title: plot title
    :param legends: legends to be used on the plots (line chart and percentage)
    :param kind: the type of plot (line chart or area chart)
    :param stacked: whether the line chart should use stacked lines
    :param percent_fig: whether the percentage chart should be included
    :param unknown_name: the column name containing data related to unknowns
    :param unknown_total: the column name (to be created) with unknown totals
    :param unknown_percent: to include unknowns in a percentage fig if enabled
    :param ma_days: number of days used when computing the moving average
    :param cmap: the matplotlib colormap to be used
    :param reverse_cmap: whether the colormap should be reversed
    :param highlight: cols to make thicker to highlight them
    :param y_formatter: function to format y axis numbers
    :param clean_end: remove days at end if there is no data (inc unknown)
    """
    if type(cols_subset) is str:
        cols = [c for c in df.columns if str(c).startswith(cols_subset)]
    else:
        cols = cols_subset

    if ma_days:
        for c in cols:
            df[f'{c} (MA)'] = df[c].rolling(f'{ma_days}d').mean()
        cols = [f'{c} (MA)' for c in cols]
        ma_suffix = ' (MA)'
    else:
        ma_suffix = ''

    # try to hone in on last day of "important" data. Assume first col
    last_update = df[cols[:1]].dropna().index[-1].date().strftime('%d %b %Y')  # date format chosen: '05 May 2021'
    # last_date_excl = df[cols].last_valid_index() # last date with some data (not inc unknown)

    if unknown_total:
        if ma_days:
            df[f'{unknown_total} (MA)'] = df[unknown_total].rolling(f'{ma_days}d').mean()
        total_col = f'{unknown_total}{ma_suffix}'
        unknown_col = f'{unknown_name}{ma_suffix}'
        other_cols = set(cols) - set([unknown_col])
        # TODO: should not be 0 when no unknown_total
        df[unknown_col] = df[total_col].sub(df[other_cols].sum(axis=1), fill_value=None).clip(lower=0)
        if unknown_col not in cols:
            cols = cols + [unknown_col]

    if percent_fig:
        perccols = [c for c in cols if not unknown_total or unknown_percent or c != f'{unknown_name}{ma_suffix}']
        for c in perccols:
            df[f'{c} (%)'] = df[f'{c}'] / df[perccols].sum(axis=1) * 100
        if unknown_total and not unknown_percent:
            df[f'{unknown_name}{ma_suffix} (%)'] = 0
        perccols = [f'{c} (%)' for c in cols]

    title = f'{title}\n'

    if ma_days:
        title = title + f'({ma_days} day rolling average)\n'
    title += f'Last Data: {last_update}\n'
    title += 'https://djay.github.io/covidthailand'

    # if legends are not specified then use the columns names else use the data passed in the 'legends' argument
    if legends is None:
        legends = [remove_suffix(c, " (MA)") for c in cols]
    elif unknown_total and unknown_name not in legends:
        legends = legends + [unknown_name]

    if unknown_total:
        colormap = custom_cm(cmap, len(cols) + 1, 'lightgrey', flip=reverse_cmap)
    else:
        colormap = custom_cm(cmap, len(cols), flip=reverse_cmap)

    # drop any rows containing 'NA' if they are in the specified columns (=subset of all columns)
    # df_clean = clip_dataframe(df_all=df, cols=cols, n_rows=10)
    last_date_unknown = df[cols].last_valid_index()  # last date with some data (inc unknown)
    if clean_end:
        df_clean = df.loc[:last_date_unknown]
    else:
        df_clean = df

    periods = {
        'all': df_clean,
        '1': df_clean[:'2020-06-01'],
        '2': df_clean['2020-12-12':],
        '3': df_clean['2021-04-01':],
        '30d': df_clean.last('30d')
    }

    quick = os.environ.get('USE_CACHE_DATA', False) == 'True'  # TODO: have its own switch
    if quick:
        periods = {key: periods[key] for key in ['2']}

    for suffix, df_plot in periods.items():
        if df_plot.empty:
            continue

        if percent_fig:
            f, (a0, a1) = plt.subplots(2, 1, gridspec_kw={'height_ratios': [3, 2]}, figsize=[20, 12])
        else:
            f, a0 = plt.subplots(figsize=[20, 12])
        # plt.rcParams["axes.prop_cycle"] = get_cycle(colormap)
        a0.set_prop_cycle(get_cycle(colormap))

        if y_formatter is not None:
            a0.yaxis.set_major_formatter(FuncFormatter(y_formatter))

        if kind == "area":
            df_plot.plot(ax=a0, y=cols, kind=kind, stacked=stacked)
        else:
            for c in cols:
                style = "--" if c in [f"{b}{ma_suffix}" for b in between] else None
                width = 5 if c in [f"{h}{ma_suffix}" for h in highlight] else None
                df_plot.plot(ax=a0, y=c, linewidth=width, style=style, kind=kind)
        #     a0.plot(df_plot.index, df_plot.reset_index()[c])
        # if between:
        #     a0.fill_between(x=df.index.values, y1=between[0], y2=between[1], data=df)

        a0.set_title(label=title)
        a0.legend(labels=legends)

        if unknown_total:
            a0.set_ylabel(unknown_total)
        a0.xaxis.label.set_visible(False)

        if percent_fig:
            df_plot.plot(ax=a1, y=perccols, kind='area', colormap=colormap, legend=False)
            a1.set_ylabel('Percent')
            a1.xaxis.label.set_visible(False)

        plt.tight_layout()
        plt.savefig(os.path.join("outputs", f'{png_prefix}_{suffix}.png'))
        plt.close()

    return None


def save_plots(df: pd.DataFrame) -> None:
    print('======== Generating Plots ==========')

    # matplotlib global settings
    matplotlib.use('AGG')
    plt.style.use('seaborn-whitegrid')
    plt.rcParams.update({'font.size': 16})
    plt.rc('legend', **{'fontsize': 14})

    # create directory if it does not exists
    pathlib.Path('./outputs').mkdir(parents=True, exist_ok=True)

    # Computed data
    # TODO: has a problem if we have local transmission but no proactive
    # TODO: put somewhere else
    walkins = pd.DataFrame(df["Cases Local Transmission"] - df["Cases Proactive"], columns=['Cases Walkin'])
    df = df.combine_first(walkins)

    cols = ['Tests XLS', 'Tests Public', 'Tested PUI', 'Tested PUI Walkin Public', ]
    legends = ['Tests Performed (All)', 'Tests Performed (Public)', 'PUI', 'PUI (Public)', ]
    plot_area(df=df, png_prefix='tests', cols_subset=cols,
              title='Thailand PCR Tests and PUI (totals exclude some proactive testing)', legends=legends,
              kind='line', stacked=False, percent_fig=False, ma_days=7, cmap='tab10')

    cols = ['Tested Cum',
            'Tested PUI Cum',
            'Tested Not PUI Cum',
            'Tested Proactive Cum',
            'Tested Quarantine Cum',
            'Tested PUI Walkin Private Cum',
            'Tested PUI Walkin Public Cum']
    plot_area(df=df, png_prefix='tested_pui', cols_subset=cols,
              title='PCR Tests and PUI in Thailand (excludes some proactive test)',
              kind='line', stacked=False, percent_fig=False, ma_days=7, cmap='tab10')

    ###############
    # Positive Rate
    ###############
    df["Positivity PUI"] = df["Cases"] / df["Tested PUI"] * 100
    df["Positivity Public"] = df["Pos Public"] / df["Tests Public"] * 100
    df["Positivity Cases/Tests"] = df["Cases"] / df["Tests XLS"] * 100
    df["Positivity Public+Private"] = (df["Pos XLS"] / df["Tests XLS"] * 100)
    df['Positivity Walkins/PUI'] = df['Cases Walkin'] / df['Tested PUI'] * 100
    df['Positive Rate Private'] = (df['Pos Private'] / df['Tests Private']) * 100
    df['Cases per PUI3'] = df['Cases'] / df['Tested PUI'] / 3.0 * 100
    df['Cases per Tests'] = df['Cases'] / df['Tests XLS'] * 100

    cols = [
        'Positivity Public+Private', 'Cases per Tests', 'Cases per PUI3',
        'Positive Rate Private'
    ]
    legends = [
        'Positive Rate: Share of PCR tests that are positive ',
        'Share of PCR tests that have Covid', 'Share of PUI*3 that have Covid',
        'Share of Private PCR tests that are positive'
    ]
    plot_area(df=df,
              png_prefix='positivity',
              cols_subset=cols,
              title='Positive Rate: Is enough testing happening?',
              legends=legends,
              kind='line',
              stacked=False,
              percent_fig=False,
              ma_days=7,
              cmap='tab10',
              highlight=['Positivity Public+Private'])

    df['PUI per Case'] = df['Tested PUI'].divide(df['Cases'])
    df['PUI3 per Case'] = df['Tested PUI'] * 3 / df['Cases']
    df['PUI3 per Walkin'] = df['Tested PUI'] * 3 / df['Cases Walkin']
    df['PUI per Walkin'] = df['Tested PUI'].divide(df['Cases Walkin'])
    df['Tests per case'] = df['Tests XLS'] / df['Cases']
    df['Tests per positive'] = df['Tests XLS'] / df['Pos XLS']

    cols = ['Tests per positive', 'Tests per case', 'PUI per Case', 'PUI3 per Case', 'PUI per Walkin']
    legends = [
        'PCR Tests per Positive',
        'PCR Tests per Case',
        'PUI per Case',
        'PUI*3 per Case',
        'PUI per Walkin Case',
    ]
    plot_area(df=df,
              png_prefix='tests_per_case',
              cols_subset=cols,
              title='Thailand Tests per Confirmed Case',
              legends=legends,
              kind='line',
              stacked=False,
              percent_fig=False,
              ma_days=7,
              cmap='tab10')

    cols = ['Positivity Cases/Tests',
            'Positivity Public',
            'Positivity PUI',
            'Positive Rate Private',
            'Positivity Public+Private']
    legends = [
        'Confirmed Cases / Tests Performed (Public)',
        'Positive Results / Tests Performed (Public)',
        'Confirmed Cases / PUI',
        'Positive Results / Tests Performed (Private)',
        'Positive Results / Tests Performed (All)',
    ]
    plot_area(df=df,
              png_prefix='positivity_all',
              cols_subset=cols,
              title='Positive Rate',
              legends=legends,
              kind='line',
              stacked=False,
              percent_fig=False,
              ma_days=7,
              cmap='tab10')

    ########################
    # Public vs Private
    ########################
    df['Tests Private Ratio'] = (df['Tests Private'] / df['Tests Public']).rolling('7d').mean()
    df['Tests Positive Private Ratio'] = (df['Pos Private'] / df['Pos Public']).rolling('7d').mean()
    df['Positive Rate Private Ratio'] = (df['Pos Private'] / (df['Tests Private'])
                                         / (df['Pos Public'] / df['Tests Public'])).rolling('7d').mean()
    df['PUI Private Ratio'] = (df['Tested PUI Walkin Private'] / df['Tested PUI Walkin Public']).rolling('7d').mean()
    cols = ['Tests Private Ratio', 'Tests Positive Private Ratio', 'PUI Private Ratio', 'Positive Rate Private Ratio']
    plot_area(df=df,
              png_prefix='tests_private_ratio',
              cols_subset=cols,
              title='Testing Private Ratio',
              kind='line',
              stacked=False,
              percent_fig=False,
              ma_days=7,
              cmap='tab10')

    ##################
    # Test Plots
    ##################
    cols = ['Cases',
            'Pos Public',
            'Pos XLS']
    legends = ['Confirmed Cases',
               'Positive Test Results (Public)',
               'Positive Test Results (All)']
    plot_area(df=df, png_prefix='cases', cols_subset=cols,
              title='Positive Test results compared to Confirmed Cases', legends=legends,
              kind='line', stacked=False, percent_fig=False, ma_days=7, cmap='tab10')

    cols = ['Cases',
            'Pos Area',
            'Pos XLS',
            'Pos Public',
            'Pos Private',
            'Pos']
    plot_area(df=df, png_prefix='cases_all', cols_subset=cols,
              title='Positive Test results compared to Confirmed Cases',
              kind='line', stacked=False, percent_fig=False, ma_days=7, cmap='tab10')

    cols = ['Cases Imported', 'Cases Walkin', 'Cases Proactive']
    plot_area(df=df,
              png_prefix='cases_types',
              cols_subset=cols,
              title='Thailand Covid Cases by Where Tested',
              legends=[
                  "Quarantine (Imported)", "Hospital (Walk-ins/Traced)",
                  "Mobile Community Testing/Prisons (Proactive)"
              ],
              unknown_name='Cases Unknown',
              unknown_total='Cases',
              kind='area',
              stacked=True,
              percent_fig=False,
              ma_days=7,
              cmap="viridis")

    cols = ['Cases Symptomatic', 'Cases Asymptomatic']
    plot_area(df=df, png_prefix='cases_sym', cols_subset=cols, title='Thailand Covid Cases by Symptoms',
              unknown_name='Cases Symptomatic Unknown', unknown_total='Cases',
              kind='area', stacked=True, percent_fig=False, ma_days=None, cmap='tab10')

    # cols = ['Cases Imported','Cases Walkin', 'Cases Proactive', 'Cases Unknown']
    # plot_area(df=df, png_prefix='cases_types_all', cols_subset=cols, title='Thailand Covid Cases by Test Type',
    #           kind='area', stacked=True, percent_fig=False, ma_days=None, cmap='tab10')

    # Thailand Covid Cases by Age
    plot_area(df=df, png_prefix='cases_ages', cols_subset='Age', title='Thailand Covid Cases by Age',
              unknown_name='Unknown', unknown_total='Cases', unknown_percent=False,
              kind='area', stacked=True, percent_fig=True, ma_days=7, cmap='summer', reverse_cmap=True)

    # Thailand Covid Cases by Risk
    cols = [c for c in df.columns if str(c).startswith("Risk: ")]
    cols = rearrange(cols, "Risk: Imported", "Risk: Pneumonia",
                     "Risk: Community", "Risk: Contact", "Risk: Work",
                     "Risk: Entertainment", "Risk: Proactive Search",
                     "Risk: Unknown")
    plot_area(df=df,
              png_prefix='cases_causes',
              cols_subset=cols,
              title='Thailand Covid Cases by Risk',
              unknown_name='Risk: Investigating',
              unknown_total='Cases',
              kind='area',
              stacked=True,
              percent_fig=True,
              ma_days=7,
              cmap='tab20')

    ##########################
    # Tests by area
    ##########################
    plt.rc('legend', **{'fontsize': 12})

    cols = rearrange([f'Tests Area {area}' for area in DISTRICT_RANGE], *FIRST_AREAS)
    plot_area(df=df, png_prefix='tests_area', cols_subset=cols[0],
              title='PCR Tests by Health District (excludes proactive & private tests)', legends=AREA_LEGEND_SIMPLE,
              kind='area', stacked=True, percent_fig=False, ma_days=None, cmap='tab20')

    cols = rearrange([f'Pos Area {area}' for area in DISTRICT_RANGE_SIMPLE], *FIRST_AREAS)
    plot_area(df=df, png_prefix='pos_area', cols_subset=cols,
              title='PCR Positive Test Results by Health District (excludes proactive & private tests)',
              legends=AREA_LEGEND_SIMPLE,
              kind='area', stacked=True, percent_fig=False, ma_days=None, cmap='tab20')

    for area in DISTRICT_RANGE_SIMPLE:
        df[f'Tests Area {area} (i)'] = df[f'Tests Area {area}'].interpolate(limit_area="inside")
    test_cols = [f'Tests Area {area} (i)' for area in DISTRICT_RANGE_SIMPLE]
    for area in DISTRICT_RANGE_SIMPLE:
        df[f'Tests Daily {area}'] = (df[f'Tests Area {area} (i)'] / df[test_cols].sum(axis=1) * df['Tests'])
    cols = rearrange([f'Tests Daily {area}' for area in DISTRICT_RANGE_SIMPLE], *FIRST_AREAS)
    plot_area(df=df,
              png_prefix='tests_area_daily',
              cols_subset=cols,
              title='PCR Tests by Thailand Health District (excludes some proactive tests)',
              legends=AREA_LEGEND_SIMPLE,
              kind='area',
              stacked=True,
              percent_fig=False,
              ma_days=7,
              cmap='tab20')

    for area in DISTRICT_RANGE_SIMPLE:
        df[f'Pos Area {area} (i)'] = df[f'Pos Area {area}'].interpolate(limit_area="inside")
    pos_cols = [f'Pos Area {area} (i)' for area in DISTRICT_RANGE_SIMPLE]
    for area in DISTRICT_RANGE_SIMPLE:
        df[f'Pos Daily {area}'] = (df[f'Pos Area {area} (i)'] / df[pos_cols].sum(axis=1) * df['Pos'])
    cols = rearrange([f'Pos Daily {area}' for area in DISTRICT_RANGE_SIMPLE], *FIRST_AREAS)
    plot_area(df=df, png_prefix='pos_area_daily',
              cols_subset=cols, legends=AREA_LEGEND_SIMPLE,
              title='Positive PCR Tests by Thailand Health District (excludes some proactive tests)',
              kind='area', stacked=True, percent_fig=False, ma_days=7, cmap='tab20')

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
              png_prefix='positivity_area',
              cols_subset=rearrange(cols, *FIRST_AREAS),
              legends=AREA_LEGEND_SIMPLE,
              title='Positive Rate by Health Area in proportion to Thailand positive rate '
              '(excludes some proactive tests)',
              kind='area',
              stacked=True,
              percent_fig=True,
              ma_days=7,
              cmap='tab20')

    for area in DISTRICT_RANGE_SIMPLE:
        df[f'Positivity Daily {area}'] = df[f'Pos Daily {area}'] / df[f'Tests Daily {area}'] * 100
    cols = [f'Positivity Daily {area}' for area in DISTRICT_RANGE_SIMPLE]
    topcols = df[cols].sort_values(by=df[cols].last_valid_index(), axis=1, ascending=False).columns[:5]
    legend = rearrange(AREA_LEGEND_ORDERED, *[cols.index(c) + 1 for c in topcols])[:5]
    plot_area(df=df, png_prefix='positivity_area_unstacked',
              cols_subset=topcols, legends=legend,
              title='Health Districts with the highest Positive Rate',
              kind='line', stacked=False, percent_fig=False, ma_days=7, cmap='tab10')

    for area in DISTRICT_RANGE_SIMPLE:
        df[f'Cases/Tests {area}'] = (
            df[f'Cases Area {area}'] / df[f'Tests Area {area}'] * 100
        )
    cols = [f'Cases/Tests {area}' for area in DISTRICT_RANGE_SIMPLE]
    plot_area(df=df, png_prefix='casestests_area_unstacked',
              cols_subset=rearrange(cols, *FIRST_AREAS), legends=AREA_LEGEND_SIMPLE,
              title='Health Districts with the highest Cases/Tests (excludes some proactive tests)',
              kind='area', stacked=False, percent_fig=False, ma_days=None, cmap='tab20')

    #########################
    # Case by area plots
    #########################
    cols = rearrange([f'Cases Area {area}' for area in DISTRICT_RANGE] + ['Cases Imported'], *FIRST_AREAS)
    plot_area(df=df, png_prefix='cases_areas',
              cols_subset=cols, legends=AREA_LEGEND + ['Imported Cases'],
              title='Thailand Covid Cases by Health District',
              unknown_name="Unknown District", unknown_total="Cases",
              kind='area', stacked=True, percent_fig=False, ma_days=7, cmap='tab20')

    cols = rearrange([f'Cases Walkin Area {area}' for area in DISTRICT_RANGE], *FIRST_AREAS)
    plot_area(df=df, png_prefix='cases_areas_walkins', cols_subset=cols,
              title='Thailand "Walk-in" Covid Cases by Health District', legends=AREA_LEGEND,
              kind='area', stacked=True, percent_fig=False, ma_days=None, cmap='tab20')

    cols = rearrange([f'Cases Proactive Area {area}' for area in DISTRICT_RANGE], *FIRST_AREAS)
    plot_area(df=df, png_prefix='cases_areas_proactive', cols_subset=cols,
              title='Thailand "Proactive" Covid Cases by Health District', legends=AREA_LEGEND,
              kind='area', stacked=True, percent_fig=False, ma_days=None, cmap='tab20')

    for area in DISTRICT_RANGE_SIMPLE:
        df[f'Case-Pos {area}'] = (
            df[f'Cases Area {area}'] - df[f'Pos Area {area}']
        )
    cols = [f'Case-Pos {area}' for area in DISTRICT_RANGE_SIMPLE]
    plot_area(df=df, png_prefix='cases_from_positives_area',
              cols_subset=rearrange(cols, *FIRST_AREAS), legends=AREA_LEGEND_SIMPLE,
              title='Which Health Districts have more cases than positive results?',
              kind='area', stacked=False, percent_fig=False, ma_days=None, cmap='tab20')

    #######################
    # Hospital plots
    #######################
    cols_delayed = ["Hospitalized", "Recovered", "Hospitalized Severe", "Hospitalized Respirator", "Hospitalized Field"]

    # TODO: we are missing some severe, ventilator mid april. why?
    df[cols_delayed] = df[cols_delayed].interpolate(limit_area="inside")

    # TODO: use unknowns to show this plot earlier?

    df["Hospitalized Severe"] = df["Hospitalized Severe"].sub(df["Hospitalized Respirator"], fill_value=0)
    non_split = df[["Hospitalized Severe", "Hospitalized Respirator", "Hospitalized Field"]].sum(skipna=False, axis=1)

    # sometimes we deaths and cases but not the rest so fillfoward.
    df["Hospitalized Hospital"] = df["Hospitalized"].sub(non_split, fill_value=0)
    cols = ["Hospitalized Respirator", "Hospitalized Severe", "Hospitalized Hospital", "Hospitalized Field"]
    legends = ['On Respirator', 'Severe Case', 'Hospitalised Other', 'Field Hospital']
    plot_area(df=df, png_prefix='cases_active', cols_subset=cols,
              title='Thailand Active Covid Cases\n(Severe, Field, and Respirator only available from '
                    '2021-04-24 onwards)',
              legends=legends,
              kind='area', stacked=True, percent_fig=False, ma_days=None, cmap='tab10')

    cols = ["Hospitalized Respirator", "Hospitalized Severe"]
    legends = ['On Ventilator', 'Severe Case']
    plot_area(df=df, png_prefix='active_severe', cols_subset=cols,
              title='Thailand Severe Covid Hospitalisations\n',
              legends=legends,
              kind='line', stacked=True, percent_fig=False, ma_days=None, cmap='tab10')



    # show cumulitive deaths, recoveres and hospitalisations (which should all add up to cases)
    df['Recovered since 2021-04-01'] = df['2021-04-14':]['Recovered'].cumsum()
    df['Died since 2021-04-01'] = df['2021-04-01':]['Deaths'].cumsum()
    df['Cases since 2021-04-01'] = df['2021-04-01':]['Cases'].cumsum()
    df['Other Active Cases'] = \
        df['Cases since 2021-04-01'].sub(non_split, fill_value=0).sub(df['Recovered since 2021-04-01'], fill_value=0)

    cols = [
        'Died since 2021-04-01',
        'Hospitalized Respirator',
        'Hospitalized Severe',
        'Other Active Cases',
        'Hospitalized Field',
        'Recovered since 2021-04-01',
    ]
    legends = [
        'Deaths from cases since 1st April', 'On Ventilator', 'In severe condition', 'In Hospital', 'In Field Hospital',
        'Recovered from cases since 1st April'
    ]
    plot_area(df=df,
              png_prefix='cases_cumulative',
              cols_subset=cols,
              title='Current outcome of Covid Cases since 1st April 2021',
              legends=legends,
              kind='area',
              stacked=True,
              percent_fig=False,
              ma_days=None,
              cmap='tab10')

    ####################
    # Vaccines
    ####################
    cols = [c for c in df.columns if str(c).startswith('Vac Group')]

    def clean_vac_leg(c):
        return c.replace(' Cum', '').replace('Vac Group', '').replace('1', 'Dose 1').replace('2', 'Dose 2')

    cols.sort(key=lambda c: clean_vac_leg(c)[-1] + clean_vac_leg(c))  # put 2nd shot at end

    legends = [clean_vac_leg(c) for c in cols]
    df_vac_groups = df['2021-02-16':][cols].interpolate(limit_area="inside")
    plot_area(df=df_vac_groups, png_prefix='vac_groups', cols_subset=cols,
              title='Thailand Vaccinations by Groups\n(% of 2 doses per Thai population)', legends=legends,
              kind='area', stacked=True, percent_fig=False, ma_days=None, cmap='Set3',
              y_formatter=thaipop2)

    cols = rearrange([f'Vac Given 1 Area {area} Cum' for area in DISTRICT_RANGE_SIMPLE], *FIRST_AREAS)
    df_vac_areas_s1 = df['2021-02-16':][cols].interpolate()
    plot_area(df=df_vac_areas_s1,
              png_prefix='vac_areas_s1',
              cols_subset=cols,
              title='Thailand Vaccinations (1st Shot) by Health District\n(% per population)',
              legends=AREA_LEGEND_SIMPLE,
              kind='area',
              stacked=True,
              percent_fig=False,
              ma_days=None,
              cmap='tab20',
              y_formatter=thaipop)

    cols = rearrange([f'Vac Given 2 Area {area} Cum' for area in DISTRICT_RANGE_SIMPLE], *FIRST_AREAS)
    df_vac_areas_s2 = df['2021-02-16':][cols].interpolate()
    plot_area(df=df_vac_areas_s2, png_prefix='vac_areas_s2', cols_subset=cols,
              title='Thailand Fully Vaccinated (2nd Shot) by Health District\n(% population full vaccinated)',
              legends=AREA_LEGEND_SIMPLE,
              kind='area', stacked=True, percent_fig=False, ma_days=None, cmap='tab20',
              y_formatter=thaipop)

    # Top 5 vaccine rollouts
    vac = import_csv("vaccinations")
    vac['Date'] = pd.to_datetime(vac['Date'])
    vac = vac.set_index('Date')
    vac = vac.join(PROVINCES['Population'], on='Province')
    top5 = vac.pipe(topprov, lambda df: df['Vac Given 2 Cum'] / df['Population'] * 100)

    cols = top5.columns.to_list()
    plot_area(df=top5, png_prefix='vac_top5_full', cols_subset=cols,
              title='Top 5 Thai Provinces Closest to Fully Vaccinated',
              kind='area', stacked=False, percent_fig=False, ma_days=None, cmap='tab20',
              )

    #######################
    # Cases by provinces
    #######################

    def increasing(adf: pd.DataFrame) -> pd.DataFrame:
        return adf["Cases"].rolling(3).mean().rolling(3).apply(trendline)

    def cases_ma(adf: pd.DataFrame) -> pd.DataFrame:
        return adf["Cases"].rolling(3).mean()

    def decreasing(adf: pd.DataFrame) -> pd.DataFrame:
        return 1 / increasing(adf)

    def cases_ma_7(adf: pd.DataFrame) -> pd.DataFrame:
        return adf["Cases"]

    cases = import_csv("cases_by_province").set_index(["Date", "Province"])

    top5 = cases.pipe(topprov, increasing, cases_ma, name="Province Cases (3d MA)", other_name=None, num=5)
    cols = top5.columns.to_list()
    plot_area(df=top5, png_prefix='cases_prov_increasing', cols_subset=cols,
              title='Provinces with Cases Trending Up\nin last 30 days (using 3 days rolling average)',
              kind='line', stacked=False, percent_fig=False, ma_days=None, cmap='tab10')

    top5 = cases.pipe(topprov, decreasing, cases_ma, name="Province Cases (3d MA)", other_name=None, num=5)
    cols = top5.columns.to_list()
    plot_area(df=top5, png_prefix='cases_prov_decreasing', cols_subset=cols,
              title='Provinces with Cases Trending Down\nin last 30 days (using 3 days rolling average)',
              kind='line', stacked=False, percent_fig=False, ma_days=None, cmap='tab10')

    top5 = cases.pipe(topprov, cases_ma_7, name="Province Cases", other_name="Other Provinces", num=6)
    cols = top5.columns.to_list()
    plot_area(df=top5, png_prefix='cases_prov_top', cols_subset=cols,
              title='Provinces with Most Cases',
              kind='line', stacked=False, percent_fig=False, ma_days=None, cmap='tab10')

    # TODO: work out based on districts of deaths / IFR for that district
    ifr = get_ifr()
    cases = cases.join(ifr[['ifr', 'Population', 'total_pop']], on="Province")
    cases['Deaths'] = cases['Deaths'].fillna(0)
    cases = cases.groupby("Province").apply(lambda df: df.assign(deaths_ma=df[
        "Deaths"].rolling(7, min_periods=1).mean()))

    cases["Infections Estimate"] = cases['Deaths'] / (cases['ifr'] / 100)
    cases["Infections Estimate (MA)"] = cases['deaths_ma'] / (cases['ifr'] / 100)
    cases_est = cases.groupby(["Date"]).sum()

    # TODO: work out unknown deaths and use whole thailand IFR for them
    # cases_est['Deaths Unknown'] = (df['Deaths'] - cases_est['Deaths']) / ifr['ifr']['Whole Kingdom'] * 100

    cases_est["Infections Estimate"] = cases_est["Infections Estimate"].shift(-14)
    cases_est["Infections Estimate (MA)"] = cases_est["Infections Estimate (MA)"].shift(-14)
    cases_est = cases_est.rename(columns=dict(Deaths="Deaths prov sum"))
    cases_est = cases_est.join(df['Deaths'], on="Date")
    cases_est['Cases (MA)'] = cases_est['Cases'].rolling("7d").mean()
    cases_est["Infections Estimate Simple"] = cases_est["Deaths"].shift(-14) / 0.0054
    cols = ["Cases (MA)", "Infections Estimate (MA)", "Infections Estimate", "Cases"]
    legend = [
        "Cases (7d moving avg.)", "Lower Estimate of Infections (7d moving avg.)", "Lower Estimate of Infections",
        "Cases"
    ]
    title = """Thailand Confirmed Covid Cases vs Estimate of Infections based on Deaths
Estimate of Infections = (Deaths - 14days)/(Province Infection Fatality Rate)
(DISCLAIMER: estimate is simple and probably lower than reality. see site below for more details on this model)"""
    plot_area(df=cases_est,
              png_prefix='cases_infections_estimate',
              cols_subset=cols,
              title=title,
              legends=legend,
              kind='line',
              stacked=False,
              percent_fig=False,
              ma_days=None,
              cmap='tab10',
              between=[
                  "Infections Estimate",
                  "Cases",
              ])

    ####################
    # Deaths
    ####################

    # predict median age of death based on population demographics

    df['Deaths Age Median (MA)'] = df['Deaths Age Median'].rolling('7d').mean()
    cols = ['Deaths Age Median (MA)', 'Deaths Age Max', 'Deaths Age Min']
    plot_area(df=df, png_prefix='deaths_age', cols_subset=cols, title='Thailand Covid Death Age Range',
              kind='line', stacked=False, percent_fig=False, ma_days=None, cmap='tab10',
              highlight=['Deaths Age Median (MA)'], between=['Deaths Age Max', 'Deaths Age Min'])

    cols = rearrange([f'Deaths Area {area}' for area in DISTRICT_RANGE], *FIRST_AREAS)
    plot_area(df=df, png_prefix='deaths_by_area', cols_subset=cols,
              title='Thailand Covid Deaths by health District', legends=AREA_LEGEND,
              kind='area', stacked=True, percent_fig=True, ma_days=7, cmap='tab20')


if __name__ == "__main__":

    df = scrape_and_combine()
    save_plots(df)
