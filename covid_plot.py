import os
import pathlib
from typing import Sequence, Union, List, Callable

import matplotlib
import matplotlib.cm
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter
import pandas as pd

from covid_data import get_ifr, scrape_and_combine
from utils_pandas import cum2daily, decreasing, get_cycle, human_format, import_csv, increasing, rearrange, \
    set_time_series_labels_2, topprov, value_ma
from utils_scraping import remove_suffix
from utils_thai import DISTRICT_RANGE, DISTRICT_RANGE_SIMPLE, AREA_LEGEND, AREA_LEGEND_SIMPLE, \
    AREA_LEGEND_ORDERED, FIRST_AREAS, get_provinces, join_provinces, thaipop


def plot_area(df: pd.DataFrame, png_prefix: str, cols_subset: Union[str, Sequence[str]], title: str,
              legends: List[str] = None, kind: str = 'line', stacked=False, percent_fig: bool = True,
              unknown_name: str = 'Unknown', unknown_total: str = None, unknown_percent=False,
              ma_days: int = None, cmap: str = 'tab20', actuals: bool = False,
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
    :param between: columns to display as dashed
    :param actuals: display non MA as dashed
    """

    if type(cols_subset) is str:
        cols = [c for c in df.columns if str(c).startswith(cols_subset)]
    else:
        cols = cols_subset

    orig_cols = cols

    plt.rcParams.update({
        "font.size": 24,
        "figure.titlesize": 30,
        "figure.titleweight": "bold",
        "axes.titlesize": 28,
        "legend.fontsize": 24,
        "axes.prop_cycle": get_cycle(cmap),
    })
    if len(cols) > 6:
        plt.rcParams.update({"legend.fontsize": 18})

    if actuals:
        # display the originals dashed along side MA
        if type(actuals) != list:
            actuals = cols
    else:
        actuals = []

    if ma_days:
        ma_suffix = ' (MA)'
        for c in cols:
            df[f'{c}{ma_suffix}'] = df[c].rolling(ma_days, min_periods=int(ma_days / 2), center=True).mean()
        cols = [f'{c}{ma_suffix}' for c in cols]
    else:
        ma_suffix = ''

    # try to hone in on last day of "important" data. Assume first col
    last_update = df[orig_cols[:1]].dropna().last_valid_index()  # date format chosen: '05 May 2021'
    # last_date_excl = df[cols].last_valid_index() # last date with some data (not inc unknown)

    if unknown_total:
        if ma_days:
            df[f'{unknown_total}{ma_suffix}'] = df[unknown_total].rolling(ma_days,
                                                                          min_periods=int(ma_days / 2),
                                                                          center=True).mean()
        total_col = f'{unknown_total}{ma_suffix}'
        unknown_col = f'{unknown_name}{ma_suffix}'
        other_cols = set(cols) - set([unknown_col])
        # TODO: should not be 0 when no unknown_total
        df[unknown_col] = df[total_col].sub(df[other_cols].sum(axis=1), fill_value=None).clip(lower=0)
        if unknown_col not in cols:
            cols = cols + [unknown_col]

    if percent_fig:
        perccols = [
            c for c in cols
            if (not unknown_total or unknown_percent or c != unknown_col) and c not in (between + actuals)
        ]
        for c in perccols:
            df[f'{c} (%)'] = df[f'{c}'] / df[perccols].sum(axis=1) * 100
        if unknown_total and not unknown_percent:
            df[f'{unknown_name}{ma_suffix} (%)'] = 0
        perccols = [f'{c} (%)' for c in perccols]

    title = f'{title}\n'

    if ma_days:
        title = title + f'({ma_days} day rolling average) '
    title += f"Last Data: {last_update.date().strftime('%d %b %Y')}\n"
    title += 'Sources: https://djay.github.io/covidthailand - (CC BY)'

    # if legends are not specified then use the columns names else use the data passed in the 'legends' argument
    if legends is None:
        legends = [remove_suffix(c, ma_suffix) for c in cols]
    if unknown_total and unknown_name not in legends:
        legends = legends + [unknown_name]

    # if unknown_total:
    #     colormap = custom_cm(cmap, len(cols) + 1, 'lightgrey', flip=reverse_cmap)
    # else:
    #     colormap = custom_cm(cmap, len(cols), flip=reverse_cmap)
#    colormap = cmap

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
            f, (a0, a1) = plt.subplots(2, 1, gridspec_kw={'height_ratios': [4, 2]}, figsize=[20, 15])
        else:
            f, a0 = plt.subplots(figsize=[20, 12])
        # plt.rcParams["axes.prop_cycle"] = get_cycle(colormap)
        a0.set_prop_cycle(None)

        if y_formatter is not None:
            a0.yaxis.set_major_formatter(FuncFormatter(y_formatter))

        if kind != "line":
            areacols = [c for c in cols if c not in between]
            df_plot.plot(ax=a0, y=areacols, kind=kind, stacked=stacked)
            linecols = between + actuals
        else:
            linecols = cols + actuals
        for c in linecols:
            style = "--" if c in [f"{b}{ma_suffix}" for b in between] + actuals else None
            width = 5 if c in [f"{h}{ma_suffix}" for h in highlight] else None
            df_plot.plot(ax=a0,
                         y=c,
                         use_index=True,
                         linewidth=width,
                         style=style,
                         kind="line",
                         x_compat=kind == 'bar'  # Putting lines on bar plots doesn't work well
                         )

        if kind == "bar":
            set_time_series_labels_2(df_plot, a0)

        a0.set_title(label=title)
        leg = a0.legend(labels=legends)
        for line in leg.get_lines():
            line.set_linewidth(4.0)

        if unknown_total:
            a0.set_ylabel(unknown_total)
        a0.xaxis.label.set_visible(False)

        if percent_fig:
            a1.set_prop_cycle(None)
            df_plot.plot(ax=a1, y=perccols, kind='area', legend=False)
            a1.set_ylabel('Percent')
            a1.xaxis.label.set_visible(False)

        plt.tight_layout()
        path = os.path.join("outputs", f'{png_prefix}_{suffix}.png')
        plt.savefig(path)
        print("Plot:", path)
        plt.close()

    return None


def save_plots(df: pd.DataFrame) -> None:
    print('======== Generating Plots ==========')

    # matplotlib global settings
    matplotlib.use('AGG')
    plt.style.use('seaborn-whitegrid')

    # create directory if it does not exists
    pathlib.Path('./outputs').mkdir(parents=True, exist_ok=True)

    # Computed data
    # TODO: has a problem if we have local transmission but no proactive
    # TODO: put somewhere else
    walkins = pd.DataFrame(df["Cases Local Transmission"] - df["Cases Proactive"], columns=['Cases Walkin'])
    # In case XLS is not updated before the pptx
    df = df.combine_first(walkins).combine_first(df[['Tests',
                                                     'Pos']].rename(columns=dict(Tests="Tests XLS", Pos="Pos XLS")))

    cols = ['Tests XLS', 'Tests Public', 'Tested PUI', 'Tested PUI Walkin Public', ]
    legends = ['Tests Performed (All)', 'Tests Performed (Public)', 'PUI', 'PUI (Public)', ]
    plot_area(df=df, png_prefix='tests', cols_subset=cols,
              title='Thailand PCR Tests and PUI (totals exclude some proactive testing)', legends=legends,
              kind='line', stacked=False, percent_fig=False, ma_days=7, cmap='tab10',
              actuals=['Tests XLS'])

    cols = ['Tested Cum',
            'Tested PUI Cum',
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
    df["Positivity PUI"] = df["Cases"].divide(df["Tested PUI"]) * 100
    df["Positivity Public"] = df["Pos Public"] / df["Tests Public"] * 100
    df["Positivity Cases/Tests"] = df["Cases"] / df["Tests XLS"] * 100
    df["Positivity Public+Private"] = (df["Pos XLS"] / df["Tests XLS"] * 100)
    df['Positivity Walkins/PUI3'] = df['Cases Walkin'].divide(df['Tested PUI']) / 3.0 * 100
    df['Positive Rate Private'] = (df['Pos Private'] / df['Tests Private']) * 100
    df['Cases per PUI3'] = df['Cases'].divide(df['Tested PUI']) / 3.0 * 100
    df['Cases per Tests'] = df['Cases'] / df['Tests XLS'] * 100

    cols = [
        'Positivity Public+Private',
        'Positive Rate Private',
        'Cases per PUI3',
        'Positivity Walkins/PUI3',
    ]
    legends = [
        'Positive Rate: Share of PCR tests that are positive ',
        'Share of Private PCR tests that are positive',
        'Share of PUI*3 that are confirmed cases',
        'Share of PUI*3 that are walkin cases'
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
    df["Cases outside Prison"] = df["Cases Local Transmission"].sub(df["Cases Area Prison"], fill_value=0)

    cols = ['Cases',
            'Cases Walkin',
            'Pos XLS',
            'Pos Public',
            ]
    legends = ['Confirmed Cases',
               'Walkin Confirmed Cases',
               'Positive Test Results (All)',
               'Positive Test Results (Public)']
    plot_area(df=df, png_prefix='cases', cols_subset=cols,
              title='Positive Test results compared to Confirmed Cases', legends=legends,
              kind='line', stacked=False, percent_fig=False, ma_days=7, cmap="tab10",
              actuals=["Cases", "Pos XLS"])

    cols = [
        'Cases',
        'Cases outside Prison',
        'Cases Walkin',
        'Pos XLS',
    ]
    legends = [
        'Confirmed Cases',
        'Confirmed Cases (excl. Prisons)',
        'Confirmed Cases (excl. All Proactive Cases)',
        'Positive Test Results',
    ]
    plot_area(
        df=df,
        png_prefix='cases_tests',
        cols_subset=cols,
        title='21 day Moving Average comparing Cases to Positive tests',
        legends=legends,
        kind='line',
        stacked=False,
        percent_fig=False,
        ma_days=21,
        cmap="tab10",
    )

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
        'Confirmed Cases (excl. Prisons)',
        'Confirmed Cases (excl. All Proactive Cases)',
        'Positive Test Results',
    ]
    plot_area(
        df=df,
        png_prefix='cases_tests_cum3',
        cols_subset=cols,
        title='3rd Wave Cumulative Cases and Positive tests',
        legends=legends,
        kind='line',
        stacked=False,
        percent_fig=False,
        ma_days=None,
        cmap="tab10",
    )

    cols = ['Cases',
            'Pos Area',
            'Pos XLS',
            'Pos Public',
            'Pos Private',
            'Pos']
    plot_area(df=df, png_prefix='cases_all', cols_subset=cols,
              title='Positive Test results compared to Confirmed Cases',
              kind='line', stacked=False, percent_fig=False, ma_days=7, cmap='tab20')

    df['Cases Proactive Community'] = df['Cases Proactive'].sub(df['Cases Area Prison'], fill_value=0)
    cols = ['Cases Imported', 'Cases Walkin', 'Cases Proactive Community', 'Cases Area Prison']
    plot_area(df=df,
              png_prefix='cases_types',
              cols_subset=cols,
              title='Thailand Covid Cases by Where Tested',
              legends=[
                  "Quarantine (Imported)", "Hospital (Walk-ins/Traced)",
                  "Mobile Community Testing (Proactive)",
                  "Prison (Proactive)"
              ],
              unknown_name='Cases Unknown',
              unknown_total='Cases',
              kind='area',
              stacked=True,
              percent_fig=True,
              ma_days=7,
              cmap="tab10")

    cols = ['Cases Symptomatic', 'Cases Asymptomatic']
    plot_area(df=df, png_prefix='cases_sym', cols_subset=cols, title='Thailand Covid Cases by Symptoms',
              unknown_name='Cases Symptomatic Unknown', unknown_total='Cases',
              kind='area', stacked=True, percent_fig=False, ma_days=None, cmap='tab10')

    # cols = ['Cases Imported','Cases Walkin', 'Cases Proactive', 'Cases Unknown']
    # plot_area(df=df, png_prefix='cases_types_all', cols_subset=cols, title='Thailand Covid Cases by Test Type',
    #           kind='area', stacked=True, percent_fig=False, ma_days=None, cmap='tab10')

    # Thailand Covid Cases by Age
    cols = [c for c in df.columns if str(c).startswith('Age')]
    plot_area(df=df, png_prefix='cases_ages', cols_subset=cols, title='Thailand Covid Cases by Age',
              unknown_name='Unknown', unknown_total='Cases', unknown_percent=False,
              kind='area', stacked=True, percent_fig=True, ma_days=7, cmap=get_cycle('summer_r', len(cols) + 1))

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
              cmap='tab10')

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
              percent_fig=False,
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
              kind='area', stacked=True, percent_fig=True, ma_days=7, cmap='tab20')

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

    # because severe includes those on respirators
    df["Hospitalized Severe excl vent"] = df["Hospitalized Severe"].sub(df["Hospitalized Respirator"], fill_value=None)
    non_split = df[["Hospitalized Severe excl vent", "Hospitalized Respirator", "Hospitalized Field"]].sum(skipna=True,
                                                                                                           axis=1)

    df["Hospitalized Hospital"] = df["Hospitalized"].sub(non_split, fill_value=None)
    cols = ["Hospitalized Respirator", "Hospitalized Severe excl vent", "Hospitalized Hospital", "Hospitalized Field"]
    legends = ['On Ventilator', 'In Serious Condition', 'In Isolation/Hospital', 'In Field Hospital']
    plot_area(df=df, png_prefix='cases_active', cols_subset=cols,
              title='Thailand Active Covid Cases\n(Severe, Field, and Respirator only available from '
                    '2021-04-24 onwards)',
              legends=legends,
              kind='area', stacked=True, percent_fig=False, ma_days=None, cmap='tab10')

    cols = ["Hospitalized Severe", "Hospitalized Severe excl vent", "Hospitalized Respirator"]
    legends = ["In Serious Condition", 'In Serious Condition (without ventilator)', 'On Ventilator']
    plot_area(df=df, png_prefix='active_severe', cols_subset=cols,
              title='Thailand Active Covid Cases in Serious Condition',
              legends=legends,
              kind='line', stacked=True, percent_fig=False, ma_days=7, cmap='tab10', actuals=True)

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
        'Deaths from cases since 1st April', 'On Ventilator', 'In Serious Condition (without Ventilator)',
        'In Hospital/Mild', 'In Field Hospital', 'Recovered from cases since 1st April'
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

    def clean_vac_leg(c, first="(Half Vaccinated)", second="(Fully Vaccinated)"):
        return c.replace(
            ' Cum', '').replace(
            'Vac ', '').replace(
            "Group ", "").replace(
            'Only 1', first).replace(
            ' 1', " " + first).replace(
            ' 2', " " + second).replace(
            'Risk: Location', 'Under 60')

    groups = [c for c in df.columns if str(c).startswith('Vac Group')]
    df_vac_groups = df['2021-02-28':][groups]

    # go backwards to get rid of "dips". ie take later cum value as correct. e.g. 2021-06-21
    df_vac_groups = df_vac_groups.reindex(index=df_vac_groups.index[::-1])
    df_vac_groups = df_vac_groups.cummin()  # if later corrected down, take that number into past
    df_vac_groups = df_vac_groups.reindex(index=df_vac_groups.index[::-1])

    df_vac_groups['Vac Given Cum'] = df['Vac Given 1 Cum'] + df['Vac Given 2 Cum']
    df['Vac Given'] = df['Vac Given 1'] + df['Vac Given 2']
    df_vac_groups['Vac Given 1 Cum'] = df['Vac Given 1 Cum']
    df_vac_groups['Vac Given 2 Cum'] = df['Vac Given 2 Cum']
    df_vac_groups['Vac Imported Cum'] = df_vac_groups[[c for c in df_vac_groups.columns if "Vac Imported" in c]].sum()

    # now convert to daily and interpolate and then normalise to real daily total.
    vac_daily = cum2daily(df_vac_groups)
    # bring in any daily figures we might have collected first
    vac_daily = df[['Vac Given', 'Vac Given 1', 'Vac Given 2']].combine_first(vac_daily)
    daily_cols = [c for c in vac_daily.columns if c.startswith('Vac Group')]  # Keep for unknown
    # interpolate to fill gaps and get some values for each group
    vac_daily[daily_cols] = vac_daily[daily_cols].interpolate()
    # now normalise the filled in days so they add to their real total
    for c in daily_cols:
        vac_daily[c] = vac_daily[c] / vac_daily[daily_cols].sum(axis=1) * vac_daily['Vac Given']

    vac_daily['7d Runway Rate'] = (df['Vac Imported Cum'].fillna(method="ffill") - df_vac_groups['Vac Given Cum']) / 7
    days_to_target = (pd.Timestamp('2022-01-01') - vac_daily.index.to_series()).dt.days
    vac_daily['Target Rate 1'] = (50000000 - df_vac_groups['Vac Given 1 Cum']) / days_to_target
    vac_daily['Target Rate 2'] = (50000000 * 2 - df_vac_groups['Vac Given Cum']) / days_to_target

    daily_cols = rearrange(daily_cols, 2, 1, 4, 3, 10, 9, 8, 7, 6, 5)
    plot_area(
        df=vac_daily,
        png_prefix='vac_groups_daily',
        cols_subset=daily_cols,
        title='Thailand Daily Vaccinations by Priority Groups',
        legends=[
            'Doses per day needed to run out in a week', 'Needed to reach 70% 1st Dose in 2021',
            'Needed to reach 70% Fully Vaccinated in 2021'
        ] + [clean_vac_leg(c, "(1st jab)", "(2nd jab)") for c in daily_cols],  # bar puts the line first?
        kind='bar',
        stacked=True,
        percent_fig=False,
        between=['7d Runway Rate', 'Target Rate 1', 'Target Rate 2'],
        ma_days=None,
        cmap='Paired_r',
    )

    # Now turn daily back to cumulative since we now have estimates for every day without dips
    vac_cum = vac_daily.cumsum().combine_first(vac_daily[daily_cols].fillna(0).cumsum())
    vac_cum.columns = [f"{c} Cum" for c in vac_cum.columns]
    # Not sure why but we end up with large cumalitive than originally so normalise
    for c in groups:
        vac_cum[c] = vac_cum[c] / vac_cum[groups].sum(axis=1) * df_vac_groups['Vac Given Cum']

    # TODO: adjust allocated for double dose group
    second_dose = [c for c in groups if "2 Cum" in c]
    first_dose = [c for c in groups if "1 Cum" in c]
    vac_cum['Available Vaccines Cum'] = df['Vac Imported Cum'].fillna(method="ffill") - vac_cum[second_dose].sum(axis=1)

    cols = []
    # We want people vaccinated not total doses
    for c in groups:
        if "1" in c:
            vac_cum[c.replace("1", "Only 1")] = vac_cum[c].sub(vac_cum[c.replace("1", "2")], fill_value=0)
            cols.extend([c.replace("1", "2"), c.replace("1", "Only 1")])

    cols_cum = rearrange(cols, 1, 2, 3, 4, 9, 10, 7, 8, )
    cols_cum = cols_cum + ['Available Vaccines Cum']

    # TODO: get paired colour map and use do 5 + 5 pairs
    legends = [clean_vac_leg(c) for c in cols_cum]

    plot_area(df=vac_cum, png_prefix='vac_groups', cols_subset=cols_cum,
              title='Thailand Population Vaccinatated by Priority Groups', legends=legends,
              kind='area', stacked=True, percent_fig=True, ma_days=None, cmap='Paired_r',
              between=['Available Vaccines Cum'],
              y_formatter=thaipop)

    # Targets for groups
    # https://www.facebook.com/informationcovid19/photos/a.106455480972785/342985323986465/

    # 712,000 for medical staff
    # 1,900,000 for frontline staffs
    # 1,000,000 for village health volunteer
    # 5,350,000 for risk: disease
    # 12,500,000 for risk: over 60
    # 28,538,000 for general population
    # TODO: put in same order and colours as other groups
    goals = [
        ('Medical Staff', (712000 + 1000000)),
        ('Other Frontline Staff', 1900000),
        ('Risk: Location', 28538000),
        ('Risk: Disease', 5350000),
        ['Over 60', 12500000],
    ]
    for group, goal in goals:
        for d in [2, 1]:
            vac_cum[f'Vac Group {group} {d} Cum % ({goal/1000000:.1f}M)'] = vac_cum[
                f'Vac Group {group} {d} Cum'] / goal * 100
    cols2 = [c for c in vac_cum.columns if " Cum %" in c and "Vac Group " in c]
    legends = [clean_vac_leg(c) for c in cols2]
    plot_area(
        df=vac_cum,
        png_prefix='vac_groups_goals',
        cols_subset=cols2,
        title='Thailand Vaccination Goal Progress (to 70% of population)',
        legends=legends,
        kind='line',
        stacked=False,
        percent_fig=False,
        ma_days=None,
        cmap='Paired_r',
    )

    cols = rearrange([f'Vac Given Area {area} Cum' for area in DISTRICT_RANGE_SIMPLE], *FIRST_AREAS)
    df_vac_areas_s1 = df['2021-02-28':][cols].interpolate(limit_area="inside")
    plot_area(df=df_vac_areas_s1,
              png_prefix='vac_areas',
              cols_subset=cols,
              title='Thailand Vaccinations Doses by Health District',
              legends=AREA_LEGEND_SIMPLE,
              kind='area',
              stacked=True,
              percent_fig=False,
              ma_days=None,
              cmap='tab20',)

    # Top 5 vaccine rollouts
    vac = import_csv("vaccinations", ['Date', 'Province'])
    vac = vac.join(get_provinces()['Population'], on='Province')
    top5 = vac.pipe(topprov, lambda df: df['Vac Given Cum'] / df['Population'] * 100)

    cols = top5.columns.to_list()
    plot_area(df=top5, png_prefix='vac_top5_doses', cols_subset=cols,
              title='Top Provinces for Vaccination Doses per 100 people',
              kind='line', stacked=False, percent_fig=False, ma_days=None, cmap='tab10',
              )

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
    cases = join_provinces(cases, "Province")  # to fill in missing health districts
    cases = cases.fillna(0)  # all the other values

    top5 = cases.pipe(topprov,
                      increasing("Cases", 3),
                      value_ma("Cases", None),
                      name="Province Cases (3d MA)",
                      other_name=None,
                      num=7)
    cols = top5.columns.to_list()
    plot_area(df=top5,
              png_prefix='cases_prov_increasing',
              cols_subset=cols,
              title='Trending Up Confirmed Cases (by Province)',
              kind='line',
              stacked=False,
              percent_fig=False,
              ma_days=7,
              cmap='tab10')

    top5 = cases.pipe(topprov,
                      decreasing("Cases", 3),
                      value_ma("Cases", None),
                      name="Province Cases (3d MA)",
                      other_name=None,
                      num=7)
    cols = top5.columns.to_list()
    plot_area(df=top5,
              png_prefix='cases_prov_decreasing',
              cols_subset=cols,
              title='Trending Down Confirmed Cases (by Province)',
              kind='line',
              stacked=False,
              percent_fig=False,
              ma_days=7,
              cmap='tab10')

    top5 = cases.pipe(topprov, value_ma("Cases", 7), name="Province Cases", other_name="Other Provinces", num=6)
    cols = top5.columns.to_list()
    plot_area(df=top5,
              png_prefix='cases_prov_top',
              cols_subset=cols,
              title='Top Confirmed Cases (by Province)',
              kind='line',
              stacked=False,
              percent_fig=False,
              ma_days=7,
              cmap='tab10')

    for risk in ['Contact', 'Proactive Search', 'Community', 'Work']:
        top5 = cases.pipe(topprov,
                          increasing(f"Cases Risk: {risk}", 5),
                          value_ma(f"Cases Risk: {risk}", 0),
                          name=f"Province Cases {risk} (7d MA)",
                          other_name=None,
                          num=7)
        cols = top5.columns.to_list()
        plot_area(df=top5,
                  png_prefix=f'cases_{risk.lower().replace(" ","_")}_increasing',
                  cols_subset=cols,
                  title=f'Trending Up {risk} related Cases (by Province)',
                  kind='line',
                  stacked=False,
                  percent_fig=False,
                  ma_days=7,
                  cmap='tab10')


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

    # 11 days was median days to death reported in situation reports I think
    cases_est["Infections Estimate"] = cases_est["Infections Estimate"].shift(-11)
    # cases_est["Infections Estimate (MA)"] = cases_est["Infections Estimate (MA)"].shift(-14)
    cases_est = cases_est.rename(columns=dict(Deaths="Deaths prov sum"))
    cases_est = cases_est.join(df['Deaths'], on="Date")
    # cases_est['Cases (MA)'] = cases_est['Cases'].rolling("7d").mean()
    cases_est["Infections Estimate Simple"] = cases_est["Deaths"].shift(-11) / 0.0054
    cols = ["Infections Estimate", "Cases", ]
    legend = ["Infections Estimate (based on deaths)", "Confirmed Cases"]
    title = """Unofficial Estimate of Covid Infections in Thailand (based on Deaths/IFR)\n
(DISCLAIMER: see site below for the assumptions of this simple estimate)"""
    plot_area(df=cases_est,
              png_prefix='cases_infections_estimate',
              cols_subset=cols,
              title=title,
              legends=legend,
              kind='line',
              stacked=False,
              percent_fig=False,
              ma_days=7,
              cmap='tab10',
              actuals=True
              )

    ####################
    # Deaths
    ####################

    # predict median age of death based on population demographics

    cols = ['Deaths', 'Deaths Risk Family', 'Deaths Comorbidity None']
    plot_area(df=df, png_prefix='deaths_reason', cols_subset=cols, title='Thailand Covid Deaths',
              legends=['Deaths', 'Infected from family', 'No underlying diseases'],
              kind='line', stacked=False, percent_fig=False, ma_days=7, cmap='tab10',
              actuals=True)

    df['Deaths Age Median (MA)'] = df['Deaths Age Median'].rolling('7d').mean()
    cols = ['Deaths Age Median (MA)', 'Deaths Age Max', 'Deaths Age Min']
    plot_area(df=df, png_prefix='deaths_age', cols_subset=cols, title='Thailand Covid Death Age Range',
              kind='line', stacked=False, percent_fig=False, ma_days=None, cmap='tab10',
              highlight=['Deaths Age Median (MA)'], between=['Deaths Age Max', 'Deaths Age Min'])

    cols = rearrange([f'Deaths Area {area}' for area in DISTRICT_RANGE], *FIRST_AREAS)
    plot_area(df=df, png_prefix='deaths_by_area', cols_subset=cols,
              title='Thailand Covid Deaths by health District', legends=AREA_LEGEND,
              kind='area', stacked=True, percent_fig=False, ma_days=7, cmap='tab20')


if __name__ == "__main__":

    df = scrape_and_combine()
    save_plots(df)
