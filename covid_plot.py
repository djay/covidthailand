import os
import pathlib
from typing import Sequence, Union, List, Callable

import matplotlib
import matplotlib.cm
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter
import pandas as pd
from pandas.tseries.offsets import MonthEnd
from dateutil.relativedelta import relativedelta

from covid_data import get_ifr, scrape_and_combine
from utils_pandas import cum2daily, cut_ages, cut_ages_labels, decreasing, get_cycle, human_format, perc_format, import_csv, increasing, normalise_to_total, \
    rearrange, set_time_series_labels_2, topprov
from utils_scraping import remove_prefix, remove_suffix, any_in
from utils_thai import DISTRICT_RANGE, DISTRICT_RANGE_SIMPLE, AREA_LEGEND, AREA_LEGEND_SIMPLE, \
    AREA_LEGEND_ORDERED, FIRST_AREAS, area_crosstab, get_provinces, join_provinces, thaipop


def plot_area(df: pd.DataFrame,
              png_prefix: str,
              cols_subset: Union[str, Sequence[str]],
              title: str,
              footnote: str = None,
              legends: List[str] = None,
              legend_pos: str = None,
              legend_cols: int = 1,
              kind: str = 'line',
              stacked=False,
              percent_fig: bool = False,
              unknown_name: str = 'Unknown',
              unknown_total: str = None,
              unknown_percent=False,
              ma_days: int = None,
              cmap: str = 'tab20',
              periods_to_plot=None,
              actuals: List[str] = [],
              highlight: List[str] = [],
              box_cols: List[str] = [],
              reverse_cmap: bool = False,
              y_formatter: Callable[[float, int], str] = human_format,
              clean_end=True,
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
        "font.size": 20,
        "figure.titlesize": 30,
        "figure.titleweight": "bold",
        "legend.fontsize": 16,
        "xtick.labelsize": 20,
        "ytick.labelsize": 20,
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
    is_dates = hasattr(last_update, 'date')

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

    subtitle = ''
    if ma_days:
        subtitle = f'{ma_days}-Day Rolling Average - '

    subtitle += 'https://djay.github.io/covidthailand'

    if is_dates:
        subtitle += f" - Last Data: {last_update.date()}"
    else:
        subtitle += f" - Last Data: {last_update}"

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
    last_date_unknown = df[cols + actuals].last_valid_index()  # last date with some data (inc unknown)
    if clean_end:
        df_clean = df.loc[:last_date_unknown]
    else:
        df_clean = df

    if is_dates:
        periods = {
            'all': df_clean,
            '1': df_clean[:'2020-06-01'],
            '2': df_clean['2020-12-12':],
            '3': df_clean['2021-04-01':],
            '30d': df_clean.last('30d')
        }
        quick = os.environ.get('USE_CACHE_DATA', False) == 'True'  # TODO: have its own switch
        if periods_to_plot:
            pass
        elif quick:
            periods_to_plot = ['all']
        else:
            periods_to_plot = set(periods.keys())

        periods = {key: periods[key] for key in periods_to_plot}
    else:
        periods = {'all': df_clean}

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

        areacols = [c for c in cols if c not in between]
        if kind != "line":
            df_plot.plot(ax=a0, y=areacols, kind=kind, stacked=stacked, legend='reverse')
            linecols = between + actuals
        else:
            linecols = cols + actuals

        # advance colour cycle so lines have correct next colour
        for _ in range(len(areacols)):
            next(a0._get_lines.prop_cycler)

        for c in linecols:
            style = "--" if c in [f"{b}{ma_suffix}" for b in between] + actuals else None
            width = 5 if c in [f"{h}{ma_suffix}" for h in highlight] else 2
            lines = df_plot.plot(ax=a0,
                         y=c,
                         use_index=True,
                         linewidth=width,
                         style=style,
                         kind="line",
                         zorder=4,
                         legend=c not in actuals,
                         x_compat=kind == 'bar'  # Putting lines on bar plots doesn't work well
                         )

        # If actuals are after cols then they are future predictions. put in a line to show today
        if actuals and df[cols].last_valid_index() < df[actuals].last_valid_index():
            a0.axvline(df[cols].last_valid_index(), color='grey', linestyle='--', lw=1)

        if box_cols and type(box_cols[0]) != list:
            box_cols = [box_cols]
        elif not box_cols:
            box_cols = []
        for dist in box_cols:
            mins, maxes, avg = df_plot[dist].min(axis=1), df_plot[dist].max(axis=1), df_plot[dist].mean(axis=1)
            a0.fill_between(df.index, mins, maxes, facecolor="orange", alpha=0.3, zorder=3, label=None, step=None)
            avg.plot(ax=a0, color="orange", style="--", zorder=5, x_compat=kind == 'bar', legend=False)
            # boxes = df_plot[box_cols].transpose()
            # boxes.boxplot(ax=a0)

        if kind == "bar" and is_dates:
            set_time_series_labels_2(df_plot, a0)

        f.suptitle(title)
        a0.set_title(label=subtitle)
        if footnote:
            plt.annotate(footnote, (0.99, 0), (0, -50),
                         xycoords='axes fraction',
                         textcoords='offset points',
                         va='top',
                         fontsize=15,
                         horizontalalignment='right')

        handles, labels = a0.get_legend_handles_labels()
        # we are skipping pandas determining which legends to show so do it manually. box lines are 'None'
        # TODO: go back to pandas doing it.
        handles, labels = zip(*[(h, l) for h, l in zip(*a0.get_legend_handles_labels()) if l not in actuals + ['None']])

        leg = a0.legend(handles=handles,
                        labels=legends,
                        loc=legend_pos,
                        frameon=True,
                        edgecolor="black",
                        fancybox=True,
                        framealpha=0.5,
                        ncol=legend_cols)

        for line in leg.get_lines():
            line.set_linewidth(4.0)

        a0.xaxis.label.set_visible(False)

        if percent_fig:
            a1.set_prop_cycle(None)
            a1.yaxis.set_major_formatter(FuncFormatter(perc_format))
            a1.tick_params(direction='out', length=6, width=1, color='lightgrey')
            df_plot.plot(ax=a1, y=perccols, kind='area', legend=False)
            a1.xaxis.label.set_visible(False)
            a1_secax_y = a1.secondary_yaxis('right', functions=(lambda x: x, lambda x: x))
            a1_secax_y.yaxis.set_major_formatter(FuncFormatter(perc_format))
            a1_secax_y.tick_params(direction='out', length=6, width=1, color='lightgrey')

        a0_secax_y = a0.secondary_yaxis('right', functions=(lambda x: x, lambda x: x))
        a0_secax_y.tick_params(direction='out', length=6, width=1, color='lightgrey')
        if y_formatter is not None:
            a0_secax_y.yaxis.set_major_formatter(FuncFormatter(y_formatter))
        a0.tick_params(direction='out', length=6, width=1, color='lightgrey')
            
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

    dash_prov = import_csv("moph_dashboard_prov", ["Date", "Province"], dir="json")

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
        'Positivity Cases/Tests',
        'Cases per PUI3',
        'Positivity Walkins/PUI3',
    ]
    legends = [
        'Positive Results per PCR Test (%) (Positive Rate)',
        'Confirmed Cases per PCR Test (%)',
        'Confirmed Cases per PUI*3 (%)',
        'Walkin Cases per PUI*3 (%)'
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
            'ATK',
            ]
    legends = ['Confirmed Cases',
               'Walkin Confirmed Cases',
               'Positive Test Results (All)',
               'Positive Test Results (Public)',
               'Antigen Test Kit Positives (ATK/Rapid)']
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

    # No longer include prisons in proactive number
    df['Cases Proactive Community'] = df['Cases Proactive'] # .sub(df['Cases Area Prison'], fill_value=0)
    #df['Cases inc ATK'] = df['Cases'].add(df['ATK'], fill_value=0)
    cols = ['Cases Imported', 'Cases Walkin', 'Cases Proactive Community', 'Cases Area Prison']
    plot_area(df=df,
              png_prefix='cases_types',
              cols_subset=cols,
              title='Thailand Covid Cases by Where Tested',
              #footnote="Rapid test positives (ATK) aren't included in Confirmed Cases without PCR Test",
              legends=[
                  "Quarantine (Imported)", "Hospital (Walk-ins/Traced)",
                  "Mobile Community Testing (Proactive)",
                  "Prison (Proactive)",
                  # "Rapid Testing (Antigen/ATK)"
              ],
              unknown_name='Cases Unknown',
              unknown_total='Cases',
              kind='area',
              stacked=True,
              percent_fig=True,
              actuals=["Cases"],
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
    #cols = ["Age 0-9", "Age 20-29", "Age 30-39", "Age 40-49", "Age 50-65", "Age 66-"]
    cols = cut_ages_labels([10, 20, 30, 40, 50, 60, 70], "Cases Age")
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
              actuals=['Cases'],
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
    # plot_area(df=df, png_prefix='cases_active', cols_subset=cols,
    #           title='Thailand Active Covid Cases\n(Severe, Field, and Respirator only available from '
    #                 '2021-04-24 onwards)',
    #           legends=legends,
    #           kind='area', stacked=True, percent_fig=False, ma_days=None, cmap='tab10')

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

    def clean_vac_leg(c, first="(1 Jab)", second="(2 Jabs)"):
        return c.replace(
            ' Cum', '').replace(
            'Vac ', '').replace(
            "Group ", "").replace(
            'Only 1', first).replace(
            ' 1', " " + first).replace(
            ' 2', " " + second).replace(
            'Given 3', "3rd Booster").replace(
            'Risk: Location', 'Aged 18-59').replace(
            'All', 'Staff & Volunteers').replace(
            'Risk: Disease', 'Risk Disease under 60'
            )

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
    df_vac_groups['Vac Imported Cum'] = df_vac_groups[[c for c in df_vac_groups.columns if "Vac Imported" in c]].sum(axis=1, skipna=False)

    # now convert to daily and interpolate and then normalise to real daily total.
    vac_daily = cum2daily(df_vac_groups)
    # bring in any daily figures we might have collected first
    vac_daily = df[['Vac Given', 'Vac Given 1', 'Vac Given 2', 'Vac Given 3']].combine_first(vac_daily)
    daily_cols = [c for c in vac_daily.columns if c.startswith('Vac Group') and ' 3' not in c] + ['Vac Given 3']  # Keep for unknown
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

    plot_area(
        df=vac_daily,
        png_prefix='vac_groups_daily',
        cols_subset=daily_cols,
        title='Thailand Daily Vaccinations by Priority Groups',
        legends=[
            # 'Doses per day needed to run out in a week',
            'Rate for 70% 1st Jab in 2021',
            'Rate for 70% 2nd Jab in 2021'
        ] + [clean_vac_leg(c, "(1st jab)", "(2nd jab)") for c in daily_cols],  # bar puts the line first?
        legend_cols=2,
        kind='bar',
        stacked=True,
        percent_fig=False,
        between=[
            # '7d Runway Rate',
            'Target Rate 1',
            'Target Rate 2'],
        ma_days=None,
        cmap=get_cycle('tab20', len(daily_cols) - 1, extras=["grey"], unpair=True),
        periods_to_plot=["30d", "2"],  # too slow to do all 
    )

    # # Now turn daily back to cumulative since we now have estimates for every day without dips
    # vac_cum = vac_daily.cumsum().combine_first(vac_daily[daily_cols].fillna(0).cumsum())
    # vac_cum.columns = [f"{c} Cum" for c in vac_cum.columns]
    # # Not sure why but we end up with large cumalitive than originally so normalise
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
            vac_cum[c.replace("1", "Only 1")] = vac_cum[c].sub(vac_cum[c.replace("1", "2")])
            cols.extend([c.replace("1", "2"), c.replace("1", "Only 1")])

    #cols_cum = rearrange(cols, 1, 2, 3, 4, 9, 10, 7, 8, )
    #cols_cum = cols_cum  # + ['Available Vaccines Cum']
    cols_cum = [c for c in cols if "2" in c] + [c for c in cols if "1" in c]
    # We have "Medical All" instead
    cols_cum = [c for c in cols_cum if not any_in(c, "Medical Staff", "Volunteer")]

    # TODO: get paired colour map and use do 5 + 5 pairs
    legends = [clean_vac_leg(c) for c in cols_cum]

    plot_area(df=vac_cum, png_prefix='vac_groups', cols_subset=cols_cum,
              title='Thailand Population Vaccinatated by Priority Groups', legends=legends,
              kind='area', stacked=True, percent_fig=True, ma_days=None,
              cmap=get_cycle('tab20', len(cols_cum), unpair=True),
              # between=['Available Vaccines Cum'],
              y_formatter=thaipop)

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
    # general public  28,634,733
    # elderly over 60 10,906,142
    # pregnant 500,000
    # Target total 50,000,000
    goals = [
        ('Medical All', 1000000 + 712000),
        # ('Health Volunteer', 1000000),
        # ('Medical Staff', 712000),
        ('Other Frontline Staff', 1900000),
        ['Over 60', 10906142],
        ('Risk: Disease', 6347125),
        ('Risk: Location', 28634733),
        ('Risk: Pregnant', 500000),
    ]
    for d in [2, 1]:
        for group, goal in goals:
            vac_cum[f'Vac Group {group} {d} Cum % ({goal/1000000:.1f}M)'] = vac_cum[
                f'Vac Group {group} {d} Cum'] / goal * 100

    for group, goal in goals:
        # calc prediction. 14day trajectory till end of the year. calc eoy and interpolate
        v = vac_cum[f'Vac Group {group} 1 Cum % ({goal/1000000:.1f}M)']
        rate = (v.loc[v.last_valid_index()] - v.loc[v.last_valid_index() - relativedelta(days=14)]) / 14
        future_dates = pd.date_range(v.last_valid_index(), v.last_valid_index() + relativedelta(days=90), name="Date")
        perc = (pd.RangeIndex(1, 92) * rate + v.loc[v.last_valid_index()])
        future = pd.DataFrame(perc, columns=[f'Vac Group {group} 1 Pred'], index=future_dates).clip(upper=100)
        vac_cum = vac_cum.combine_first(future)

        # 2nd dose is 1st dose from 2 months previous
        # TODO: factor in 2 months vs 3 months AZ?
        last_2m = v[v.last_valid_index() - relativedelta(days=60): v.last_valid_index()]
        v2 = pd.concat([last_2m, future[f'Vac Group {group} 1 Pred'].iloc[1:31]], axis=0)
        start_pred = vac_cum[f'Vac Group {group} 2 Cum % ({goal/1000000:.1f}M)'].loc[v.last_valid_index()]
        perc2 = (v2 - v2[v2.index.min()] + start_pred).clip(upper=100)
        perc2.index = future_dates
        vac_cum = vac_cum.combine_first(perc2.to_frame(f'Vac Group {group} 2 Pred'))

    cols2 = [c for c in vac_cum.columns if " 2 Cum %" in c and "Vac Group " in c]
    actuals = [c for c in vac_cum.columns if " 2 Pred" in c]
    legends = [clean_vac_leg(c) for c in cols2]
    plot_area(
        df=vac_cum,
        png_prefix='vac_groups_goals_full',
        cols_subset=cols2,
        title='Thailand Full Vaccination Progress',
        legends=legends,
        kind='line',
        stacked=False,
        percent_fig=False,
        actuals=actuals,
        ma_days=None,
        cmap=get_cycle('tab20', len(cols2) * 2, unpair=True, start=len(cols2)),
    )
    cols2 = [c for c in vac_cum.columns if " 1 Cum %" in c and "Vac Group " in c]
    actuals = [c for c in vac_cum.columns if " 1 Pred" in c]
    legends = [clean_vac_leg(c) for c in cols2]
    plot_area(
        df=vac_cum,
        png_prefix='vac_groups_goals_half',
        cols_subset=cols2,
        title='Thailand Half Vaccination Progress',
        legends=legends,
        kind='line',
        stacked=False,
        percent_fig=False,
        actuals=actuals,
        ma_days=None,
        cmap=get_cycle('tab20', len(cols2) * 2, unpair=True, start=len(cols2)),  # TODO: seems to be getting wrong colors
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
    # Let's trust the dashboard more but they could both be different
    vac = dash_prov.combine_first(vac)
    #vac = vac.combine_first(vac_dash[[f"Vac Given {d} Cum" for d in range(1, 4)]])
    # Add them all up
    vac = vac.combine_first(vac[[f"Vac Given {d} Cum" for d in range(1, 4)]].sum(axis=1, skipna=False).to_frame("Vac Given Cum"))
    vac = vac.join(get_provinces()['Population'], on='Province')
    # Bring in vac populations
    pops = vac["Vac Population"].groupby("Province").max().to_frame("Vac Population")  # It's not on all data
    vac = vac.join(pops, rsuffix="2")

    top5 = vac.pipe(topprov, lambda df: df['Vac Given Cum'] / df['Vac Population2'] * 100)
    cols = top5.columns.to_list()
    plot_area(df=top5, png_prefix='vac_top5_doses', cols_subset=cols,
              title='Top Provinces for Vaccination Doses per 100 people',
              kind='line', stacked=False, percent_fig=False, ma_days=None, cmap='tab10',
              )

    top5 = vac.pipe(topprov, lambda df: df['Vac Given 1 Cum'] / df['Vac Population2'] * 100)
    cols = top5.columns.to_list()
    plot_area(df=top5, png_prefix='vac_top5_doses_1', cols_subset=cols,
              title='Top Provinces for Vaccination 1st Dose per 100 people',
              kind='line', stacked=False, percent_fig=False, ma_days=None, cmap='tab10',
              )

    top5 = vac.pipe(topprov, lambda df: df['Vac Given 2 Cum'] / df['Vac Population2'] * 100)
    cols = top5.columns.to_list()
    plot_area(df=top5, png_prefix='vac_top5_doses_2', cols_subset=cols,
              title='Top Provinces for Vaccination 2nd Dose per 100 people',
              kind='line', stacked=False, percent_fig=False, ma_days=None, cmap='tab10',
              )

    top5 = vac.pipe(topprov, lambda df: -df['Vac Given 1 Cum'] / df['Vac Population2'] * 100,
                    lambda df: df['Vac Given 1 Cum'] / df['Vac Population2'] * 100,
                    other_name=None, num=7)
    cols = top5.columns.to_list()
    plot_area(df=top5, png_prefix='vac_low_doses_1', cols_subset=cols,
              title='Lowesst Provinces for Vaccination 1st Dose per 100 people',
              kind='line', stacked=False, percent_fig=False, ma_days=None, cmap='tab10',
              )

    top5 = vac.pipe(topprov, lambda df: -df['Vac Given 2 Cum'] / df['Vac Population2'] * 100,
                    lambda df: df['Vac Given 2 Cum'] / df['Vac Population2'] * 100,
                    other_name=None, num=7)
    cols = top5.columns.to_list()
    plot_area(df=top5, png_prefix='vac_low_doses_2', cols_subset=cols,
              title='Lowest Provinces for Vaccination 2nd Dose per 100 people',
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
    cases = join_provinces(cases, "Province", ["Health District Number", "region"])  # to fill in missing health districts
    # cases = cases.fillna(0)  # all the other values
    ifr = get_ifr()
    cases = cases.join(ifr[['ifr', 'Population', 'total_pop']], on="Province")

    def cases_per_capita(col):
        def func(adf):
            return adf[col] / adf['Population'] * 100000
        return func

    top5 = cases.pipe(topprov,
                      increasing(cases_per_capita("Cases"), 3),
                      cases_per_capita("Cases"),
                      name="Province Cases (3d MA)",
                      other_name="Other Provinces",
                      num=7)
    cols = top5.columns.to_list()
    plot_area(df=top5,
              png_prefix='cases_prov_increasing',
              cols_subset=cols,
              title='Trending Up Confirmed Cases per 100,000',
              kind='line',
              stacked=False,
              percent_fig=False,
              ma_days=7,
              cmap='tab10')

    top5 = cases.pipe(topprov,
                      decreasing(cases_per_capita("Cases"), 3),
                      cases_per_capita("Cases"),
                      name="Province Cases (3d MA)",
                      other_name="Other Provinces",
                      num=7)
    cols = top5.columns.to_list()
    plot_area(df=top5,
              png_prefix='cases_prov_decreasing',
              cols_subset=cols,
              title='Trending Down Confirmed Cases per 100,000',
              kind='line',
              stacked=False,
              percent_fig=False,
              ma_days=7,
              cmap='tab10')

    top5 = cases.pipe(topprov,
                      cases_per_capita("Cases"),
                      name="Province Cases",
                      other_name="Other Provinces",
                      num=6)
    cols = top5.columns.to_list()
    plot_area(df=top5,
              png_prefix='cases_prov_top',
              cols_subset=cols,
              title='Top Confirmed Cases per 100,000',
              kind='line',
              stacked=False,
              percent_fig=False,
              ma_days=7,
              cmap='tab10')

    top5 = cases.pipe(topprov,
                      increasing(cases_per_capita('Cases Walkin'), 5),
                      cases_per_capita('Cases Walkin'),
                      name="Province Cases Walkin (7d MA)",
                      other_name="Other Provinces",
                      num=6)
    cols = top5.columns.to_list()
    plot_area(df=top5, png_prefix='cases_walkins_increasing', cols_subset=cols,
              title='Thailand Top Provinces with Walkin Cases',
              kind='line', stacked=False, percent_fig=False, ma_days=7, cmap='tab10')

    for risk in ['Contact', 'Proactive Search', 'Community', 'Work', 'Unknown']:
        top5 = cases.pipe(topprov,
                          increasing(cases_per_capita(f"Cases Risk: {risk}"), 5),
                          cases_per_capita(f"Cases Risk: {risk}"),
                          name=f"Province Cases {risk} (7d MA)",
                          other_name="Other Provinces",
                          num=6)
        cols = top5.columns.to_list()
        plot_area(df=top5,
                  png_prefix=f'cases_{risk.lower().replace(" ","_")}_increasing',
                  cols_subset=cols,
                  title=f'Trending Up {risk} related Cases per 100,000',
                  kind='line',
                  stacked=False,
                  percent_fig=False,
                  ma_days=7,
                  cmap='tab10')

    def top(func, _):
        return func

    for direction, title in zip([increasing, decreasing, top], ["Trending Up ", "Trending Down ", ""]):
        top5 = cases.pipe(topprov,
                          direction(cases_per_capita('Hospitalized Severe'), 5),
                          cases_per_capita('Hospitalized Severe'),
                          name="Province Active Cases Severe (7d MA)",
                          other_name="Other Provinces",
                          num=8)
        cols = top5.columns.to_list()
        plot_area(
            df=top5,
            png_prefix=f'active_severe_{direction.__name__}',
            cols_subset=cols,
            title=f'Thailand {title}Provinces with Severe Active Cases (per 100,000)',
            kind='line',
            stacked=False,
            percent_fig=False,
            ma_days=7,
            cmap='tab10')

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
    cols = ["Cases", "Infections Estimate", ]
    legend = ["Confirmed Cases", "Infections Estimate (based on deaths)"]
    title = """Unofficial Estimate of Covid Infections in Thailand (based on Deaths/IFR)
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

    # TODO: predict median age of death based on population demographics

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
              kind='area', stacked=True, percent_fig=True, ma_days=7, cmap='tab20')

    # Work out Death ages from CFR from situation reports
    age_ranges = ["15-39", "40-59", "60-"]

    cols = [f'W3 CFR {age}' for age in age_ranges]
    plot_area(df=df, png_prefix='deaths_w3cfr', cols_subset=cols, title='Thailand Covid CFR since 2021-04-01',
              kind='line', stacked=False, percent_fig=False, ma_days=None, cmap='tab10')

    #ages = ["Age 0-14", "Age 15-39", "Age 40-59", "Age 60-"]
    ages = cut_ages_labels([15, 40, 60], "Cases Age")
    # Put unknowns into ages based on current ratios. But might not be valid for prison unknowns?
    # w3_cases = df[ages + ['Cases', 'Deaths']].pipe(normalise_to_total, ages, "Cases")
    w3_cases = df[ages + ['Cases', 'Deaths']]

    cols = ages
    plot_area(df=w3_cases, png_prefix='cases_ages2', cols_subset=cols, title='Thailand Covid Cases by Age',
              unknown_name='Unknown', unknown_total='Cases', unknown_percent=False,
              kind='area', stacked=True, percent_fig=True, ma_days=7,
              cmap=get_cycle('summer_r', len(cols), extras=["gainsboro"])
              )

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
    deaths_by_age['Deaths (MA)'] = deaths_by_age['Deaths'].rolling(7, min_periods=3, center=True).mean()
    deaths_by_age['Deaths Ages Sum'] = deaths_by_age[death_cols].sum(axis=1)
    deaths_by_age = deaths_by_age.pipe(normalise_to_total, death_cols, 'Deaths (MA)')
    cols = death_cols + ['Deaths (MA)', 'Deaths Ages Sum']
    plot_area(df=deaths_by_age,
              png_prefix='deaths_age_bins',
              cols_subset=cols,
              title='Thailand Covid Death Age Range',
              kind='line',
              stacked=False,
              percent_fig=False,
              ma_days=None,
              cmap='tab10')
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
              png_prefix='deaths_age_dash',
              cols_subset=death_cols,
              title='Thailand Covid Death Age Distribution',
              kind='area',
              stacked=True,
              percent_fig=True,
              unknown_name='Unknown', unknown_total='Deaths', unknown_percent=False,
              ma_days=7,
              cmap=get_cycle('summer_r', len(death_cols), extras=["gainsboro"]))

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
            res['PScore'] = res['Excess Deaths'] / death5_avg * 100
            res['Pre Avg'], res['Pre Min'], res['Pre Max'] = death3_avg, death3_min, death3_max
            res['Pre 5 Avg'], res['Pre 5 Min'], res['Pre 5 Max'] = death5_avg, death5_min, death5_max
            res['Deaths All Month'] = months[year]
            for y in range(2012, 2022):
                res[f'Deaths {y}'] = months[y]
            res['Date'] = pd.to_datetime(f'{year}-' + res.index.astype(int).astype(str) + '-1',
                                         format='%Y-%m') + MonthEnd(0)
            result = result.combine_first(res.reset_index().set_index("Date"))
        result = result.dropna(subset=['PScore'])
        return result.drop(columns=["Month"])

    excess = import_csv("deaths_all", dir="json", date_cols=[])
    excess = join_provinces(excess, 'Province', ['region', 'Health District Number'])
    years5 = list(range(2015, 2020))
    years3 = [2015, 2016, 2017, 2018]

    all = calc_pscore(excess)
    all['Deaths Covid'] = df['Deaths'].groupby(pd.Grouper(freq='M')).sum()
    all['Deaths (ex. Known Covid)'] = all['Deaths All Month'] - all['Deaths Covid']
    all['Deaths 2021 (ex. Known Covid)'] = all['Deaths 2021'] - all['Deaths Covid']
    all['Expected Deaths'] = all['Pre 5 Avg'] + all['Deaths Covid']
    all['Deviation from expected Deaths'] = (all['Excess Deaths'] - all['Deaths Covid']) / all['Pre Avg'] * 100
    plot_area(df=all, png_prefix='deaths_pscore',
              cols_subset=['Deviation from expected Deaths', 'PScore'],
              legends=["Deviation from normal deaths (removing Covid Deaths) %", "Deviation from Normal deaths (avg 2015-29)"],
              footnote="There is some variability in comparison years 2015-19 so normal is a not a certain value",
              title='Thailand Monthly Deaths above Normal (Avg 2015-2019',
              kind='line', stacked=False, percent_fig=False, ma_days=None, cmap='tab10',
              )

    cols = [f'Deaths {y}' for y in range(2012, 2021, 1)]
    by_month = pd.DataFrame(all)
    by_month['Month'] = by_month.index.strftime('%B')
    years2020 = by_month["2020-01-01":"2021-01-01"][cols + ['Month']].reset_index().set_index("Month")
    cols2021 = ['Deaths 2021', 'Deaths 2021 (ex. Known Covid)']
    years2021 = by_month["2021-01-01":"2022-01-01"][cols2021 + ['Month']].reset_index().set_index("Month")
    by_month = years2020.combine_first(years2021).sort_values("Date")
    cols = cols + cols2021

    plot_area(df=by_month, png_prefix='deaths_excess_years', cols_subset=cols,
              legend_pos="lower center", legend_cols=3,
              title='Thailand Excess Deaths\nNumber of deaths from all causes compared to previous years',
              kind='bar', stacked=False, percent_fig=False, ma_days=None, cmap='tab10',
              )

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
    # pnemonia? - https://nucleuswealth.com/articles/is-thailand-hiding-covid-19-cases/
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

    # Do comparion bar charts to historical distribution of years

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
NOTE: Excess deaths can be changed by many factors other than Covid.
    """.strip()
    footnote3 = f"""{footnote}
2015-2018 was used to compare for the most stable death rates. For other comparisons see
https://djay.github.io/covidthailand/#excess-deaths
    """.strip()
    footnote5 = f"""{footnote}
For a comparison exluding 2019 (which had higher than expected deaths) 
see https://djay.github.io/covidthailand/#excess-deaths
    """.strip()

    for years in [years5, years3]:
        year_span = f"{min(years)}-{max(years)}"
        cols_y = [f'Deaths {y}' for y in years]
        note = (footnote5 if len(years) > 4 else footnote3).format(year_span=year_span)
        suffix = "_5y" if len(years) > 4 else ""

        plot_area(df=pan_months,
                  png_prefix=f'deaths_excess_covid{suffix}',
                  cols_subset=['Deaths (ex. Known Covid)', 'Deaths Covid'],
                  legends=["Deaths (ex. Covid)", "Confirmed Covid Deaths"],
                  legend_cols=2,
                  legend_pos="lower center",
                  title=f'Thailand Deaths from all causes compared to {year_span}',
                  footnote=note,
                  kind='bar',
                  stacked=True,
                  cmap='tab10',
                  box_cols=cols_y,
                  periods_to_plot=['all'])

        plot_area(df=by_region,
                  png_prefix=f'deaths_excess_region{suffix}',
                  cols_subset=[f'Deaths All Month {reg}' for reg in regions],
                  legends=[f'{reg}' for reg in regions],
                  legend_cols=4,
                  legend_pos="lower center",
                  title=f'Thailand Deaths from all causes by Region vs Expected Deaths ({year_span})',
                  footnote=note,
                  kind='bar',
                  stacked=True,
                  periods_to_plot=['all'],
                  box_cols=[[f"{y} {reg}" for y in cols_y] for reg in regions],
                  cmap='tab10')

        plot_area(df=by_age,
                  png_prefix=f'deaths_excess_age_bar{suffix}',
                  cols_subset=[f'Deaths All Month {age}' for age in ages],
                  legends=[f'{age}' for age in ages],
                  legend_cols=2,
                  legend_pos="center left",
                  title=f'Thailand Deaths from all causes by Age vs. Expected Deaths ({year_span})',
                  footnote=note,
                  kind='bar',
                  stacked=True,
                  periods_to_plot=['all'],
                  box_cols=[[f"{y} {age}" for y in cols_y] for age in ages],
                  cmap='tab10')

    by_province = excess.groupby(["Province"]).apply(calc_pscore)
    by_province['Deaths Covid'] = cases.groupby(["Province", pd.Grouper(level=0, freq='M')])['Deaths'].sum()
    top5 = by_province.pipe(topprov, lambda adf: (adf["Excess Deaths"] - adf['Deaths Covid']) / adf['Pre 5 Avg'] * 100, num=5)
    cols = top5.columns.to_list()
    plot_area(df=top5, png_prefix='deaths_expected_prov', cols_subset=cols,
              title='Deviation from Expected Monthly Deaths (Avg 2015-19 + Known Covid Deaths)',
              footnote=footnote5,
              kind='line', stacked=False, percent_fig=False, ma_days=None, cmap='tab10',
              periods_to_plot=['all']
              )

    top5 = by_province.pipe(topprov, lambda adf: adf["Excess Deaths"], num=7)
    cols = top5.columns.to_list()
    plot_area(df=top5, png_prefix='deaths_excess_prov', cols_subset=cols,
              title='Thai Provinces with most Excess Deaths',
              kind='line', stacked=False, percent_fig=False, ma_days=None, cmap='tab10',
              periods_to_plot=['all']
              )

    by_district = excess.groupby("Health District Number").apply(calc_pscore)
    by_district['Deaths Covid'] = cases.groupby(["Health District Number", pd.Grouper(level=0, freq='M')])['Deaths'].sum()
    by_district['Deviation from expected Deaths'] = (by_district['Excess Deaths'] - by_district['Deaths Covid']) / by_district['Pre 5 Avg'] * 100
    top5 = area_crosstab(by_district, "Deviation from expected Deaths", "")
    cols = rearrange([f'Deviation from expected Deaths Area {area}' for area in DISTRICT_RANGE_SIMPLE], *FIRST_AREAS)
    plot_area(df=top5, png_prefix='deaths_expected_area',
              cols_subset=cols, legends=AREA_LEGEND,
              title='Deviation from Expected Monthly Deaths (Avg 2015-19 + Known Covid Deaths)',
              kind='line', stacked=False, percent_fig=False, ma_days=None, cmap='tab20',
              periods_to_plot=['all']
              )

    by_age = excess.pipe(cut_ages, [15, 65, 75, 85]).groupby(["Age Group"]).apply(calc_pscore)
    by_age = by_age.reset_index().pivot(values=["PScore"], index="Date", columns="Age Group")
    by_age.columns = [' '.join(c) for c in by_age.columns]

    plot_area(df=by_age,
              png_prefix='deaths_pscore_age',
              cols_subset=list(by_age.columns),
              title='Thailand Excess Deaths (P-Score) by age',
              kind='line',
              stacked=False,
              periods_to_plot=['all'],
              cmap='tab10')


if __name__ == "__main__":

    df = scrape_and_combine()
    save_plots(df)
