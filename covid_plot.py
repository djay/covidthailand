import os
import pathlib
from typing import Sequence, Union, List, Callable

import matplotlib
import matplotlib.cm
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter
import numpy as np
import pandas as pd
import numpy as np
from pandas.tseries.offsets import MonthEnd

from covid_data import get_ifr, scrape_and_combine
from utils_pandas import cum2daily, cut_ages, cut_ages_labels, decreasing, get_cycle, human_format, perc_format, \
    import_csv, increasing, normalise_to_total, rearrange, set_time_series_labels_2, topprov, pred_vac, fix_gaps
from utils_scraping import remove_prefix, remove_suffix, any_in, logger
from utils_thai import DISTRICT_RANGE, DISTRICT_RANGE_SIMPLE, AREA_LEGEND, AREA_LEGEND_SIMPLE, \
    AREA_LEGEND_ORDERED, FIRST_AREAS, area_crosstab, get_provinces, join_provinces, thaipop, thaipop2

theme = 'Black'
theme_label_text = '#F1991F'
theme_light_text = '#E9E8E9'
theme_dark_text = '#424242'
theme_light_back = '#202020'
theme_dark_back = '#0C1111'

def plot_area(df: pd.DataFrame,
              png_prefix: str,
              cols_subset: Union[str, Sequence[str]],
              title: str,
              table: pd.DataFrame = [],
              footnote: str = None,
              footnote_left: str = None,
              legends: List[str] = None,
              legend_pos: str = 'upper left',
              legend_cols: int = 1,
              kind: str = 'line',
              stacked=False,
              percent_fig: bool = False,
              show_last_values: bool = True,
              limit_to_zero: bool = True,
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
    :param show_last_values: show the last actual values on the right axis
    :param limit_to_zero: limit the bottom of the y-axis to 0
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
        "legend.fontsize": 18,
        "xtick.labelsize": 20,
        "ytick.labelsize": 20,
        # "axes.prop_cycle": get_cycle(cmap),
    })

    if theme == 'Black':
        plt.rcParams.update({
            "text.color": theme_light_text,
            "legend.facecolor": theme_light_back,
            "legend.edgecolor": theme_label_text,
            "legend.frameon": True,
            "legend.framealpha": 0.3,
            "legend.shadow": True,
            "axes.grid" : True, 
            "axes.facecolor": theme_dark_back,
            "axes.linewidth": 0,
            "grid.color": theme_label_text,
            "grid.alpha": 0.5,
            "xtick.color": theme_label_text,
            "xtick.minor.size": 0,
            "ytick.color": theme_label_text,
            "ytick.minor.size": 0,
        })
        dim_color = '#784d00'
        invisible_color = theme_dark_back
    else:
        dim_color = 'lightgrey'
        invisible_color = 'white'

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

    if is_dates:
        subtitle += f"Last Data: {last_update.date()}"
    else:
        subtitle += f"Last Data: {last_update}"

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

        plt.rcParams["axes.prop_cycle"] = get_cycle(cmap, len(cols) + len(between))

        show_province_tables = len(table) > 0

        # element heights
        main_height = 17 if percent_fig or show_province_tables else 21
        perc_height = 10
        table_height = 10
        spacing = 3
        fn_left_lines = len(footnote_left.split('\n')) if footnote_left else 0
        fn_right_lines = len(footnote.split('\n')) if footnote else 0
        footnote_height = max(fn_left_lines, fn_right_lines)

        # figure out the figure dimensions
        figure_height = main_height
        figure_width = 20
        if percent_fig:
            figure_height += perc_height + spacing
        if show_province_tables:
            figure_height += table_height + spacing
        fig = plt.figure(figsize=[figure_width, 0.5 * figure_height + 0.2 * footnote_height])

        grid_rows = figure_height
        grid_columns = 4

        grid_offset = 0
        # main chart
        a0 = plt.subplot2grid((grid_rows, grid_columns), (grid_offset, 0), colspan=grid_columns, rowspan=main_height)
        grid_offset += main_height + spacing

        # percent chart
        if percent_fig:
            a1 = plt.subplot2grid((grid_rows, grid_columns), (grid_offset, 0), colspan=grid_columns, rowspan=perc_height)
            grid_offset += perc_height + spacing

        # province tables
        if show_province_tables:
            ax_provinces = []
            ax_provinces.append(plt.subplot2grid((grid_rows, grid_columns), (grid_offset, 0), colspan=1, rowspan=table_height))
            add_footnote(footnote_left, 'left')
            ax_provinces.append(plt.subplot2grid((grid_rows, grid_columns), (grid_offset, 1), colspan=1, rowspan=table_height))
            ax_provinces.append(plt.subplot2grid((grid_rows, grid_columns), (grid_offset, 2), colspan=1, rowspan=table_height))
            ax_provinces.append(plt.subplot2grid((grid_rows, grid_columns), (grid_offset, 3), colspan=1, rowspan=table_height))
            add_footnote(footnote, 'right')

            fill_province_tables(ax_provinces, list(table.index), list(table))
        else:
            add_footnote(footnote_left, 'left')
            add_footnote(footnote, 'right')

        a0.set_prop_cycle(None)
        if kind != "line":
            areacols = [c for c in cols if c not in between]
            df_plot.plot(ax=a0, y=areacols, kind=kind, stacked=stacked, legend='reverse')
            linecols = between
        else:
            areacols = []
            linecols = cols

        # advance colour cycle so lines have correct next colour
        for _ in range(len(areacols)):
            next(a0._get_lines.prop_cycler)

        for c in linecols:
            style = "--" if c in [f"{b}{ma_suffix}" for b in between] + actuals else None
            width = 5 if c in [f"{h}{ma_suffix}" for h in highlight] else 2
            df_plot.plot(ax=a0,
                         y=c,
                         use_index=True,
                         linewidth=width,
                         style=style,
                         kind="line",
                         zorder=4,
                         legend=c not in actuals,
                         x_compat=kind == 'bar'  # Putting lines on bar plots doesn't work well
                         )

        # reset colours and plot actuals repeating same colours used by lines
        if actuals:
            # TODO: There has to be a less dodgy way than this?
            #a0._get_lines.prop_cycler = iter(get_cycle(cmap))
            # a0._get_lines.set_prop_cycle(get_cycle(cmap))
            # plt.rcParams["axes.prop_cycle"] = get_cycle(cmap)
            # a0.set_prop_cycle(None)
            # plt.gca().set_prop_cycle(None)
            df_plot.plot(ax=a0,
                         y=actuals,
                         use_index=True,
                         linewidth=2,
                         style="--",
                         kind="line",
                         zorder=4,
                         legend=False,
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
            a0.fill_between(df.index, mins, maxes, facecolor="yellow", alpha=0.3, zorder=3, label=None, step=None)
            avg.plot(ax=a0, color="orange", style="--", zorder=5, x_compat=kind == 'bar', legend=False)
            # boxes = df_plot[box_cols].transpose()
            # boxes.boxplot(ax=a0)

        if kind == "bar" and is_dates:
            set_time_series_labels_2(df_plot, a0)

        fig.suptitle(title)
        a0.set_title(label=subtitle)

        handles, labels = a0.get_legend_handles_labels()
        # we are skipping pandas determining which legends to show so do it manually. box lines are 'None'
        # TODO: go back to pandas doing it.
        handles, labels = zip(*[(h, l) for h, l in zip(*a0.get_legend_handles_labels()) if l not in actuals + ['None']])

        leg = a0.legend(handles=handles, labels=legends)

        for line in leg.get_lines():
            line.set_linewidth(4.0)

        clean_axis(a0, y_formatter)
        if limit_to_zero: a0.set_ylim(bottom=0)

        if percent_fig:
            clean_axis(a1, perc_format)
            a1.set_ylim(bottom=0, top=100)
            df_plot.plot(ax=a1, y=perccols, kind='area', legend=False)

            right_axis(a1, perc_format, show_last_values)
            right_value_axis(df_plot, a1, leg, perccols, True, perc_format, show_last_values, 13)
            legends = rewrite_legends(df_plot, legends, perccols, perc_format)

        right_axis(a0, y_formatter, show_last_values)
        right_value_axis(df_plot, a0, leg, cols, stacked, y_formatter, show_last_values)

        legends = rewrite_legends(df_plot, legends, cols, y_formatter)

        a0.legend(handles=handles,
                  labels=legends,
                  loc=legend_pos,
                  ncol=legend_cols)

        plt.tight_layout()
        path = os.path.join("outputs", f'{png_prefix}_{suffix}.png')
        plt.savefig(path, facecolor=theme_light_back)
        logger.info("Plot: {}", path)
        plt.close()

    return None


def fill_province_tables(ax_provinces, provinces, values):
    """Create an info table showing last values."""
    number_columns = len(ax_provinces)
    provinces_per_column = int(np.ceil(len(provinces) / number_columns))

    for ax_number, axis in enumerate(ax_provinces):
        row_labels = provinces[ax_number * provinces_per_column : (ax_number + 1) * provinces_per_column ]
        row_values = values[ax_number * provinces_per_column : (ax_number + 1) * provinces_per_column ]

        cell_text = []
        cell_colors = []
        for value_number, province in enumerate(row_labels):
            value = row_values[value_number]
            cell_text.append([f'{human_format(value,0)}', ])
            cell_colors.append([theme_dark_back, ])
            
        axis.set_axis_off() 
        table = axis.table(cellLoc='right',  loc='upper right',
            rowLabels=row_labels, cellText=cell_text,  cellColours=cell_colors)       
        table.auto_set_column_width((0, 1))
        table.auto_set_font_size(False)
        table.set_fontsize(14)
        table.scale(1, 1.35)

        for cell in table.get_celld().values():
            cell.visible_edges = 'open'
            cell.set_text_props(color=theme_light_text)


def rewrite_legends(df, legends, cols, y_formatter):
    """Rewrite the legends."""
    new_legends = []
    if y_formatter is thaipop: y_formatter = thaipop2

    # add the values to the legends
    values = df.ffill().loc[df.index.max()][cols].apply(pd.to_numeric, downcast='float', errors='coerce')
    for number, value in enumerate(values):
        if not np.isnan(value) and number < len(legends): 
            new_legends.append(f'{y_formatter(value, 0)} {legends[number]}')
    
    # add the remaining legends without values
    while len(new_legends) < len(legends):
        new_legends.append(legends[len(new_legends)])

    return new_legends
        

def add_footnote(footnote, location):
    if footnote:
        if location == 'left':
            plt.annotate(footnote, (0, 0), (0, -70),
                         xycoords='axes fraction', textcoords='offset points',
                         fontsize=15, va='top', horizontalalignment='left')
        if location == 'right':
            plt.annotate(footnote, (1, 0), (0, -70),
                         xycoords='axes fraction',textcoords='offset points',
                         fontsize=15, va='top', horizontalalignment='right')


def clean_axis(axis, y_formatter):
    """Clean up the axis."""
    # axis.spines[:].set_visible(False)
    axis.tick_params(direction='out', length=6, width=0)
    axis.xaxis.label.set_visible(False)
    axis.set_prop_cycle(None)
    if y_formatter is not None:
        axis.yaxis.set_major_formatter(FuncFormatter(y_formatter))


def right_axis(axis, y_formatter, show_last_values):
    """Create clean secondary right axis."""
    new_axis = axis.secondary_yaxis('right', functions=(lambda x: x, lambda x: x))
    clean_axis(new_axis, y_formatter)
    if show_last_values:
        new_axis.set_color(color='#784d00')
    return new_axis


def right_value_axis(df, axis, legend, cols, stacked, y_formatter, show_last_values, max_ticks=27):
    """Create clean secondary right axis showning actual values."""
    if not show_last_values: return

    if y_formatter is thaipop: y_formatter = thaipop2
    new_axis = right_axis(axis, y_formatter, show_last_values)

    values = df.ffill().loc[df.index.max()][cols].apply(pd.to_numeric, downcast='float', errors='coerce')
    bottom, top = axis.get_ylim()
    ticks = Ticks(max_ticks, bottom, top)
    if stacked:
        sum = 0.0
        for number, value in enumerate(values):
            sum += value
            if not np.isnan(value) and number < len(legend.get_patches()): 
                ticks.append(Tick(sum - value/2.0, y_formatter(value,0), legend.get_patches()[number].get_facecolor()))
    else:
        for number, value in enumerate(values):
            if not np.isnan(value) and number < len(legend.get_lines()): 
                ticks.append(Tick(value, y_formatter(value,0), legend.get_lines()[number].get_color()))

    set_ticks(new_axis, ticks)


def set_ticks(axis, ticks):
    """Set the ticks for the axis."""
    ticks.reduce_overlap()
    axis.set_yticks(ticks.get_ticks())
    axis.set_yticklabels(ticks.get_labels())
    for number, label in enumerate(axis.get_yticklabels()):
        label.set_color(ticks.get_color(number))


def sort_by_actual(e):
    return e.actual


class Ticks:
    """All the ticks of an axis."""
    def __init__(self, max_ticks, bottom, top):
        self.ticks = []
        self.max_ticks = max_ticks
        self.bottom = bottom
        self.top = top
        self.spacing = (top - bottom) / max_ticks

    def append(self, tick):
        """Append a tick to the ticks list."""
        self.ticks.append(tick)

    def reduce_overlap(self):
        """Move the tickmark positions of the ticks so that they don't overlap."""
        if len(self.ticks) > self.max_ticks:
            self.spacing = (self.top - self.bottom) / len(self.ticks)

        # move them up if overlapping
        self.ticks.sort(key=sort_by_actual)
        last_value = self.bottom - self.spacing
        for tick in self.ticks:
            if tick.value < last_value + self.spacing: 
                tick.value = last_value + self.spacing
            last_value = tick.value

        # move them halfway back and down if over the top
        adjusted_last = False
        self.ticks.reverse()
        last_value = self.top + self.spacing
        for tick in self.ticks:
            if tick.value > last_value - self.spacing: 
                tick.value = last_value - self.spacing
            else:
                adjusted_last = False
            if not adjusted_last and tick.value > tick.actual: 
                tick.value -= (tick.value - tick.actual) / 2.0
                adjusted_last = True
            last_value = tick.value

        # move them up if they hit the bottom
        self.ticks.reverse()
        last_value = self.bottom - self.spacing
        for tick in self.ticks:
            if tick.value < last_value + self.spacing: 
                tick.value = last_value + self.spacing
            last_value = tick.value

    def get_ticks(self): 
        """Get the tick marks list."""
        return [ tick.value for tick in self.ticks ]

    def get_labels(self): 
        """Get the tick labels list."""
        return [ tick.label for tick in self.ticks ]

    def get_color(self, number): 
        """Get a single tick color."""
        return self.ticks[number].color
    

class Tick:
    """A single tick including tickmarks, labels and colors."""
    def __init__(self, actual, label, color):
        self.value = actual
        self.actual = actual
        self.label = label
        self.color = color


def save_plots(df: pd.DataFrame) -> None:
    logger.info('======== Generating Plots ==========')
    source = 'Source: https://djay.github.io/covidthailand - (CC BY)\n'

    # matplotlib global settings
    matplotlib.use('AGG')
    if theme == 'Black':
        plt.style.use('dark_background') 
    else:
        plt.style.use('seaborn-whitegrid')

    # create directory if it does not exists
    pathlib.Path('./outputs').mkdir(parents=True, exist_ok=True)

    dash_prov = import_csv("moph_dashboard_prov", ["Date", "Province"], dir="inputs/json")

    # Computed data
    # TODO: has a problem if we have local transmission but no proactive
    # TODO: put somewhere else
    walkins = pd.DataFrame(df["Cases Local Transmission"] - df["Cases Proactive"], columns=['Cases Walkin'])
    # In case XLS is not updated before the pptx
    df = df.combine_first(walkins).combine_first(df[['Tests',
                                                     'Pos']].rename(columns=dict(Tests="Tests XLS", Pos="Pos XLS")))

    cols = ['Tests XLS', 'Tests Public', 'Tested PUI', 'Tested PUI Walkin Public', ]
    legends = ['Tests Performed (All)', 'Tests Performed (Public)', 'PUI', 'PUI (Public)', ]
    plot_area(df=df,
              title='PCR Tests and PUI - Thailand', 
              legends=legends,
              png_prefix='tests', cols_subset=cols,
              ma_days=7,
              kind='line', stacked=False, percent_fig=False,
              cmap='tab10',
              actuals=['Tests XLS'],
              footnote='Note: Totals exclude some proactive testing.\nPCR: Polymerase Chain Reaction\nPUI: Person Under Investigation',
              footnote_left=f'{source}Data Sources: Daily Situation Reports\n  DMSC: Thailand Laboratory Testing Data')

    cols = ['Tested Cum',
            'Tested PUI Cum',
            'Tested Proactive Cum',
            'Tested Quarantine Cum',
            'Tested PUI Walkin Private Cum',
            'Tested PUI Walkin Public Cum']
    plot_area(df=df, 
              title='PCR Tests and PUI - Thailand',
              png_prefix='tested_pui', cols_subset=cols,
              ma_days=7,
              kind='line', stacked=False, percent_fig=False,
              cmap='tab10',
              footnote='Note: Excludes some proactive tests.\nPCR: Polymerase Chain Reaction\nPUI: Person Under Investigation',
              footnote_left=f'{source}Data Sources: Daily Situation Reports\n  DMSC: Thailand Laboratory Testing Data')

    # kind of dodgy since ATK is subset of positives but we don't know total ATK
    cols = ['Cases', 'Cases Proactive', 'Tests XLS', 'ATK']
    legend = [
        "Cases (PCR)", 
        "Proactive Cases (PCR)", 
        "PCR Tests", 
        "Probable Case (Registered for home isolation from ATK)"
    ]
    peaks = df[cols] / df.rolling(7).mean().max(axis=0) * 100
    plot_area(df=peaks,
              title='Tests as % of Peak - Thailand',
              png_prefix='tests_peak', cols_subset=cols, legends=legend,
              ma_days=7,
              kind='line', stacked=False, percent_fig=False, clean_end=True,
              cmap='tab10',
              y_formatter=perc_format,
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
              title='Positive Rate: Is enough testing happening? - Thailand',
              legends=legends,
              highlight=['Positivity Public+Private'],
              png_prefix='positivity', cols_subset=cols,
              ma_days=7,
              kind='line', stacked=False, percent_fig=False,
              cmap='tab10',
              y_formatter=perc_format,
              footnote='\nPUI: Person Under Investigation\nPCR: Polymerase Chain Reaction',
              footnote_left=f'\n{source}Data Sources: Daily Situation Reports\n  DMSC: Thailand Laboratory Testing Data')

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
              title='Tests per Confirmed Covid Cases - Thailand',
              legends=legends,
              png_prefix='tests_per_case', cols_subset=cols,
              ma_days=7,
              kind='line', stacked=False, percent_fig=False, show_last_values=False,
              cmap='tab10',
              footnote='\nPUI: Person Under Investigation\nPCR: Polymerase Chain Reaction',
              footnote_left=f'\n{source}Data Sources: Daily Situation Reports\n  DMSC: Thailand Laboratory Testing Data')

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
              title='Positive Rate - Thailand',
              legends=legends,
              png_prefix='positivity_all', cols_subset=cols,
              ma_days=7,
              kind='line', stacked=False, percent_fig=False,
              cmap='tab10',
              footnote='\nPUI: Person Under Investigation',
              footnote_left=f'\n{source}Data Sources: Daily Situation Reports\n  DMSC: Thailand Laboratory Testing Data')

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
              title='Testing Private Ratio - Thailand',
              png_prefix='tests_private_ratio', cols_subset=cols,
              ma_days=7,
              kind='line', stacked=False, percent_fig=False,
              cmap='tab10',
              footnote='\nPUI: Person Under Investigation',
              footnote_left=f'\n{source}Data Sources: Daily Situation Reports\n  DMSC: Thailand Laboratory Testing Data')

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
               "Probable Case (Registered for home isolation from ATK)"]
    plot_area(df=df,
              title='Positive Test Results vs. Confirmed Covid Cases - Thailand',
              legends=legends,
              png_prefix='cases', cols_subset=cols,
              actuals=["Cases", "Pos XLS"],
              ma_days=7,
              kind='line', stacked=False, percent_fig=False,
              cmap="tab10",
              footnote_left=f'{source}Data Sources: Daily Situation Reports\n  DMSC: Thailand Laboratory Testing Data')

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
    plot_area(df=df,
        title='Covid Cases vs. Positive Tests - Thailand',
        legends=legends,
        png_prefix='cases_tests', cols_subset=cols,
        ma_days=21,
        kind='line', stacked=False, percent_fig=False,
        cmap="tab10",
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
        'Confirmed Cases (excl. Prisons)',
        'Confirmed Cases (excl. All Proactive Cases)',
        'Positive Test Results',
    ]
    plot_area(df=df,
        title='3rd Wave Cumulative Covid Cases and Positive Tests - Thailand',
        legends=legends,
        png_prefix='cases_tests_cum3', cols_subset=cols,
        ma_days=None,
        kind='line', stacked=False, percent_fig=False,
        cmap="tab10",
        footnote_left=f'{source}Data Sources: Daily Situation Reports\n  DMSC: Thailand Laboratory Testing Data')

    cols = ['Cases',
            'Pos Area',
            'Pos XLS',
            'Pos Public',
            'Pos Private',
            'Pos']
    plot_area(df=df,
              title='Positive Test Results vs. Confirmed Covid Cases - Thailand',
              png_prefix='cases_all', cols_subset=cols,
              ma_days=7,
              kind='line', stacked=False, percent_fig=False,
              cmap='tab20',
              footnote_left=f'{source}Data Sources: Daily Situation Reports\n  DMSC: Thailand Laboratory Testing Data')

    # No longer include prisons in proactive number
    df['Cases Proactive Community'] = df['Cases Proactive']  # .sub(df['Cases Area Prison'], fill_value=0)
    #df['Cases inc ATK'] = df['Cases'].add(df['ATK'], fill_value=0)
    cols = ['Cases Imported', 'Cases Walkin', 'Cases Proactive Community', 'Cases Area Prison']
    plot_area(df=df,
              title='Covid Cases by Where Tested - Thailand',
              legends=[
                  "Quarantine (Imported)", "Hospital (Walk-ins/Traced)",
                  "Mobile Community Testing (Proactive)",
                  "Prison (Proactive)",
                  # "Rapid Testing (Antigen/ATK)"
              ],
              png_prefix='cases_types', cols_subset=cols,
              unknown_name='Cases Unknown', unknown_total='Cases',
              ma_days=7,
              kind='area', stacked=True, percent_fig=True,
              actuals=["Cases"],
              cmap="tab10",
              #footnote="Rapid test positives (ATK) aren't included in Confirmed Cases without PCR Test",
              footnote_left=f'{source}Data Sources: CCSA Daily Briefing\n  MOPH Daily Situation Report')

    cols = ['Cases Symptomatic', 'Cases Asymptomatic']
    plot_area(df=df,
              title='Covid Cases by Symptoms - Thailand',
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
              unknown_name='Unknown', unknown_total='Cases', unknown_percent=False,
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
              footnote='Note: Excludes some proactive and private tests.\nPCR: Polymerase Chain Reaction',
              footnote_left=f'{source}Data Source: DMSC: Thailand Laboratory Testing Data')

    cols = rearrange([f'Pos Area {area}' for area in DISTRICT_RANGE_SIMPLE], *FIRST_AREAS)
    plot_area(df=df, 
              title='PCR Positive Test Results by Health District - Thailand',
              legends=AREA_LEGEND_SIMPLE,
              png_prefix='pos_area', cols_subset=cols,
              ma_days=None,
              kind='area', stacked=True, percent_fig=False,
              cmap='tab20',
              footnote='Note: Excludes some proactive and private tests.\nPCR: Polymerase Chain Reaction',
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
              footnote='Note: Excludes some proactive tests.\nPCR: Polymerase Chain Reaction',
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
              footnote='Note: Excludes some proactive tests.\nPCR: Polymerase Chain Reaction',
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
              footnote='Note: Excludes some proactive tests.',
              y_formatter=perc_format,
              footnote_left=f'{source}Data Source: DMSC: Thailand Laboratory Testing Data')


    # for area in DISTRICT_RANGE_SIMPLE:
    #     df[f'Positivity Daily {area}'] = df[f'Pos Daily {area}'] / df[f'Tests Daily {area}'] * 100
    # cols = [f'Positivity Daily {area}' for area in DISTRICT_RANGE_SIMPLE]
    pos_areas = join_provinces(dash_prov, "Province", ["Health District Number", "region"])
    pos_areas = area_crosstab(pos_areas, "Positive Rate Dash", aggfunc="mean") * 100
    cols = rearrange([f'Positive Rate Dash Area {area}' for area in DISTRICT_RANGE_SIMPLE], *FIRST_AREAS)
    topcols = df[cols].sort_values(by=df[cols].last_valid_index(), axis=1, ascending=False).columns[:5]
    legend = rearrange(AREA_LEGEND_ORDERED, *[cols.index(c) + 1 for c in topcols])[:5]
    plot_area(df=pos_areas,
              title='Average Positive Rate - by Health District - Thailand',
              legends=legend,
              png_prefix='positivity_area_unstacked', cols_subset=topcols,
              ma_days=7,
              kind='line', stacked=False, percent_fig=False,
              cmap='tab10',
              y_formatter=perc_format,
              footnote_left=f'{source}Data Source: MOPH Covid-19 Dashboard')

    pos_areas = join_provinces(dash_prov, "Province", ["Health District Number", "region"]).reset_index()
    pos_areas = pd.crosstab(pos_areas['Date'], pos_areas['region'], values=pos_areas["Positive Rate Dash"], aggfunc="mean") * 100
    plot_area(df=pos_areas,
              title='Average Positive Rate - by Region - Thailand',
              png_prefix='positivity_region', cols_subset=list(pos_areas.columns),
              ma_days=7,
              kind='line', stacked=False, percent_fig=False,
              cmap='tab10',
              y_formatter=perc_format,
              footnote_left=f'{source}Data Source: MOPH Covid-19 Dashboard')

    top5 = dash_prov.pipe(topprov,
                      lambda df: df["Positive Rate Dash"] * 100,
                      name="Province Positive Rate",
                      other_name=None,
                      num=7)
    cols = top5.columns.to_list()
    plot_area(df=top5,
              title='Positive Rate - Top Provinces - Thailand',
              png_prefix='positivity_prov_top', cols_subset=cols,
              ma_days=7,
              kind='line', stacked=False, percent_fig=False,
              cmap='tab10',
              y_formatter=perc_format,
              footnote_left=f'{source}Data Source: MOPH Covid-19 Dashboard')

    top5 = dash_prov.pipe(topprov,
                      lambda df: -df["Positive Rate Dash"] * 100,
                      lambda df: df["Positive Rate Dash"] * 100,
                      name="Province Positive Rate",
                      other_name=None,
                      num=7)
    cols = top5.columns.to_list()
    plot_area(df=top5,
              title='Positive Rate - Lowest Provinces - Thailand',
              png_prefix='positivity_prov_low', cols_subset=cols,
              ma_days=7,
              kind='line', stacked=False, percent_fig=False,
              cmap='tab10',
              y_formatter=perc_format,
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
              kind='area', stacked=False, percent_fig=False, show_last_values=False,
              cmap='tab20',
              footnote='Note: Excludes some proactive tests.',
              footnote_left=f'{source}Data Source: DMSC: Thailand Laboratory Testing Data')

    #########################
    # Case by area plots
    #########################
    cols = rearrange([f'Cases Area {area}' for area in DISTRICT_RANGE] + ['Cases Imported'], *FIRST_AREAS)
    plot_area(df=df,
              title='Covid Cases by Health District - Thailand',
              legends=AREA_LEGEND + ['Imported Cases'],
              png_prefix='cases_areas', cols_subset=cols,
              unknown_name="Unknown District", unknown_total="Cases",
              ma_days=7,
              kind='area', stacked=True, percent_fig=True,
              cmap='tab20',
              footnote_left=f'{source}Data Source: CCSA Daily Briefing')

    cols = rearrange([f'Cases Walkin Area {area}' for area in DISTRICT_RANGE], *FIRST_AREAS)
    plot_area(df=df,
              title='"Walk-in" Covid Cases by Health District - Thailand',
              legends=AREA_LEGEND,
              png_prefix='cases_areas_walkins', cols_subset=cols,
              ma_days=None,
              kind='area', stacked=True, percent_fig=False,
              cmap='tab20',
              footnote_left=f'{source}Data Source: CCSA Daily Briefing')

    cols = rearrange([f'Cases Proactive Area {area}' for area in DISTRICT_RANGE], *FIRST_AREAS)
    plot_area(df=df,
              title='"Proactive" Covid Cases by Health District - Thailand',
              legends=AREA_LEGEND,
              png_prefix='cases_areas_proactive', cols_subset=cols,
              ma_days=None,
              kind='area', stacked=True, percent_fig=False, show_last_values=False,
              cmap='tab20',
              footnote_left=f'{source}Data Source: CCSA Daily Briefing')

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
              kind='area', stacked=False, percent_fig=False, show_last_values=False, limit_to_zero=False,
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
    cols = ["Hospitalized Respirator", "Hospitalized Severe excl vent", "Hospitalized Hospital", "Hospitalized Field"]
    legends = ['On Ventilator', 'In Serious Condition', 'In Isolation/Hospital', 'In Field Hospital']
    # plot_area(df=df, png_prefix='cases_active', cols_subset=cols,
    #           title='Thailand Active Covid Cases\n(Severe, Field, and Respirator only available from '
    #                 '2021-04-24 onwards)',
    #           legends=legends,
    #           kind='area', stacked=True, percent_fig=False, ma_days=None, cmap='tab10')

    cols = ["Hospitalized Severe", "Hospitalized Severe excl vent", "Hospitalized Respirator"]
    legends = ["In Serious Condition", 'In Serious Condition (without ventilator)', 'On Ventilator']
    plot_area(df=df,
              title='Active Covid Cases in Serious Condition - Thailand',
              legends=legends,
              png_prefix='active_severe', cols_subset=cols,
              actuals=False,
              ma_days=7,
              kind='line', stacked=True, percent_fig=False,
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
        'Deaths from cases since 1st April', 'On Ventilator', 'In Serious Condition (without Ventilator)',
        'In Hospital/Mild', 'In Field Hospital', 'Recovered from cases since 1st April'
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

    cols = ['Hospitalized Respirator', 'Hospitalized Severe', "Hospitalized Field Unknown", "Hospitalized Field Hospitel", "Hospitalized Field HICI",]
    df["Hospitalized Mild"] = df["Hospitalized"].sub(df[cols].sum(axis=1, skipna=True), fill_value=0)
    cols = ['Hospitalized Respirator', 'Hospitalized Severe',
            "Hospitalized Mild", "Hospitalized Field Unknown",
            "Hospitalized Field Hospitel",
            "Hospitalized Field HICI", ]
    legend = [
        'Serious On Ventilator', 'Serious without Ventilator',
        'Mild In Hospital', 'Mild In Field Hospital/Other',
        "Mild in Hotel Field Hospital (Hospitel)",
        "Mild in Home/Community Isolation (HICI)"
    ]
    plot_area(df=df,
              title='Acive Cases by Condition - Thailand',
              png_prefix='active_hospital', cols_subset=cols, legends=legend,
              # unknown_name='Hospitalized Other', unknown_total='Hospitalized', unknown_percent=True,
              ma_days=7,
              kind='area', stacked=True, percent_fig=True, clean_end=True,
              cmap='tab10',
              footnote_left='Data Source: MOPH Covid-19 Dashboard,  CCSA Daily Briefing')

    df["Hospitalized All Mild"] = df["Hospitalized Mild"] + df["Hospitalized Field"]
    cols = [
        "Hospitalized Respirator",
        "Hospitalized Severe",
        "Hospitalized All Mild",
    ]
    legends = [
        "Serious Condition with Ventilator",
        "Serious Condition without Ventilator",
        "Mild Condition",
    ]
    peaks = df[cols] / df.rolling(7).mean().max(axis=0) * 100
    plot_area(df=peaks,
              title='Active Cases by Condition as % of Peak - Thailand',
              png_prefix='active_peak', cols_subset=cols,
              legends=legends,
              ma_days=7,
              kind='line', stacked=False, percent_fig=False, clean_end=True,
              cmap='tab10',
              y_formatter=perc_format,
              footnote_left='Data Source: MOPH Covid-19 Dashboard')

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

    plot_area(df=vac_daily,
        title='Daily Covid Vaccinations by Priority Groups - Thailand',
        legends=[
            # 'Doses per day needed to run out in a week',
            'Rate for 70% 1st Jab in 2021',
            'Rate for 70% 2nd Jab in 2021'
        ] + [clean_vac_leg(c, "(1st jab)", "(2nd jab)") for c in daily_cols],  # bar puts the line first?
        legend_cols=2,
        png_prefix='vac_groups_daily', cols_subset=daily_cols,
        between=[
            # '7d Runway Rate',
            'Target Rate 1',
            'Target Rate 2'],
        periods_to_plot=["30d", "2"],  # too slow to do all
        ma_days=None,
        kind='bar', stacked=True, percent_fig=False, show_last_values=False,
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


    dose1 = vac_cum[[f'Vac Group {group} 1 Cum % ({goal/1000000:.1f}M)' for group, goal in goals]]
    dose2 = vac_cum[[f'Vac Group {group} 2 Cum % ({goal/1000000:.1f}M)' for group, goal in goals]]
    pred1, pred2 = pred_vac(dose1, dose2)
    pred1 = pred1.clip(upper=pred1.iloc[0].clip(100), axis=1)  # no more than 100% unless already over
    pred2 = pred2.clip(upper=pred2.iloc[0].clip(100), axis=1)  # no more than 100% unless already over
    vac_cum = vac_cum.combine_first(pred1).combine_first(pred2)

    cols2 = [c for c in vac_cum.columns if " 2 Cum %" in c and "Vac Group " in c and "Pred" not in c]
    legends = [clean_vac_leg(c) for c in cols2]
    plot_area(df=vac_cum.combine_first(pred2),
        title='Full Covid Vaccination Progress - Thailand',
        legends=legends,
        png_prefix='vac_groups_goals_full', cols_subset=cols2,
        kind='line',
        actuals=list(pred2.columns),
        ma_days=None,
        stacked=False, percent_fig=False, show_last_values=False,
        y_formatter=perc_format,
        cmap=get_cycle('tab20', len(cols2) * 2, unpair=True, start=len(cols2)),
        footnote_left=f'{source}Data Source: DDC Daily Vaccination Reports',
        footnote="Assumes 2 months between doses")

    cols2 = [c for c in vac_cum.columns if " 1 Cum %" in c and "Vac Group " in c and "Pred" not in c]
    actuals = [c for c in vac_cum.columns if " 1 Pred" in c]
    legends = [clean_vac_leg(c) for c in cols2]
    plot_area(df=vac_cum.combine_first(pred1),
        title='Half Covid Vaccination Progress - Thailand',
        legends=legends,
        png_prefix='vac_groups_goals_half', cols_subset=cols2,
        actuals=list(pred1.columns),
        ma_days=None,
        kind='line', stacked=False, percent_fig=False, show_last_values=False,
        y_formatter=perc_format,
        cmap=get_cycle('tab20', len(cols2) * 2, unpair=True, start=len(cols2)),  # TODO: seems to be getting wrong colors
        footnote_left=f'{source}Data Source: DDC Daily Vaccination Reports')

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

    # Top 5 vaccine rollouts
    vac = import_csv("vaccinations", ['Date', 'Province'])

    vac = vac.groupby("Province", group_keys=False).apply(fix_gaps)
    # Let's trust the dashboard more but they could both be different
    # TODO: dash gives different higher values. Also glitches cause problems
    # vac = dash_prov.combine_first(vac)
    #vac = vac.combine_first(vac_dash[[f"Vac Given {d} Cum" for d in range(1, 4)]])
    # Add them all up
    vac = vac.combine_first(vac[[f"Vac Given {d} Cum" for d in range(1, 4)]].sum(axis=1, skipna=False).to_frame("Vac Given Cum"))
    vac = vac.join(get_provinces()['Population'], on='Province')
    # Bring in vac populations
    pops = vac["Vac Population"].groupby("Province").max().to_frame("Vac Population")  # It's not on all data
    vac = vac.join(pops, rsuffix="2")

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
              footnote="Percentage include ages 0-18")

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
              footnote="Percentage include ages 0-18")

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
              footnote="Percentage include ages 0-18")

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
              title='Confirmed Covid Cases/100k - Trending Up Provinces - Thailand',
              png_prefix='cases_prov_increasing', cols_subset=cols,
              ma_days=7,
              kind='line', stacked=False, percent_fig=False,
              cmap='tab10',
              footnote='\nNote: Per 100,000 people.',
              footnote_left=f'\n{source}Data Sources: CCSA Daily Briefing\n  API: Daily Reports of COVID-19 Infections')

    top5 = cases.pipe(topprov,
                      decreasing(cases_per_capita("Cases"), 3),
                      cases_per_capita("Cases"),
                      name="Province Cases (3d MA)",
                      other_name="Other Provinces",
                      num=7)
    cols = top5.columns.to_list()
    plot_area(df=top5,
              title='Confirmed Covid Cases/100k - Trending Down Provinces - Thailand',
              png_prefix='cases_prov_decreasing', cols_subset=cols,
              ma_days=7,
              kind='line', stacked=False, percent_fig=False,
              cmap='tab10',
              footnote='\nNote: Per 100,000 people.',
              footnote_left=f'\n{source}Data Sources: CCSA Daily Briefing\n  API: Daily Reports of COVID-19 Infections')

    top5 = cases.pipe(topprov,
                      cases_per_capita("Cases"),
                      name="Province Cases",
                      other_name="Other Provinces",
                      num=6)
    cols = top5.columns.to_list()
    provtable = cases.reset_index()
    provtable = pd.crosstab(index=provtable['Date'], columns=provtable['Province'], values=provtable['Cases'], aggfunc="max")
    provtable = provtable.loc[provtable.last_valid_index()]
    provtable = provtable.nlargest(len(provtable))  # Sort it 
    plot_area(df=top5,
              title='Confirmed Covid Cases/100k - Top Provinces - Thailand',
              png_prefix='cases_prov_top', cols_subset=cols,
              ma_days=7,
              kind='line', stacked=False, percent_fig=False,
              cmap='tab10',
              table = provtable,
              footnote='Note: Per 100,000 people.',
              footnote_left=f'{source}Data Sources: CCSA Daily Briefing\n  API: Daily Reports of COVID-19 Infections')

    top5 = cases.pipe(topprov,
                      increasing(cases_per_capita('Cases Walkin'), 5),
                      cases_per_capita('Cases Walkin'),
                      name="Province Cases Walkin (7d MA)",
                      other_name="Other Provinces",
                      num=6)
    cols = top5.columns.to_list()
    plot_area(df=top5,
              title='"Walk-in" Covid Cases/100k - Top Provinces - Thailand',
              png_prefix='cases_walkins_increasing', cols_subset=cols,
              ma_days=7,
              kind='line', stacked=False, percent_fig=False,
              cmap='tab10',
              footnote='\nNote: Per 100,000 people.',
              footnote_left=f'\n{source}Data Sources: CCSA Daily Briefing\n  API: Daily Reports of COVID-19 Infections')

    for risk in ['Contact', 'Proactive Search', 'Community', 'Work', 'Unknown']:
        top5 = cases.pipe(topprov,
                          increasing(cases_per_capita(f"Cases Risk: {risk}"), 5),
                          cases_per_capita(f"Cases Risk: {risk}"),
                          name=f"Province Cases {risk} (7d MA)",
                          other_name="Other Provinces",
                          num=6)
        cols = top5.columns.to_list()
        plot_area(df=top5,
                  title=f'{risk} Related Covid Cases/100k - Trending Up Provinces - Thailand',
                  png_prefix=f'cases_{risk.lower().replace(" ","_")}_increasing', cols_subset=cols,
                  ma_days=7,
                  kind='line', stacked=False, percent_fig=False,
                  cmap='tab10',
                  footnote='\nNote: Per 100,000 people.',
                  footnote_left=f'\n{source}Data Sources: CCSA Daily Briefing\n  API: Daily Reports of COVID-19 Infections')

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
        plot_area(df=top5,
            title=f'Severe Active Covid Cases/100k - {title}Provinces - Thailand',
            png_prefix=f'active_severe_{direction.__name__}', cols_subset=cols,
            ma_days=7,
            kind='line', stacked=False, percent_fig=False,
            cmap='tab10',
            footnote='Note: Per 100,000 people.',
            footnote_left=f'{source}Data Source: CCSA Daily Briefing')

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
    plot_area(df=cases_est,
              title='Covid Infections (unofficial estimate) - Thailand',
              legends=legend,
              png_prefix='cases_infections_estimate', cols_subset=cols,
              actuals=True,
              ma_days=7,
              kind='line', stacked=False, percent_fig=False,
              cmap='tab10',
              footnote='Note: Based on Deaths/IFR.\nIFR: Infection Fatality Rate\nDISCLAIMER: See website for the assumptions of this simple estimate.',
              footnote_left=f'{source}Data Sources: CCSA Daily Briefing\n  Covid IFR Analysis, Thailand Population by Age')

    ####################
    # Deaths
    ####################

    # TODO: predict median age of death based on population demographics

    cols = ['Deaths', 'Deaths Risk Family', 'Deaths Comorbidity None']
    plot_area(df=df,
              title='Covid Deaths - Thailand',
              legends=['Deaths', 'Infected from Family', 'No Underlying Diseases'],
              png_prefix='deaths_reason', cols_subset=cols,
              actuals=['Deaths'],
              ma_days=7,
              kind='line', stacked=False, percent_fig=False,
              cmap='tab10',
              footnote_left=f'{source}Data Source: CCSA Daily Briefing')

    df['Deaths Age Median (MA)'] = df['Deaths Age Median'].rolling('7d').mean()
    cols = ['Deaths Age Median (MA)', 'Deaths Age Max', 'Deaths Age Min']
    plot_area(df=df,
              title='Covid Deaths Age Range - Thailand',
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
              unknown_name='Unknown', unknown_total='Cases', unknown_percent=False,
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
    deaths_by_age['Deaths (MA)'] = deaths_by_age['Deaths'].rolling(7, min_periods=3, center=True).mean()
    deaths_by_age['Deaths Ages Sum'] = deaths_by_age[death_cols].sum(axis=1)
    deaths_by_age = deaths_by_age.pipe(normalise_to_total, death_cols, 'Deaths (MA)')
    cols = death_cols + ['Deaths (MA)', 'Deaths Ages Sum']
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
              unknown_name='Unknown', unknown_total='Deaths', unknown_percent=False,
              ma_days=7,
              kind='area', stacked=True, percent_fig=True, clean_end=True,
              cmap=get_cycle('summer_r', len(death_cols), extras=["gainsboro"]),
              footnote_left=f'{source}Data Source: MOPH Covid-19 Dashboard')

    # Do a % of peak chart for death vs cases
    cols = ['Cases', 'Deaths']
    peaks = df[cols] / df.rolling(7).mean().max(axis=0) * 100
    plot_area(df=peaks,
              title='Daily Averages as % of Peak - Thailand',
              png_prefix='cases_peak', cols_subset=cols,
              ma_days=7,
              kind='line', stacked=False, percent_fig=False, clean_end=True,
              cmap='tab20_r',
              y_formatter=perc_format,
              footnote_left=f'{source}Data Source: MOPH Covid-19 Dashboard,  CCSA Daily Briefing')

    # kind of dodgy since ATK is subset of positives but we don't know total ATK
    cols = ['Cases', 'Tests XLS', 'ATK']
    peaks = df[cols] / df.rolling(7).mean().max(axis=0) * 100
    legend = ["Cases (PCR Tested Only)", "PCR Tests", "Home Isolation from ATK Positive"]
    plot_area(df=peaks,
              title='Tests as % of Peak - Thailand',
              png_prefix='tests_peak', cols_subset=cols, legends=legend,
              ma_days=7,
              kind='line', stacked=False, percent_fig=False, clean_end=True,
              cmap='tab20_r',
              y_formatter=perc_format,
              footnote_left=f'{source}Data Source: MOPH Covid-19 Dashboard,  CCSA Daily Briefing')

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
    plot_area(df=all, png_prefix='deaths_pscore',
              title='Monthly Deaths above Normal - Thailand',
              legends=["Deviation from Normal Deaths (Removing Covid Deaths)", "Deviation from Normal Deaths (Average 2015-19)"],
              cols_subset=['Deviation from expected Deaths', 'PScore'],
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
              kind='bar', stacked=False, percent_fig=False, show_last_values=False, limit_to_zero=False,
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
NOTE: Excess deaths can be changed by many factors other than Covid.
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

        plot_area(df=pan_months,
                  title=f'Deaths from All Causes {year_span} - Thailand',
                  legends=["Deaths (ex. Covid)", "Confirmed Covid Deaths"],
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
    by_age = by_age.reset_index().pivot(values=["PScore"], index="Date", columns="Age Group")
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


if __name__ == "__main__":

    df = scrape_and_combine()
    save_plots(df)
