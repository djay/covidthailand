import json
import os
import xml.etree.ElementTree as ET
from io import BytesIO
from typing import Callable
from typing import List
from typing import Sequence
from typing import Union

import matplotlib.cm
import matplotlib.image as mpimg
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from matplotlib.colors import ListedColormap
from matplotlib.offsetbox import AnnotationBbox
from matplotlib.offsetbox import DrawingArea
from matplotlib.offsetbox import OffsetImage
from matplotlib.offsetbox import TextArea
from matplotlib.ticker import FuncFormatter

from utils_pandas import get_cycle
from utils_pandas import human_format
from utils_pandas import perc_format
from utils_pandas import set_time_series_labels_2
from utils_scraping import logger
from utils_scraping import remove_suffix
from utils_thai import thaipop
from utils_thai import thaipop2

source = 'Source: https://djay.github.io/covidthailand - (CC BY)\n'


theme = 'Black'
theme_label_text = '#F1991F'
theme_light_text = '#E9E8E9'
theme_dark_text = '#424242'
theme_light_back = '#202020'
theme_dark_back = '#0C1111'

cmap_regions = ListedColormap([
    "#CC551B",  # Bangkok Region
    "#FFC200",  # Central Region
    "#77B251",  # Eastern Region
    "#F68E4D",  # Western Region
    "#26E2FD",  # Northeastern Region
    "#277CE5",  # Northern Region
    "#BF2C54",  # Southern Region
    "olive",  # Prisons
    "lawngreen",  # Imported
    "silver",  # Thailand
],
    name='Region Colors',
    N=10,
)

region_colors = {
    'Bangkok Metropolitan Region': cmap_regions(0),
    '  Bangkok Region': cmap_regions(0),
    'Central': cmap_regions(1),
    '  Central Region': cmap_regions(1),
    'Eastern': cmap_regions(2),
    '  Eastern Region': cmap_regions(2),
    'Western': cmap_regions(3),
    '  Western Region': cmap_regions(3),
    'Northeastern': cmap_regions(4),
    '  Northeast Region': cmap_regions(4),
    'Northern': cmap_regions(5),
    '  Northern Region': cmap_regions(5),
    'Southern': cmap_regions(6),
    '  Southern Region': cmap_regions(6),
    'Imported/Prisons': cmap_regions(7),
    '  Prisons': cmap_regions(7),
    '  Imported': cmap_regions(8),
    'Thailand': cmap_regions(9),
}


def plot_area(df: pd.DataFrame,
              png_prefix: str,
              cols_subset: Union[str, Sequence[str]],
              title: str,
              footnote: str = None,
              footnote_left: str = None,
              legends: List[str] = None,
              legend_pos: str = 'upper left',
              legend_cols: int = 1,
              kind: str = 'line',
              stacked=False,
              percent_fig: bool = False,
              mini_map: bool = False,
              table: pd.DataFrame = [],
              limit_to_zero: bool = True,
              unknown_name: str = 'Unknown',
              unknown_total: str = None,
              unknown_percent=False,
              ma_days: int = None,
              cmap: str = 'tab20',
              periods_to_plot=['3'],
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
    :param mini_map: whether the mini map of Thailand should be shown and the region colors fixed
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
            "axes.grid": True,
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

    # Make a copy

    if ma_days:
        ma_suffix = ' (MA)'
        df = df.assign(**{f'{c}{ma_suffix}': df[c].rolling(ma_days,
                       min_periods=int(ma_days / 2), center=True).mean() for c in cols})
        cols = [f'{c}{ma_suffix}' for c in cols]
    else:
        ma_suffix = ''

    # try to hone in on last day of "important" data. Assume first col
    last_update = df[orig_cols[:1]].dropna().last_valid_index()  # date format chosen: '05 May 2021'
    # last_date_excl = df[cols].last_valid_index() # last date with some data (not inc unknown)
    is_dates = hasattr(last_update, 'date')

    if unknown_total:
        if ma_days:
            df = df.assign(**{f'{unknown_total}{ma_suffix}': df[unknown_total].rolling(ma_days,
                                                                                       min_periods=int(ma_days / 2),
                                                                                       center=True).mean()})
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
            df = df.assign(**{f'{c} (%)': df[f'{c}'] / df[perccols].sum(axis=1) * 100})
        if unknown_total and not unknown_percent:
            df = df.assign(**{f'{unknown_name}{ma_suffix} (%)': 0})
        perccols = [f'{c} (%)' for c in perccols]

    if mini_map:
        cmap = cmap_regions

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
            # '1': df_clean[:'2020-06-01'],
            # '2': df_clean['2020-12-12':],
            '3': df_clean['2021-04-01':],
            '4': df_clean['2022-02-01':],
            '30d': df_clean.last('30d')
        }
        quick = os.environ.get('USE_CACHE_DATA', False) == 'True'  # TODO: have its own switch
        if periods_to_plot:
            pass
        elif quick:
            periods_to_plot = ['3']
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
        fn_left_lines = len(footnote_left.split('\n')) if footnote_left else 0
        fn_right_lines = len(footnote.split('\n')) if footnote else 0
        footnote_height = max(fn_left_lines, fn_right_lines)

        # figure out the figure dimensions
        figure_height = 21
        figure_width = 20
        grid_rows = 1
        grid_columns = 5
        main_rows = 1
        if percent_fig:
            figure_height += 7
            grid_rows += 2
            main_rows = 2
        if show_province_tables:
            figure_height += 7
            grid_rows += 2
            main_rows = 2
        fig = plt.figure(figsize=[figure_width, 0.5 * figure_height + 0.4 * footnote_height])

        grid_offset = 0
        # main chart
        a0 = plt.subplot2grid((grid_rows, grid_columns), (0, 0), colspan=grid_columns, rowspan=main_rows)
        grid_offset += main_rows

        # percent chart
        if percent_fig:
            a1 = plt.subplot2grid((grid_rows, grid_columns), (grid_offset, 0), colspan=grid_columns, rowspan=1)
            grid_offset += 1

        # province tables
        if show_province_tables:
            ax_provinces = []
            ax_provinces.append(plt.subplot2grid((grid_rows, grid_columns), (grid_offset, 0), colspan=1, rowspan=1))
            add_footnote(footnote_left, 'left')
            ax_provinces.append(plt.subplot2grid((grid_rows, grid_columns), (grid_offset, 1), colspan=1, rowspan=1))
            ax_provinces.append(plt.subplot2grid((grid_rows, grid_columns), (grid_offset, 2), colspan=1, rowspan=1))
            ax_provinces.append(plt.subplot2grid((grid_rows, grid_columns), (grid_offset, 3), colspan=1, rowspan=1))
            ax_provinces.append(plt.subplot2grid((grid_rows, grid_columns), (grid_offset, 4), colspan=1, rowspan=1))
            add_footnote(footnote, 'right')

            add_to_table(ax_provinces[0], table, ['Bangkok Metropolitan Region', 'Central', ])
            add_to_table(ax_provinces[1], table, ['Western', 'Eastern'])
            add_to_table(ax_provinces[2], table, ['Northeastern'])
            add_to_table(ax_provinces[3], table, ['Northern'])
            add_to_table(ax_provinces[4], table, ['Southern'])

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
                         alpha=0.5,
                         zorder=4,
                         legend=False,
                         x_compat=kind == 'bar'  # Putting lines on bar plots doesn't work well
                         )

        # If actuals are after cols then they are future predictions. put in a line to show today
        if actuals and df[actuals].last_valid_index() and df[cols].last_valid_index() < df[actuals].last_valid_index():
            a0.axvline(df[cols].last_valid_index(), color='grey', linestyle='--', lw=1)

        if box_cols and type(box_cols[0]) != list:
            box_cols = [box_cols]
        elif not box_cols:
            box_cols = []
        for dist in box_cols:
            if len(dist) == 3:
                # assume its already min, mean max
                # TODO: maybe should be other setting?
                mins, avg, maxes = [df_plot[c] for c in dist]
            else:
                mins, maxes, avg = df_plot[dist].min(axis=1), df_plot[dist].max(axis=1), df_plot[dist].mean(axis=1)
            a0.fill_between(mins.index, mins, maxes, facecolor="yellow", alpha=0.3, zorder=3, label=None, step=None)
            avg.plot(ax=a0, color="orange", style="--", zorder=5, x_compat=kind == 'bar', legend=True)
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
        if limit_to_zero:
            a0.set_ylim(bottom=0)

        if percent_fig:
            clean_axis(a1, perc_format)
            a1.set_ylim(bottom=0, top=100)
            df_plot.plot(ax=a1, y=perccols, kind='area', legend=False)

            right_axis(a1, perc_format)
            right_value_axis(df_plot, a1, leg, perccols, True, perc_format, 13)
            # legends = rewrite_legends(df_plot, legends, perccols, perc_format)

        right_axis(a0, y_formatter)
        if not (kind == 'bar' and stacked == False):
            right_value_axis(df_plot, a0, leg, cols, stacked, y_formatter)

        # legends = rewrite_legends(df_plot, legends, cols, y_formatter)

        a0.legend(handles=handles,
                  labels=legends,
                  loc=legend_pos,
                  ncol=legend_cols)

        if mini_map:
            add_minimap(a0)

        plt.tight_layout(pad=1.107, w_pad=-10.0, h_pad=1.0)
        path = os.path.join("outputs", f'{png_prefix}_{suffix}.png')
        plt.savefig(path, facecolor=theme_light_back)
        svg_hover(df_plot[cols + list(orig_cols)], plt, fig, leg, stacked,
                  os.path.join("outputs", f'{png_prefix}_{suffix}.svg'))
        logger.info("Plot: {}", path)
        plt.close()

    return None


def add_minimap(axis):
    image = mpimg.imread('regions.png')
    imagebox = OffsetImage(image, zoom=0.3, interpolation='bilinear')
    annotationbox = AnnotationBbox(imagebox, (0.23, 0.75), xycoords='axes fraction', frameon=False)
    axis.add_artist(annotationbox)


def trend_indicator(trend, style):
    """Get the trend indicator and corresponding color."""
    if trend == 0.00042 or np.isnan(trend):
        return '?', (0, 0, 0, 0)
    arrows = ('→', '↗', '↑', '↓', '↘')
    trend = min(max(trend, -1), 1)  # limit the trend

    trend_color = (1, 0, 0, trend * trend) if (trend > 0) != ("_up" in style) else (0, 1, 0, trend * trend)
    return arrows[round(trend * 2)], trend_color


def append_row(row_labels, row_texts, row_colors, trend_colors,
               labels='', texts=['', ''], colors=[(0, 0, 0, 0), (0, 0, 0, 0)], trend_color=(0, 0, 0, 0)):
    """Append a table row."""
    row_labels.append(labels)
    row_texts.append(texts)
    row_colors.append(colors)
    trend_colors.append(trend_color)


def add_regions_to_axis(axis, table_regions):
    """Add a sorted table with multiple regions to the axis."""
    row_labels = []
    row_texts = []
    row_colors = []
    trend_colors = []

    # get the regions and add the heading
    regions = list(table_regions.loc[:, 'region'].tolist())
    if len(regions) < 7:
        # TODO: fix to at least show some
        return
    # TODO: should fix region name at the source so appears everywhere
    regions[0] = {'Bangkok Metropolitan Region': 'Bangkok', 'Northeastern': 'Northeast'}.get(regions[0], regions[0])
    current_region = regions[0]
    append_row(row_labels, row_texts, row_colors, trend_colors, '  ' + current_region + ' Region')

    # get the remaining values
    provinces = list(table_regions.index)
    values = list(table_regions.loc[:, 'Value'].tolist())
    trends = list(table_regions.loc[:, 'Trend'].tolist())
    if "Trend_style" in table_regions.columns:
        styles = list(table_regions.loc[:, 'Trend_style'].tolist())
    else:
        styles = None

    # generate the the cell values and colors
    for row_number, province in enumerate(provinces):
        if provinces[row_number] == 'Phra Nakhon Si Ayutthaya':
            provinces[row_number] = 'Ayutthaya'
        if provinces[row_number] == 'Nakhon Si Thammarat':
            provinces[row_number] = 'Nakhon Si Tham.'
        if regions[row_number] == 'Bangkok Metropolitan Region':
            regions[row_number] = 'Bangkok'
        if regions[row_number] == 'Northeastern':
            regions[row_number] = 'Northeast'
        if not current_region == regions[row_number]:
            append_row(row_labels, row_texts, row_colors, trend_colors)
            current_region = regions[row_number]
            append_row(row_labels, row_texts, row_colors, trend_colors, '  ' + current_region + ' Region')

        trend_arrow, trend_color = trend_indicator(trends[row_number], style=styles[row_number] if styles else "green_up")
        append_row(row_labels, row_texts, row_colors, trend_colors,
                   provinces[row_number], [f'{human_format(values[row_number], 0)}', trend_arrow],
                   [(0, 0, 0, 0), trend_color], trend_color)

    # create the table
    axis.set_axis_off()
    table = axis.table(cellLoc='right', loc='upper right', colWidths=[0.6, 0.17],
                       rowLabels=row_labels, cellText=row_texts, cellColours=row_colors)
    table.auto_set_column_width((0, 1))
    table.auto_set_font_size(False)
    table.set_fontsize(15)
    table.scale(1.1, 1.42)

    # fix the formating and trend colors
    for cell in table.get_celld().values():
        cell.set_text_props(color=theme_light_text, fontsize=15)
    for row_number, color in enumerate(trend_colors):
        if row_labels[row_number] in region_colors:
            table[(row_number, -1)].set_text_props(color=region_colors[row_labels[row_number]])
        table[(row_number, 1)].set_text_props(color='blue')
        table[(row_number, 1)].set_color(color)
        table[(row_number, -1)].set_color(theme_light_back)
        table[(row_number, 0)].set_color(theme_light_back)


def add_to_table(axis, table, regions):
    """Add selected regions to a table."""
    regions_to_add = table[table['region'].isin(regions)]
    regions_to_add = regions_to_add.sort_values(by=['region', 'Value'], ascending=[True, False])
    regions_to_add['Trend'].replace(np.nan, 0.00042, inplace=True)
    add_regions_to_axis(axis, regions_to_add)


def rewrite_legends(df, legends, cols, y_formatter):
    """Rewrite the legends."""
    new_legends = []
    if y_formatter is thaipop:
        y_formatter = thaipop2

    # add the values to the legends
    values = df.ffill().iloc[-1][cols].apply(pd.to_numeric, downcast='float', errors='coerce')
    for number, value in enumerate(values):
        if not np.isnan(value) and number < len(legends):
            new_legends.append(f'{y_formatter(value, 0)} {legends[number]}')

    # add the remaining legends without values
    while len(new_legends) < len(legends):
        new_legends.append(legends[len(new_legends)])

    return new_legends


def add_footnote(footnote, location):
    """Add left or right footnotes."""
    if footnote:
        if location == 'left':
            plt.annotate(footnote, (0, 0), (0, -70),
                         xycoords='axes fraction', textcoords='offset points',
                         fontsize=15, va='top', horizontalalignment='left')
        if location == 'right':
            plt.annotate(footnote, (1, 0), (0, -70),
                         xycoords='axes fraction', textcoords='offset points',
                         fontsize=15, va='top', horizontalalignment='right')


def clean_axis(axis, y_formatter):
    """Clean up the axis."""
    # axis.spines[:].set_visible(False)
    axis.tick_params(direction='out', length=6, width=0)
    axis.xaxis.label.set_visible(False)
    axis.set_prop_cycle(None)
    if y_formatter is not None:
        axis.yaxis.set_major_formatter(FuncFormatter(y_formatter))


def right_axis(axis, y_formatter):
    """Create clean secondary right axis."""
    new_axis = axis.secondary_yaxis('right', functions=(lambda x: x, lambda x: x))
    clean_axis(new_axis, y_formatter)
    new_axis.set_color(color='#784d00')
    return new_axis


def right_value_axis(df, axis, legend, cols, stacked, y_formatter, max_ticks=27):
    """Create clean secondary right axis showning actual values."""
    if y_formatter is thaipop:
        y_formatter = thaipop2
    new_axis = right_axis(axis, y_formatter)

    values = df.ffill().iloc[-1][cols].apply(pd.to_numeric, downcast='float', errors='coerce')
    bottom, top = axis.get_ylim()
    ticks = Ticks(max_ticks, bottom, top)
    if stacked:
        sum = 0.0
        for number, value in enumerate(values):
            sum += value
            if not np.isnan(value) and number < len(legend.get_patches()):
                ticks.append(Tick(sum - value / 2.0, y_formatter(value, 0), legend.get_patches()[number].get_facecolor()))
    else:
        for number, value in enumerate(values):
            if not np.isnan(value) and number < len(legend.get_lines()):
                ticks.append(Tick(value, y_formatter(value, 0), legend.get_lines()[number].get_color()))

    set_ticks(new_axis, ticks)


def set_ticks(axis, ticks):
    """Set the ticks for the axis."""
    ticks.reduce_overlap()
    axis.set_yticks(ticks.get_ticks())
    axis.set_yticklabels(ticks.get_labels())
    for number, label in enumerate(axis.get_yticklabels()):
        label.set_color(ticks.get_color(number))


def sort_by_actual(e):
    """Sort the values by the Actual."""
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
            self.spacing = (self.top - self.bottom) / (len(self.ticks) - 1)

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
        return [tick.value for tick in self.ticks]

    def get_labels(self):
        """Get the tick labels list."""
        return [tick.label for tick in self.ticks]

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


def svg_hover(df, plt, fig, legend, stacked, path):
    f = BytesIO()
    plt.savefig(f, format="svg", facecolor=theme_light_back)

    # --- Add interactivity ---
    ax = fig.axes[0]

    # Create XML tree from the SVG file.
    tree, xmlid = ET.XMLID(f.getvalue())
    tree.set('onload', 'init(event)')

    # # patch_2
    # box = xmlid["patch_2"]
    # box.set('onmouseover', "ShowTooltip(this)")
    # box.set('onmouseout', "HideTooltip(this)")

    def make_tootip_entry(number, text, color):
        """Make a single tooltip entry."""

        color = matplotlib.colors.to_hex(color, keep_alpha=False)
        text = text.replace("&", "&amp;")
        entry = f'<tr style="color:{color}"><td >{text}</td><td id="value{number}">999999.99</td></tr>'
        return entry

    value_rows = ''
    colours = []
    legends = []
    for number, patch in enumerate(legend.get_patches() if stacked else legend.get_lines()):
        text = legend.get_texts()[number].get_text()
        color = list(patch.get_facecolor() if stacked else patch.get_color())
        value_rows += make_tootip_entry(number, text, color)
        legends.append(text)
        colours.append(color)

    # insert svg to for tooltip in - https://codepen.io/billdwhite/pen/rgEbc
    tooltipsvg = f"""
      <g transform="scale(1.0)" xmlns="http://www.w3.org/2000/svg" pointer-events="none">
        <g class="tooltip mouse" visibility="hidden" style="background:#0000ff50;">
            <!-- The rectangle and text are positioned
                 to the right and above the <g> element's
                 0,0 point, purely to help with all the
                 overlapping tooltips! -->
            <foreignObject id="tooltiptext" width="500" height="550">
            <body xmlns="http://www.w3.org/1999/xhtml" >
            <div style="border:2px; color: white;  display:table; background-color: rgb(0, 0, 0, 0.75); font-family: 'DejaVu Sans', sans-serif;">
              <h3 id="date">2022-01-01</h3>
                <table id="tooltip_table">
                </table>
            </div>
            </body>
            </foreignObject>
        </g>

    </g>
    """
    linesvg = """
    <g id="date_line" xmlns="http://www.w3.org/2000/svg" pointer-events="none">
        <line x1="500" y1="0" x2="500" y2="2000" visibility="hidden" style="fill:none;stroke:#808080;stroke-dasharray:3.7,1.6;stroke-dashoffset:0;"/>
    </g>
    """
    xmlid["figure_1"].append(ET.XML(linesvg))
    xmlid["figure_1"].append(ET.XML(tooltipsvg))
    xmlid["figure_1"].set("fill", "black")  # some browsers don't seem to respect background

    # TODO: get json list with [[start, date, [color, label, val_avg, val],...],...]). start is ratio
    # TODO: on mousemove turn coords into ratio and lookup date etc
    # TODO: insert values and resize tooltip as needed (maybe sort based on value?)
    # TODO: hide the legend. make the tooltip look like the legend (fonts colours etc)
    # TODO: put colored dots on the lines at the right place
    # TODO: have a vertical line that moves
    # TODO: move tooltip out of the way. maybe flip down on lower half and up on upper half?
    # TODO: show % as well as avg and actual (or maybe only when there is a % chart visible?)

    # This is the script defining the ShowTooltip and HideTooltip functions.
    script = """
        <script type="text/ecmascript" xmlns="http://www.w3.org/2000/svg">
        <![CDATA[
        function display(value) {
            if(value == null) {
                return "--";
            }
            var sensible_number = "";
            if (Math.abs(value) < 10.0) {
                sensible_number = value.toString().replace(/[.]0+$/, '');
            }
            else if (Math.abs(value) < 100.0) {
                sensible_number = (Math.round(value*10)/10).toString().replace(/[.]0$/, '');
            }
            else {
                sensible_number = Math.round(value).toString();
            }
            sensible_number = sensible_number.replace(/[.]$/, '');
            return sensible_number;
        };

        function init(event) {
            var tooltip = d3.select("g.tooltip.mouse");
            var line = d3.select("g#date_line line");
            var plot = d3.select("#patch_2");
            var offset = plot.node().getBBox().x;
            var date_label = d3.select("#date");
            // var border = d3.select("#tooltiprect");
            var gap = 15;
            let padding = 4;
            let bbox = d3.select("#tooltiptext table").node().getBoundingClientRect();
            tooltip.attr("width", bbox.width);
            tooltip.attr("hieght", bbox.height);

            d3.select("#figure_1").on("mousemove", function (evt) {
                // from https://codepen.io/billdwhite/pen/rgEbc
                tooltip.attr('visibility', "visible")
                var plotpos = d3.pointer(evt, plot.node())[0] - offset;
                var index = Math.floor(plotpos / plot.node().getBBox().width * data.index.length);
                var date = data.index[index];
                if (date) {
                    date = date.split("T")[0];
                } else {
                    tooltip.attr('visibility', "hidden");
                    line.attr('visibility', "hidden");
                }
                date_label.node().textContent = date;
                values = [];
                for ( let number = 0; number < legends.length; number++ ) {
                    values.push([data.data[index][number], display(data.data[index][number]), legends[number]])
                }
                values.sort(function(a,b) {return a[0] - b[0]});
                values.reverse();

                table = "";
                for ( let number = 0; number < values.length; number++ ) {
                    table += "<html:tr><html:td>" + values[number][2] + "</html:td><html:td>" + values[number][1] + "</html:td></html:tr>";
                }
                d3.select("#tooltip_table").html(table);

                var mouseCoords = d3.pointer(evt, tooltip.node().parentElement);
                let width = bbox.width;
                var x = mouseCoords[0] - width + offset - gap;
                if (x < width) {
                    x = mouseCoords[0] + offset + gap;
                }
                tooltip
                    .attr("transform", "translate("
                        + (x) + ","
                        + (mouseCoords[1] - bbox.height/2) + ")");
                line.attr("x1", mouseCoords[0]);
                line.attr("x2", mouseCoords[0]);
                line.attr("y1", plot.node().getBBox().y);
                line.attr("y2", plot.node().getBBox().y + plot.node().getBBox().height);
                line.attr('visibility', "visible");

            })
            .on("mouseout", function () {
                line.attr('visibility', "hidden");
                return tooltip.attr('visibility', "hidden");
            });

        }
        """
    script += f"""
        var data = {df.round(2).to_json(orient="split", date_format="iso")};
        var colours = {json.dumps(colours)};
        var legends = {json.dumps(legends)};
        ]]>
        </script>
        """

    # Insert the script at the top of the file and save it.
    tree.insert(0, ET.XML(script))
    tree.insert(0, ET.XML('<script href="https://d3js.org/d3.v7.min.js" xmlns="http://www.w3.org/2000/svg"></script>'))

    ET.ElementTree(tree).write(path)
