import datetime
import difflib
import os
from typing import List, Union

import matplotlib.cm
from matplotlib.colors import ListedColormap, LinearSegmentedColormap
from matplotlib.pyplot import cycler
import matplotlib.dates as mdates
from cycler import Cycler
import pandas as pd
import numpy as np
from matplotlib import colors as mcolors
import mpld3
from dateutil.relativedelta import relativedelta
import functools

from utils_scraping import logger


def daterange(start_date, end_date, offset=0):
    "return a range of dates from start_date until before end_date. Offset extends range by offset days"
    for n in range(int((end_date - start_date).days) + offset):
        yield start_date + datetime.timedelta(n)


def spread_date_range(start, end, row, columns):
    "take some values and spread it over a period of dates in proportion to data already there"
    r = list(daterange(start, end, offset=1))
    stats = [float(p) / len(r) for p in row]
    results = pd.DataFrame([[date] + stats for date in r], columns=columns).set_index("Date")
    return results


def add_data(data, df):
    "Appends while dropping any duplicate rows"
    try:
        data = data.append(df, verify_integrity=True)
    except ValueError:
        logger.info('detected duplicates; dropping only the duplicate rows')
        idx_names = data.index.names
        if [None] != idx_names:
            data = data.reset_index()
        data = data.append(df.reset_index()).drop_duplicates()
        if [None] != idx_names:
            data = data.set_index(idx_names)
    return data


def check_cum(df, results, cols):
    if results.empty:
        return True
    next_day = results.loc[results.index[0]][[c for c in results.columns if " Cum" in c]]
    last = df.loc[df.index[-1]][[c for c in df.columns if " Cum" in c]]
    if (next_day.fillna(0)[cols] >= last.fillna(0)[cols]).all():
        return True
    else:
        raise Exception(str(next_day - last))


def cum2daily(results):
    cum = results[(c for c in results.columns if " Cum" in c)]
    all_days = pd.date_range(cum.index.min(), cum.index.max(), name="Date")
    cum = cum.reindex(all_days)  # put in missing days with NaN
    # cum = cum.interpolate(limit_area="inside") # missing dates need to be filled so we don't get jumps
    cum = cum - cum.shift(+1)  # we got cumilitive data
    renames = dict((c, c.rstrip(' Cum')) for c in list(cum.columns) if 'Cum' in c)
    cum = cum.rename(columns=renames)
    return cum


def daily2cum(results):
    cols = [c for c in results.columns if " Cum" not in c]
    daily = results[cols]
    names = daily.index.names
    # bit of a hack.pick first value to fill in the gaps later
    extra_index = [(n, daily.first_valid_index()[names.index('Province')]) for n in names if n != 'Date']

    daily = daily.reset_index().set_index("Date")
    all_days = pd.date_range(daily.index.min(), daily.index.max(), name="Date")
    daily = daily.reindex(all_days)
    # cum = cum.interpolate(limit_area="inside") # missing dates need to be filled so we don't get jumps
    cum = daily[cols].fillna(0).cumsum()  # we got cumilitive data
    renames = dict((c, c + ' Cum') for c in list(cum.columns))
    cum = cum.rename(columns=renames)
    # Add back in the extra index.
    cum = cum.assign(**dict([(n, daily[n].fillna(value)) for n, value in extra_index]))
    # what about gaps in province names?

    cum = cum.reset_index().set_index(names)
    return cum[cum.columns]

def fix_gaps(df):
    # Some gaps in the data so fill them in. df.groupby("Province").apply(fix_gaps)
    df = df.reset_index("Province")
    all_days = pd.date_range(df.index.min(), df.index.max(), name="Date", normalize=True, closed=None)
    df = df.reindex(all_days, fill_value=np.nan)
    df = df.interpolate()
    df['Province'] = df['Province'].iloc[0]  # Ensure they all have same province
    return df.reset_index().set_index(["Date", "Province"])


def normalise_to_total(df, cols, total_col):
    "adjust cols so they add up to total"
    col_total = df[cols].sum(axis=1)
    for c in cols:
        df[c] = df[c] / col_total * df[total_col]
    return df


def sensible_precision(num: float) -> str:
    """Convert a number to a string with sensible precission (3 digits maximum)."""
    sensible_number = ''
    if not np.isnan(num):
        if abs(num) < 10.0:
            num = round(num, 2)
            sensible_number = f'{num:.2f}'.rstrip('0').rstrip('.')
        elif abs(num) < 100.0:
            num = round(num, 1)
            sensible_number = f'{num:.1f}'.rstrip('0').rstrip('.')
        else:
            num = round(num)
            sensible_number = f'{num:.0f}'
    return sensible_number

def human_format(num: float, pos: int) -> str:
    """Convert a number to a more human readable string."""
    magnitude = 0
    while abs(num) >= 1000:
        magnitude += 1
        num /= 1000.0
    sensible_number = sensible_precision(num)
    suffix = ['', 'k', 'M', 'G', 'T', 'P'][magnitude]
    return f'{sensible_number}{suffix}'


def perc_format(num: float, pos: int) -> str:
    """Convert a number to a more human readablepercent string."""
    sensible_number = sensible_precision(num)
    return f'{sensible_number}%'


def rearrange(lst, *first):
    "reorder a list by moving first items to the front. Can be index or value"
    lst = list(lst)
    result = []
    for f in first:
        if type(f) != int:
            f = lst.index(f) + 1
        result.append(lst[f - 1])
        lst[f - 1] = None
    return result + [i for i in lst if i is not None]


def cut_ages_labels(ages=[10, 20, 30, 40, 50, 60, 70], prefix=None):
    bins = [0] + ages + [140]
    prefix = prefix + " " if prefix else ""
    labels = [f"{prefix}{p}-{n-1}" if n else f"{prefix}{p}+" for p, n in zip(bins[:-1], bins[1:-1] + [None])]
    return labels


def cut_ages(df, ages=[10, 20, 30, 40, 50, 60, 70], age_col="Age", group_col="Age Group"):
    bins = [0] + ages + [140]
    labels = cut_ages_labels(ages)
    df[group_col] = pd.cut(df[age_col], bins=bins, labels=labels, right=False)
    return df


def fuzzy_join(a,
               b,
               on,
               assert_perfect_match=False,
               trim=None,
               replace_on_with=None,
               return_unmatched=False,
               cutoff=0.74):
    "does a pandas join but matching very similar entries"
    trim = trim if trim is not None else lambda x: x
    old_index = None
    if on not in a.columns:
        old_index = a.index.names
        a = a.reset_index()
    first = a.join(b, on=on)
    test = list(b.columns)[0]
    unmatched = first[first[test].isnull() & first[on].notna()]
    if unmatched.empty:
        second = first
    else:
        a["fuzzy_match"] = unmatched[on].map(
            lambda x: next(iter(difflib.get_close_matches(trim(x), b.index, 1, cutoff=cutoff)), None),
            na_action="ignore")
        second = first.combine_first(a.join(b, on="fuzzy_match"))
        del second["fuzzy_match"]
        unmatched2 = second[second[test].isnull() & second[on].notna()]
        if assert_perfect_match:
            assert unmatched2.empty, f"Still some values left unmatched {list(unmatched2[on])}"
    unmatched_counts = pd.DataFrame()
    if return_unmatched and not unmatched.empty:
        to_keep = [test, replace_on_with] if replace_on_with is not None else [test]
        counts = unmatched.reset_index()[on].value_counts().to_frame('count')
        guessed = second[[on] + to_keep].set_index(on)
        unmatched_counts = counts.join(guessed).reset_index().rename(columns=dict(index=on))

    if replace_on_with is not None:
        second[on] = second[replace_on_with]
        del second[replace_on_with]
    if old_index is not None:
        second = second.set_index(old_index)
    if return_unmatched:
        return second, unmatched_counts
    else:
        return second


def export(df, name, csv_only=False, dir="api"):
    logger.info("Exporting: {}", name)
    df = df.reset_index()
    for c in set(list(df.select_dtypes(include=['datetime64']).columns)):
        df[c] = df[c].dt.strftime('%Y-%m-%d')
    os.makedirs(dir, exist_ok=True)
    # TODO: save space by dropping nan
    # json.dumps([row.dropna().to_dict() for index,row in df.iterrows()])
    if not csv_only:
        df.to_json(
            os.path.join(dir, name),
            date_format="iso",
            indent=3,
            orient="records",
        )
    df.to_csv(
        os.path.join(dir, f"{name}.csv"),
        index=False
    )


def import_csv(name, index=None, return_empty=False, date_cols=['Date'], dir="api"):
    path = os.path.join(dir, f"{name}.csv")
    if not os.path.exists(path) or return_empty:
        return pd.DataFrame(columns=index).set_index(index)
    logger.info("Importing CSV: {}", path)
    old = pd.read_csv(path)
    for c in date_cols:
        old[c] = pd.to_datetime(old[c])
    if index:
        return old.set_index(index)
    else:
        return old


def increasing(col, ma=7):
    def increasing_func(adf: pd.DataFrame) -> pd.DataFrame:
        if callable(col):
            series = col(adf)
        else:
            series = adf[col]
        return series.rolling(ma, min_periods=1).mean().rolling(ma, min_periods=ma).apply(trendline)
    return increasing_func


def decreasing(col, ma=7):
    inc_func = increasing(col, ma)

    def decreasing_func(adf: pd.DataFrame) -> pd.DataFrame:
        return 1 / inc_func(adf)
    return decreasing_func


def value_ma(col, ma=3):
    if ma:
        def cases_ma(adf: pd.DataFrame) -> pd.DataFrame:
            return adf[col].rolling(ma, min_periods=1).mean()
    else:
        def cases_ma(adf: pd.DataFrame) -> pd.DataFrame:
            return adf[col]
    return cases_ma


def trendline(data: pd.DataFrame) -> float:
    slope = (list(data)[-1] - list(data)[0]) / len(data.index.values)
    return float(slope)


def trendline_slow(data: pd.DataFrame, order: int = 1) -> float:
    # simulate dates with monotonic inc numbers
    dates = range(0, len(data.index.values))
    coeffs = np.polyfit(dates, list(data), order)
    return float(coeffs[-2])


def topprov(df, metricfunc, valuefunc=None, name="Top 5 Provinces", num=5, other_name="Rest of Thailand", return_all=False):
    "return df with columns of valuefunc for the top x provinces by metricfunc"
    # Top 5 dfcine rollouts
    # old_index = df.index.names
    valuefunc = metricfunc if valuefunc is None else valuefunc

    # Apply metric on each province by itself
    with_metric = df.groupby(level="Province", group_keys=False).apply(metricfunc)
    with_metric = with_metric.reset_index().set_index("Date")
    metric_col = [c for c in with_metric.columns if c != 'Province']

    # = metricfunc(df)
    last_day = with_metric.loc[with_metric.dropna().last_valid_index()]
    top5 = last_day.nlargest(num, metric_col).reset_index()

    # top5 = df.groupby(level="Province", group_keys=False).agg({metric_col:metricfunc}).nlargest(num, metric_col)

    # sort data into top 5 + rest
    top5[name] = top5['Province']
    df = df.join(top5.set_index("Province")[name], on="Province").reset_index()
    if other_name:
        df[name] = df[name].fillna(other_name)
        # TODO: sum() might have to be configurable?
        # TODO: we only really need to do this for one value not all the individual values
        df = df.groupby(["Date", name]).sum(min_count=1).reset_index()  # condense all the "other" fields
    # apply the value function to get all the values
    values = df.set_index(["Date", name]).groupby(level=name, group_keys=False).apply(valuefunc).rename(0).reset_index()
    # put the provinces into cols. use max to ensure NA aren't included. Should only be one value anyway?
    # TODO: is aggfunc=lambda df: df.sum(skipna=False) better?
    series = pd.crosstab(index=values['Date'], columns=values[name], values=values[0], aggfunc="max")

    cols = list(top5[name])  # in right order
    if other_name:
        return series[cols + [other_name]]
    else:
        return series[cols]


def pred_vac(dose1, dose2=None, ahead=90, lag=60, suffix=" Pred"):
    "Pred dose 1 using linear progression using 14 day rate and dose {lag} using 2month from dose1"
    cur = dose1.last_valid_index()
    rate = (dose1.loc[cur] - dose1.loc[cur - relativedelta(days=14)]) / 14
    future_dates = pd.date_range(cur, cur + relativedelta(days=ahead), name="Date")
    # increasing sequence 
    future1 = pd.DataFrame(dict([(col, pd.RangeIndex(1, ahead + 2)) for col in dose1.columns]), index=future_dates) * rate
    future1 = future1 + dose1.loc[dose1.last_valid_index()]
    future1.columns = [col + suffix for col in future1.columns]

    if dose2 is None:
        return future1

    # 2nd dose is 1st dose from 2 months previous
    # TODO: factor in 2 months vs 3 months AZ?
    from_past = dose1[cur - relativedelta(days=lag): cur]
    from_past.columns = [col + suffix for col in dose2.columns]
    from_future = future1.iloc[1:ahead - lag + 1]
    from_future.columns = from_past.columns
    v2 = pd.concat([from_past, from_future], axis=0)
    # adjust to start where dose2 finished
    future2 = (v2 - v2.loc[v2.index.min()]).add(list(dose2.loc[cur]))
    future2.index = future_dates
    return (future1, future2)


#################
# Plot helpers
#################
def custom_cm(cm_name: str, size: int, last_colour: str = None, flip: bool = False) -> ListedColormap:
    """Returns a ListedColorMap object built with the supplied color scheme and with the last color forced to be equal
    to the parameter passed. The flip parameter allows to reverse the colour scheme if needed.
    """
    summer = matplotlib.cm.get_cmap(cm_name)
    if flip:
        newcolors = summer(np.linspace(1, 0, size))
    else:
        newcolors = summer(np.linspace(0, 1, size))

    if last_colour:
        newcolors[size - 1, :] = matplotlib.colors.to_rgba(last_colour)  # used for unknowns (ex: 'lightgrey')

    return ListedColormap(newcolors)


def clip_dataframe(df_all: pd.DataFrame, cols: Union[str, List[str]], n_rows: int) -> pd.DataFrame:
    """Removes the last n rows in the event that they contain any NaN

    :param df_all: the pandas DataFrame containing all data
    :param cols: specify columns from which to assess presence of NaN in the last n rows
    :param n_rows: the number of rows (counting from the last row, going backwards) to evaluate whether they contain
                   any NaN and if so then delete them. This deals with (possible) data missing for the most recent data
                   updates.
    """
    # detect the number of NaN in the last n rows of the DataFrame subset (i.e. only using the columns specified)
    sum_nans = df_all[cols][-n_rows:].isna().sum(axis=1)
    index_nans = sum_nans[sum_nans > 0].index

    # remove these indices from the pandas DataFrame
    cleaned_df = df_all.drop(index=index_nans)

    return cleaned_df


def get_cycle(cmap, n=None, use_index="auto", extras=[], unpair=False, start=0):
    if isinstance(cmap, Cycler):
        return cmap
    if isinstance(cmap, str):
        if use_index == "auto":
            if cmap in ['Pastel1', 'Pastel2', 'Paired', 'Accent',
                        'Dark2', 'Set1', 'Set2', 'Set3',
                        'tab10', 'tab20', 'tab20b', 'tab20c']:
                use_index = True
            else:
                use_index = False
        cmap = matplotlib.cm.get_cmap(cmap)
    if not n:
        n = cmap.N
    if use_index == "auto":
        if cmap.N > 100:
            use_index = False
        elif isinstance(cmap, LinearSegmentedColormap):
            use_index = False
        elif isinstance(cmap, ListedColormap):
            use_index = True
    if use_index:
        ind = np.arange(int(n)) % cmap.N
        colors = cmap(ind)
    else:
        colors = cmap(np.linspace(0, 1, n))
    if unpair:
        colors1 = colors[::2]
        colors2 = colors[1::2]
        colors = np.concatenate([colors1, colors2])
    extras = [mcolors.to_rgba(mcolors.CSS4_COLORS[c]) for c in extras]
    if extras:
        colors = np.concatenate([colors, extras])
    colors = colors[start:]
    return cycler("color", colors)


def line_format(label):
    """
    Convert time label to the format of pandas line plot
    """
    month = label.month_name()[:3]
    if month == 'Jan':
        month += f'\n{label.year}'
    return month


def set_time_series_labels(df, ax):
    # https://stackoverflow.com/questions/30133280/pandas-bar-plot-changes-date-format
    # Create list of monthly timestamps by selecting the first weekly timestamp of each
    # month (in this example, the first Sunday of each month)
    monthly_timestamps = [
        timestamp for idx, timestamp in enumerate(df.index) if (timestamp.month != df.index[idx - 1].month) | (idx == 0)
    ]

    # Automatically select appropriate number of timestamps so that x-axis does
    # not get overcrowded with tick labels
    step = 1
    while len(monthly_timestamps[::step]) > 10:  # increase number if time range >3 years
        step += 1
    timestamps = monthly_timestamps[::step]

    # Create tick labels from timestamps
    labels = [
        ts.strftime('%b\n%Y') if ts.year != timestamps[idx - 1].year else ts.strftime('%b')
        for idx, ts in enumerate(timestamps)
    ]

    # Set major ticks and labels
    ax.set_xticks([df.index.get_loc(ts) for ts in timestamps])
    ax.set_xticklabels(labels)

    # Set minor ticks without labels
    ax.set_xticks([df.index.get_loc(ts) for ts in monthly_timestamps], minor=True)

    # Rotate and center labels
    ax.figure.autofmt_xdate(rotation=0, ha='center')


def set_time_series_labels_2(df, ax):

    # Compute width of bars in matplotlib date units, 'md' (in days) and adjust it if
    # the bar width in df.plot.bar has been set to something else than the default 0.5
    bar_width_md_default, = np.diff(mdates.date2num(df.index[:2])) / 2
    bar_width = ax.patches[0].get_width()
    bar_width_md = bar_width * bar_width_md_default / 0.5

    # Compute new x values in matplotlib date units for the patches (rectangles) that
    # make up the stacked bars, adjusting the positions according to the bar width:
    # if the frequency is in months (or years), the bars may not always be perfectly
    # centered over the tick marks depending on the number of days difference between
    # the months (or years) given by df.index[0] and [1] used to compute the bar
    # width, this should not be noticeable if the bars are wide enough.
    x_bars_md = mdates.date2num(df.index) - bar_width_md / 2
    nvar = len(ax.get_legend_handles_labels()[1])
    x_patches_md = np.ravel(nvar * [x_bars_md])

    # Set bars to new x positions and adjust width: this loop works fine with NaN
    # values as well because in bar plot NaNs are drawn with a rectangle of 0 height
    # located at the foot of the bar, you can verify this with patch.get_bbox()
    for patch, x_md in zip(ax.patches, x_patches_md):
        patch.set_x(x_md)
        patch.set_width(bar_width_md)

    # Set major ticks
    maj_loc = mdates.AutoDateLocator()
    ax.xaxis.set_major_locator(maj_loc)

    # Show minor tick under each bar (instead of each month) to highlight
    # discrepancy between major tick locator and bar positions seeing as no tick
    # locator is available for first-week-of-the-month frequency
    ax.set_xticks(x_bars_md + bar_width_md / 2, minor=True)

    # Set major tick formatter
    zfmts = ['', '%b\n%Y', '%b', '%d\n%b', '%H:%M', '%H:%M']
    fmt = mdates.ConciseDateFormatter(maj_loc, zero_formats=zfmts, show_offset=False)
    ax.xaxis.set_major_formatter(fmt)

    # Shift the plot frame to where the bars are now located
    xmin = min(x_bars_md) - bar_width_md
    xmax = max(x_bars_md) + 2 * bar_width_md
    ax.set_xlim(xmin, xmax)

    # Adjust tick label format last, else it may sometimes not be applied correctly
    ax.figure.autofmt_xdate(rotation=0, ha='center')

class HighlightLines(mpld3.plugins.PluginBase):
    """A plugin to highlight lines on hover"""

    JAVASCRIPT = """
    mpld3.register_plugin("linehighlight", LineHighlightPlugin);
    LineHighlightPlugin.prototype = Object.create(mpld3.Plugin.prototype);
    LineHighlightPlugin.prototype.constructor = LineHighlightPlugin;
    LineHighlightPlugin.prototype.requiredProps = ["line_ids"];
    LineHighlightPlugin.prototype.defaultProps = {alpha_bg:0.3, alpha_fg:1.0}
    function LineHighlightPlugin(fig, props){
        mpld3.Plugin.call(this, fig, props);
    };

    LineHighlightPlugin.prototype.draw = function(){
      for(var i=0; i<this.props.line_ids.length; i++){
         var obj = mpld3.get_element(this.props.line_ids[i], this.fig),
             alpha_fg = this.props.alpha_fg;
             alpha_bg = this.props.alpha_bg;
         obj.elements()
             .on("mouseover", function(d, i){
                            d3.select(this).transition().duration(50)
                              .style("stroke-opacity", alpha_fg); })
             .on("mouseout", function(d, i){
                            d3.select(this).transition().duration(200)
                              .style("stroke-opacity", alpha_bg); });
      }
    };
    """

    def __init__(self, lines):
        self.lines = lines
        self.dict_ = {"type": "linehighlight",
                      "line_ids": [mpld3.utils.get_id(line) for line in lines],
                      "alpha_bg": lines[0].get_alpha(),
                      "alpha_fg": 1.0}

# write value at nearest x 
# - https://stackoverflow.com/questions/34886070/multiseries-line-chart-with-mouseover-tooltip/34887578#34887578
# - https://stackoverflow.com/questions/21417298/d3js-chart-with-crosshair-as-tooltip-how-to-add-2-lines-which-intersect-at-curs
# - https://stackoverflow.com/questions/32783433/d3-multiples-with-linked-focus-mouseover-tooltip-crosshair-focus-line-not-fitti
# - http://jsfiddle.net/Nivaldo/79fxL/
# - https://jsfiddle.net/gerardofurtado/ayta89cz/5/
class MousePositionDatePlugin(mpld3.plugins.PluginBase):
    """Plugin for displaying mouse position with a datetime x axis."""

    JAVASCRIPT = """
    mpld3.register_plugin("mousepositiondate", MousePositionDatePlugin);
    MousePositionDatePlugin.prototype = Object.create(mpld3.Plugin.prototype);
    MousePositionDatePlugin.prototype.constructor = MousePositionDatePlugin;
    MousePositionDatePlugin.prototype.requiredProps = [];
    MousePositionDatePlugin.prototype.defaultProps = {
    fontsize: 12,
    xfmt: "%Y-%m-%d %H:%M:%S",
    yfmt: ".3g"
    };
    function MousePositionDatePlugin(fig, props) {
    mpld3.Plugin.call(this, fig, props);
    }
    MousePositionDatePlugin.prototype.draw = function() {
    var fig = this.fig;
    var xfmt = d3.time.format(this.props.xfmt);
    var yfmt = d3.format(this.props.yfmt);
    var coords = fig.canvas.append("text").attr("class", "mpld3-coordinates").style("text-anchor", "end").style("font-size", this.props.fontsize).attr("x", this.fig.width - 5).attr("y", this.fig.height - 5);
    for (var i = 0; i < this.fig.axes.length; i++) {
      var update_coords = function() {
        var ax = fig.axes[i];
        return function() {
          var pos = d3.mouse(this);
          x = ax.xdom.invert(pos[0]);
          y = ax.ydom.invert(pos[1]);
          coords.text("(" + xfmt(x) + ", " + yfmt(y) + ")");
        };
      }();
      fig.axes[i].baseaxes.on("mousemove", update_coords).on("mouseout", function() {
        coords.text("");
      });
    }
    };
    """
    def __init__(self, fontsize=14, xfmt="%Y-%m-%d %H:%M:%S", yfmt=".3g"):
        self.dict_ = {"type": "mousepositiondate",
                      "fontsize": fontsize,
                      "xfmt": xfmt,
                      "yfmt": yfmt}
