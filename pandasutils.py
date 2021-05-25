from typing import List, Union
import pandas as pd
import difflib
import datetime
import numpy as np
import os
from matplotlib.colors import ListedColormap, LinearSegmentedColormap
from matplotlib.pyplot import cycler
import matplotlib.cm


def daterange(start_date, end_date, offset=0):
    "return a range of dates from start_date until before end_date. Offset extends range by offset days"
    for n in range(int((end_date - start_date).days) + offset):
        yield start_date + datetime.timedelta(n)



def spread_date_range(start, end, row, columns):
    "take some values and spread it over a period of dates in proportion to data already there"
    r = list(daterange(start, end, offset=1))
    stats = [float(p) / len(r) for p in row]
    results = pd.DataFrame(
        [
            [
                date,
            ]
            + stats
            for date in r
        ],
        columns=columns,
    ).set_index("Date")
    return results


def add_data(data, df):
    "Appends while dropping any duplicate rows"
    try:
        data = data.append(df, verify_integrity=True)
    except ValueError:
        print('detected duplicates; dropping only the duplicate rows')
        idx_names = data.index.names
        data = data.reset_index().append(df.reset_index()).drop_duplicates()
        data = data.set_index(idx_names)
    return data


def check_cum(df, results):
    if results.empty:
        return True
    next_day = results.loc[results.index[0]][[c for c in results.columns if " Cum" in c]]
    last = df.loc[df.index[-1]][[c for c in df.columns if " Cum" in c]]
    if (next_day.fillna(0) >= last.fillna(0)).all():
        return True
    else:
        raise Exception(str(next_day - last))


def cum2daily(results):
    cum = results[(c for c in results.columns if " Cum" in c)]
    all_days = pd.date_range(cum.index.min(), cum.index.max(), name="Date")
    cum = cum.reindex(all_days)  # put in missing days with NaN
    #cum = cum.interpolate(limit_area="inside") # missing dates need to be filled so we don't get jumps
    cum = cum - cum.shift(+1)  # we got cumilitive data
    renames = dict((c, c.rstrip(' Cum')) for c in list(cum.columns) if 'Cum' in c)
    cum = cum.rename(columns=renames)
    return cum


def human_format(num: float, pos: int) -> str:
    magnitude = 0
    while abs(num) >= 1000:
        magnitude += 1
        num /= 1000.0
    # add more suffixes if you need them
    suffix = ['', 'K', 'M', 'G', 'T', 'P'][magnitude]
    return f'{num:.1f}{suffix}'



def rearrange(l, *first):
    "reorder a list by moving first items to the front. Can be index or value"
    l = list(l)
    result = []
    for f in first:
        if type(f) != int:
            f = l.index(f)+1
        result.append(l[f-1])
        l[f-1] = None
    return result + [i for i in l if i is not None]


def fuzzy_join(a, b, on, assert_perfect_match=False, trim=lambda x: x, replace_on_with=None, return_unmatched=False):
    "does a pandas join but matching very similar entries"
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
        a["fuzzy_match"] = unmatched[on].map(lambda x: next(iter(difflib.get_close_matches(trim(x), b.index)), None),
                                             na_action="ignore")
        second = first.combine_first(a.join(b, on="fuzzy_match"))
        del second["fuzzy_match"]
        unmatched2 = second[second[test].isnull() & second[on].notna()]
        if assert_perfect_match:
            assert unmatched2.empty, f"Still some values left unmatched {list(unmatched2[on])}"

    unmatched_counts = pd.DataFrame()
    if return_unmatched and not unmatched.empty:
        to_keep = [test, replace_on_with] if replace_on_with is not None else [test]
        unmatched_counts = unmatched[[on]].join(second[to_keep]).value_counts().reset_index().rename(columns={0: "count"})

    if replace_on_with is not None:
        second[on] = second[replace_on_with]
        del second[replace_on_with]
    if old_index is not None:
        second = second.set_index(old_index)
    if return_unmatched:
        return second, unmatched_counts
    else:
        return second


# Combine and plot
def export(df, name, csv_only=False):
    print(f"Exporting: {name}")
    df = df.reset_index()
    for c in set(list(df.select_dtypes(include=['datetime64']).columns)):
        df[c] = df[c].dt.strftime('%Y-%m-%d')
    os.makedirs("api", exist_ok=True)
    # TODO: save space by dropping nan
    # json.dumps([row.dropna().to_dict() for index,row in df.iterrows()])
    if not csv_only:
        df.to_json(
            os.path.join("api", name),
            date_format="iso",
            indent=3,
            orient="records",
        )
    df.to_csv(
        os.path.join("api", f"{name}.csv"),
        index=False 
    )


def import_csv(name):
    path = os.path.join("api", f"{name}.csv")
    if not os.path.exists(path):
        return None
    old = pd.read_csv(path)
    old['Date'] = pd.to_datetime(old['Date'])
    return old


def topprov(df, metricfunc, valuefunc=None, name="Top 5 Provinces", num=5, other_name="Rest of Thailand"):
    "return df with columns of valuefunc for the top x provinces by metricfunc"
    # Top 5 dfcine rollouts
    # old_index = df.index.names
    valuefunc = metricfunc if valuefunc is None else valuefunc

    # Apply metric on each province by itself
    with_metric = df.reset_index().set_index("Date").groupby("Province").apply(metricfunc).rename(
        0).reset_index().set_index("Date")

    # = metricfunc(df)
    last_day = with_metric.loc[with_metric.last_valid_index()]
    top5 = last_day.nlargest(num, 0).reset_index()
    # sort data into top 5 + rest
    top5[name] = top5['Province']
    df = df.join(top5.set_index("Province")[name], on="Province").reset_index()
    if other_name:
        df[name] = df[name].fillna(other_name)
        # TODO: sum() might have to be configurable?
        df = df.groupby(["Date", name]).sum().reset_index()  # condense all the "other" fields
    # apply the value function to get all the values
    values = df.set_index(["Date", name]).groupby(name, group_keys=False).apply(valuefunc).rename(0).reset_index()
    # put the provinces into cols
    series = pd.crosstab(values['Date'], values[name], values[0], aggfunc="sum")

    cols = list(top5[name])  # in right order
    if other_name:
        return series[cols + [other_name]]
    else:
        return series[cols]

def trendline(data: pd.DataFrame, order: int = 1) -> float:
    # simulate dates with monotonic inc numbers
    dates = range(0, len(data.index.values))
    coeffs = np.polyfit(dates, list(data), order)
    slope = coeffs[-2]
    return float(slope)


# Plot helpers

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


def get_cycle(cmap, n=None, use_index="auto"):
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
        return cycler("color", cmap(ind))
    else:
        colors = cmap(np.linspace(0, 1, n))
        return cycler("color", colors)
