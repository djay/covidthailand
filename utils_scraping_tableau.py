import datetime
import itertools
import json
import time

import numpy as np
import pandas as pd
import requests
import tableauscraper

from utils_scraping import any_in
from utils_scraping import fix_timeouts
from utils_scraping import logger


###########################
# Tableau scraping
###########################


def workbook_explore(workbook):
    print()
    print("storypoints: {}", workbook.getStoryPoints())
    print("parameters {}", workbook.getParameters())
    for t in workbook.worksheets:
        print()
        print("worksheet name : {}", t.name)  # show worksheet name
        print(t.data)  # show dataframe for this worksheet
        print("filters: ")
        for f in t.getFilters():
            print("  {} : {} {}", f['column'], f['values'][:10], '...' if len(f['values']) > 10 else '')
        print("selectableItems: ")
        for f in t.getSelectableItems():
            print("  {} : {} {}", f['column'], f['values'][:10], '...' if len(f['values']) > 10 else '')


def workbook_flatten(wb, date=None, **mappings):
    """return a single DataFrame from a workbook flattened according to mappings
    mappings is worksheetname=columns
    if columns is type str puts a single value into column
    if columns is type dict will map worksheet columns to defined dataframe columns
    if those column names are in turn dicts then the worksheet will be pivoted and the values mapped to columns
    e.g.
    worksheet1="Address",
    worksheet2=dict(ws_phone="phone", ws_state="State"),
    worksheet3=dict(ws_state=dict(NSW="State: New South Wales", ...))
    """
    # TODO: generalise what to index by and default value for index
    res = pd.DataFrame()
    data = dict()
    if date is not None:
        data["Date"] = [date]
    for name, col in mappings.items():
        try:
            df = wb.getWorksheet(name).data
        except (KeyError, TypeError, AttributeError):
            # TODO: handle error getting wb properly earlier
            logger.info("Error getting tableau {}/{} {}", name, col, date)
            continue

        if type(col) != str:
            if df.empty:
                logger.info("Error getting tableau {}/{} {}", name, col, date)
                continue
            # if it's not a single value can pass in mapping of cols
            df = df[col.keys()].rename(columns={k: v for k, v in col.items() if type(v) == str})
            df['Date'] = pd.to_datetime(df['Date']).dt.normalize()
            # if one mapping is dict then do pivot
            pivot = [(k, v) for k, v in col.items() if type(v) != str]
            if pivot:
                pivot_cols, pivot_mapping = pivot[0]  # can only have one
                # Any other mapped cols are what are the values of the pivot
                df = df.pivot(index="Date", columns=pivot_cols)
                df = df.drop(columns=[c for c in df.columns if not any_in(c, *pivot_mapping.keys())])  # Only keep cols we want
                df = df.rename(columns=pivot_mapping)
                df.columns = df.columns.map(' '.join)
                df = df.reset_index()
            df = df.set_index("Date")
            # This seems to be 0 in these graphs. and if we don't then any bad previous values won't get corrected. TODO: param depeden
            df = df.replace("%null%", 0)
            # Important we turn all the other data to numberic. Otherwise object causes div by zero errors
            df = df.apply(pd.to_numeric, errors='coerce', axis=1)

            # Some series have gaps where its assumed missing values are 0. Like deaths
            # TODO: we don't know how far back to look? Currently 30days for tests and 60 for others?
            #start = date - datetime.timedelta(days=10) if date is not None else df.index.min()
            #start = min([start, df.index.min()])
            start = df.index.min()
            # Some data like tests can be a 2 days late
            # TODO: Should be able to do better than fixed offset?
            #end = date - datetime.timedelta(days=5) if date is not None else df.index.max()
            #end = max([end, df.index.max()])
            end = df.index.max()
            assert date is None or end <= date
            all_days = pd.date_range(start, end, name="Date", normalize=True, closed=None)
            try:
                df = df.reindex(all_days, fill_value=0.0)
            except ValueError:
                return pd.DataFrame()  # Sometimes there are duplicate dates. if so best abort the whole workbook since something is wrong

            res = res.combine_first(df)
        elif df.empty:
            # TODO: Seems to mean that this is 0? Should be confirgurable?
            data[col] = [0.0]
        elif col == "Date":
            data[col] = [pd.to_datetime(list(df.loc[0])[0], dayfirst=False)]
        else:
            data[col] = list(df.loc[0])
            if data[col] == ["%null%"]:
                data[col] = [np.nan]
    # combine all the single values with any subplots from the dashboard
    df = pd.DataFrame(data)
    if not df.empty:
        df['Date'] = df['Date'].dt.normalize()  # Latest has time in it which creates double entries
        res = df.set_index("Date").combine_first(res)
    return res


def workbook_iterate(url, **selects):
    "generates combinations of workbooks from combinations of parameters, selects or filters"

    def do_reset(attempt=0):
        if attempt == 3:
            return None
        ts = tableauscraper.TableauScraper()
        try:
            ts.loads(url)
        except Exception as err:
            # ts library fails in all sorts of weird ways depending on the data sent back
            logger.info("MOPH Dashboard Error: Exception TS loads url {}: {}", url, str(err))
            return do_reset(attempt=attempt + 1)
        fix_timeouts(ts.session, timeout=30)
        wb = ts.getWorkbook()
        return wb
    wb = do_reset()
    if wb is None:
        return
    set_value = []
    # match the params to iterate to param, filter or select
    for name, values in selects.items():
        param = next((p for p in wb.getParameters() if p['column'] == name), None)
        if param is not None:
            if type(values) == str:
                selects[name] = param['values']
            else:
                # assume its a list of values to use
                pass

            def do_param(wb, value, name=name):
                value = value if type(value) != datetime.datetime else str(value.date())
                return force_setParameter(wb, name, value)

            set_value.append(do_param)
            continue
        ws = next(ws for ws in wb.worksheets if ws.name == name)
        # TODO: allow a select to be manual list of values
        svalues = ws.getSelectableValues(values)
        if svalues:
            selects[name] = svalues

            # weird bug where sometimes .getWorksheet doesn't work or missign data
            def do_select(wb, value, name=name, values=values):
                ws = next(ws for ws in wb.worksheets if ws.name == name)
                return ws.select(values, value)
            set_value.append(do_select)
        else:
            items = ws.getFilters()
            # TODO: allow filter to manual list of values
            selects[name] = next((item['values'] for item in items if item['column'] == values), [])
            # TODO: should raise an error if there is no matching filter?

            # weird bug where sometimes .getWorksheet doesn't work or missign data
            def do_filter(wb, value, ws_name=name, filter_name=values):
                ws = next(ws for ws in wb.worksheets if ws.name == ws_name)
                # return ws.setFilter(values, value)
                return force_setFilter(wb, ws_name, filter_name, [value])
            set_value.append(do_filter)

    last_idx = [None] * len(selects)
    # Get all combinations of the values of params, select or filter
    for next_idx in itertools.product(*selects.values()):
        def get_workbook(wb=wb, next_idx=next_idx):
            nonlocal last_idx
            reset = False
            for _ in range(3):
                if reset:
                    wb = do_reset()
                    if wb is None:
                        continue
                    reset = False
                for do_set, last_value, value in zip(set_value, last_idx, next_idx):
                    if last_value != value:
                        try:
                            wb = do_set(wb, value)
                        except Exception as err:
                            logger.info("{} MOPH Dashboard Retry: {}={} Error: {}", next_idx, do_set.__name__, value, err)
                            reset = True
                            break
                    if not wb.worksheets:
                        logger.info("{} MOPH Dashboard Retry: Missing worksheets in {}={}.", next_idx, do_set.__name__, value)
                        reset = True
                        break
                if reset:
                    last_idx = (None,) * len(last_idx)  # need to reset filters etc
                    continue
                last_idx = next_idx
                return wb
                # Try again
            logger.info("MOPH Dashboard Skip: {}. Retries exceeded", next_idx)
            return None
        yield get_workbook, next_idx


def force_setParameter(wb, parameterName, value):
    scraper = wb._scraper
    tableauscraper.api.delayExecution(scraper)
    payload = (
        ("fieldCaption", (None, parameterName)),
        ("valueString", (None, value)),
    )
    r = scraper.session.post(
        f'{scraper.host}{scraper.tableauData["vizql_root"]}/sessions/{scraper.tableauData["sessionid"]}/commands/tabdoc/set-parameter-value',
        files=payload,
        verify=scraper.verify
    )
    scraper.lastActionTime = time.time()
    if r.status_code >= 400:
        raise requests.exceptions.RequestException(r.content)
    resp = r.json()

    wb.updateFullData(resp)
    return tableauscraper.dashboard.getWorksheetsCmdResponse(scraper, resp)


# :path: /vizql/w/SATCOVIDDashboard/v/2-dash-tiles-province-w/sessions/B42533EE979D4E389C1F8119C87E70C8-0:0/commands/tabdoc/dashboard-categorical-filter
# referer: https://public.tableau.com/views/SATCOVIDDashboard/2-dash-tiles-province-w?:size=1200,1050&:embed=y&:showVizHome=n&:bootstrapWhenNotified=y&:tabs=n&:toolbar=n&:apiID=host0
# dashboard: 2-dash-tiles-province-w
# qualifiedFieldCaption: province
# exclude: false
# filterUpdateType: filter-replace
# filterValues: ["กรุงเทพมหานคร"]


# visualIdPresModel: {"worksheet":"D4_CHART","dashboard":"4-dash-trend-w"}
# globalFieldName: [sqlproxy.0ti7s471dkws67105310p0g3vagu].[none:age_range:nk]
# membershipTarget: filter
# filterUpdateType: filter-delta
# filterAddIndices: []
# filterRemoveIndices: [2]
def force_setFilter(wb, ws_name, columnName, values):
    "setFilter but ignore the listed filter options. also gets around wrong ordinal value which makes index value incorrect"

    scraper = wb._scraper
    tableauscraper.api.delayExecution(scraper)
    ws = next(ws for ws in wb.worksheets if ws.name == ws_name)

    filter = next(
        {
            "globalFieldName": t["globalFieldName"],
        }
        for t in ws.getFilters()
        if t["column"] == columnName
    )

    payload = (
        ("dashboard", scraper.dashboard),
        ("globalFieldName", (None, filter["globalFieldName"])),
        ("qualifiedFieldCaption", (None, columnName)),
        ("membershipTarget", (None, "filter")),
        ("exclude", (None, "false")),
        ("filterValues", (None, json.dumps(values))),
        ("filterUpdateType", (None, "filter-replace"))
    )
    try:
        r = scraper.session.post(
            f'{scraper.host}{scraper.tableauData["vizql_root"]}/sessions/{scraper.tableauData["sessionid"]}/commands/tabdoc/dashboard-categorical-filter',
            files=payload,
            verify=scraper.verify
        )
        scraper.lastActionTime = time.time()

        if r.status_code >= 400:
            raise requests.exceptions.RequestException(r.content)
        resp = r.json()
        errors = [
            res['commandReturn']['commandValidationPresModel']['errorMessage']
            for res in resp['vqlCmdResponse']['cmdResultList']
            if not res['commandReturn'].get('commandValidationPresModel', {}).get('valid', True)
        ]
        if errors:
            wb._scraper.logger.error(str(", ".join(errors)))
            raise tableauscraper.api.APIResponseException(", ".join(errors))

        wb.updateFullData(resp)
        return tableauscraper.dashboard.getWorksheetsCmdResponse(scraper, resp)
    except ValueError as e:
        scraper.logger.error(str(e))
        return tableauscraper.TableauWorkbook(
            scraper=scraper, originalData={}, originalInfo={}, data=[]
        )
    except tableauscraper.api.APIResponseException as e:
        wb._scraper.logger.error(str(e))
        return tableauscraper.TableauWorkbook(
            scraper=scraper, originalData={}, originalInfo={}, data=[]
        )
