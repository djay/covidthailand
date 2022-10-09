import datetime
import itertools
import json
import time
from json.decoder import JSONDecodeError

import dateutil.parser
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


def workbook_value(wb, date, name, col, default=0.0, is_date=False):
    # TODO: generalise what to index by and default value for index
    res = pd.DataFrame()
    data = dict()
    if date is not None:
        data["Date"] = [date]
    # closest = {s.name.replace(" (2)", ""): s.name for s in wb.worksheets}.get(name)  # HACK handle renames
    try:
        df = wb.getWorksheet(name).data
    except (KeyError, TypeError, AttributeError):
        # TODO: handle error getting wb properly earlier
        logger.info("Error getting tableau {}/{} {}", name, col, date)
        return pd.DataFrame()

    if df.empty:
        if default is not None:
            data[col] = [default]
    elif is_date:
        data[col] = [pd.to_datetime(list(df.loc[0])[0], dayfirst=False)]
    else:
        try:
            data[col] = list(pd.to_numeric(df.loc[0]))  # HACK: shouldn't assume we want numbers
        except ValueError:
            data[col] = list(pd.to_numeric(df.loc[0].str.replace(",", "").replace("%null%", "")))

    # combine all the single values with any subplots from the dashboard
    df = pd.DataFrame(data)
    if date is None:
        return df.iloc[0, 0]
    if not df.empty and date is not None:
        df['Date'] = df['Date'].dt.normalize()  # Latest has time in it which creates double entries
        res = df.set_index("Date")
    return res


def workbook_series(wb, name, mappings, defaults={"": 0.0}, index_col="Date", end=None, index_date=True, index_value=None):
    name = name if type(name) == str else next((n for n in name if n in [s.name for s in wb.worksheets]), None)
    if name is None:
        return pd.DataFrame()
    try:
        df = wb.getWorksheet(name).data
    except (KeyError, TypeError, AttributeError):
        # TODO: handle error getting wb properly earlier
        logger.info("Error getting tableau {}/{} {}", name, mappings)
        return pd.DataFrame()

    if df.empty:
        # logger.info("Error getting tableau {}/{} {}", name, col, date)
        return pd.DataFrame()
    # if it's not a single value can pass in mapping of cols
    renames = {k: v for k, v in mappings.items() if type(v) == str and k in df.columns}
    cols = [key for key in mappings.keys() if key in df.columns]
    df = df[cols].rename(columns=renames)
    if index_date:
        try:
            df[index_col] = pd.to_datetime(df[index_col], dayfirst=False).dt.normalize()
        except pd.errors.OutOfBoundsDatetime:
            # Could be a Thai year. Hack to convert
            df[index_col] = df[index_col].str.replace("2564", "2021").str.replace("2565", "2022")
            df[index_col] = pd.to_datetime(df[index_col], dayfirst=False).dt.normalize()
    # if one mapping is dict then do pivot
    pivot = [(k, v) for k, v in mappings.items() if type(v) != str]
    if pivot:
        pivot_cols, pivot_mapping = pivot[0]  # can only have one
        # Any other mapped cols are what are the values of the pivot
        if index_col not in df.columns:
            df[index_col] = index_value
        df = df.pivot(index=index_col, columns=pivot_cols)
        df = df.drop(columns=[c for c in df.columns if not any_in(c, *pivot_mapping.keys())])  # Only keep cols we want
        df = df.rename(columns=pivot_mapping)
        df.columns = df.columns.map(' '.join)
        df = df.reset_index()
    df = df.set_index(index_col)
    # This seems to be 0 in these graphs. and if we don't then any bad previous values won't get corrected. TODO: param depeden
    if type(defaults) != dict:
        default = [defaults] * len(df.columns)
    else:
        default = [defaults.get(c, defaults.get("")) if defaults else 0.0 for c in df.columns]
    df = df.replace("%null%", default[0])
    # Important we turn all the other data to numberic. Otherwise object causes div by zero errors
    df = df.apply(lambda x: x.str.replace(',', '').astype(float) if x.dtype in [str, object] else x, axis=1)

    if index_date:
        # Some series have gaps where its assumed missing values are 0. Like deaths
        # TODO: we don't know how far back to look? Currently 30days for tests and 60 for others?
        #start = date - datetime.timedelta(days=10) if date is not None else df.index.min()
        #start = min([start, df.index.min()])
        start = df.index.min()
        # Some data like tests can be a 2 days late
        # TODO: Should be able to do better than fixed offset?
        #end = date - datetime.timedelta(days=5) if date is not None else df.index.max()
        #end = max([end, df.index.max()])
        end = df.index.max() if end is None else end
        # assert date is None or end <= date, f"getting {date} found {end}"
        all_days = pd.date_range(start, end, name="Date", normalize=True, inclusive="both")
        try:
            df = df.reindex(all_days, fill_value=default[0])  # TODO: work out how to have default for each column
        except ValueError:
            return pd.DataFrame()  # Sometimes there are duplicate dates. if so best abort the whole workbook since something is wrong

    return df


def workbook_flatten(wb, date=None, defaults={"": 0.0}, **mappings):
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
        closest = {s.name.replace(" (2)", ""): s.name for s in wb.worksheets}.get(name)  # HACK handle renames
        try:
            df = wb.getWorksheet(closest).data
        except (KeyError, TypeError, AttributeError):
            # TODO: handle error getting wb properly earlier
            logger.info("Error getting tableau {}/{} {}", name, col, date)
            continue

        if type(col) != str:
            if df.empty:
                # logger.info("Error getting tableau {}/{} {}", name, col, date)
                continue
            # if it's not a single value can pass in mapping of cols
            df = df[col.keys()].rename(columns={k: v for k, v in col.items() if type(v) == str})
            try:
                df['Date'] = pd.to_datetime(df['Date'], dayfirst=False).dt.normalize()
            except pd.errors.OutOfBoundsDatetime:
                # Could be a Thai year. Hack to convert
                df['Date'] = df['Date'].str.replace("2564", "2021").str.replace("2565", "2022")
                df['Date'] = pd.to_datetime(df['Date'], dayfirst=False).dt.normalize()
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
            assert date is None or end <= date, f"getting {date} found {end}"
            all_days = pd.date_range(start, end, name="Date", normalize=True, inclusive="both")
            default = [defaults.get(c, defaults.get("")) if defaults else 0.0 for c in df.columns]
            try:
                df = df.reindex(all_days, fill_value=default[0])  # TODO: work out how to have default for each column
            except ValueError:
                return pd.DataFrame()  # Sometimes there are duplicate dates. if so best abort the whole workbook since something is wrong

            res = res.combine_first(df)
        elif df.empty:
            # TODO: Seems to mean that this is 0? Should be confirgurable?
            default = defaults.get(col, defaults.get("")) if defaults else 0.0
            data[col] = [default]
        elif col == "Date":
            data[col] = [pd.to_datetime(list(df.loc[0])[0], dayfirst=False)]
        else:
            try:
                data[col] = list(pd.to_numeric(df.loc[0]))  # HACK: shouldn't assume we want numbers
            except ValueError:
                data[col] = list(pd.to_numeric(df.loc[0].str.replace(",", "").replace("%null%", "")))
    # combine all the single values with any subplots from the dashboard
    df = pd.DataFrame(data)
    if not df.empty:
        df['Date'] = df['Date'].dt.normalize()  # Latest has time in it which creates double entries
        res = df.set_index("Date").combine_first(res)
    return res


def workbook_iterate(url, verify=True, inc_no_param=False, max_errors=20, **selects):
    "generates combinations of workbooks from combinations of parameters, selects or filters"

    def do_reset():
        for _ in range(2):
            ts = tableauscraper.TableauScraper(verify=verify)
            try:
                ts.loads(url)
            except Exception as err:
                # ts library fails in all sorts of weird ways depending on the data sent back
                logger.info("MOPH Dashboard Error: Exception TS loads url {}: {}", url, str(err))
                continue
            fix_timeouts(ts.session, timeout=30)
            wb = ts.getWorkbook()
            return wb
        return None

    wb = do_reset()
    if wb is None:
        return
    set_value = []
    # match the params to iterate to param, filter or select
    for name, values in selects.items():
        param = next((p for p in wb.getParameters() if p['column'] == name), None)
        ws = next((ws for ws in wb.worksheets if ws.name.replace(" (2)", "") == name), None)
        if param is not None or ws is None:
            # We will force param if it's not select
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
        # TODO: allow a select to be manual list of values
        svalues = ws.getSelectableValues(values)
        if svalues:
            selects[name] = svalues

            # weird bug where sometimes .getWorksheet doesn't work or missign data
            def do_select(wb, value, name=name, values=values):
                ws = next((ws for ws in wb.worksheets if ws.name.replace(" (2)", "") == name), None)
                wb = ws.select(values, value)
                assert wb.worksheets
                return wb
            set_value.append(do_select)
        else:
            items = ws.getFilters()
            # TODO: allow filter to manual list of values
            selects[name] = next((item['values'] for item in items if item['column'] == values), [])
            # TODO: should raise an error if there is no matching filter?

            # weird bug where sometimes .getWorksheet doesn't work or missign data
            def do_filter(wb, value, ws_name=name, filter_name=values):
                ws = next((ws for ws in wb.worksheets if ws.name.replace(" (2)", "") == ws_name), None)
                # return ws.setFilter(values, value)
                return force_setFilter(wb, ws_name, filter_name, [value])
            set_value.append(do_filter)
    if inc_no_param:
        yield lambda: wb, None

    last_idx = [None] * len(selects)  # Outside so we know if we need to change teh params or not
    # Get all combinations of the values of params, select or filter
    for next_idx in itertools.product(*selects.values()):
        def get_workbook(*checks, wb=wb, next_idx=next_idx):
            nonlocal last_idx, max_errors
            reset = False
            if max_errors <= 0:
                logger.warning("MOPH Dashboard Skip {}: Finish iteration due to excess errors", next_idx)
                return None
            for _ in range(2):
                if reset:
                    wb = do_reset()
                    if wb is None:
                        continue
                    reset = False
                for do_set, last_value, value in zip(set_value, last_idx, next_idx):
                    if last_value != value and value is not None:
                        # None means to skip setting this value. #TODO: but does it make sense unless it's just reset?
                        try:
                            wb = do_set(wb, value)
                        except Exception as err:
                            logger.info("{} MOPH Dashboard Retry: {}={} Error: {}", next_idx, do_set.__name__, value, err)
                            reset = True
                            break
                    if not wb.worksheets or len(checks) > 0 and not any_in([ws.name for ws in wb.worksheets], *checks):
                        logger.info("{} MOPH Dashboard Retry: Missing worksheets in {}={}.", next_idx, do_set.__name__, value)
                        reset = True
                        break
                if reset:
                    last_idx = (None,) * len(last_idx)  # need to reset filters etc
                    max_errors -= 1
                    continue
                last_idx = next_idx
                return wb
                # Try again
            logger.warning("MOPH Dashboard Skip: {}. Retries exceeded", next_idx)
            return None
        yield get_workbook, next_idx


def force_setParameter(wb, parameterName, value):
    "Allow for setting a parameter even if it's not present in getParameters"
    # TODO: remove if they fix https://github.com/bertrandmartel/tableau-scraping/issues/49
    scraper = wb._scraper
    tableauscraper.api.delayExecution(scraper)
    payload = (
        ("fieldCaption", (None, parameterName)),
        ("valueString", (None, value)),
    )
    r = scraper.session.post(
        f'{scraper.host}{scraper.tableauData["vizql_root"]}/sessions/{scraper.tableauData["sessionid"]}/commands/tabdoc/set-parameter-value',
        # data=dict(fieldCaption=parameterName, valueString=value),
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
    # TODO: remove if they fix https://github.com/bertrandmartel/tableau-scraping/issues/50

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


def get_woorkbook_updated_time(tableau_scapper: 'tableauscraper.TableauScraper') -> datetime.datetime:
    time_str = tableau_scapper.tableauData.get('workbookLastPublishedAt')
    if time_str is None:
        tableau_scapper.logger.warn("please call `.loads()` first")
        return None

    return dateutil.parser.isoparse(time_str)


def force_select(self, column, value, storyboard=None, storyPointId=None):
    values = self.getSelectableValues(column)
    if not values:
        values = list(self.data[column])
    tupleItems = self.getTupleIds()
    try:

        indexedByTuple = False
        for tupleItem in tupleItems:
            if len(tupleItem) >= len(values):
                index = values.index(value)
                index = tupleItem[index]
                indexedByTuple = True
                break
        if not indexedByTuple:
            index = values.index(value)
            index = index + 1
        if storyboard is not None and storyPointId is not None:
            r = select(self._scraper, self.name, storyboard, storyPointId, [index])
        else:
            r = tableauscraper.api.select(self._scraper, self.name, [index])
        self.updateFullData(r)
        return tableauscraper.dashboard.getWorksheetsCmdResponse(self._scraper, r)
    except ValueError as e:
        self._scraper.logger.error(str(e))
        return tableauscraper.TableauWorkbook(
            scraper=self._scraper, originalData={}, originalInfo={}, data=[]
        )

# visualIdPresModel: {"worksheet":"map_total","dashboard":"Dashboard_Province_index_new_v3","storyboard":"Story 1","storyPointId":12}
# zoneId: 3
# zoneSelectionType: replace


def select(scraper, worksheetName, dashboard, storyPointId, selection):
    tableauscraper.api.delayExecution(scraper)
    payload = (
        (
            "visualIdPresModel", (None, json.dumps({
                "worksheet": worksheetName,
                "dashboard": dashboard,  # TODO: where to get this value from?
                "storyboard": scraper.dashboard,
                "storyPointId": storyPointId,
            }))
        ),
        ("selection", (None, json.dumps(
            {"objectIds": selection, "selectionType": "tuples"}))),
        ("selectOptions", (None, "select-options-simple")),
        #        ("zoneId", (None, 3)),
        ("zoneSelectionType", (None, "replace")),
    )
    r = scraper.session.post(
        f'{scraper.host}{scraper.tableauData["vizql_root"]}/sessions/{scraper.tableauData["sessionid"]}/commands/tabdoc/select',
        files=payload,
        verify=scraper.verify
    )
    scraper.lastActionTime = time.time()
    try:
        return r.json()
    except (ValueError, JSONDecodeError):
        raise tableauscraper.api.APIResponseException(message=r.text)
