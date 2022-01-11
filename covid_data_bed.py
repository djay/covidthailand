import datetime

import pandas as pd
from dateutil.tz import tzutc
from tableauscraper import TableauScraper as TS

from utils_pandas import export
from utils_pandas import import_csv
from utils_scraping import fix_timeouts
from utils_scraping import logger
from utils_scraping_tableau import force_select
from utils_scraping_tableau import get_woorkbook_updated_time
from utils_scraping_tableau import workbook_explore
from utils_thai import join_provinces


def get_df(should_be_newer_than=datetime.datetime(2000, 1, 1, tzinfo=tzutc())):
    """
    get province bed number (ICU + cohort)
    """

    url = "https://public.tableau.com/views/moph_covid_v3/Story1"
    ts = TS()
    ts.loads(url)
    fix_timeouts(ts.session, timeout=30)
    updated_time = get_woorkbook_updated_time(ts)

    if not should_be_newer_than < updated_time:
        return None

    workbook = ts.getWorkbook()

    def getSPID(name, workbook):
        return next(sp['storyPointId'] for sp in workbook.getStoryPoints()['storyPoints'][0] if name in sp['storyPointCaption'])

    df = import_csv("moph_bed", ["Date", "Province"], False, dir="inputs/json")
    #if not df.empty and df.reset_index()['Date'].max() >= updated_time.date():
    #    return df

    # get bed types and ventilator tabs and iterate through prvinces
    # Break down of beds types
    id = getSPID("เตียง", workbook)
    wb = workbook.goToStoryPoint(storyPointId=id)
    data = []
    for prov in (map_total := wb.getWorksheet("province_total")).data["Prov Name-value"].unique():
        try:
            wb = force_select(map_total, "Prov Name-value", prov, "Dashboard_Province_layout", id)
        except Exception:
            logger.warning(f"{updated_time.date()} Beds {prov}: Failed")
            continue
        row = {"Province": prov}
        for name, sheet in zip(
                ['AIIR', 'Modified AIIR', 'Isolation', 'Cohort'],
                ['doughtnut_aiir', "modified_aiir", 'isolation', 'doughtnut_cohort']):
            sheetdf = wb.getWorksheet(sheet).data
            if sheetdf.empty:
                continue  # TODO: go back and retry
            for vtype, value in zip(["Total", "Occupied"], sheetdf['Measure Values-alias']):
                row[f"Bed {name} {vtype}"] = int(value.replace(",", ""))
        data.append(row)
        logger.info("{} Beds {}: {}", updated_time.date(), prov, " ".join(str(s) for s in row.values()))
    provs = pd.DataFrame(data)
    provs['Date'] = updated_time.date()
    # provs['Date'] = provs['Date'].dt.normalize()
    provs = provs.set_index(["Date", "Province"])
    provs = join_provinces(provs, "Province")  # Ensure we get the right names
    provs = provs.drop_duplicates()

    # Ventitalors
    ts = TS()
    ts.loads(url)
    fix_timeouts(ts.session, timeout=30)
    workbook = ts.getWorkbook()
    sp = workbook.goToStoryPoint(storyPointId=getSPID("VENTILATOR", workbook))
    ws = sp.getWorksheet("province_respirator")
    vent = ws.data[['Prov Name-value', 'SUM(Ventilator)-value']]
    vent.columns = ["Province", "Bed Ventilator Total"]
    vent = join_provinces(vent, 'Province')
    vent['Date'] = updated_time.date()
    vent = vent.set_index(["Date", "Province"])

    # Get total beds per province
    ts = TS()
    ts.loads(url)
    fix_timeouts(ts.session, timeout=30)
    workbook = ts.getWorkbook()
    sp = workbook.goToStoryPoint(storyPointId=getSPID('ทรัพยากรภาพรวม', workbook))

    ws = sp.getWorksheet('province_total')
    total = ws.data[['Prov Name-value', 'Measure Names-alias', 'Measure Values-value']]
    total.columns = ['Province', 'ValueType', 'Value']
    total = total.pivot('Province', 'ValueType', 'Value')
    total.columns = ['Bed All Total', 'Bed All Occupied']
    # total['update_time'] = updated_time
    total['Date'] = updated_time.date()
    total = join_provinces(total, "Province")
    total = total.reset_index().set_index(['Date', 'Province'])

    # data is clean
    assert not total.isna().any().any(), 'some datapoints contain NA'

    df = provs.combine_first(vent).combine_first(total).combine_first(df)
    export(df, "moph_bed", csv_only=True, dir="inputs/json")

    return df


if __name__ == '__main__':
    df = get_df()
    print(df)
    #df.to_csv(f"bed_data_{date.isoformat()}.csv")
