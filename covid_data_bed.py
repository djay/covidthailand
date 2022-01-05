import datetime

import pandas as pd
from dateutil.tz import tzutc
from tableauscraper import TableauScraper as TS

from utils_pandas import export
from utils_pandas import import_csv
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
    updated_time = get_woorkbook_updated_time(ts)

    if not should_be_newer_than < updated_time:
        return None

    workbook = ts.getWorkbook()

    # assumption
    storypoints = workbook.getStoryPoints()

    def getSPID(name):
        return next(sp['storyPointId'] for sp in storypoints['storyPoints'][0] if name in sp['storyPointCaption'])

    df = import_csv("moph_bed", ["Date", "Province"], False, dir="inputs/json")
    if not df.empty and df.reset_index()['Date'].max() >= updated_time.date():
        return df

    # get bed types and ventilator tabs and iterate through prvinces
    # Break down of beds types
    id = getSPID("เตียง")
    sp = workbook.goToStoryPoint(storyPointId=id)
    map_total = sp.getWorksheet("map_total")
    data = []
    for prov in map_total.data["Prov Name En-value"]:
        wb = force_select(map_total, "Prov Name En-value", prov, "Dashboard_Province_layout", id)
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
        logger.info("{} Bed {}", updated_time.date(), prov, " ".join(str(s) for s in row.values()))
    provs = pd.DataFrame(data)
    provs['Date'] = updated_time.date()
    # provs['Date'] = provs['Date'].dt.normalize()
    provs = provs.set_index(["Date", "Province"])
    provs = join_provinces(provs, "Province")  # Ensure we get the right names

    # Get total beds per province
    ts.loads(url)
    workbook = ts.getWorkbook()
    sp = workbook.goToStoryPoint(storyPointId=getSPID('ทรัพยากรภาพรวม'))

    ws = sp.getWorksheet('province_total')
    row = ws.data[['Prov Name-value', 'Measure Names-alias', 'Measure Values-value']]
    row.columns = ['Province', 'ValueType', 'Value']
    row = row.pivot('Province', 'ValueType', 'Value')
    row.columns = ['Bed All Total', 'Bed All Occupied']
    row['update_time'] = updated_time
    row['Date'] = updated_time.date()
    row = join_provinces(row, "Province")
    row = row.reset_index().set_index(['Date', 'Province'])

    # data is clean
    assert not row.isna().any().any(), 'some datapoints contain NA'

    # Ventitalors
    ts.loads(url)
    workbook = ts.getWorkbook()
    sp = workbook.goToStoryPoint(storyPointId=getSPID('ทรัพยากรภาพรวม'))
    sp = workbook.goToStoryPoint(storyPointId=getSPID("VENTILATOR"))
    ws = sp.getWorksheet("province_respirator")
    vent = ws.data[['Prov Name-value', 'SUM(Ventilator)-value']]
    vent.columns = ["Province", "Bed Ventilator Total"]
    vent = join_provinces(vent, 'Province')
    vent['Date'] = updated_time.date()
    vent = vent.set_index(["Date", "Province"])

    df = df.combine_first(row).combine_first(provs).combine_first(vent)
    export(df, "moph_bed", csv_only=True, dir="inputs/json")

    return df


if __name__ == '__main__':
    df = get_df()
    print(df)
    #df.to_csv(f"bed_data_{date.isoformat()}.csv")
