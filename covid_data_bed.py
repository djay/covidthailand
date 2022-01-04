import datetime

from dateutil.tz import tzutc
from tableauscraper import TableauScraper as TS

from utils_pandas import export
from utils_pandas import import_csv
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
    # assert storypoints['storyPoints'][0][0]['storyPointId'] == 12 and 'ทรัพยากรภาพรวม' in storypoints['storyPoints'][0][0]['storyPointCaption']
    id = next(sp['storyPointId'] for sp in storypoints['storyPoints'][0] if 'ทรัพยากรภาพรวม' in sp['storyPointCaption'])

    sp = workbook.goToStoryPoint(storyPointId=id)

    ws = sp.getWorksheet('province_total')
    row = ws.data[['Prov Name-value', 'Measure Names-alias', 'Measure Values-value']]
    row.columns = ['Province', 'ValueType', 'Value']
    row = row.pivot('Province', 'ValueType', 'Value')
    row.columns = ['Bed All Availiable', 'Bed All Occupied']
    row['update_time'] = updated_time
    row['Date'] = updated_time
    row['Date'] = row['Date'].dt.normalize()
    row = join_provinces(row, "Province")
    row = row.reset_index().set_index(['Date', 'Province'])

    # data is clean
    assert not row.isna().any().any(), 'some datapoints contain NA'

    df = import_csv("moph_bed", ["Date", "Province"], False, dir="inputs/json")
    if df.empty or df.reset_index()['Date'].max() < updated_time.date():
        df = df.combine_first(row)
        export(df, "moph_bed", csv_only=True, dir="inputs/json")

    # TODO: get bed types and ventilator tabs and iterate through prvinces
    prov = sp.getWorksheet("map_total")
    wb = force_select(prov, "Prov Name En-value", 'Nakhon Ratchasima', "Story 1", id)

    return df, updated_time


if __name__ == '__main__':
    df, date = get_df()
    print(date)
    print(df)
    #df.to_csv(f"bed_data_{date.isoformat()}.csv")
