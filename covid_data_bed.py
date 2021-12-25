from tableauscraper import TableauScraper as TS
import datetime
from dateutil.tz import tzutc
from utils_scraping_tableau import get_woorkbook_updated_time

def get_df(should_be_newer_than=datetime.datetime(2000,1,1, tzinfo=tzutc())):
    """
    get province bed number (ICU + cohort)
    """

    url= "https://public.tableau.com/views/moph_covid_v3/Story1"
    ts = TS()
    ts.loads(url)
    updated_time = get_woorkbook_updated_time(ts)

    if not should_be_newer_than < updated_time:
        return None

    workbook = ts.getWorkbook()

    # assumption
    storypoints = workbook.getStoryPoints() 
    assert storypoints['storyPoints'][0][0]['storyPointId'] == 12 and 'ทรัพยากรภาพรวม' in storypoints['storyPoints'][0][0]['storyPointCaption']

    sp = workbook.goToStoryPoint(storyPointId=12)

    ws = sp.getWorksheet('province_total')
    df = ws.data[['Prov Name-value', 'Measure Names-alias', 'Measure Values-value']]
    df.columns = ['Province', 'ValueType', 'Value']
    df = df.pivot('Province', 'ValueType', 'Value')
    df.columns = ['Availiable', 'Occupied']

    # data is clean
    assert not df.isna().any().any(), 'some datapoints contain NA'

    return df, updated_time

if __name__ == '__main__':
    df, date = get_df()
    print(date)
    print(df)
    df.to_csv(f"bed_data_{date.isoformat()}.csv")