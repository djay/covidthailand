from typing import OrderedDict
import requests
import tabula
import os
from tika import parser
import re
import urllib.parse
import pandas as pd
import matplotlib
matplotlib.use('AGG')
import matplotlib.pyplot as plt
import datetime
from io import StringIO
from bs4 import BeautifulSoup
from pptx import Presentation
from urllib3.util.retry import Retry
import dateutil
#pd.options.display.mpl_style = 'default'

requests.adapters.DEFAULT_RETRIES = 5 # for other tools that use requests internally

from requests.adapters import HTTPAdapter, Retry
s = requests.Session()
retry = Retry(total=10, backoff_factor=1) # should make it more reliable as ddc.moph.go.th often fails
s.mount('http://', HTTPAdapter(max_retries=retry))
s.mount('https://', HTTPAdapter(max_retries=retry))

CHECK_NEWER = bool(os.environ.get('CHECK_NEWER', False))

def is_remote_newer(file, remote_date):
    if not os.path.exists(file):
        print(f"Missing: {file}")
        return True
    if remote_date is None:
        return False # TODO: do we want to redownload each time? 
    if type(remote_date) == str:
        remote_date = dateutil.parser.parse(remote_date).astimezone()
    fdate  = datetime.datetime.fromtimestamp(os.path.getmtime(file)).astimezone()
    if remote_date > fdate:
        timestamp = fdate.strftime("%Y%m%d-%H%M%S")
        os.rename(file, f"{file}.{timestamp}")
        return True
    return False


def all_pdfs(*index_urls):
    urls = []
    for index_url in index_urls:
        # skip "https://ddc.moph.go.th/viralpneumonia/situation_more.php" as they are harder to parse
        index = s.get(index_url)
        if index.status_code > 399:
            continue
        links = re.findall("href=[\"\'](.*?)[\"\']", index.content.decode('utf-8'))
        links = [urllib.parse.urljoin(index_url, l) for l in links if 'pdf' in l]
        urls.extend(links)


    for url in urls:
        file = url.rsplit('/', 1)[-1]
        if CHECK_NEWER:
            r = s.head(url)
            modified = r.headers.get('last-modified')
        else:
            modified = None
        if is_remote_newer(file, modified):
            r = s.get(url)
            if r.status_code != 200:
                continue
            with open(file, 'wb') as f:
                for chunk in r.iter_content(chunk_size=512 * 1024): 
                    if chunk: # filter out keep-alive new chunks
                        f.write(chunk)
        parsedPDF = parser.from_file(file)
        yield file, parsedPDF

def dav_files(url, username, password, ext=".pdf .pptx"):
    from webdav3.client import Client
    options = {
    'webdav_hostname': url,
    'webdav_login':    username,
    'webdav_password': password
    }
    client = Client(options)
    client.session.mount('http://', HTTPAdapter(max_retries=retry))
    client.session.mount('https://', HTTPAdapter(max_retries=retry))
    # important we get them sorted newest files first as we only fill in NaN from each additional file
    files = sorted(client.list(get_info=True), key=lambda info:dateutil.parser.parse(info['modified']), reverse=True)
    for info in files:
        file = info['path'].split('/')[-1]
        if not any([ext == file[-len(ext):] for ext in ext.split()]):
            continue
        if is_remote_newer(file, info['modified']):
            client.download_file(file, file)
        yield file


def get_next_numbers(content, *matches, debug=False):
    if len(matches) == 0:
        matches = [""]
    for match in matches:
        s = content.split(match) if match else ("",content)
        if len(s) >= 2:  #TODO if > 2 should check its the same first number?
            content = s[1]
            numbers = re.findall(r"[,0-9]+", content)
            numbers = [n.replace(',','') for n in numbers]
            numbers = [int(n) for n in numbers if n]
            return numbers, match + ' ' + content
    if debug and matches:
        print("Couldn't find '{}'".format(match))
        print(content)
    return [],content

def file2date(file):
    date = file.rsplit(".pdf",1)[0].rsplit('-',1)[1]
    date = datetime.datetime(day=int(date[0:2]), month=int(date[2:4]), year=int(date[4:6])-43+2000)
    return date

thai_abbr_months = [
    "ม.ค.",
    "ก.พ.",
    "มี.ค.",
    "เม.ย.",
    "พ.ค.",
    "มิ.ย.",
    "ก.ค.",
    "ส.ค.",
    "ก.ย.",
    "ต.ค.",
    "พ.ย.",
    "ธ.ค.",
]
thai_full_months = [
    "มกราคม",
    "กุมภาพันธ์",
    "มีนาคม",
    "เมษายน",
    "พฤษภาคม",
    "มิถุนายน",
    "กรกฎาคม",
    "สิงหาคม",
    "กันยายน",
    "ตุลาคม",
    "พฤศจิกายน",
    "ธันวาคม",
]

def find_dates(content):
    # 7 - 13/11/2563
    dates = re.findall(r"([0-9]+)/([0-9]+)/([0-9]+)", content)
    dates = set([datetime.datetime(day=int(d[0]),month=int(d[1]), year=int(d[2])-543) for d in dates])
    return sorted([d for d in dates])

def previous_date(end, day):
    start = end
    while start.day != int(day):
        start = start - datetime.timedelta(days=1)
    return start


def find_date_range(content):
    # 11-17 เม.ย. 2563 or 04/04/2563 12/06/2563
    m1 = re.search(r"([0-9]+)/([0-9]+)/([0-9]+) [-–] ([0-9]+)/([0-9]+)/([0-9]+)", content)
    m2 = re.search(r"([0-9]+) *[-–] *([0-9]+)/([0-9]+)/(25[0-9][0-9])", content)
    m3 = re.search(r"([0-9]+) *[-–] *([0-9]+) *([^ ]+) *(25[0-9][0-9])", content)
    if m1:
        d1,m1,y1,d2,m2,y2 = m1.groups()
        start = datetime.datetime(day=int(d1),month=int(m1),year=int(y1)-543)
        end = datetime.datetime(day=int(d2),month=int(m2),year=int(y2)-543)
        return start,end
    elif m2:
        d1,d2,month,year = m2.groups()
        end = datetime.datetime(year=int(year)-543, month=int(month), day=int(d2))
        start = previous_date(end, d1)
        return start,end
    elif m3:
        d1,d2,month,year = m3.groups()
        month=thai_abbr_months.index(month)+1 if month in thai_abbr_months else thai_full_months.index(month)+1 if month in thai_full_months else None
        end = datetime.datetime(year=int(year)-543, month=month, day=int(d2))
        start = previous_date(end, d1)
        return start,end
    else:
        return None,None


def daterange(start_date, end_date, offset=0):
    for n in range(int((end_date - start_date).days)+offset):
        yield start_date + datetime.timedelta(n)


def parse_file(filename, as_html=False):
    pages_txt = []

    # Read PDF file
    data = parser.from_file(filename, xmlContent=True)
    xhtml_data = BeautifulSoup(data['content'], features="lxml")
    pages = xhtml_data.find_all('div', attrs={'class': ['page', 'slide-content']})
    # TODO: slides are divided by slide-content and slide-master-content rather than being contained
    for i, content in enumerate(pages):
        # Parse PDF data using TIKA (xml/html)
        # It's faster and safer to create a new buffer than truncating it
        # https://stackoverflow.com/questions/4330812/how-do-i-clear-a-stringio-object
        _buffer = StringIO()
        _buffer.write(str(content))
        parsed_content = parser.from_buffer(_buffer.getvalue())
        if parsed_content['content'] is None:
            continue

        # Add pages
        text = parsed_content['content'].strip()
        if as_html:
            pages_txt.append(repr(content))
        else:
            pages_txt.append(text)

    return pages_txt

def slide2text(slide):
    text = ""
    if slide.shapes.title:
        text += slide.shapes.title.text
    for shape in slide.shapes:
        if shape.has_text_frame:
            #for p in shape.text_frame:
                text += "\n" + shape.text
    return text

def slide2chartdata(slide):
    for shape in slide.shapes:
        if shape.has_chart:
            yield shape.chart

def spread_date_range(start, end, row, columns):
    r = list(daterange(start,end, offset=1))
    stats = [float(p)/len(r) for p in row]
    results = pd.DataFrame([[date,]+stats for date in r], columns=columns).set_index('Date')
    return results

def get_cases():
    timeline = s.get('https://covid19.th-stat.com/api/open/timeline').json()['Data']
    results = []
    for d in timeline:
        date = datetime.datetime.strptime(d['Date'], '%m/%d/%Y')
        cases = d['NewConfirmed']
        #merge('timeline', date, (date, None, None, None, None, None, cases))
        results.append((date,cases))
    data = pd.DataFrame(results, columns=['Date','Cases']).set_index('Date')
    print(data)
    return data

POS_AREA_COLS = ["Pos Area {}".format(i+1) for i in range(13)] 
TESTS_AREA_COLS = ["Tests Area {}".format(i+1) for i in range(13)]

def get_tests_by_area():
    columns = ['Date'] + POS_AREA_COLS + TESTS_AREA_COLS + ["Pos Area", "Tests Area"]
    data = pd.DataFrame()

    # some additional data from pptx files
    for file in dav_files("http://nextcloud.dmsc.moph.go.th/public.php/webdav", "wbioWZAQfManokc", "null", ext=".pptx"):
        prs = Presentation(file)
        for chart in (chart for slide in prs.slides for chart in slide2chartdata(slide)):
            if not chart:
                continue
            title = chart.chart_title.text_frame.text if chart.has_title else ''
            start,end = find_date_range(title)
            if start is None:
                continue
            series=dict([(s.name,s.values) for s in chart.series])
            if not "เริ่มเปิดบริการ" in title and any(t in title for t in ["เขตสุขภาพ", "เขตสุขภำพ"]):
                # the graph for X period split by health area.
                # Need both pptx and pdf as one pdf is missing
                pos = list(series['จำนวนผลบวก'])
                tests = list(series["จำนวนตรวจ"])
                row = pos+tests+[sum(pos),sum(tests)]
                results = spread_date_range(start, end, row, columns)
                print(results)
                data = data.combine_first(results)
            elif "และอัตราการตรวจพบ" in title and "รายสัปดาห์" not in title:
                # The graphs at the end with all testing numbers private vs public
                private = "Private" if "ภาคเอกชน" in title else "Public"

                #pos = series["Pos"]
                if 'จำนวนตรวจ' not in series:
                    continue
                tests = series['จำนวนตรวจ']
                positivity = series['% Detection']
                dates = list(daterange(start,end,1))
                df = pd.DataFrame({"Date":dates,f"Tests {private}":tests, "% Detection":positivity}).set_index('Date')
                df[f'Pos {private}'] = df[f"Tests {private}"] * df["% Detection"]/100.0  
                print(df)
                data = data.combine_first(df)
            #TODO: There is also graphs splt by hospital



    for file in dav_files("http://nextcloud.dmsc.moph.go.th/public.php/webdav", "wbioWZAQfManokc", "null", ext=".pdf"):
        #parsedPDF = parser.from_file(file)

        #pages = parsedPDF['content'].split("\n\n\n\n") #  # วันที่ท้ำรำยงำน
        pages = parse_file(file)
        not_whole_year = [page for page in pages if "เริ่มเปิดบริการ" not in page]
        by_area = [page for page in not_whole_year if "เขตสุขภาพ" in page or "เขตสุขภำพ" in page]
        # Can't parse '35_21_12_2020_COVID19_(ถึง_18_ธันวาคม_2563)(powerpoint).pptx' because data is a graph
        # no pdf available so data missing
        # Also missing 14-20 Nov 2020 (no pptx or pdf)

        for page in by_area:
            start,end = find_date_range(page)
            if start is None:
                continue
            if '349585' in page:
                page = page.replace('349585', '349 585')
            # if '16/10/2563' in page:
            #     print(page)
            # First line can be like จดัท ำโดย เพญ็พชิชำ ถำวงศ ์กรมวิทยำศำสตณก์ำรแพทย์ วันที่ท ำรำยงำน 15/02/2564 เวลำ 09.30 น.
            first, rest = page.split("\n", 1)
            page = rest if 'เพญ็พชิชำ' in first or '/' in first else page # get rid of first line that sometimes as date and time in it
            numbers, content = get_next_numbers(
                page,
                "", # "ภาคเอกชน", 
                debug=True
                )
            # ภาครัฐ
            # ภาคเอกชน
            # จดัท ำโดย เพญ็พชิชำ ถำวงศ ์กรมวิทยำศำสตณก์ำรแพทย์        
            #print(numbers)
            # TODO: should really find and parse X axis labels which contains 'เขต' and count
            tests_start = 13 if 'total' not in page else 14
            pos = numbers[0:13]
            tests = numbers[tests_start:tests_start+13]
            row = pos+tests+[sum(pos),sum(tests)]
            results = spread_date_range(start, end, row, columns)
            print(results)
            data = data.combine_first(results)

    return data

def get_thai_situation():
    results = []
    for file, parsedPDF in all_pdfs("https://ddc.moph.go.th/viralpneumonia/situation.php","https://ddc.moph.go.th/viralpneumonia/situation_more.php"):
        if 'situation' not in file:
            continue
        date = file2date(file)
        numbers,content = get_next_numbers(
            parsedPDF['content'],
            "ด่านโรคติดต่อระหว่างประเทศ",
            "ด่านโรคติดต่อระหวา่งประเทศ", # 'situation-no346-141263n.pdf'
            "นวนการตรวจทางห้องปฏิบัติการ", 
            "นวนการตรวจทางหVองปฏิบัติการ",
            "นวนการตรวจทางหWองปฏิบัติการ",
            "นวนการตรวจทางหองปฏิบัติการ",
            #"จำนวนการตรวจทางหอ้งปฏิบัติการ",
            )
        if not numbers:
            break
        # cases = None
        screened_port, screened_cw, tests_total, pui, active_finding, asq, not_pui, pui, pui_port, *rest  = numbers
        if tests_total < 30000:
            tests_total, pui, active_finding, asq, not_pui, *rest = numbers
            if pui == 4534137:
                pui = 453413 #situation-no273-021063n.pdf 
        if tests_total > 2000000 < 30000 or pui > 1500000 < 100000:
            raise Exception(f"Bad data in {file}")
        #merge(file, date, (date, tests_total, pui, active_finding, asq, not_pui, None))
        results.append((date, tests_total, pui, active_finding, asq, not_pui))
        print(file,results[-1])
    results = pd.DataFrame(results, columns=["Date","Tested", "PUI", "Active case finding", "ASQ", "Not PUI"]).set_index('Date')
    print(results)
    return results

def get_en_situation():
    results = []
    for file, parsedPDF in all_pdfs("https://ddc.moph.go.th/viralpneumonia/eng/situation.php"):
        if 'situation' not in file:
            continue
        date = file2date(file)
        #table = tabula.read_pdf(file, pages=[2])
        # print(table)
        #numbers = [n for n in table[0]['Total Number'] if type(n) == str]
        #numbers = [n for n in table[0][table[0].columns[1]] if type(n) == str]
        numbers, content = get_next_numbers(
            parsedPDF['content'], 
            #"Ports of entry", 
            #"DDC Thailand   2",
            #"Total number of screened people",
            "Total number of laboratory tests",
            "Total  number of laboratory tests",
            #"Situation Total number of PUI",
            #"Point of entry",
            #"Screening passengers at ports of entry"
            debug=False
            )
        if numbers:
            tests_total, pui, active_finding, asq, not_pui, pui, pui_port, *rest  = numbers
        else:
            tests_total, active_finding, asq, not_pui = [None]*4
            numbers, content = get_next_numbers(
                parsedPDF['content'], 
                "Total number of people who met the criteria of patients",
                debug=False
            )
            if not numbers:
                break
            pui, pui_airport, pui_seaport, pui_hospital, *rest = numbers
            pui_port = pui_airport + pui_seaport
        if pui in [1103858, 3891136, 433807, 96989]: #mistypes #TODO: should use thai version as likely more accurate
            pui=None
        if tests_total in [783679, 849874, 936458]:
            tests_total = None
        results.append((date, tests_total, pui, active_finding, asq, not_pui))
        print(file,results[-1])
    
    results = pd.DataFrame(results, columns=["Date","Tested", "PUI", "Active case finding", "ASQ", "Not PUI"]).set_index('Date')
    print(results)
    return results

def get_tests_by_day():
    file = list(dav_files("http://nextcloud.dmsc.moph.go.th/public.php/webdav", "wbioWZAQfManokc", "null", 'xlsx'))[0]
    tests = pd.read_excel(file, parse_dates=True, usecols=[0,1,2])
    tests.dropna(how="any", inplace=True) # get rid of totals row
    tests = tests.set_index("Date")
    #row = tests[['Pos','Total']]['Cannot specify date'] 
    pos = tests.loc['Cannot specify date'].Pos 
    total = tests.loc['Cannot specify date'].Total
    tests.drop('Cannot specify date', inplace=True)
    # Need to redistribute the unknown values across known values
    # Documentation tells us it was 11 labs and only before 3 April
    unknown_end_date = datetime.datetime(day=3,month=4,year=2020)
    all_pos = tests['Pos'][:unknown_end_date].sum()
    all_total = tests['Total'][:unknown_end_date].sum()
    for index, row in tests.iterrows():
        if index > unknown_end_date:
            continue
        row.Pos = float(row.Pos) + row.Pos/all_pos*pos
        row.Total = float(row.Total) + row.Total/all_total*total
    # TODO: still doesn't redistribute all missing values due to rounding. about 200 left
    print(tests['Pos'].sum(), pos+all_pos)
    print(tests['Total'].sum(), total+all_total)
    # fix datetime
    tests.reset_index(drop=False, inplace=True)
    tests['Date']= pd.to_datetime(tests['Date'])
    tests.set_index('Date', inplace=True)

    tests.rename(columns=dict(Pos="Pos XLS", Total="Tests XLS"), inplace=True)

    return tests


tests = get_tests_by_day()
print(tests)
#data.combine_first(tests)


areas = get_tests_by_area()

cases = get_cases()
print(cases)  

th_situation = get_thai_situation()
th_situation = th_situation - th_situation.shift(-1)
en_situation = get_en_situation()
en_situation = en_situation - en_situation.shift(-1)
situation = th_situation.combine_first(en_situation)
print(situation)

df = situation
df = df.combine_first(cases)
df = df.combine_first(areas)
df = df.combine_first(tests)
print(df)


# create a plot
fig, ax = plt.subplots()

df['Tested (MA)'] = df['Tested'].rolling(7, 1, center=True).mean()
df['PUI (MA)'] = df['PUI'].rolling(7, 1, center=True).mean()
df['Cases (MA)'] = df['Cases'].rolling(7, 1, center=True).mean()
df['Tests Area (MA)'] = df['Tests Area'].rolling(7, 1, center=True).mean()
df['Pos Area (MA)'] = df['Pos Area'].rolling(7, 1, center=True).mean()
df['Tests XLS (MA)'] = df['Tests XLS'].rolling(7, 1, center=True).mean()
df['Pos XLS (MA)'] = df['Pos XLS'].rolling(7, 1, center=True).mean()

df['Positivity Tested (MA)'] = df['Cases (MA)'] / df['Tested (MA)'] * 100
df['Positivity PUI (MA)'] = df['Cases (MA)'] / df['PUI (MA)'] * 100
df['Positivity'] = df['Cases'] / df['Tested'] * 100
df['Positivity Area (MA)'] = df['Pos Area (MA)'] / df['Tests Area (MA)'] * 100
df['Positivity Area'] = df['Pos Area'] / df['Tests Area'] * 100
df['Positivity XLS (MA)'] = df['Pos XLS (MA)'] / df['Tests XLS (MA)'] * 100
df['Positivity XLS'] = df['Pos XLS'] / df['Tests XLS'] * 100
df['Positivity Cases/Tests (MA)'] = df['Cases (MA)'] / df['Tests XLS (MA)'] * 100

df['Pos Public (MA)'] = df['Pos Public'].rolling(7, 1, center=True).mean() 
df['Pos Private (MA)'] = df['Pos Private'].rolling(7, 1, center=True).mean()
df['Pos Corrected+Private (MA)'] = df['Pos Private (MA)'] + df['Pos XLS (MA)']
df['Tests Public (MA)'] = df['Tests Public'].rolling(7, 1, center=True).mean() 
df['Tests Private (MA)'] = df['Tests Private'].rolling(7, 1, center=True).mean()
df['Tests Private+Public (MA)'] = df['Tests Public (MA)'] + df['Tests Private (MA)']
df['Tests Corrected+Private (MA)'] = df['Tests XLS (MA)'] + df['Tests Private (MA)']

df['Positivity Private (MA)'] = df['Pos Private (MA)'] / df['Tests Private (MA)'] * 100
df['Positivity Public+Private (MA)'] = df['Pos Corrected+Private (MA)'] / df['Tests Corrected+Private (MA)'] * 100


#print(df.to_string())
df.plot(y=["Tested (MA)", "PUI (MA)", 'Tests Corrected+Private (MA)', "Tests Private (MA)", "Cases (MA)"], ax=ax, use_index=True, kind="line", figsize=[20,10], title="People Tested (7 day rolling average)")
ax.legend(['Situation reports "Tests"', "PUI", "Tests Performed (Corrected + Private)", "Tests Private", "Confirmed Cases", ])
plt.tight_layout()
plt.savefig("tests.png")

fig, ax = plt.subplots()
df.plot(ax=ax, use_index=True, y=["Positivity PUI (MA)", "Positivity XLS (MA)", 'Positivity Cases/Tests (MA)', 'Positivity Public+Private (MA)', 'Positivity Private (MA)'], kind="line", figsize=[20,10], title="Thailand Covid positivity (7day rolling average)")
ax.legend(['Confirmed Cases / PUI', "Positive Results / Tests Performed", "Confirmed Cases   / Tests Performed", 'Positivity Public+Private (MA)', 'Positivity Private (MA)'])
plt.tight_layout()
plt.savefig("positivity.png")

fig, ax = plt.subplots()
df.plot(ax=ax, use_index=True, y=["Cases (MA)", 'Pos Corrected+Private (MA)', "Pos Private (MA)", ], kind="line", figsize=[20,10], title="Positive Cases (7 day rolling average)")
ax.legend(["Confirmed Cases", "Positive Test Results (Corrected + Private)", "Pos Private", ])
plt.tight_layout()
plt.savefig("cases.png")

fig, ax = plt.subplots()
df.plot(ax=ax, use_index=True, y=["Cases (MA)", "Pos Area (MA)", "Pos XLS (MA)", "Pos Public (MA)", "Pos Private (MA)", 'Pos Corrected+Private (MA)'], kind="line", figsize=[20,10], title="Positive Cases (7 day rolling average)")
plt.tight_layout()
plt.savefig("cases_all.png")


#df = df.cumsum()
AREA_LEGEND = [
    "1: Upper N: Chiang Mai, Chiang Rai,...", 
    "2: Lower N: Tak, Phitsanulok, Phetchabun, Sukhothai, Uttaradit",
    "3: Upper C: Kamphaeng Phet, Nakhon Sawan, Phichit, Uthai Thani, Chai Nat",
    "4: Mid C: Nonthaburi-Ayutthaya",
    "5: Lower C: Kanchanaburi-Samut Sakhon",
    "6: E: Trat, Rayong, Chonburi, Samut Prakan, ...",
    "7: Mid NE:  Khon Kaen...",
    "8: Upper NE: Loei-Sakon Nakhon",
    "9: Lower NE 1: Buriram, Surin...",
    "10: Lower NE 2: Ubon Ratchathani...",
    "11: SE: Ranong-Krabi-Surat Thani...",
    "12: SW: Trang-Narathiwat",
    "13: Bangkok?"
    ]


def rearrange(l, *first):
    l = list(l)
    result = []
    for f in first:
        result.append(l[f])
        l[f] = None
    return result+[i for i in l if i is not None]

first = [12, 3, 5, 0, 4]
area_legend = rearrange(AREA_LEGEND, *first)

fig, ax = plt.subplots()
df.plot(ax=ax, use_index=True, y=rearrange(TESTS_AREA_COLS, *first), kind="area", figsize=[20,10], title="Tests by Health Area")
ax.legend(area_legend)
plt.tight_layout()
plt.savefig("tests_area.png")

fig, ax = plt.subplots()
df.plot(ax=ax, use_index=True, y=rearrange(POS_AREA_COLS, *first), kind="area", figsize=[20,10], title="Positive Results by Health Area")
ax.legend(area_legend)
plt.tight_layout()
plt.savefig("pos_area.png")

# Workout positivity for each area as proportion of positivity for that period
fig, ax = plt.subplots()

for area in range(1,14):
    df[f'Positivity {area}'] = df[f'Pos Area {area}'] / df[f'Tests Area {area}'] * 100
cols = [f'Positivity {area}' for area in range(1,14)]
df['Total Positivity Area'] = df[cols].sum(axis=1)
for area in range(1,14):
    df[f'Positivity {area}'] = df[f'Positivity {area}'] / df['Total Positivity Area'] * df['Positivity Area']
print(df[['Total Positivity Area','Positivity Area', 'Pos Area', 'Tests Area']+cols])

df.plot(ax=ax, use_index=True, y=rearrange(cols, *first), kind="area", figsize=[20,10], title="Positivity by Health Area")
ax.legend(area_legend)
plt.tight_layout()
plt.savefig("positivity_area.png")

