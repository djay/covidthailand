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
#pd.options.display.mpl_style = 'default'

requests.adapters.DEFAULT_RETRIES = 5


def all_pdfs(*index_urls):
    urls = []
    for index_url in index_urls:
        # skip "https://ddc.moph.go.th/viralpneumonia/situation_more.php" as they are harder to parse
        index = requests.get(index_url)
        links = re.findall("href=[\"\'](.*?)[\"\']", index.content.decode('utf-8'))
        links = [urllib.parse.urljoin(index_url, l) for l in links if 'pdf' in l]
        urls.extend(links)


    for url in urls:
        file = url.rsplit('/', 1)[1]
        if not os.path.exists(file):
            r = requests.get(url)
            if r.status_code != 200:
                continue
            with open(file, 'wb') as f:
                for chunk in r.iter_content(chunk_size=512 * 1024): 
                    if chunk: # filter out keep-alive new chunks
                        f.write(chunk)
        parsedPDF = parser.from_file(file)
        yield file, parsedPDF

def dav_files(url, username, password, ext="pdf"):
    from webdav3.client import Client
    options = {
    'webdav_hostname': url,
    'webdav_login':    username,
    'webdav_password': password
    }
    client = Client(options)
    files = reversed(client.list())
    for file in files:
        if ".{}".format(ext) not in file:
            continue
        if not os.path.exists(file):
            client.download_file(file, file)
        yield file


def get_next_numbers(content, *matches, debug=False):
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

# def merge(file, date, stats):
#     if date not in data:
#         data[date] = stats
#     else:
#         data[date] = tuple(existing if not new or (new and existing and new < existing) else new for existing,new in zip(data[date],stats))
#     print(" ".join([str(s) for s in (file, ) + data[date]]))

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
    m1 = re.search(r"([0-9]+)/([0-9]+)/([0-9]+) - ([0-9]+)/([0-9]+)/([0-9]+)", content)
    m2 = re.search(r"([0-9]+) *- *([0-9]+)/([0-9]+)/(25[0-9][0-9])", content)
    m3 = re.search(r"([0-9]+) *- *([0-9]+) *([^ ]+) *(25[0-9][0-9])", content)
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


def parse_pdf(filename, as_html=False):
    pages_txt = []

    # Read PDF file
    data = parser.from_file(filename, xmlContent=True)
    xhtml_data = BeautifulSoup(data['content'], features="lxml")
    for i, content in enumerate(xhtml_data.find_all('div', attrs={'class': 'page'})):
        # Parse PDF data using TIKA (xml/html)
        # It's faster and safer to create a new buffer than truncating it
        # https://stackoverflow.com/questions/4330812/how-do-i-clear-a-stringio-object
        _buffer = StringIO()
        _buffer.write(str(content))
        parsed_content = parser.from_buffer(_buffer.getvalue())

        # Add pages
        text = parsed_content['content'].strip()
        if as_html:
            pages_txt.append(repr(content))
        else:
            pages_txt.append(text)

    return pages_txt


def get_cases():
    timeline = requests.get('https://covid19.th-stat.com/api/open/timeline').json()['Data']
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
    for file in dav_files("http://nextcloud.dmsc.moph.go.th/public.php/webdav", "wbioWZAQfManokc", "null"):
        #parsedPDF = parser.from_file(file)

        #pages = parsedPDF['content'].split("\n\n\n\n") #  # วันที่ท้ำรำยงำน
        pages = parse_pdf(file)
        pages = [page for page in pages if "เริ่มเปิดบริการ" not in page]
        pages = [page for page in pages if "เขตสุขภาพ" in page or "เขตสุขภำพ" in page]

        for page in pages:
            start,end = find_date_range(page)
            if start is None:
                continue
            if '349585' in page:
                page = page.replace('349585', '349 585')
            if '16/10/2563' in page:
                print(page)
            _, page = page.split("\n", 1) # get rid of first line that sometimes as date and time in it
            numbers, content = get_next_numbers(
                page,
                "", # "ภาคเอกชน", 
                debug=True
                )
            # ภาครัฐ
            # ภาคเอกชน
            # จดัท ำโดย เพญ็พชิชำ ถำวงศ ์กรมวิทยำศำสตณก์ำรแพทย์        
            #print(numbers)
            pos = numbers[0:13]
            tests = numbers[13:26]
            r = list(daterange(start,end, offset=1))
            stats = [float(p)/len(r) for p in pos+tests+[sum(pos),sum(tests)]]
            if stats[-2] > 50000:
                pass
            results = pd.DataFrame([[date,]+stats for date in r], columns=columns).set_index('Date')
            print(results)
            data = data.combine_first(results)
    return data

def get_thai_situation():
    results = []
    for file, parsedPDF in all_pdfs("https://ddc.moph.go.th/viralpneumonia/situation.php"):
        if 'situation' not in file:
            continue
        date = file2date(file)
        numbers,content = get_next_numbers(
            parsedPDF['content'], 
            "ด่านโรคติดต่อระหว่างประเทศ",
            #"จำนวนการตรวจทางหอ้งปฏิบัติการ",
            )
        if not numbers:
            break
        # cases = None
        screened_port, screened_cw, tests_total, pui, active_finding, asq, not_pui, pui, pui_port, *rest  = numbers
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
        if pui in [1103858, 3891136, 433807, 96989]: #mistypes
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
    tests = pd.read_excel(file, index_col=0, parse_dates=True, usecols=[0,1,2])
    #row = tests[['Pos','Total']]['Cannot specify date'] 
    pos = tests.loc['Cannot specify date'].Pos 
    total = tests.loc['Cannot specify date'].Total
    tests.drop('Cannot specify date', inplace=True)
    # Need to redistribute the unknown values across known values
    all_pos = tests['Pos'].sum()
    all_total = tests['Total'].sum()
    for index, row in tests.iterrows():
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


en_situation = get_en_situation()
en_situation = en_situation - en_situation.shift(-1)
th_situation = get_thai_situation()
th_situation = th_situation - th_situation.shift(-1)
situation = en_situation.combine_first(th_situation)
df = situation
# df['Tested'] = df['Tested'] - df['Tested'].shift(1)
# df['PUI'] = df['PUI'] - df['PUI'].shift(1)
# df['ASQ'] = df['ASQ'] - df['ASQ'].shift(1)
# df['Not PUI'] = df['Not PUI'] - df['Not PUI'].shift(1)
# #df['Hospitals'] = df['Hospitals'] - df['Hospitals'].shift(1)
# df["Active case finding"] = df["Active case finding"] - df["Active case finding"].shift(1)

print(situation)
cases = get_cases()
print(cases)  
df = df.combine_first(cases)
print(df)
df = df.combine_first(get_tests_by_area())
print(df)
df = df.combine_first(tests)
print(df)

#df = pd.DataFrame(sorted(data.values()), columns=["Date","Tests", "PUI", "Active case finding", "ASQ", "Not PUI", "Cases"])
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
df['Positivity XLS (MA)'] = df['Pos XLS (MA)'] / df['Tests XLS (MA)'] * 100
df['Positivity Cases/Tests (MA)'] = df['Cases (MA)'] / df['Tests XLS (MA)'] * 100
#print(df.to_string())
df.plot(ax=ax, use_index=True, y=["Tested (MA)", "PUI (MA)", "Cases (MA)", "Tests Area (MA)", "Pos Area (MA)", "Pos XLS (MA)", "Tests XLS (MA)"], kind="line", figsize=[20,10], title="People Tested (7 day rolling average)")
plt.tight_layout()
plt.savefig("tests.png")

fig, ax = plt.subplots()
df.plot(ax=ax, use_index=True, y=["Positivity PUI (MA)", "Positivity Tested (MA)", "Positivity Area (MA)", "Positivity XLS (MA)", 'Positivity Cases/Tests (MA)'], kind="line", figsize=[20,10], title="Thailand Covid positivity (7day rolling average)")
plt.tight_layout()
plt.savefig("positivity.png")

fig, ax = plt.subplots()
df.plot(ax=ax, use_index=True, y=["Cases (MA)", "Pos Area (MA)", "Pos XLS (MA)", ], kind="line", figsize=[20,10], title="Positive Cases (7 day rolling average)")
plt.tight_layout()
plt.savefig("cases.png")

fig, ax = plt.subplots()
#df = df.cumsum()
df.plot(ax=ax, use_index=True, y=TESTS_AREA_COLS, kind="area", figsize=[20,10], title="Hospital Area Tests")
plt.tight_layout()
plt.savefig("tests_area.png")
