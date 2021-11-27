import datetime
import os
import pandas as pd
import requests
from utils_pandas import daterange, export, import_csv, spread_date_range
from utils_scraping import USE_CACHE_DATA, any_in, get_next_numbers, \
    parse_file, pptx2chartdata, web_files, all_in, logger, local_files
from utils_thai import find_date_range, POS_COLS, TEST_COLS


##########################################
# Testing data
##########################################

# def get_test_dav_files(url="http://nextcloud.dmsc.moph.go.th/public.php/webdav",
#                        username="wbioWZAQfManokc",
#                        password="null",
#                        ext=".pdf .pptx",
#                        dir="inputs/testing_moph"):
#     return dav_files(url, username, password, ext, dir)


def get_test_files(ext="pdf", dir="inputs/testing_moph"):
    key = os.environ.get('DRIVE_API_KEY', None)
    if key is None:
        logger.warning("env DRIVE_API_KEY missing: Using local cahced testing data only")
        yield from local_files(ext, dir)
        return
    folder_id = "1yUVwstf5CmdvBVtKBs0uReV0BTbjQYlT"
    url = f"https://www.googleapis.com/drive/v3/files?q=%27{folder_id}%27+in+parents&key={key}"
    res = requests.get(url).json()
    for data in res['files']:
        id = data['id']
        name = data['name']
        # TODO: how to get modification date?
        if not name.endswith(ext):
            continue
        target = os.path.join(dir, name)
        os.makedirs(os.path.dirname(target), exist_ok=True)

        def get_file(id=id, name=name):
            url = f"https://www.googleapis.com/drive/v2/files/{id}?alt=media&key={key}"
            file, _, _ = next(iter(web_files(url, dir="inputs/testing_moph", filenamer=lambda url, _: name)))
            return file

        yield target, get_file


def get_tests_by_day():
    logger.info("========Tests by Day==========")

    def from_reports():
        file, dl = next(get_test_files(ext="xlsx"))
        dl()
        tests = pd.read_excel(file, parse_dates=True, usecols=[0, 1, 2])
        tests.dropna(how="any", inplace=True)  # get rid of totals row
        # tests.drop("Cannot specify date", inplace=True)
        tests = tests.iloc[1:]  # Get rid of first row with unspecified data
        return file, tests

    def from_data():
        url = "https://data.go.th/dataset/9f6d900f-f648-451f-8df4-89c676fce1c4/resource/0092046c-db85-4608-b519-ce8af099315e/download/thailand_covid-19_testing_data_update091064.csv"  # NOQA
        file, _, _ = next(iter(web_files(url, dir="inputs/testing_moph")))
        tests = pd.read_csv(file, parse_dates=True, usecols=[0, 1, 2])
        return file, tests.rename(columns={'positive': "Pos", 'Total Testing': "Total"})

    file, tests = from_reports()
    tests['Date'] = pd.to_datetime(tests['Date'], dayfirst=True)
    tests = tests.set_index("Date")

    def redistribute(tests):
        pos = tests[tests["Date"] == "Cannot specify date"]['Pos']
        total = tests[tests["Date"] == "Cannot specify date"]['Total']
        tests = tests[tests["Date"] != "Cannot specify date"]

        # Need to redistribute the unknown values across known values
        # Documentation tells us it was 11 labs and only before 3 April
        unknown_end_date = datetime.datetime(day=3, month=4, year=2020)
        all_pos = tests["positive"][:unknown_end_date].sum()
        all_total = tests["Total Testing"][:unknown_end_date].sum()
        for index, row in tests.iterrows():
            if index > unknown_end_date:
                continue
            row['positive'] = float(row['positive']) + row['positive'] / all_pos * pos
            row['Total Testing'] = float(row['Total Testing']) + row['Total Testing'] / all_total * total
        # TODO: still doesn't redistribute all missing values due to rounding. about 200 left
        # print(tests["Pos"].sum(), pos + all_pos)
        # print(tests["Total"].sum(), total + all_total)
        # fix datetime
        # tests.reset_index(drop=False, inplace=True)
        return tests

    tests.rename(columns={'Pos': "Pos XLS", 'Total': "Tests XLS"}, inplace=True)
    logger.info("{} {}", file, len(tests))

    return tests


def get_tests_by_area_chart_pptx(file, title, series, data, raw):
    start, end = find_date_range(title)
    if start is None or "เริ่มเปิดบริการ" in title or not any_in(title, "เขตสุขภาพ", "เขตสุขภำพ"):
        return data, raw

    # the graph for X period split by health area.
    # Need both pptx and pdf as one pdf is missing
    if "จำนวนผลบวก" not in series:
        # 2021-08-24 they added another graph with %
        return data, raw
    pos = list(series["จำนวนผลบวก"])
    tests = list(series["จำนวนตรวจ"])
    row = pos + tests + [sum(pos), sum(tests)]
    results = spread_date_range(start, end, row, ["Date"] + POS_COLS + TEST_COLS + ["Pos Area", "Tests Area"])
    # print(results)
    data = data.combine_first(results)
    raw = raw.combine_first(pd.DataFrame(
        [[start, end, ] + pos + tests],
        columns=["Start", "End", ] + POS_COLS + TEST_COLS
    ).set_index("Start"))
    logger.info("Tests by Area {} - {} {}", start.date(), end.date(), file)
    return data, raw


def get_tests_by_area_pdf(file, page, data, raw):
    start, end = find_date_range(page)
    if start is None or any_in(page, "เริ่มเปิดบริการ", "90%") or not any_in(page, "เขตสุขภาพ", "เขตสุขภำพ"):
        return data, raw
    # Can't parse '35_21_12_2020_COVID19_(ถึง_18_ธันวาคม_2563)(powerpoint).pptx' because data is a graph
    # no pdf available so data missing
    # Also missing 14-20 Nov 2020 (no pptx or pdf)

    if "349585" in page:
        page = page.replace("349585", "349 585")
    # First line can be like จดัท ำโดย เพญ็พชิชำ ถำวงศ ์กรมวิทยำศำสตณก์ำรแพทย์ วันที่ท ำรำยงำน 15/02/2564 เวลำ 09.30 น.
    first, rest = page.split("\n", 1)
    page = (
        rest if "เพญ็พชิชำ" in first or "/" in first else page
    )  # get rid of first line that sometimes as date and time in it
    numbers, _ = get_next_numbers(page, "", debug=True)  # "ภาคเอกชน",
    # ภาครัฐ
    # ภาคเอกชน
    # จดัท ำโดย เพญ็พชิชำ ถำวงศ ์กรมวิทยำศำสตณก์ำรแพทย์
    # print(numbers)
    # TODO: should really find and parse X axis labels which contains 'เขต' and count
    tests_start = 13 if "total" not in page else 14
    pos = numbers[0:13]
    tests = numbers[tests_start:tests_start + 13]
    row = pos + tests + [sum(pos), sum(tests)]
    results = spread_date_range(start, end, row, ["Date"] + POS_COLS + TEST_COLS + ["Pos Area", "Tests Area"])
    data = data.combine_first(results)
    raw = raw.combine_first(pd.DataFrame(
        [[start, end, ] + pos + tests],
        columns=["Start", "End", ] + POS_COLS + TEST_COLS
    ).set_index("Start"))
    logger.info("Tests by Area {} - {} {}", start.date(), end.date(), file)
    return data, raw


def get_tests_private_public_pptx(file, title, series, data):
    start, end = find_date_range(title)
    if start is None:
        return data
    elif "เริ่มเปิดบริการ" not in title and any_in(title, "เขตสุขภาพ", "เขตสุขภำพ"):
        # It's a by area chart
        return data
    elif not ("และอัตราการตรวจพบ" in title and "รายสัปดาห์" not in title and "จำนวนตรวจ" in series):
        return data

    # The graphs at the end with all testing numbers private vs public
    private = " Private" if "ภาคเอกชน" in title else ""

    # pos = series["Pos"]
    tests = series["จำนวนตรวจ"]
    positivity = series["% Detection"]
    dates = list(daterange(start, end, 1))
    df = pd.DataFrame(
        {
            "Date": dates,
            f"Tests{private}": tests,
            f"% Detection{private}": positivity,
        }
    ).set_index("Date")
    df[f"Pos{private}"] = (
        df[f"Tests{private}"] * df[f"% Detection{private}"] / 100.0
    )
    logger.info("Tests {} {} - {} {}", private, start.date(), end.date(), file)
    return data.combine_first(df)


def get_test_reports():
    data = pd.DataFrame()
    raw = import_csv("tests_by_area", ["Start"], not USE_CACHE_DATA, date_cols=["Start", "End"])
    pubpriv = import_csv("tests_pubpriv", ["Date"], not USE_CACHE_DATA)

    for file, dl in get_test_files(ext=".pptx"):
        dl()
        for chart, title, series, pagenum in pptx2chartdata(file):
            data, raw = get_tests_by_area_chart_pptx(file, title, series, data, raw)
            if not all_in(pubpriv.columns, 'Tests', 'Tests Private'):
                # Latest file as all the data we need
                pubpriv = get_tests_private_public_pptx(file, title, series, pubpriv)
        assert not data.empty
        # TODO: assert for pubpriv too. but disappeared after certain date
    # Also need pdf copies because of missing pptx
    for file, dl in get_test_files(ext=".pdf"):
        dl()
        pages = parse_file(file, html=False, paged=True)
        for page in pages:
            data, raw = get_tests_by_area_pdf(file, page, data, raw)
    export(raw, "tests_by_area")

    pubpriv['Pos Public'] = pubpriv['Pos'] - pubpriv['Pos Private']
    pubpriv['Tests Public'] = pubpriv['Tests'] - pubpriv['Tests Private']
    export(pubpriv, "tests_pubpriv")
    data = data.combine_first(pubpriv)

    return data
