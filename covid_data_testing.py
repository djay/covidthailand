import datetime
import os
import re

import numpy as np
import pandas as pd
import requests
from dateutil.parser import parse as d

from utils_pandas import daterange
from utils_pandas import export
from utils_pandas import import_csv
from utils_pandas import spread_date_range
from utils_scraping import all_in
from utils_scraping import any_in
from utils_scraping import camelot_cache
from utils_scraping import get_next_numbers
from utils_scraping import local_files
from utils_scraping import logger
from utils_scraping import parse_file
from utils_scraping import pptx2chartdata
from utils_scraping import USE_CACHE_DATA
from utils_scraping import web_files
from utils_thai import find_date_range
from utils_thai import POS_COLS
from utils_thai import TEST_COLS


##########################################
# Testing data
##########################################

# def get_test_dav_files(url="http://nextcloud.dmsc.moph.go.th/public.php/webdav",
#                        username="wbioWZAQfManokc",
#                        password="null",
#                        ext=".pdf .pptx",
#                        dir="inputs/testing_moph"):
#     return dav_files(url, username, password, ext, dir)


def get_drive_files(folder_id, ext="pdf", dir="inputs/testing_moph", check=True):
    key = os.environ.get('DRIVE_API_KEY', None)
    if key is None or not check:
        logger.warning("env DRIVE_API_KEY missing: Using local cached testing data only")
        yield from local_files(ext, dir)
        return
    url = f"https://www.googleapis.com/drive/v3/files?q=%27{folder_id}%27+in+parents&key={key}"
    res = requests.get(url).json()
    if "files" not in res:
        logger.warning(f"Error accessing drive files. Using local cache: {res}")
        yield from local_files(ext, dir)
        return
    files = res["files"]
    while "nextPageToken" in res:
        token = res['nextPageToken']
        url = f"https://www.googleapis.com/drive/v3/files?q=%27{folder_id}%27+in+parents&key={key}&pageToken={token}"
        res = requests.get(url).json()
        if "files" not in res:
            logger.error(str(res))
            break
        files.extend(res["files"])

    count = 0
    for data in files:
        id = data['id']
        name = data['name']
        # TODO: how to get modification date?
        if not name.endswith(ext):
            continue
        count += 1
        target = os.path.join(dir, name)
        os.makedirs(os.path.dirname(target), exist_ok=True)

        def get_file(id=id, name=name):
            url = f"https://www.googleapis.com/drive/v2/files/{id}?alt=media&key={key}"
            file, _, _ = next(iter(web_files(url, dir=dir, filenamer=lambda url, _: name)))
            return file

        yield target, get_file

    if count == 0:
        logger.warning("No files found: Using local cached testing data only")
        yield from local_files(ext, dir)
        return


def get_test_files(ext="pdf", dir="inputs/testing_moph", check=True):
    folder_id = "1yUVwstf5CmdvBVtKBs0uReV0BTbjQYlT"
    yield from get_drive_files(folder_id, ext, dir, check=check)


def get_variant_files(ext=".pdf", dir="inputs/variants", check=True):
    # https://drive.google.com/drive/folders/13k14Hs61pgrK8raSMS9LFQn83PKswS-b
    folder_id = "13k14Hs61pgrK8raSMS9LFQn83PKswS-b"
    yield from get_drive_files(folder_id, ext=ext, dir=dir, check=check)


def get_tests_by_day():

    def from_reports():
        df = pd.DataFrame()
        files = ""
        missing = "Thailand_COVID-19_ATK_data-update-20220604.xlsx"  # Until they bring it back, get from local cache
        for file, dl in list(get_test_files(ext="xlsx")) + list(get_test_files(ext=missing)):
            dl()
            tests = pd.read_excel(file, parse_dates=True, usecols=[0, 1, 2])
            if "ATK" in file:
                tests = tests.rename(columns={"approve date": "Date", "countPositive": "Pos ATK", "total": "Tests ATK"})
            else:
                tests.rename(columns={'Pos': "Pos XLS", 'Total': "Tests XLS"}, inplace=True)
                tests["Tests ATK"] = np.nan
                tests["Pos ATK"] = np.nan
            tests = tests.drop(tests[tests['Date'].isna()].index)  # get rid of totals row
            tests = tests.iloc[1:]  # Get rid of first row with unspecified data
            tests['Date'] = pd.to_datetime(tests['Date'], dayfirst=True)
            tests = tests.set_index("Date")
            # tests.drop("Cannot specify date", inplace=True)
            files += " " + file
            df = df.combine_first(tests)
        return files, df

    def from_data():
        url = "https://data.go.th/dataset/9f6d900f-f648-451f-8df4-89c676fce1c4/resource/0092046c-db85-4608-b519-ce8af099315e/download/thailand_covid-19_testing_data_update091064.csv"  # NOQA
        file, _, _ = next(iter(web_files(url, dir="inputs/testing_moph")))
        tests = pd.read_csv(file, parse_dates=True, usecols=[0, 1, 2])
        return file, tests.rename(columns={'positive': "Pos", 'Total Testing': "Total"})

    file, tests = from_reports()
    if file is None:
        return tests

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
    if not any_in(page, "เขตสุขภาพ", "เขตสุขภำพ"):
        return data, raw
    elif any_in(page, "เริ่มเปิดบริการ", "90%"):
        return data, raw
    start, end = find_date_range(page)
    if start is None:
        return data, raw

    # Can't parse '35_21_12_2020_COVID19_(ถึง_18_ธันวาคม_2563)(powerpoint).pptx' because data is a graph
    # no pdf available so data missing
    # Also missing 14-20 Nov 2020 (no pptx or pdf)

    page = page.replace(
        "349585", "349 585").replace(
        "4869151.1", "48691 51.1").replace(
        "6993173.8", "69931 73.8").replace(
        "988114.3", "9881 14.3").replace(
        "2061119828", "2061 119828").replace(
        "9881 14.3", "98811 4.3").replace(
        "2061 119828", "20611 19828")
    # First line can be like จดัท ำโดย เพญ็พชิชำ ถำวงศ ์กรมวิทยำศำสตณก์ำรแพทย์ วันที่ท ำรำยงำน 15/02/2564 เวลำ 09.30 น.
    first, rest = page.split("\n", 1)
    page = (
        rest if "เพญ็พชิชำ" in first or "/" in first else page
    )  # get rid of first line that sometimes as date and time in it
    numbers, _ = get_next_numbers(page, "", debug=True, ints=False)  # "ภาคเอกชน",
    # ภาครัฐ
    # ภาคเอกชน
    # จดัท ำโดย เพญ็พชิชำ ถำวงศ ์กรมวิทยำศำสตณก์ำรแพทย์
    # print(numbers)
    # TODO: should really find and parse X axis labels which contains 'เขต' and count
    tests_start = 13 if "total" not in page else 14
    pos = list(map(int, numbers[0:13]))
    # assert all([p < 500000 for p in pos])
    tests = numbers[tests_start:tests_start + 13]
    assert tests == list(map(int, tests))  # last number sometimes is joined to next %
    tests = list(map(int, tests))
    pos_rate = numbers[tests_start + 13: tests_start + 26]
    if start > d("2020-12-05"):
        assert all([r <= 100 for r in pos_rate])
        assert all([round(p / t * 100, 1) == r for p, t, r in zip(pos, tests, pos_rate)])  # double check we got right values

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

    # Also need pdf copies because of missing pptx
    raw_pdf = pd.DataFrame()
    for file, dl in get_test_files(ext=".pdf"):
        dl()
        pages = parse_file(file, html=False, paged=True)
        for page in pages:
            data, raw_pdf = get_tests_by_area_pdf(file, page, data, raw_pdf)

    # pptx less prone to scraping issues so should be trusted more
    raw_ppt = pd.DataFrame()
    for file, dl in get_test_files(ext=".pptx"):
        dl()
        for chart, title, series, pagenum in pptx2chartdata(file):
            data, raw_ppt = get_tests_by_area_chart_pptx(file, title, series, data, raw_ppt)
            if not all_in(pubpriv.columns, 'Tests', 'Tests Private'):
                # Latest file as all the data we need
                pubpriv = get_tests_private_public_pptx(file, title, series, pubpriv)
        assert not data.empty
        # TODO: assert for pubpriv too. but disappeared after certain date

    export(raw.combine_first(raw_ppt).combine_first(raw_pdf), "tests_by_area")

    pubpriv['Pos Public'] = pubpriv['Pos'] - pubpriv['Pos Private']
    pubpriv['Tests Public'] = pubpriv['Tests'] - pubpriv['Tests Private']
    export(pubpriv, "tests_pubpriv")

    data = data.combine_first(pubpriv)

    return data


def get_variants_by_area_pdf(file, page, page_num):
    if "frequency distribution" not in page:
        return pd.DataFrame()
    df = camelot_cache(file, page_num + 1, process_background=False)
    if len(df.columns) == 13:
        totals = df[[2, 5, 8, 11]]  # only want this week not whole year
    elif len(df.columns) == 16:  # 2022-06-24 switched to inc BA4/5
        totals = df[[1, 2, 3, 5, 8, 11, 14]]
    elif len(df.columns) == 19:  # 2022-08-18 add BA.2.75
        # TODO: inc BA.2.75/BA2.76
        totals = df[[1, 2, 3, 5, 8, 11, 14, 17]]
    # TODO: get totals from previous week also since report is sometimes missing

    else:
        assert False, "Unknown Area Variant table"
    cols = [v.replace("Potentially ", "").replace("\n", "") for v in df.iloc[1] if v]
    totals.columns = cols

    assert len(df) == 17
    totals = totals.iloc[3:16]

    totals = totals.replace(r"([0-9]*) \+ \([0-9]\)", r"\1", regex=True).apply(pd.to_numeric)

    totals["Health Area"] = range(1, 14)

    # start, end = find_date_range(page) whole year
    # start, end = find_date_range(df.iloc[2][2])  # e.g. '26 FEB – 04 \nMAR 22'
    date_range = list(df.iloc[2])[-2]  # Last is total, 2nd last is this date range
    start_txt, end_txt = re.split("(?:–|-)", date_range)
    end = pd.to_datetime(end_txt)
    start = pd.to_datetime(f"{start_txt} {end.strftime('%B')} {end.year}", dayfirst=True, errors="coerce")
    if pd.isnull(start):
        # Start includes the month
        start = pd.to_datetime(f"{start_txt} {end.year}", dayfirst=True, errors="coerce")
    assert not pd.isnull(start)

    totals["Start"] = start
    totals["End"] = end

    if "BA.2 (Omicron)" in totals.columns:
        # HACK: Cols are totals not daily. Hack since they are likely 0
        # TODO: return these seperate and work out diffs in case any were added
        totals[["B.1.1.7 (Alpha)", "B.1.351 (Beta)", "B.1.617.2 (Delta)"]] = 0

    return totals.set_index("End")


def get_variants_plot_pdf(file, page, page_num):
    if "National prevalence" not in page:
        return pd.DataFrame()
    # national = camelot_cache(file, page_num + 1, process_background=False, table=2)
    # none of the other tables work with camelot

    def splitline(line):
        v, line = line.split(")")
        return [v + ")"] + get_next_numbers(line, return_rest=False)

    rows = [splitline(line) for line in page.split("\n") if line.startswith("B.1.")]
    bangkok = pd.DataFrame(rows[4:8]).transpose()
    bangkok.columns = bangkok.iloc[0]
    bangkok = bangkok.iloc[1:]
    return bangkok


def get_variant_sequenced_table(file, pages):
    fileseq = pd.DataFrame()
    first_seq_table = None
    for page_num, page in enumerate(pages):
        if "Prevalence of Pangolin lineages" not in page:
            continue
        df = camelot_cache(file, page_num + 1, process_background=False)
        if "Total" in df[0].iloc[-1]:
            # Vertical
            df = df.transpose()
        # clean up "13 MAY 2022\nOther BA.2" , "BA.2.27\n13 MAY 2022"
        df.iloc[0] = df.iloc[0].str.replace(r" \(.*\)", "", regex=True)
        df.iloc[0] = df.iloc[0].str.replace(r"(.*2022\n|\n.*2022)", r"", regex=True)
        # Some columns get combined. e.g.
        df.iloc[0] = [c for c in sum(df.iloc[0].str.replace(" Other ", "| Other ").str.split("|").tolist(), []) if c]
        if "20220715" in file and df.iloc[0, 3] == "Other BA.2":
            # Other BA.2 is there twice. One is wrong
            df.iloc[0, 3] = "Other BA.2.9"
        elif "20220610" in file and page_num == 11:
            df.iloc[1, 0] = "w126"
            df.iloc[2, 0] = "w127"
        df.columns = df.iloc[0]
        df = df.iloc[1:]
        # Convert week number to a date
        # Need to handle when two weeks have ended up in one cell
        weeks = df["Lineage"].astype(str).str.replace("w", "").str.replace(
            "W", "").str.split(" ", expand=True).stack().reset_index()[0]
        df["Lineage"] = list(pd.to_numeric(weeks).dropna())
        df['End'] = (df['Lineage'] * 7).apply(lambda x: pd.DateOffset(x) + d("2019-12-27"))
        df = df.set_index("End")
        df = df.drop(columns=["Total Sequences", "Lineage"])
        # TODO: Ensure Other is always counted in rest of numbers. so far seems to
        # df = df.drop(columns=[c for c in df.columns if "Other BA" in c])
        df = df.apply(pd.to_numeric)
        # get rid of mistake duplicate columns - 14_20220610_DMSc_Variant.pdf
        df = df.loc[:, ~df.columns.duplicated()].copy()
        df.columns = [c.strip("*") for c in df.columns]
        # match = re.search("Thailand with (.*)sequence data", page)
        # if match and match.group(1) in ["", "Omicron"]:
        #     # first table. Ignore others so no double counted
        #     # 2022-06-24 - Other=5 in first table but this is BA4/5 in next table
        #     # 2022-07-15 - Other=1 in first table but not represented in other tables
        #     # TODO: need more general method
        #     df = df.drop(columns=[c for c in df.columns if "Other" in c])
        seq_table = df

        # The first table should include all the numbers from the next more detailed tables
        if first_seq_table is None and not seq_table.empty:
            first_seq_table = seq_table
        else:
            fileseq = seq_table.combine_first(fileseq)
    # HACK: Most reports have only 2 rows. BA.1 data is from older time periods. Just keep last 2 and let
    # older reports include that data so we get the full row
    fileseq = fileseq.iloc[-2:]

    # Check sub tables add up to the initial table. If not these are our "others"
    exceptions = ["Other BA.5"] if "20220715" in file else []
    fileseq = fileseq.drop(columns=[c for c in fileseq.columns if "Other" in c and c not in exceptions])
    if first_seq_table is None:
        return fileseq
    others = first_seq_table.sum(axis=1) - fileseq.sum(axis=1)
    if any_in(file, "20220715", "20220610"):
        # Other doesn't add up. 2 is a mistake?
        # 2022-05-07 is 1 out in 20220610
        pass
    elif any_in(file, "20220708", "20220701", "20220627"):
        # BA4/5 are the "Other" in the first table but counted later on
        pass
    elif any_in(file, '20220916', '20221021', '20221125'):
        # 20220916: "Other B" doesn't seem to appear in any later tables?
        # 20221021: TODO: seems like mix between two sets of weeks? 142/143, 143/144
        # 20221125: BQ.X should be combined with Other to make 7?
        pass
    else:
        assert others.sum() == 0 or (first_seq_table['Other'] == others).all()
    fileseq['Other'] = others
    return fileseq


def get_variant_reports():
    # TODO: historical variant data 2021 is in https://tncn.dmsc.moph.go.th/

    sequenced = pd.DataFrame()
    raw = import_csv("variants", ["Start"], not USE_CACHE_DATA, date_cols=["Start", "End"])
    area = import_csv("variants_by_area", ["Start"], not USE_CACHE_DATA, date_cols=["Start", "End"])

    # Get national numbers. Also gives us date ranges
    for file, dl in get_variant_files(ext=".xlsx"):
        file = dl()
        nat = pd.read_excel(file)
        nat.iloc[0, 0] = "End"
        nat.columns = list(nat.iloc[0])
        nat = nat.iloc[1:-1]  # Get rid of header and totals at the bottom
        # nat = nat[[c for c in nat.columns if not pd.isna(c)]]  # get rid of empty cols
        dates = nat["End"].str.split("-", expand=True)
        a = pd.to_datetime(dates[1], errors="coerce", format="%d %b") + pd.offsets.DateOffset(years=121)
        b = pd.to_datetime(dates[1], errors="coerce", format="%d%b") + pd.offsets.DateOffset(years=121)
        c = pd.to_datetime(dates[1], errors="coerce")
        ends = a.combine_first(b).combine_first(c)
        nat["End"] = ends
        nat = nat.set_index("End")
        # There is now 3 lots of numbers. pick the last set?
        nat = nat.iloc[:, 8:12]
        nat = nat.rename(columns={"B.1617.2 (Delta)": "B.1.617.2 (Delta)", "B.1.1.529 (Omicron": "B.1.1.529 (Omicron)"})
        break

    for file, dl in get_variant_files(ext=".pdf"):
        dl()
        pages = parse_file(file, html=False, paged=True)
        # page 1 title
        # page 2 people + sample sizes
        # page 3 table year + week per variant (4) per district
        # page 4 pie charts national + bangkok + regional
        # page 5 area chart: weekly national, bangkok, regional
        # page 6 samples submitted GSAID: weekly
        for page_num, page in enumerate(pages):
            bangkok = get_variants_plot_pdf(file, page, page_num)
            if not bangkok.empty:
                # dates from pdf too hard to parse so assume as as xslx
                # TODO: date ranges don't line up so can't do this
                bangkok.index = nat.index[:len(bangkok.index)]
            area = area.combine_first(get_variants_by_area_pdf(file, page, page_num))
        fileseq = get_variant_sequenced_table(file, pages)

        # Later files can update prev reports
        sequenced = pd.concat([sequenced, fileseq]).reset_index().drop_duplicates(subset="End").set_index("End").sort_index()

    # TODO: variants_by_area is now totals but we can convert it to weekly if esp if we get the seed totals in the earliest report
    # area_grouped = area.groupby(["Start", "End"]).sum()
    export(nat, "variants")
    export(area, "variants_by_area")
    export(sequenced, "variants_sequenced")

    return nat


if __name__ == '__main__':
    variants = get_variant_reports()
    df_daily = get_tests_by_day()
    df = get_test_reports()
