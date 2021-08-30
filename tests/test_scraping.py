import os
import fnmatch
from utils_thai import file2date

from bs4 import BeautifulSoup
from utils_scraping import parse_file, pptx2chartdata, sanitize_filename
from covid_data import briefing_deaths, briefing_deaths_provinces, briefing_documents, get_tests_by_area_chart_pptx, test_dav_files, vaccination_daily, vaccination_reports_files2, vaccination_tables, get_tests_by_area_pdf
import pandas as pd
import pytest
from utils_pandas import export, import_csv
import dateutil


# def write_csv(df, input, parser_name):
#     export(df, f"{input.rsplit('.', 1)[0]}.{parser_name}", csv_only=True)


# def find_files(dir, pat):
#     dir_path = os.path.dirname(os.path.realpath(__file__))
#     dir_path = os.path.join(dir_path, dir)
#     for root, dir, files in os.walk(dir_path):
#         for file in fnmatch.filter(files, pat):
#             base, ext = file.rsplit(".", 1)
#             testdf = None

#             csvs = fnmatch.filter(files, f"{base}*.csv")
#             if not csvs:
#                 yield os.path.join(root, file), testdf, None
#                 continue

#             for check in csvs:
#                 _, func, ext = check.rsplit(".", 2)
#                 try:
#                     testdf = import_csv(check.rsplit(".", 1)[0], dir=root, index=["Date"])
#                 except pd.errors.EmptyDataError:
#                     pass
#                 yield os.path.join(root, file), testdf, func


def dl_files(dir, dl_gen):
    "find csv files and match them to dl files, either by filename or date"
    dir_path = os.path.dirname(os.path.realpath(__file__))
    dir_path = os.path.join(dir_path, dir)
    downloads = {}
    for url, date, get_file in dl_gen():
        fname = sanitize_filename(url.rsplit("/", 1)[-1])
        fname, _ = fname.rsplit(".", 1)  # remove ext
        if date is not None:
            sdate = str(date.date())
            downloads[sdate] = (sdate, get_file)
        # put in file so test is identified if no date
        downloads[fname] = (str(date.date()) if date is not None else fname, get_file)

    for root, dir, files in os.walk(dir_path):
        for check in fnmatch.filter(files, "*.json"):
            base, ext = check.rsplit(".", 1)
            # special format of name with .2021-08-01 to help make finding test files easier
            if "." in base:
                rest, dateish = base.rsplit(".", 1)
                if file2date(dateish):
                    base = rest
                    # throw away date since rest is file to check against
            try:
                testdf = pd.read_json(os.path.join(root, check), orient="table")
            except ValueError:
                testdf = None
            # try:
            #     testdf = import_csv(check.rsplit(".", 1)[0], dir=root, index=["Date"])
            # except pd.errors.EmptyDataError:
            #     testdf = None
            date, get_file = downloads.get(base, (None, None))
            yield date, testdf, get_file


# @pytest.mark.parametrize("input, testdf, parser", find_files("testing_moph", "*.pptx"))
# def test_pptx(input, testdf, parser):
#     data = pd.DataFrame()
#     raw = pd.DataFrame()
#     for chart, title, series, pagenum in pptx2chartdata(input):
#         data, raw = get_tests_by_area_chart_pptx(input, title, series, data, raw)
#     pd.testing.assert_frame_equal(testdf, data)



# 021-07-05          0.0
# 2021-07-06          0.0
# 2021-07-07          0.0
# 2021-07-08          0.0
# 2021-07-09          0.0
# 2021-07-10          0.0
# 2021-07-11          0.0

@pytest.mark.parametrize("date, testdf, get_file", dl_files("vaccination_daily", vaccination_reports_files2))
def test_vac_reports(date, testdf, get_file):
    assert get_file is not None
    file = get_file()  # Actually download
    assert file is not None
    df = pd.DataFrame(columns=["Date"]).set_index(["Date"])
    for page in parse_file(file):
        df = vaccination_daily(df, None, file, page)
    # df.to_json(f"tests/vaccination_daily/{date}.json", orient='table', indent=2)
    pd.testing.assert_frame_equal(testdf, df)


def testing_pptx():
    return [(file, None, dl) for file, dl in test_dav_files(ext=".pptx")]


def testing_pdf():
    return [(file, None, dl) for file, dl in test_dav_files(ext=".pdf")]


@pytest.mark.parametrize("fname, testdf, dl", dl_files("testing_moph", testing_pptx))
def test_get_tests_by_area_chart_pptx(fname, testdf, dl):
    data, raw = pd.DataFrame(), pd.DataFrame()
    assert dl is not None
    file = dl()
    assert file is not None
    for chart, title, series, pagenum in pptx2chartdata(file):
        data, raw = get_tests_by_area_chart_pptx(input, title, series, data, raw)
    # raw.to_json(f"tests/testing_moph/{fname}.json", orient='table', indent=2)
    pd.testing.assert_frame_equal(testdf, raw, check_dtype=False)


@pytest.mark.parametrize("fname, testdf, dl", dl_files("testing_moph", testing_pdf))
def test_get_tests_by_area_chart_pdf(fname, testdf, dl):
    data, raw = pd.DataFrame(), pd.DataFrame()
    if fname is None:
        # It's a pptx that doesn't have pdf version
        return
    assert dl is not None
    file = dl()
    assert file is not None
    pages = parse_file(file, html=False, paged=True)
    for page in pages:
        data, raw = get_tests_by_area_pdf(file, page, data, raw)
    # raw.to_json(f"tests/testing_moph/{fname}.json", orient='table', indent=2)
    if testdf.index.max() >= dateutil.parser.parse("2021-08-08"):
        # plots stopped having numbers for positives so aren't scraped
        return
    pd.testing.assert_frame_equal(testdf, raw, check_dtype=False)


@pytest.mark.parametrize("date, testdf, dl", dl_files("briefing_deaths_provinces", briefing_documents))
def test_briefing_deaths_provinces(date, testdf, dl):
    dfprov = pd.DataFrame(columns=["Date", "Province"]).set_index(["Date", "Province"])
    assert dl is not None
    file = dl()
    assert file is not None

    pages = parse_file(file, html=True, paged=True)
    pages = [BeautifulSoup(page, 'html.parser') for page in pages]

    for i, soup in enumerate(pages):
        text = soup.get_text()
        df = briefing_deaths_provinces(text, dateutil.parser.parse(date), file)
        dfprov = dfprov.combine_first(df)
    # dfprov.to_json(f"tests/briefing_deaths_provinces/{date}.json", orient='table', indent=2)
    pd.testing.assert_frame_equal(testdf, dfprov)
