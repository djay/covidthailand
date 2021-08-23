import os
import fnmatch
from utils_scraping import parse_file, pptx2chartdata, sanitize_filename
from covid_data import get_tests_by_area_chart_pptx, vaccination_daily, vaccination_reports_files, vaccination_tables
import pandas as pd
import pytest
from utils_pandas import export, import_csv


def write_csv(df, input, parser_name):
    export(df, f"{input.rsplit('.', 1)[0]}.{parser_name}", csv_only=True)


def find_files(dir, pat):
    dir_path = os.path.dirname(os.path.realpath(__file__))
    dir_path = os.path.join(dir_path, dir)
    for root, dir, files in os.walk(dir_path):
        for file in fnmatch.filter(files, pat):
            base, ext = file.rsplit(".", 1)
            testdf = None

            csvs = fnmatch.filter(files, f"{base}*.csv")
            if not csvs:
                yield os.path.join(root, file), testdf, None
                continue

            for check in csvs:
                _, func, ext = check.rsplit(".", 2)
                try:
                    testdf = import_csv(check.rsplit(".", 1)[0], dir=root, index=["Date"])
                except pd.errors.EmptyDataError:
                    pass
                yield os.path.join(root, file), testdf, func


def dl_files(dir, dl_gen):
    "find csv files and match them to dl files, either by filename or date"
    dir_path = os.path.dirname(os.path.realpath(__file__))
    dir_path = os.path.join(dir_path, dir)
    tests = dict()
    for root, dir, files in os.walk(dir_path):
        for check in fnmatch.filter(files, "*.csv"):
            testdf = None
            base, ext = check.rsplit(".", 1)
            try:
                testdf = import_csv(check.rsplit(".", 1)[0], dir=root, index=["Date"])
            except pd.errors.EmptyDataError:
                pass
            tests[base] = testdf
    for url, date, get_file in dl_gen():
        fname = sanitize_filename(url.rsplit("/", 1)[-1])
        fname, _ = fname.rsplit(".", 1)
        sdate = str(date.date())
        if sdate in tests:
            testdf = tests[sdate]
        elif fname in tests:
            testdf = tests[fname]
        else:
            continue
        yield get_file, date, testdf


@pytest.mark.parametrize("input, testdf, parser", find_files("testing_moph", "*.pptx"))
def test_pptx(input, testdf, parser):
    data = pd.DataFrame()
    raw = pd.DataFrame()
    for chart, title, series, pagenum in pptx2chartdata(input):
        data, raw = get_tests_by_area_chart_pptx(input, title, series, data, raw)
    pd.testing.assert_frame_equal(testdf, data)



# 021-07-05          0.0
# 2021-07-06          0.0
# 2021-07-07          0.0
# 2021-07-08          0.0
# 2021-07-09          0.0
# 2021-07-10          0.0
# 2021-07-11          0.0

@pytest.mark.parametrize("get_file, date, testdf", dl_files("vaccination_daily", vaccination_reports_files))
def test_vac_reports(get_file, date, testdf):
    file = get_file()  # Actually download
    assert file is not None
    assert testdf is not None
    df = pd.DataFrame(columns=["Date"]).set_index(["Date"])
    for page in parse_file(file):
        df = vaccination_daily(df, date, file, page)
    pd.testing.assert_frame_equal(testdf, df)
    
