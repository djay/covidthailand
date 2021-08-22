import os
import fnmatch
from utils_scraping import pptx2chartdata
from covid_data import get_tests_by_area_chart_pptx
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
                    testdf = import_csv(check.rsplit(".", 1)[0], dir=root)
                except pd.errors.EmptyDataError:
                    pass
                yield os.path.join(root, file), testdf, func


@pytest.mark.parametrize("input, testdf, parser", find_files("testing_moph", "*.pptx"))
def test_pptx(input, testdf, parser):
    data = pd.DataFrame()
    raw = pd.DataFrame()
    for chart, title, series, pagenum in pptx2chartdata(input):
        data, raw = get_tests_by_area_chart_pptx(input, title, series, data, raw)
    pd.testing.assert_frame_equal(testdf, data)
