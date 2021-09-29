import os
import fnmatch
from utils_thai import file2date

from bs4 import BeautifulSoup
from utils_scraping import parse_file, pptx2chartdata, sanitize_filename
from covid_data import briefing_case_types, briefing_deaths, briefing_deaths_provinces, briefing_deaths_summary, briefing_documents, get_tests_by_area_chart_pptx, get_thai_situation_files, situation_pui_th, get_test_dav_files, vac_briefing_totals, vac_manuf_given, vac_slides_files, vaccination_daily, vaccination_reports_files2, vaccination_tables, get_tests_by_area_pdf
import pandas as pd
import pytest
from utils_pandas import export, import_csv
import dateutil


def dl_files(target_dir, dl_gen, check=False):
    "find csv files and match them to dl files, either by filename or date"
    dir_path = os.path.dirname(os.path.realpath(__file__))
    dir_path = os.path.join(dir_path, target_dir)
    downloads = {}
    for url, date, get_file in dl_gen(check):
        fname = sanitize_filename(url.rsplit("/", 1)[-1])
        fname, _ = fname.rsplit(".", 1)  # remove ext
        if date is not None:
            sdate = str(date.date())
            downloads[sdate] = (sdate, get_file)
        # put in file so test is identified if no date
        downloads[fname] = (str(date.date()) if date is not None else fname, get_file)
    tests = []
    missing = False
    for root, dir, files in os.walk(dir_path):
        for test in fnmatch.filter(files, "*.json"):
            base, ext = test.rsplit(".", 1)
            # special format of name with .2021-08-01 to help make finding test files easier
            if "." in base:
                rest, dateish = base.rsplit(".", 1)
                if file2date(dateish):
                    base = rest
                    # throw away date since rest is file to check against
            try:
                testdf = pd.read_json(os.path.join(root, test), orient="table")
            except (ValueError, TypeError):
                testdf = None
            # try:
            #     testdf = import_csv(check.rsplit(".", 1)[0], dir=root, index=["Date"])
            # except pd.errors.EmptyDataError:
            #     testdf = None
            date, get_file = downloads.get(base, (None, None))
            if get_file is None:
                if check:
                    raise Exception(f"Can't match test file {dir_path}/{test} to any downloadable file")
                missing = True
            tests.append((date, testdf, get_file))
    if missing and not check:
        # files not cached yet so try again
        return dl_files(target_dir, dl_gen, check=True)
    else:
        return tests


def write_scrape_data_back_to_test(df, dir, fname=None, date=None):
    "Use this when you are sure the scraped data is correct"
    if fname is not None:
        *_, fname = fname.rsplit("/", 1)
        fname, *_ = fname.rsplit(".", 1)  # get rid of extension
    if date is None:
        date = str(df.index.max().date())
    else:
        date = str(date.date())
    if fname:
        # .{date} is ignored but helps to have when fname doesn't have date in it
        df.to_json(f"tests/{dir}/{fname}.{date}.json", orient='table', indent=2)
    else:
        df.to_json(f"tests/{dir}/{date}.json", orient='table', indent=2)


# 2021-07-05          0.0
# 2021-07-06          0.0
# 2021-07-07          0.0
# 2021-07-08          0.0
# 2021-07-09          0.0
# 2021-07-10          0.0
# 2021-07-11          0.0


@pytest.mark.parametrize("fname, testdf, get_file", dl_files("vaccination_daily", vaccination_reports_files2))
def test_vac_reports(fname, testdf, get_file):
    assert get_file is not None
    file = get_file()  # Actually download
    assert file is not None
    df = pd.DataFrame(columns=["Date"]).set_index(["Date"])
    for page in parse_file(file):
        df = vaccination_daily(df, None, file, page)
    # write_scrape_data_back_to_test(df, "vaccination_daily", fname)
    pd.testing.assert_frame_equal(testdf, df, check_dtype=False)


@pytest.mark.parametrize("fname, testdf, get_file", dl_files("vaccination_tables", vaccination_reports_files2))
def test_vac_tables(fname, testdf, get_file):
    assert get_file is not None
    file = get_file()  # Actually download
    assert file is not None
    df = pd.DataFrame(columns=["Date"]).set_index(["Date"])
    for page in parse_file(file):
        df = vaccination_tables(df, None, page, file)
    # write_scrape_data_back_to_test(df, "vaccination_tables", fname)
    pd.testing.assert_frame_equal(testdf, df, check_dtype=False)


@pytest.mark.parametrize("fname, testdf, get_file", dl_files("vac_manuf_given", vac_slides_files))
def test_vac_manuf_given(fname, testdf, get_file):
    assert get_file is not None
    file = get_file()  # Actually download
    assert file is not None
    df = pd.DataFrame(columns=["Date"]).set_index(["Date"])
    for i, page in enumerate(parse_file(file), 1):
        df = vac_manuf_given(df, page, file, i)
    # write_scrape_data_back_to_test(df, "vac_manuf_given", fname)
    pd.testing.assert_frame_equal(testdf, df, check_dtype=False)


def find_testing_pptx(check):
    return [(file, None, dl) for file, dl in get_test_dav_files(ext=".pptx")]


def find_testing_pdf(check):
    return [(file, None, dl) for file, dl in get_test_dav_files(ext=".pdf")]


@pytest.mark.parametrize("fname, testdf, dl", dl_files("testing_moph_pptx", find_testing_pptx))
def test_get_tests_by_area_chart_pptx(fname, testdf, dl):
    data, raw = pd.DataFrame(), pd.DataFrame()
    assert dl is not None
    file = dl()
    assert file is not None
    for chart, title, series, pagenum in pptx2chartdata(file):
        data, raw = get_tests_by_area_chart_pptx(input, title, series, data, raw)
    # write_scrape_data_back_to_test(raw, "testing_moph", fname)
    pd.testing.assert_frame_equal(testdf, raw, check_dtype=False)


@pytest.mark.parametrize("fname, testdf, dl", dl_files("testing_moph_pdf", find_testing_pdf))
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
    # write_scrape_data_back_to_test(raw, "testing_moph", fname)
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
    # write_scrape_data_back_to_test(raw, "briefing_deaths_provinces")
    pd.testing.assert_frame_equal(testdf, dfprov, check_dtype=False)


@pytest.mark.parametrize("date, testdf, dl", dl_files("briefing_deaths_summary", briefing_documents))
def test_briefing_deaths_summary(date, testdf, dl):
    dfprov = pd.DataFrame(columns=["Date"]).set_index(["Date"])
    assert dl is not None
    file = dl()
    assert file is not None

    pages = parse_file(file, html=True, paged=True)
    pages = [BeautifulSoup(page, 'html.parser') for page in pages]

    for i, soup in enumerate(pages):
        text = soup.get_text()
        df = briefing_deaths_summary(text, dateutil.parser.parse(date), file)
        dfprov = dfprov.combine_first(df)
    # write_scrape_data_back_to_test(dfprov, "briefing_deaths_summary")
    pd.testing.assert_frame_equal(testdf, dfprov, check_dtype=False)


@pytest.mark.parametrize("date, testdf, dl", dl_files("briefing_case_types", briefing_documents))
def test_briefing_case_types(date, testdf, dl):
    """
    The following json files check code that was added by corresponding commits.

    tests/briefing_case_types/{2021-05-17,\
                               2021-06-25,\
                               2021-08-13}.json

    2021-05-17 8998d907: fix parsing briefings
    2021-06-25 5d054122: parse vac in briefing
    2021-08-13 74aa7877: fix parse briefing cases
    """
    assert dl is not None
    file = dl()
    assert file is not None

    pages = parse_file(file, html=True, paged=True)
    pages = [BeautifulSoup(page, 'html.parser') for page in pages]

    df = briefing_case_types(dateutil.parser.parse(date), pages, file)
    # write_scrape_data_back_to_test(df, "briefing_case_types")
    pd.testing.assert_frame_equal(testdf, df, check_dtype=False)


@pytest.mark.parametrize("date, testdf, dl", dl_files("vac_briefing_totals", briefing_documents))
def test_vac_briefing_totals(date, testdf, dl):
    """
    2021-09-25.json: checks special case for lack of daily vacc data that day
    2021-09-26.json: checks fix for fourth dose being added to briefing
    """
    df = pd.DataFrame(columns=["Date"]).set_index(["Date"])
    assert dl is not None
    file = dl()
    assert file is not None

    pages = parse_file(file, html=True, paged=True)
    pages = [BeautifulSoup(page, 'html.parser') for page in pages]
    date = dateutil.parser.parse(date)

    for i, soup in enumerate(pages):
        text = soup.get_text()
        df = vac_briefing_totals(df, date, file, soup, text)
    # write_scrape_data_back_to_test(df, "vac_briefing_totals", date=date)
    pd.testing.assert_frame_equal(testdf, df, check_dtype=False)


@pytest.mark.parametrize("date, testdf, dl", dl_files("situation_pui_th", get_thai_situation_files))
def test_situation_pui_th(date, testdf, dl):
    results = pd.DataFrame(columns=["Date"]).set_index("Date")
    file = dl()
    assert dl is not None
    date = dateutil.parser.parse(date)

    parsed_pdf = parse_file(file, html=False, paged=False)
    df = situation_pui_th(results, parsed_pdf, date, file)

    # write_scrape_data_back_to_test(df, "situation_pui_th", fname=file)
    pd.testing.assert_frame_equal(testdf, df, check_dtype=False)
