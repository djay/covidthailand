import datetime
import pathlib
import time
from multiprocessing import Pool

import matplotlib.cm
import matplotlib.pyplot as plt
import pandas as pd

import covid_plot_active
import covid_plot_cases
import covid_plot_deaths
import covid_plot_tests
import covid_plot_vacs
from covid_data import scrape_and_combine
from utils_scraping import logger


def do_work(job, df):
    global job_data
    start = time.time()
    logger.info(f"==== Plot: {job.__name__} Start ====")
    data = job(df)
    logger.info(f"==== Plot: {job.__name__} in {datetime.timedelta(seconds=time.time() - start)} ====")
    return (job.__name__, data)


def save_plots(df: pd.DataFrame) -> None:
    logger.info('======== Generating Plots ==========')

    # matplotlib global settings
    matplotlib.use('AGG')
    plt.style.use('dark_background')

    # create directory if it does not exists
    pathlib.Path('./outputs').mkdir(parents=True, exist_ok=True)

    jobs = [
        covid_plot_cases.save_caseprov_plots,
        covid_plot_cases.save_cases_plots,
        covid_plot_tests.save_test_area_plots,
        covid_plot_tests.save_variant_plots,
        covid_plot_vacs.save_vacs_plots,
        covid_plot_vacs.save_vacs_prov_plots,
        covid_plot_active.save_active_plots,
        covid_plot_deaths.save_deaths_plots,
        covid_plot_deaths.save_excess_death_plots,
        covid_plot_tests.save_tests_plots,
    ]

    with Pool() as pool:
        res = dict(pool.imap_unordered(do_work, jobs, [df]))
        pool.close()
        pool.join()
    logger.info(f"data={len(res)}")


if __name__ == "__main__":

    df = scrape_and_combine()
    df = df.drop(columns=['Source Cases', 'Source Vac Given'])
    save_plots(df)
