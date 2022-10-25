import pathlib
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


def save_plots(df: pd.DataFrame) -> None:
    logger.info('======== Generating Plots ==========')

    # matplotlib global settings
    matplotlib.use('AGG')
    plt.style.use('dark_background')

    # create directory if it does not exists
    pathlib.Path('./outputs').mkdir(parents=True, exist_ok=True)

    awaits = []
    with Pool() as pool:
        awaits = [pool.apply_async(f, [df]) for f in [
            covid_plot_cases.save_caseprov_plots,
            covid_plot_cases.save_cases_plots,
            covid_plot_tests.save_tests_plots,
            covid_plot_tests.save_area_plots,
            covid_plot_vacs.save_vacs_plots,
            covid_plot_vacs.save_vacs_prov_plots,
            covid_plot_active.save_active_plots,
            covid_plot_deaths.save_deaths_plots,
        ]]
        [a.get() for a in awaits]


if __name__ == "__main__":

    df = scrape_and_combine()
    save_plots(df)
