import pathlib

import matplotlib.cm
import matplotlib.pyplot as plt
import pandas as pd

from covid_data import scrape_and_combine
from covid_plot_active import save_active_plots
from covid_plot_cases import save_cases_plots
from covid_plot_deaths import save_deaths_plots
from covid_plot_tests import save_tests_plots
from covid_plot_vacs import save_vacs_plots
from utils_scraping import logger


def save_plots(df: pd.DataFrame) -> None:
    logger.info('======== Generating Plots ==========')

    # matplotlib global settings
    matplotlib.use('AGG')
    plt.style.use('dark_background')

    # create directory if it does not exists
    pathlib.Path('./outputs').mkdir(parents=True, exist_ok=True)

    # Cases Plots
    save_cases_plots(df)

    # Tests Plots
    save_tests_plots(df)

    # Vaccinations Plots
    save_vacs_plots(df)

    # Deaths Plots
    save_deaths_plots(df)

    # active/hosp Plots
    save_active_plots(df)


if __name__ == "__main__":

    df = scrape_and_combine()
    save_plots(df)
