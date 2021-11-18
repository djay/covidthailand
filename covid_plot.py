import pathlib

import matplotlib
import matplotlib.cm
import matplotlib.pyplot as plt
import pandas as pd

from covid_data import scrape_and_combine
from utils_scraping import logger

from covid_plot_prov import save_prov_plots
from covid_plot_vac import save_vac_plots
from covid_plot_cases import save_cases_plots
from covid_plot_deaths import save_deaths_plots

def save_plots(df: pd.DataFrame) -> None:
    logger.info('======== Generating Plots ==========')
    source = 'Source: https://djay.github.io/covidthailand - (CC BY)\n'

    # matplotlib global settings
    matplotlib.use('AGG')
    plt.style.use('dark_background') 

    # create directory if it does not exists
    pathlib.Path('./outputs').mkdir(parents=True, exist_ok=True)

    # Province Plots
    save_prov_plots(df)

    # Province Plots
    save_vac_plots(df)

    # Cases Plots
    save_cases_plots(df)

    # Deaths Plots
    save_deaths_plots(df)


if __name__ == "__main__":

    df = scrape_and_combine()
    save_plots(df)
