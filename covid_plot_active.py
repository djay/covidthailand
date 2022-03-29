import pandas as pd

from covid_plot_utils import plot_area
from covid_plot_utils import source
from utils_pandas import perc_format
from utils_scraping import logger


def save_active_plots(df: pd.DataFrame) -> None:
    logger.info('======== Generating Tests Plots ==========')

    #######################
    # Hospital plots
    #######################
    cols_delayed = ["Hospitalized", "Recovered", "Hospitalized Severe", "Hospitalized Respirator", "Hospitalized Field"]

    # TODO: we are missing some severe, ventilator mid april. why?
    df[cols_delayed] = df[cols_delayed].interpolate(limit_area="inside")

    # TODO: use unknowns to show this plot earlier?

    # because severe includes those on respirators
    df["Hospitalized Severe excl vent"] = df["Hospitalized Severe"].sub(df["Hospitalized Respirator"], fill_value=None)
    non_split = df[["Hospitalized Severe excl vent", "Hospitalized Respirator", "Hospitalized Field"]].sum(skipna=True,
                                                                                                           axis=1)

    df["Hospitalized Hospital"] = df["Hospitalized"].sub(non_split, fill_value=None)
    cols = [
        'Hospitalized Respirator',
        'Hospitalized Severe excl vent',
        'Hospitalized Hospital',
        'Hospitalized Field',
    ]
    legends = [
        'In Serious Condition on Ventilator',
        'In Serious Condition without Ventilator',
        'In Isolation/Hospital',
        'In Field Hospital',
    ]
    # plot_area(df=df, png_prefix='cases_active', cols_subset=cols,
    #           title='Thailand Active Covid Cases\n(Severe, Field, and Respirator only available from '
    #                 '2021-04-24 onwards)',
    #           legends=legends,
    #           kind='area', stacked=True, percent_fig=False, ma_days=None, cmap='tab10')

    cols = [
        'Hospitalized Severe',
        'Hospitalized Severe excl vent',
        'Hospitalized Respirator',
    ]
    legends = [
        'In Serious Condition',
        'In Serious Condition without Ventilator',
        'In Serious Condition on Ventilator',
    ]
    plot_area(df=df,
              title='Active Covid Cases in Serious Condition - Thailand',
              legends=legends,
              png_prefix='active_severe', cols_subset=cols,
              actuals=False,
              ma_days=7,
              kind='line', stacked=False, percent_fig=False,
              cmap='tab10',
              footnote_left=f'{source}Data Source: CCSA Daily Briefing')

    # show cumulative deaths, recoveries and hospitalisations (which should all add up to cases)
    df['Recovered since 2021-04-01'] = df['2021-04-14':]['Recovered'].cumsum()
    df['Died since 2021-04-01'] = df['2021-04-01':]['Deaths'].cumsum()
    df['Cases since 2021-04-01'] = df['2021-04-01':]['Cases'].cumsum()
    # This is those in hospital but we make this the catch all
    exits = df[['Recovered since 2021-04-01', 'Died since 2021-04-01']].sum(axis=1, skipna=True)
    df['Other Active Cases'] = \
        df['Cases since 2021-04-01'].sub(non_split, fill_value=0).sub(exits, fill_value=0).clip(0)

    cols = [
        'Died since 2021-04-01',
        'Hospitalized Respirator',
        'Hospitalized Severe',
        'Other Active Cases',
        'Hospitalized Field',
        'Recovered since 2021-04-01',
    ]
    legends = [
        'Deaths from Cases since 1st April',
        'In Serious Condition on Ventilator',
        'In Serious Condition without Ventilator',
        'In Hospital/Mild Condition',
        'In Field Hospital',
        'Recovered from Cases Confirmed since 1st April',
    ]
    plot_area(df=df,
              title='Covid Cases by Current Outcome since 1st April 2021 - Thailand',
              legends=legends,
              png_prefix='cases_cumulative', cols_subset=cols,
              ma_days=None,
              kind='area', stacked=True, percent_fig=False,
              cmap='tab10',
              footnote_left=f'{source}Data Source: CCSA Daily Briefing')

    # TODO: I think we can replace the recovered since april with plot showing just hospitalisations?
    df["Hospitalized Field Unknown"] = df["Hospitalized Field"].sub(
        df[["Hospitalized Field Hospitel", "Hospitalized Field HICI"]].sum(axis=1, skipna=True), fill_value=0)

    cols = [
        'Hospitalized Respirator',
        'Hospitalized Severe',
        'Hospitalized Field Unknown',
        'Hospitalized Field Hospitel',
        'Hospitalized Field HICI',
    ]
    df["Hospitalized Mild"] = df["Hospitalized"].sub(df[cols].sum(axis=1, skipna=True), fill_value=0)
    cols = [
        'Hospitalized Respirator',
        'Hospitalized Severe',
        'Hospitalized Mild',
        'Hospitalized Field Unknown',
        'Hospitalized Field Hospitel',
        'Hospitalized Field HICI',
    ]
    legends = [
        'In Serious Condition on Ventilator',
        'In Serious Condition without Ventilator',
        'In Mild Condition in Hospital',
        'In Mild Condition in Field Hospital/Other',
        'In Mild Condition in Hotel Field Hospital (Hospitel)',
        'In Mild Condition in Home/Community Isolation (HICI)',
    ]
    plot_area(df=df,
              title='Active Cases by Condition - Thailand',
              png_prefix='active_hospital', cols_subset=cols, legends=legends,
              # unknown_name='Hospitalized Other', unknown_total='Hospitalized', unknown_percent=True,
              ma_days=7,
              kind='area', stacked=True, percent_fig=True, clean_end=True,
              cmap='tab10',
              footnote_left='Data Source: MOPH Covid-19 Dashboard,  CCSA Daily Briefing')

    df["Hospitalized All Mild"] = df["Hospitalized Mild"] + df["Hospitalized Field"]
    cols = [
        'Hospitalized Respirator',
        'Hospitalized Severe',
        'Hospitalized All Mild',
    ]
    legends = [
        'In Serious Condition on Ventilator',
        'In Serious Condition without Ventilator',
        'In Mild Condition',
    ]
    peaks = df[cols] / df.rolling(7).mean().max(axis=0) * 100
    plot_area(df=peaks,
              title='Active Covid Cases by Condition as % of Peak - Thailand',
              png_prefix='active_peak', cols_subset=cols,
              legends=legends,
              ma_days=7,
              kind='line', stacked=False, percent_fig=False, clean_end=True,
              cmap='tab10',
              y_formatter=perc_format,
              footnote_left='Data Source: MOPH Covid-19 Dashboard')
