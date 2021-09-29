# Thailand COVID-19 Data

*Note* Share via https://djay.github.io/covidthailand

Thailand COVID-19 case/test/vaccination data gathered and combined from various government sources for others to view or download. 
- Updated daily 8-9am summary info, 1-3pm from full briefing. Testing data is updated every 1-3 weeks.

[![last update was](https://github.com/djay/covidthailand/actions/workflows/main.yml/badge.svg)](https://github.com/djay/covidthailand/actions/workflows/main.yml). 

## [Cases](#cases) |  [Active Cases](#active-cases) |  [Deaths](#deaths) | [Testing](#testing) | [Vaccinations](#vaccinations) | [Downloads](downloads) | [About](#about)

**NEW** [Excess Deaths](#excess-deaths)

## Disclaimer
*Data offered here is offered as is with no guarantees. As much as possible government reports
and data feeds have been used effort has gone into making this data collection accurate and timely.
This sites only intention is to give an accurate representation of all the available Covid data for Thailand in one place.*

Links to all data sources are including in [Downloads](downloads)

# Cases <a name="cases">

## Cases by Where Tested
![Source of Confirmed Cases](https://github.com/djay/covidthailand/wiki/cases_types_2.png)
 - [Source of Confirmed Cases: 2020-2021](https://github.com/djay/covidthailand/wiki/cases_types_all.png)
- Contact tracing normally counts as a "Walk-in"
- Proactive tests are normally done at specific high risk locations or places of known cases, rather than random sampling (but it's possible random sampling may also be included).
- Sources: [CCSA Daily Briefing](downloads#dl-briefings),
  [MOPH daily situation report](downloads#dl-situation-reports)

## Cases by Risk Group

![Cases by Risk](https://github.com/djay/covidthailand/wiki/cases_causes_2.png)

- Grouped from original data which has over 70 risk categories. Clusters have
  been [grouped into either Work (Factories), Entertainment (bars/gambling etc) or Community (markets) related](https://github.com/djay/covidthailand/wiki/risk_groups.csv).
- Note: SS Cluster is classified as "Work", but some other market clusters are classified as "Community". This is because there isn't enough data to separate out SS cluster cases
  between those from factories and those from the market. This could change later. 
- Risk is most likely determined as part of the PUI criteria process?
- [Cases by Risk: Full Year](https://github.com/djay/covidthailand/wiki/cases_causes_all.png)
- Source: [API: Daily reports of COVID-19 infections](https://data.go.th/dataset/covid-19-daily)

## Provinces with Cases Trending Up

To see cases for every province go to [The Researcher Covid Tracker](https://covid-19.researcherth.co/) 

![Trending Up Confirmed Cases (by Provinces)](https://github.com/djay/covidthailand/wiki/cases_prov_increasing_30d.png)

![Trending Up Contact Cases (by Provinces)](https://github.com/djay/covidthailand/wiki/cases_contact_increasing_30d.png)

![Trending Up "Unknown" Cases (by Provinces)](https://github.com/djay/covidthailand/wiki/cases_unknown_increasing_30d.png)

![Provinces with Most Cases](https://github.com/djay/covidthailand/wiki/cases_prov_top_30d.png)

![Provinces with Most Walkin Cases](https://github.com/djay/covidthailand/wiki/cases_walkins_increasing_30d.png)

- Sources: [CCSA Daily Briefing](downloads#dl-briefings), [API: Daily reports of COVID-19 infections](https://data.go.th/dataset/covid-19-daily)

- see also
   [Trending Down Confirmed Cases](https://github.com/djay/covidthailand/wiki/cases_prov_decreasing_30d.png), 
   [Trending Up Contact Cases](https://github.com/djay/covidthailand/wiki/cases_contact_increasing_30d.png)
   [Trending Up Unknown Cases](https://github.com/djay/covidthailand/wiki/cases_unknown_increasing_30d.png)
   [Trending Up Community Cases](https://github.com/djay/covidthailand/wiki/cases_community_increasing_30d.png),
   [Trending Up Work Cases](https://github.com/djay/covidthailand/wiki/cases_work_increasing_30d.png) and
   [Trending Up Proactive Cases](https://github.com/djay/covidthailand/wiki/cases_proactive_increasing_30d.png)


## Cases by Health District

![Cases by Health District](https://github.com/djay/covidthailand/wiki/cases_areas_2.png)

- To see cases for every province go to [The Researcher Covid Tracker](https://covid-19.researcherth.co/)
- [Cases by Health District: Full Year](https://github.com/djay/covidthailand/wiki/cases_areas_all.png)
- [Thailand Health Districts](https://mophgis.maps.arcgis.com/apps/opsdashboard/index.html#/bcd61791c8b441fa9224d129f28e8be0)
- You can also see [Cases by District broken down by walk-in vs proactive](downloads#dl-situation-reports) but there is no longer a data source to keep this updated.
- Sources: [CCSA Daily Briefing](downloads#dl-briefings)

## Cases by Age
![Ages of Confirmed cases](https://github.com/djay/covidthailand/wiki/cases_ages_2.png)

- see [Ages of confirmed cases 2020-2021]((https://github.com/djay/covidthailand/wiki/cases_ages_all.png))
- Source: [API: Daily reports of COVID-19 infections](https://data.go.th/dataset/covid-19-daily)

## Unofficial Estimated Infections based on Deaths/IFR

![Estimated Infections Thailand](https://github.com/djay/covidthailand/wiki/cases_infections_estimate_2.png)

- Due to the Asymptomatic nature of Covid all countries have more infections than can be confirmed via testing.
- Research has been done to work out how many real infections there are in many countries to work out an [estimated global Infection Fatality Rate of the virus](http://epimonitor.net/Covid-IFR-Analysis.htm) for each age group. A simple estimate based on reported deaths using a per province IFR back-dated 11 days (median reported time till death for Thailand) gives an estimate of infections, however there are [many assumptions](https://github.com/djay/covidthailand/wiki#are-there-a-lot-more-infections-than-confirmed-cases), that if wrong, could make this estimate higher e.g. uncounted covid deaths.
- This doesn't mean there is not enough testing being done in Thailand. [Positive rate](#positive-rate) is another indication of testing effectiveness.
- More detailed models with predictions that take into account factors like [Google mobility data](https://ourworldindata.org/grapher/changes-visitors-covid?time=2021-04-01..latest&country=~THA) to predict infections based on adherence to social distancing measures.
   - [ICL Covid Model](https://mrc-ide.github.io/global-lmic-reports/THA/) ([OWID ICL](https://ourworldindata.org/grapher/daily-new-estimated-covid-19-infections-icl-model?country=~THA)), 
   - [IHME Covid Model](https://covid19.healthdata.org/thailand) 
([OWID IHME](https://ourworldindata.org/grapher/daily-new-estimated-covid-19-infections-ihme-model?country=~THA)) 
   - [LSHTM Model](https://epiforecasts.io/covid/posts/national/thailand/).
   - [OWID Covid Models for Thailand](https://ourworldindata.org/grapher/daily-new-estimated-infections-of-covid-19?country=~THA) lets you compare these infection estimates.
- [IHME Policy Briefing PDFs](http://www.healthdata.org/covid/updates) provide a lot of detail on the current situation in a country and what factors are that drive their predictions.
- Sources: [CCSA Daily Briefing](downloads#dl-briefings), [Covid IFR Analysis](http://epimonitor.net/Covid-IFR-Analysis.htm), [Thailand population by Age](http://statbbi.nso.go.th/staticreport/Page/sector/EN/report/sector_01_11101_EN_.xlsx)


# Active Cases Since April 1st <a name="active-cases">

![Thailand Active Cases](https://github.com/djay/covidthailand/wiki/cases_cumulative_3.png)

![Thailand Cases in ICU](https://github.com/djay/covidthailand/wiki/active_severe_3.png)

![Trending Up Severe Hospitalisations](https://github.com/djay/covidthailand/wiki/active_severe_increasing_30d.png)

![Top Severe Hospitalisations](https://github.com/djay/covidthailand/wiki/active_severe_top_30d.png)

- Break down of active case status only available from 2020-04-24 onwards.
- Other Active Cases + ICU + Ventilator + Field hospitals = Hospitalised, which is everyone who is 
  confirmed (for 14days at least)
- see [Thailand Active Cases 2020-2021]((https://github.com/djay/covidthailand/wiki/cases_active_all.png))
- Source: [CCSA Daily Briefing ](downloads#dl-briefings)


# Deaths <a name="deaths">

## COVID-19 Deaths

![Thailand Covid Deaths by Reason](https://github.com/djay/covidthailand/wiki/deaths_reason_3.png)
- source: [CCSA Daily Briefing ](downloads#dl-briefings)

## COVID-19 Deaths by Health District

![Thailand Covid Deaths by Health District](https://github.com/djay/covidthailand/wiki/deaths_by_area_3.png)
- source: [CCSA Daily Briefing ](downloads#dl-briefings)


## COVID-19 Deaths Age Range

![Thailand Covid Death Age Range](https://github.com/djay/covidthailand/wiki/deaths_age_3.png)
- Source: [CCSA Daily Briefing ](downloads#dl-briefings) 

![Thailand Covid Death Age Range](https://github.com/djay/covidthailand/wiki/deaths_age_dash_3.png)
- Source: [MOPH Covid-19 Dashboard](downloads#dl-moph-dashboard) 

# Testing <a name="testing">

## Positive Rate

![Positive Rate](https://github.com/djay/covidthailand/wiki/positivity_3.png)

- *NOTE* Walkin Cases/3*PUI seems to give an estimate of positive rate (when cases are high), so it is included for when testing data is delayed. *Note* it is not the actual positive rate.
- Positive rate is little like fishing in a lake. If you get few nibbles each time you put your line in you can guess that there is few fish in the lake. Less positives per test, less infections likely in the population.
- [WHO considers enough testing is happening if positive rate is under %5](https://www.jhsph.edu/covid-19/articles/covid-19-testing-understanding-the-percent-positive.html) rather than tests per population but only if 0.1% of the population is being tested per week (avg 7k tests per day for Thailand). Note this recommendation works best if everyone who might have COVID-19 is equally likely to get tested and there are reasons why this might not be the case in Thailand.
- It's likely [Thailand excludes some test data](https://github.com/djay/covidthailand/wiki#more-cases-than-positive-results) so there could be more tests than this data shows. Excluding
proactive tests from positive rate is perhaps better for [comparison with other countries](https://ourworldindata.org/grapher/positive-rate-daily-smoothed) they are less random and more likely to be positive as its testing known clusters.
- Rapid antigen tests are not included in the test data, or in confirmed case numbers (unless they also had a positive PCR test). This is similar to most countries however some like [UK count antigen tests in both tests and confirmed cases](https://coronavirus.data.gov.uk/details/about-data#testing-capacity).
- This positive rate is based on [DMSC: Thailand Laboratory testing data](downloads#dl-testing). In the [Daily MOPH Situation Reports](downloads#dl-situation-reports) is a number labelled ```Total number of laboratory tests```.  [```Total number of laboratory tests``` is mislabelled and is exactly the same as the PUI number](https://github.com/djay/covidthailand/wiki). 
- see also [Positive Rate: Full year](https://github.com/djay/covidthailand/wiki/positivity_all.png), [Tests per Case Graph](https://github.com/djay/covidthailand/wiki/tests_per_case_3.png) (Positive rate inverted) could be easier to understand.
- Sources: [DMSC: Thailand Laboratory testing data](downloads#dl-testing), [Daily situation Reports](downloads#dl-situation-reports)

## PCR Tests in Thailand by day

![Private and Public Tests](https://github.com/djay/covidthailand/wiki/tests_3.png)

![Private and Public Positive Results](https://github.com/djay/covidthailand/wiki/cases_3.png)

- [There are more confirmed cases than positives in Thailand's testing data](https://github.com/djay/covidthailand/wiki#more-cases-than-positive-results), this could be for various
  reasons but could make the positive rate lower.
- Sources: [Daily situation Reports](downloads#dl-situation-reports), [DMSC: Thailand Laboratory testing data](downloads#dl-testing)

## PCR Tests by Health District

![Tests by health area](https://github.com/djay/covidthailand/wiki/tests_area_daily_3.png)

- [Tests by health area: Full Year](https://github.com/djay/covidthailand/wiki/tests_area_daily_all.png)
- *NOTE* Excludes some proactive tests (non-PCR) so actual tests is higher
- Source: [DMSC: Thailand Laboratory testing data](downloads#dl-testing)
## Positive Rate by Health District

![Proportion of positive rate contributed by health districts](https://raw.githubusercontent.com/wiki/djay/covidthailand/positivity_area_3.png)
- Shows if all health districts are testing similarly

![Health Districts with high Positive Rate (ex. some proactive tests)](https://github.com/djay/covidthailand/wiki/positivity_area_unstacked_3.png)
- Shows which districts have the highest positive rate

- see also [Positive Rate by Health District: Full Year](https://github.com/djay/covidthailand/wiki/positivity_area_unstacked_all.png)

- Source: [DMSC: Thailand Laboratory testing data](downloads#dl-testing)

# Vaccinations <a name="vaccinations">

## Vaccinations by Priority Groups

![Daily Vaccinations in Thailand](https://github.com/djay/covidthailand/wiki/vac_groups_daily_30d.png)
- Source: [DDC Daily Vaccination Reports](downloads#dl-vac)

![Vaccinations in Thailand](https://github.com/djay/covidthailand/wiki/vac_groups_3.png)
- Source: [DDC Daily Vaccination Reports](downloads#dl-vac)

![Progress towards Full Vaccination](https://github.com/djay/covidthailand/wiki/vac_groups_goals_full_3.png)
- Source: [DDC Daily Vaccination Reports](downloads#dl-vac)

![Progress towards Half Vaccination](https://github.com/djay/covidthailand/wiki/vac_groups_goals_half_3.png)
- Source: [DDC Daily Vaccination Reports](downloads#dl-vac)

![Top Provinces by Vaccination 2nd Jab](https://github.com/djay/covidthailand/wiki/vac_top5_doses_2_30d.png)
![Top Provinces by Vaccination 1st Jab](https://github.com/djay/covidthailand/wiki/vac_top5_doses_1_30d.png)
![Lowest Provinces by Vaccination 2nd Jab](https://github.com/djay/covidthailand/wiki/vac_low_doses_2_30d.png)
![Lowest Provinces by Vaccination 1st Jab](https://github.com/djay/covidthailand/wiki/vac_low_doses_1_30d.png)
- Source: [MOPH Covid-19 Dashboard](downloads#dl-moph-dashboard), [DDC Daily Vaccination Reports](downloads#dl-vac)

![Vaccine Doses given by Heath District](https://github.com/djay/covidthailand/wiki/vac_areas_3.png)
- Source: [MOPH Covid-19 Dashboard](downloads#dl-moph-dashboard), [DDC Daily Vaccination Reports](downloads#dl-vac)

- see also [Map of Vaccinations: The Researcher Covid Tracker](https://covid-19.researcherth.co/vaccination)

# Excess Deaths <a name="excess-deaths">
Shows Deaths from all causes in comparison to the min, max and mean of Deaths from the 5 years pre-pandemic.
- *Note: there are many possible factors alter deaths up or down other than uncounted Covid Deaths*

![Thailand Excess Deaths by Region](https://github.com/djay/covidthailand/wiki/deaths_excess_region_5y_all.png)
- [Compare 2015-2018 only](https://github.com/djay/covidthailand/wiki/deaths_excess_region_all.png)

![Thailand Excess Deaths by Age](https://github.com/djay/covidthailand/wiki/deaths_excess_age_bar_5y_all.png)
- [Compare 2015-2018 only](https://github.com/djay/covidthailand/wiki/deaths_excess_age_bar_all.png)

![Thailand Excess deaths with Covid Deaths](https://github.com/djay/covidthailand/wiki/deaths_excess_covid_5y_all.png)
- [Compare 2015-2018 only](https://github.com/djay/covidthailand/wiki/deaths_excess_covid_all.png)

![Thailand Deaths Years Compared](https://github.com/djay/covidthailand/wiki/deaths_excess_years_all.png)

Notes
- [2019 had an unusual increase in deaths compared to the previous 4 years](https://github.com/djay/covidthailand/wiki/deaths_excess_years_all.png) but is unclear yet why. Links excluding 2019 have additionally been included as 
it's not yet clear which range of years provides the best baseline to compare against. 
- Compare Excess deaths across countries with [OWID Excess Deaths](https://ourworldindata.org/excess-mortality-covid#excess-mortality-p-scores-by-age-group) or
  [Economist Excess Death Tracker](https://www.economist.com/graphic-detail/coronavirus-excess-deaths-tracker).
- Source [Office of Registration Administration, Department of Provincial Administration](downloads#dl-deaths-all)


## How to contribute
- As the different sources of the data has increased so has the code needed fetch, extract and
  display this data. All the code is fairly simple python however. It is a fun way to learn scraping
  data and/or pandas and matplotlib.
- Find a [github issue](https://github.com/djay/covidthailand/issues) and have a go. Many are marked as suitable for beginners
  - making new plots
  - improve existing plots
  - adding tests so it's faster to make future fixes
  - improving scrapers that miss past data, e.g. vaccination reports
  - [Spotting breaking updates](https://github.com/djay/covidthailand/actions) and submitting a pull request to revise the scraper
  - If unsure if you are on the right track, submit a draft pull request and request a review
- Spotted a problem or got an idea how to improve? [Submit an issue](https://github.com/djay/covidthailand/issues) and then have a go making it happen.
- Got Questions? [Start a discussion](https://github.com/djay/covidthailand/discussions) or comment on an issue

### Install
- To install (requires python >=3.9)
  ```
  python -m venv .venv
  .venv/bin/pip install -r requirements.txt
  ```
### Adding tests

- To run the tests (will only get files needed for tests)
  ```
  bin/pytest
  ```
- To add a test
  - Only add test data for dates where the format changed and so the scraper had to get updated. See commit history for dates where this happened or use code coverage.
  - Logs from a full scrape can be used to also identify files/dates that are not scraped correctly
     - if you are trying to add in past regression tests you can also use [```git blame covid_data.py```](https://github.com/djay/covidthailand/blame/45ab729d5cdba862de2c5940264f790a5504907a/covid_data.py) on the scraping function to see the dates that lines were added or changed. in some cases comments indicated important dates where code had to change. 
  - Add empty file in tests/*scraper_type*/*dl_name*.json
     - for some tests can be use date of file instead or filename.date.json (the date is ignored but helps for readability)
  - Run tests. This will download just the document needed for that test, scrape it and compare the results against the json.
     - of course this will fail but you can look at the generated data and compare it to the original file or other sources to make sure it looks right
  - If the results are correct there is commented out code in the test function to export the data to the 
    test json file.
    - if you are using vscode to run pytests you need to refresh the tests list at this point for some reason
  - Note that not all scrapers have a test framework setup yet. But follow the existing code to add one or ask for help.

### Running just plots (or latest files)
- To get latest files
  ```
  wget --recursive --level=1 --accept="*.csv" --no-host-directories --cut-dirs=2 https://github.com/djay/covidthailand/blob/main/downloads.md
  mv --no-target-directory wiki api
  mkdir --parents json
  cp api/{deaths_all.csv,moph_*} json
  ```
- To do just plots
  ```
  USE_CACHE_DATA=True MAX_DAYS=0 bin/python covid_plot.py
  ```
- When debugging, to scrape just one part first, rearrange the lines in covid_data.py/scrape_and_combine so that the scraping function you want to debug gets called before the others do

### Running full code (warning will take a long time)
You can just use the test framework without a full download if you want to work on scraping.

- to download only the files that interest you first, you can comment out or rearrange the lines in covid_data.scrape_and_combine
- to work on plots you can download the csv files from the website into the api directory and set env MAX_DAYS=0

- To run the full scrape (warning this will take a long time as it downloads all the documents into a local cache)
  ```
  bin/python covid_plot.py
  ```
# Contributors
- [Dylan Jay](https://github.com/djay)
- [Vincent Casagrande](https://github.com/flyingvince)
- [Peter Scully](https://github.com/pmdscully)
- join us?

# About

Made with python/pandas/matplotlib. Dylan Jay gave a talk on how easy it is to extract data from PDFs
and powerpoints and plot data at [Bangkok's Monthly ThaiPy Event](https://www.meetup.com/en-AU/ThaiPy-Bangkok-Python-Meetup) [Video: "How I scraped Thailand's covid data" (1h mark)](https://www.facebook.com/watch/live/?v=2890282134582879)

Why do this? Originally to answer the question ["Was Thailand doing enough testing?"](https://github.com/djay/covidthailand/wiki) for myself and because ![Someone was wrong on the internet](https://imgs.xkcd.com/comics/duty_calls.png).

## License

<a rel="license" href="http://creativecommons.org/licenses/by/4.0/"><img alt="Creative Commons License" style="border-width:0" src="https://i.creativecommons.org/l/by/4.0/88x31.png" /></a><br />This work is licensed under a <a rel="license" href="http://creativecommons.org/licenses/by/4.0/">Creative Commons Attribution 4.0 International License</a>.


## Other sources of visualisations/Data for Thailand

- [Our World in Data: Thailand Profile](https://ourworldindata.org/coronavirus/country/thailand?country=~THA#what-is-the-daily-number-of-confirmed-cases) - the best way to compare against other countries
- [Pete Scully: COVID-19 Thailand Public Data](https://petescully.co.uk/research/covid19-thailand-dashboards/) for added visualisations and comparisons
- [The Researcher Covid Tracker](https://covid-19.researcherth.co)
- [Stefano Starita](https://twitter.com/DrSteStarita) - more excellent analysis and visualisations
- [Richard Barrow](https://www.facebook.com/richardbarrowthailand) - maybe the fastest way to get COVID-19 updates in English
- Thai Gov news feeds
  - [Thai Gov Covid Information: FB](https://www.facebook.com/informationcovid19) - has daily briefing infographics and broadcast (eng and thai) updated quickly
  - [Thai Gov Spokesman: FB](https://www.facebook.com/ThaigovSpokesman), 
  - [Thai Government PR: FB](https://www.facebook.com/thailandprd),
  - [Ministry of Health: Twitter](https://twitter.com/thaimoph), 
  - [DMSC PR: FB](https://www.facebook.com/DMSc.PR.Network)
- [MOPH ArcGIS](https://mophgis.maps.arcgis.com/apps/opsdashboard/index.html#/210413ebb5ff49bb8914808af6473322) - PUI + worldwide covid stats
- [MOPH OPS Dashboard: ArcGIS](https://mophgis.maps.arcgis.com/apps/opsdashboard/index.html#/bcd61791c8b441fa9224d129f28e8be0?) - current usage of hospital resource but seems no longer updated (since mid last year?)
  
## Change log
- 2021-08-16 - Move ATK to tests plot and remove from types plot
- 2021-08-16 - Plots of more age ranges for deaths, excess deaths and cases
- 2021-08-15 - Dashboard parsing for provinces and ages with downloads 
- 2021-08-02 - Add ATK cases parsing from dashboard and put in case_types plot
- 2021-07-30 - Add plots for excess deaths
- 2021-07-18 - Add data on vaccines by manufacturer from vaccine slides
- 2021-07-17 - Add estimate of death ages
- 2021-07-13 - Remove import vaccines due to coldchain data being restricted
- 2021-07-10 - Switch province plots to per 100,000
- 2021-07-10 - Put actuals on main case plots
- 2021-06-29 - Use coldchain data to plot deliveries and province vac data
- 2021-06-22 - Add trending provinces for contact cases
- 2021-06-12 - Add vaccination daily and improve cumulative vaccinations
- 2021-06-05 - update vaccination reports to parse summary timeline data only (missing source)
- 2021-06-30 - death reasons and hospitalisation critical plots
- 2021-05-21 - Estimate of infections from deaths
- 2021-05-18 - Include prisons as separate province/health district (because briefings do)
- 2021-05-15 - improve highest positive rate plot to show top 5 only
- 2021-05-10 - parse unofficial RB tweet to get cases and deaths earlier
- 2021-05-07 - add trending up and down provinces for cases
- 2021-05-06 - add top 5 fully vaccinated provinces
- 2021-05-05 - added recovered to active cases
- 2021-05-04 - plots of deaths and vaccinations
- 2021-04-28 - rolling averages on area graphs to make them easier to read
- 2021-04-25 - Add graph of cases by risk and active cases (inc severe)
- 2021-04-25 - Scrape hospitalisation stats from briefing reports
- 2021-04-23 - Fixed mistake in testing data where private tests was added again
- 2021-04-22 - data for sym/asymptomatic and pui private vs pui public
- 2021-04-20 - Added case age plot
- 2021-04-18 - Added clearer positive rate by district plot and made overall positive rate clearer
- 2021-04-15 - Quicker province case type breakdowns from daily briefing reports
- 2021-04-13 - get quicker PUI count from https://ddc.moph.go.th/viralpneumonia/index.php
- 2021-04-12 - Put in "unknown area" for tests and cases by district so totals are correct
- 2021-04-05 - add tweets with province/type break down to get more up to date stats


<a href="https://github.com/djay/covidthailand" class="github-corner" aria-label="View source on GitHub"><svg width="80" height="80" viewBox="0 0 250 250" style="fill:#151513; color:#fff; position: absolute; top: 0; border: 0; right: 0;" aria-hidden="true"><path d="M0,0 L115,115 L130,115 L142,142 L250,250 L250,0 Z"></path><path d="M128.3,109.0 C113.8,99.7 119.0,89.6 119.0,89.6 C122.0,82.7 120.5,78.6 120.5,78.6 C119.2,72.0 123.4,76.3 123.4,76.3 C127.3,80.9 125.5,87.3 125.5,87.3 C122.9,97.6 130.6,101.9 134.4,103.2" fill="currentColor" style="transform-origin: 130px 106px;" class="octo-arm"></path><path d="M115.0,115.0 C114.9,115.1 118.7,116.5 119.8,115.4 L133.7,101.6 C136.9,99.2 139.9,98.4 142.2,98.6 C133.8,88.0 127.5,74.4 143.8,58.0 C148.5,53.4 154.0,51.2 159.7,51.0 C160.3,49.4 163.2,43.6 171.4,40.1 C171.4,40.1 176.1,42.5 178.8,56.2 C183.1,58.6 187.2,61.8 190.9,65.4 C194.5,69.0 197.7,73.2 200.1,77.6 C213.8,80.2 216.3,84.9 216.3,84.9 C212.7,93.1 206.9,96.0 205.4,96.6 C205.1,102.4 203.0,107.8 198.3,112.5 C181.9,128.9 168.3,122.5 157.7,114.1 C157.9,116.9 156.7,120.9 152.7,124.9 L141.0,136.5 C139.8,137.7 141.6,141.9 141.8,141.8 Z" fill="currentColor" class="octo-body"></path></svg></a><style>.github-corner:hover .octo-arm{animation:octocat-wave 560ms ease-in-out}@keyframes octocat-wave{0%,100%{transform:rotate(0)}20%,60%{transform:rotate(-25deg)}40%,80%{transform:rotate(10deg)}}@media (max-width:500px){.github-corner:hover .octo-arm{animation:none}.github-corner .octo-arm{animation:octocat-wave 560ms ease-in-out}}</style>
