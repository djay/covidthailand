# Covidthailand: Thailand Covid testing stats

Thailand testing and case data gathered and combined from various sources for others to download and use.

The data is updated daily with most data changing around midday once the government daily briefing has been uploaded [![last update was](https://github.com/djay/covidthailand/actions/workflows/main.yml/badge.svg)](https://github.com/djay/covidthailand/actions/workflows/main.yml)). Want to know how to do similar data collection and analysis? Watch the [Thailand Python Meetup where I explained how I did this](https://www.facebook.com/watch/live/?v=2890282134582879&ref=search) (1h mark).

## [Cases](#cases) | [Active Cases](#active-cases) | [Testing](#testing) | [Downloads](#downloads)


# Cases

## Cases by Health District

![Cases by Health District](https://github.com/djay/covidthailand/wiki/cases_areas_2.png)

- [Cases by Health District: Full Year](https://github.com/djay/covidthailand/wiki/cases_areas_all.png)
- [Thailand Health Districts](https://mophgis.maps.arcgis.com/apps/opsdashboard/index.html#/bcd61791c8b441fa9224d129f28e8be0)
- You can also see [Cases by District broken down by walkin vs proactive](#cases-by-type-and-province) but there is no longer a data source to keep this updated.
- Sources: [CCSA Daily Briefing](#cases-by-type-and-province),
  [MOPH daily situation report](#cases-types-and-pui-counts-daily-situation-reports)

## Cases by test type
![Source of Confirmed Cases](https://github.com/djay/covidthailand/wiki/cases_types.png)
 - [Source of Confirmed Cases: 2020-2021](https://github.com/djay/covidthailand/wiki/cases_types_all.png)
- Contact tracing normally counts as a "Walkin"
- Proactive tests are normally done on specific high risk locations
- Sources: [CCSA Daily Briefing](#cases-by-type-and-province),
  [MOPH daily situation report](#cases-types-and-pui-counts-daily-situation-reports)

## Cases by Risk Group

![Cases by Risk](https://github.com/djay/covidthailand/wiki/cases_causes_2.png)

- Grouped from original data which has over 70 risk categories. Clusters have
  been [grouped into either Work (Factories), Entertainment (bars/gambling etc) or Community (markets) related](https://github.com/djay/covidthailand/wiki/risk_groups.csv).
- Note: SS Cluster is classified as "Work", but some other market clusters are classified as "Community". This is because there isn't enough data to seperate out SS cluster cases
  between those from factories and those from the market. This could change later. 
- Risk is most likely determined as part of the PUI criteria process?
- [Cases by Risk: Full Year](https://github.com/djay/covidthailand/wiki/cases_causes_all.png)
- Source: [API: Daily reports of COVID-19 infections](https://data.go.th/dataset/covid-19-daily)

## Cases by Age
![Ages of Confirmed cases](https://github.com/djay/covidthailand/wiki/cases_ages_2.png)

- see [Ages of confirmed cases 2020-2021]((https://github.com/djay/covidthailand/wiki/cases_ages_all.png))
- Source: [API: Daily reports of COVID-19 infections](https://data.go.th/dataset/covid-19-daily)

# Active Cases

![Thailand Active Cases](https://github.com/djay/covidthailand/wiki/cases_active_2.png)

- Break down of active case status only available from 2020-04-24 onwards.
- ```Hospitilised Other``` is everyone after they are a confirmed case once you take out recovered and those that died. This means it includes those confirmed that are yet to find a bed (isolating at home). There is currently no data source for actual hospital beds occupied.
- see [Thailand Active Cases 2020-2021]((https://github.com/djay/covidthailand/wiki/cases_active_all.png))
- Source: [API: Daily reports of COVID-19 infections](https://data.go.th/dataset/covid-19-daily), [CCSA Daily Briefing ](https://www.facebook.com/informationcovid19) - 12pm each day


# Testing

## Positive Rate

![Positive Rate](https://github.com/djay/covidthailand/wiki/positivity_2.png)

- [Enough testing is happening if positive rate is < %3](https://www.jhsph.edu/covid-19/articles/covid-19-testing-understanding-the-percent-positive.html) (not tests per population), however this works only if everyone who might have covid is equally likely to get tested. This has changed over time in thailand.
- *NOTE* Cases/3*PUI seems to give an esitmate of positive rate (when proactive testing is low) so it is included for when testing data is delayed. *Note* it is not the actual positive rate.
- [Positive Rate: Full year](https://github.com/djay/covidthailand/wiki/positivity.png) 
- *WARNING* - Many people incorrectly take a number labeled as ```Total number of laboratory tests``` from the [Daily MOPH Situation Reports](#cases-types-and-pui-counts-daily-situation-reports) as the number of tests. [```Total number of laboratory tests``` is mislablled and is exactly the same as the PUI number](https://github.com/djay/covidthailand/wiki). The true number of tests per day is often 3 times higher. If someone is using this incorrect number to determine a positive rate then they will get a incorrect rate higher than reality. 
- [In appears not all proactive cases have been confirmed with PCR tests in the past](https://github.com/djay/covidthailand/wiki) which could make Thailands positive rate lower [compared to other countries](https://ourworldindata.org/grapher/positive-rate-daily-smoothed). You could argue excluding proactive testing gives a better indication of how many more cases might be found if you tested more since proactive testing is normally done in a high risk specific area, ie it's less of a random sampling.
- [Tests per Case Graph](https://github.com/djay/covidthailand/wiki/tests_per_case.png) (Positive rate inversed) could be easier to understand.
- Sources: [Daily situation Reports](#cases-types-and-pui-counts-daily-situation-reports), [DMSC: Thailand Laboratory testing data](#tests-privatepublic)

## PCR Tests in Thailand by day

![Private and Public Tests](https://github.com/djay/covidthailand/wiki/tests.png)

- Tests and PUI numbers don't seem to include all proactive tests so the actual tests could be higher. see [Understanding Thailands Covid Positive Rate](https://github.com/djay/covidthailand/wiki)
- Sources: [Daily situation Reports](#cases-types-and-pui-counts-daily-situation-reports), [DMSC: Thailand Laboratory testing data](#tests-privatepublic)

## PCR Tests by Health District

![Tests by health area](https://github.com/djay/covidthailand/wiki/tests_area_daily_2.png)

- [Tests by health area: Full Year](https://github.com/djay/covidthailand/wiki/tests_area_daily.png)
- *NOTE* Excludes some proactive tests (non-PCR) so actual tests is higher
- Source: [DMSC: Thailand Laboratory testing data](#tests-by-health-area)
## Positive Rate by Health District

![Health Districts with high Positive Rate (ex. some proactive tests)](https://github.com/djay/covidthailand/wiki/positivity_area_unstacked_2.png)

- [Positive Rate by Health District: Full Year](https://github.com/djay/covidthailand/wiki/positivity_area_unstacked.png)
- [Proportion of positive rate contributed by health districts](https://raw.githubusercontent.com/wiki/djay/covidthailand/positivity_area_2.png)
- Gives an indication of which areas are doing less testing compared to cases.
- *NOTE* Excludes some proactive tests (non-PCR) so actual rate would be lower
- Source: [DMSC: Thailand Laboratory testing data](#tests-by-health-area)

# Downloads

## Disclaimer
- Some data sources are missing days or numbers (e.g. situation reports and tests)
- Some are scraped from text and the code to do this might not be perfect
- Some are translations where mistypings have happened
- I take no responsibility for the accuracy of this data.

## Daily Tests Private+Public

![Private and Public Positive Test Results](https://github.com/djay/covidthailand/wiki/cases.png)
![Private and Public Positive Tests](https://github.com/djay/covidthailand/wiki/tests.png)

- Source: 
   - [DMSC: Thailand Laboratory testing data - weekly summary reports](https://service.dmsc.moph.go.th/labscovid19/indexen.php#rtpcr) (link at bottom) (updated weekly but sparodic)
   -  Latest [Thailand_COVID-19_testing_data-update.xlsx](https://service.dmsc.moph.go.th/labscovid19/indexen.php#rtpcr) (link at bottom of page) (updated weekly but sparodic)
- Downloads: [Download JSON](https://github.com/djay/covidthailand/wiki/tests_pubpriv), [Download CSV](https://github.com/djay/covidthailand/wiki/tests_pubpriv.csv)
   - Date: e.g "2021-04-06"
   - Tests: PCR tests
   - Tests Private: PCR tests from private labs
   - Pos: Positive result
   - Pos Private: Positive result from private labs
   - Pos XLS: Tests positive results (includes corrected dataless data)
   - Tests XLS: Tests conducted (includes corrected dataless data)
- Notes:
  - Uses case history graphs from latest PPTX
  - data seems to exclude some non-PCR tests (likely used in some proactive testing)
  - The Test XLS data includes a number tests and results for which the date is unknown. This has been redistributed into the Pos XLS and Tests XLS numbers. Other than this it
  should be the same numbers as ```Pos``` and ```Tests```. 

## Tests by Health District  

![Positive Test Results by health area](https://github.com/djay/covidthailand/wiki/pos_area_daily.png)

![PCR Tests by health area](https://github.com/djay/covidthailand/wiki/tests_area_daily.png)

![Positive Rate by Health District in overall positive rate (ex. some proactive tests)](https://github.com/djay/covidthailand/wiki/positivity_area.png)


- Source: 
   - [DMSC: Thailand Laboratory testing data - weekly summary reports](https://service.dmsc.moph.go.th/labscovid19/indexen.php#rtpcr) (link at bottom) (updated weekly but sparodic)
- Downloads: [Download JSON](https://github.com/djay/covidthailand/wiki/tests_by_area), [Download CSV](https://github.com/djay/covidthailand/wiki/tests_by_area.csv)
   - Start: e.g "2021-04-06"
   - End: e.g "2021-04-13"
   - Pos Area {1-13} - Positive test results
   - Tests Area {1-13} - Total tests (PCR)
- Notes:
  - not all periods are a week
  - data seems to exclude non-PCR tests (likely used in some proactive testing)
  - There is missing files so some weeks data are not shown
  - The example graphs shown have been extrapolated using daily totals from the test daily data

## Cases Types and PUI counts (Daily Situation Reports)

![PUI from situation reports](https://github.com/djay/covidthailand/wiki/tested_pui.png)
![Case Types](https://github.com/djay/covidthailand/wiki/cases_types_all.png)


- Source: 
  - [MOPH daily situation report PDFs](https://ddc.moph.go.th/viralpneumonia/situation.php) (Updated daily in the evening)
  - [MOPH daily situation report PDFs (english translations)](https://ddc.moph.go.th/viralpneumonia/eng/situation.php)) (Updated a few days later)
  - [DDC Website](https://ddc.moph.go.th/viralpneumonia/index.php) - Todays PUI count

- Downloads: [Download JSON](https://github.com/djay/covidthailand/wiki/situation_reports), [Download CSV](https://github.com/djay/covidthailand/wiki/situation_reports.csv)
  - Date: e.g "2021-04-06"
  - Cases: Total cases that day. Cases Imported + Cases Local Transmission
  - Cases In Quarantine: "Cases found in quarantine facilities/centers"
  - Cases Imported: Cases In Quarantine + Cases outside quarantine
  - Cases Proavtive: Local transmissions that aren't walkins
  - Cases Local Transmission: "Cases infected in Thailand". Cases Walkins + Cases Proactive
  - Tested PUI: People Classified as Person Under Infestigation.
  - Tested PUI Walkin Public: PUI classified at public hospitals/labs
  - Tested PUI Walkin Private: PUI classified at private hospitals/labs
- The follwing are included but are *not useful data since 2020-08*.
  - Tested: *Not different from PUI since 2020-08* says "Total number of laboratory tests" but is mislabeled. ~PUI + 30%
  - Tested Quarantine: *Not changed since 2020-08*. "Returnees in quarantine facilities/centers".
  - Tested Proactive: *Not changed since 2020-08*.Tested from "active case finding".
  - Tested Not PUI: *Not changed since 2020-08*. "People who did not meet the PUI criteria".

- Notes:
  - The only useful Tested number is "Tested PUI".
  - All the daily numbers have cumulative raw data columns (ending in "Cum") from which the daily numbers are calculated
     - except for all the Cases numbers from 2020-11-02 where daily numbers are taken from the reports
     - to calculate daily numbers missing data is interpolated
  - There are some figures in these reports not included
    - Screened Ports: Screened at "Ports of entry"
    - Screened Immigration: "People renewing their passports at the Immigration
Bureau, Chaeng Watthana"
    - Data found in other places e.g.
        - Deaths
        - Recovered
        - Hospitized

# Cases by province by case type
![Cases by Health Area](https://github.com/djay/covidthailand/wiki/cases_areas_all.png)
![Walkin Cases by Health Area](https://github.com/djay/covidthailand/wiki/cases_areas_walkins.png)
![Proactive Cases by Health Area](https://github.com/djay/covidthailand/wiki/cases_areas_proactive.png)
![Cases by symptoms by Health Area](https://github.com/djay/covidthailand/wiki/cases_sym.png)

- Source: 
   - [CCSA Daily Briefing ](https://www.facebook.com/informationcovid19) - 12pm each day
   - [API: Daily reports of COVID-19 infections](https://data.go.th/dataset/covid-19-daily) - 1-2 days delayed
   - [Daily infographics translated and tweeted](https://twitter.com/search?q=%22%F0%9F%91%89%22%20%F0%9F%93%8D%20(from%3ARichardBarrow)&src=typed_query&f=live) Updated daily around midday (after gov briefing) - *No Longer updated*
- Downloads by Province: [Download JSON](https://github.com/djay/covidthailand/wiki/cases_by_province), [Download CSV](https://github.com/djay/covidthailand/wiki/cases_by_province.csv)
  - "Date": e.g "2021-04-06"
  - "Province": e.g "Samut Sakhon"
  - "Cases": Confirmed cases in this province
  - "Cases Walkin": Confirmed cases found those requestings tests or asked to from contact tracing or the media. Paid or having met the PUI criteria. *No longer updated*
  - "Cases Proactive": Confirmed cases found goverment has gone to specific areas to mass test due to high risk of covid. *No longer updated*
  - "Health District Number": 1-13 - see [Thailand Health Areas](https://mophgis.maps.arcgis.com/apps/opsdashboard/index.html#/bcd61791c8b441fa9224d129f28e8be0)  

- Downloads summary by health district [Download JSON](https://github.com/djay/covidthailand/wiki/cases_by_area), [Download CSV](https://github.com/djay/covidthailand/wiki/cases_by_area.csv)
  - Date: e.g "2021-04-06"
  - Cases Area {1-13}: Confirmed cases in a given [Health Area](https://mophgis.maps.arcgis.com/apps/opsdashboard/index.html#/bcd61791c8b441fa9224d129f28e8be0)  
  - Cases Proactive Area {1-13}: Cases found by people requesting tests in a given [Health Area](https://mophgis.maps.arcgis.com/apps/opsdashboard/index.html#/bcd61791c8b441fa9224d129f28e8be0)  
  - Cases Walkin Area {1-13}: Cases found by government testing in specific location with in a given [Health Area](https://mophgis.maps.arcgis.com/apps/opsdashboard/index.html#/bcd61791c8b441fa9224d129f28e8be0)  
- Notes:
  - [Thailand Health Areas](https://mophgis.maps.arcgis.com/apps/opsdashboard/index.html#/bcd61791c8b441fa9224d129f28e8be0)  

# Vaccinations
- Source: [DDC Daily Vaccination Reports](https://ddc.moph.go.th/dcd/pagecontent.php?page=643&dept=dcd)
- [Download JSON](https://github.com/djay/covidthailand/wiki/vaccinations), [Download CSV](https://github.com/djay/covidthailand/wiki/vaccinations.csv)
- e.g.
```
      "Date":"2021-04-25",
      "Province":"Mae Hong Son",
      "Vaccinations Allocated Sinovac 1":3840.0,
      "Vaccinations Allocated Sinovac 2":3840.0,
      "Vaccinations Allocated AstraZeneca 1":0.0,
      "Vaccinations Allocated AstraZeneca 2":0.0,
      "Vaccinations Given 1 Cum":3189.0,
      "Vaccinations Given 1 %":83.0,
      "Vaccinations Given 2 Cum":37.0,
      "Vaccinations Given 2 %":1.0,
      "Vaccinations Medical 1 Cum":1939.0,
      "Vaccinations Medical 2 Cum":19.0,
      "Vaccinations Frontline 1 Cum":1081.0,
      "Vaccinations Frontline 2 Cum":18.0,
      "Vaccinations Over60 1 Cum":0.0,
      "Vaccinations Over60 2 Cum":0.0,
      "Vaccinations Disease 1 Cum":54.0,
      "Vaccinations Disease 2 Cum":0.0,
      "Vaccinations RiskArea 1 Cum":115.0,
      "Vaccinations RiskArea 2 Cum":0.0
```
- Note
   - "Vaccinations Given 1/2 %" refers to the % of allocation, not against population.
   - 1/2 refers to shot 1 or shot 2.
   - #TODO: put in thai group explanations.

# Combined

- Source: 
  - All of the above
  - plus [COVID-19 report, periodic summary](https://data.go.th/dataset/covid-19-daily)
- Downloads: [Download JSON](https://github.com/djay/covidthailand/wiki/combined), [Download CSV](https://github.com/djay/covidthailand/wiki/combined.csv)
  - See all of the above for data definitions

# Other sources of data
## Thailand
  - [Pete Scully: COVID-19 Thailand Public Data](https://petescully.co.uk/research/covid19-thailand-dashboards/) for added visulisations and comparisons
  - [Stefano Starita](https://twitter.com/DrSteStarita) - more excellent analysis and visualisations
  - Gov news feeds
    - [Thai Gov Spokesman: FB](https://www.facebook.com/ThaigovSpokesman), 
    - [Thai Government PR: FB](https://www.facebook.com/thailandprd),
    - [Ministry of Health: Twitter](https://twitter.com/thaimoph), 
    - [DMSC PR: FB](https://www.facebook.com/DMSc.PR.Network)
  - [MOPH GIS](https://mophgis.maps.arcgis.com/apps/opsdashboard/index.html#/210413ebb5ff49bb8914808af6473322)
  - [Our World in Data: Thailand Profile](https://ourworldindata.org/coronavirus/country/thailand?country=~THA#what-is-the-daily-number-of-confirmed-cases)

# Change log
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

# TODO (looking for contributors!)
- Fix unknowns to make more clear
  - e.g. risks should be "under investigation" or just don't show for data no collected yet?
  - active cases looks like severe etc disappear for a day. Maybe need "unknow condition" category? or just don't show the data?
- start collecting data on hospital capacity from
  - https://mophgis.maps.arcgis.com/apps/opsdashboard/index.html#/bcd61791c8b441fa9224d129f28e8be0?
  - probably no access to historical data
  - not sure yet how accurate it is or how often it changes
  - will have to checkout wiki store of data or start new report to keep data storage.
- put hospitalisation data into situation_reports export rather than cases_by_area
- find historical source for mild vs severe hospitalisations
- get source for sym/asym for latest cases
  - stopped being put in briefings
- plot % subplots to get closer detail
  - esp for the by district plots
- switch to plotly to interactively inspect values
  - https://towardsdatascience.com/how-to-create-a-plotly-visualization-and-embed-it-on-websites-517c1a78568b
- Extract from briefings
  - State Quarantine vs ASQ
- get data source for antigen and antibody tests
- get data source 
  - PUIs that didn't make the criteria - rejected. screened number?
  - proactive screened
- put in for date of last record in graph titles
  - and more detail of start and end dates for data in data source descriptions
- fix briefings parser to get more historical data 
   - for sym/asym
   - more province data
- work out if can incorporate province wealth
  - https://data.go.th/dataset/http-mis-m-society-go-th-tab030104-php
  - maybe for vaccinations or positive rate?