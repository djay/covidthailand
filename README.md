# Covidthailand: Thailand Covid testing stats

Thailand testing and case data gathered and combined from various sources for others to download and use.

The data is updated twice daily at 12:20 UTC+7 and 23:20 UTC+7. [![last update was](https://github.com/djay/covidthailand/actions/workflows/main.yml/badge.svg)](https://github.com/djay/covidthailand/actions/workflows/main.yml)). Want to know how to do similar data collection and analysis? Watch the [Thailand Python Meetup where I explained how I did this](https://www.facebook.com/watch/live/?v=2890282134582879&ref=search) (1h mark).

# Dashboard

*WARNING* - Many people incorrectly take a number labeled as ```Total number of laboratory tests``` from the [Daily MOPH Situation Reports](#cases-types-and-pui-counts-daily-situation-reports) as the number of tests. [```Total number of laboratory tests``` is mislablled and is exactly the same as the PUI number](https://github.com/djay/covidthailand/wiki). The true number of tests per day is often 3 times higher. If someone is using this incorrect number to determine a positive rate then they will get a incorrect rate higher than reality. 

# Positive Rate

![Positive Rate](https://github.com/djay/covidthailand/wiki/positivity_2.png)
- *NOTE* The actual positive rate is often delayed due to when testing data reports become available. Included is some other measures that give an indication what the rate might be. 
- Read [Understanding the Positive Rate](https://www.jhsph.edu/covid-19/articles/covid-19-testing-understanding-the-percent-positive.html) to know why number of tests per population is not the best measure to compare countries on testing and why WHO suggests a positive rate of < %3.
- [In appears not all proactive cases have been confirmed with PCR tests in the past](https://github.com/djay/covidthailand/wiki) which could make Thailands positive rate lower [compared to other countries](https://ourworldindata.org/grapher/positive-rate-daily-smoothed). You could argue excluding proactive testing gives a better indication of how many more cases might be found if you tested more since proactive testing is normally done in a high risk specific area, ie it's less of a random sampling.
- [Tests per Case Graph](https://github.com/djay/covidthailand/wiki/tests_per_case.png) (Positive rate inversed) could be easier to understand.
- Sources: [Daily situation Reports](#cases-types-and-pui-counts-daily-situation-reports), [DMSC: Thailand Laboratory testing data](#tests-privatepublic)

## PCR Tests in Thailand by day
![Private and Public Tests](https://github.com/djay/covidthailand/wiki/tests.png)
- Tests and PUI numbers don't seem to include all proactive tests so the actual tests could be higher. see [Understanding Thailands Covid Positive Rate](https://github.com/djay/covidthailand/wiki)
- Sources: [Daily situation Reports](#cases-types-and-pui-counts-daily-situation-reports), [DMSC: Thailand Laboratory testing data](#tests-privatepublic)

## PCR Tests by Health District (Public Labs only)
![Tests by health area](https://github.com/djay/covidthailand/wiki/tests_area_daily.png)
- *NOTE* Excludes private test labs and some proactive tests (non-PCR) so actual tests is higher
- Source: [DMSC: Thailand Laboratory testing data](#tests-by-health-area)
## Positive Rate by Health District (Public Labs only)

![Health Districts with high Positive Rate (public tests ex. some proactive tests)](https://github.com/djay/covidthailand/wiki/positivity_area_unstacked_2.png)

- Gives an indication of which areas are doing less testing compared to cases.
- *NOTE* Excludes private test labs and some proactive tests (non-PCR) so actual rate would be lower
- Source: [DMSC: Thailand Laboratory testing data](#tests-by-health-area)

## Cases by Health District
![Cases by Health District](https://github.com/djay/covidthailand/wiki/cases_areas_2.png)
- [Thailand Health Districts](https://mophgis.maps.arcgis.com/apps/opsdashboard/index.html#/bcd61791c8b441fa9224d129f28e8be0)
- You can also see [Cases by District broken down by walkin vs proactive](#cases-by-type-and-province) but there is no longer a data source to keep this updated.
- Sources: [CCSA Daily Briefing](#cases-by-type-and-province),
  [MOPH daily situation report](#cases-types-and-pui-counts-daily-situation-reports)
## Cases by test type
![Source of Confirmed Cases](https://github.com/djay/covidthailand/wiki/cases_types.png)
- Contact tracing normally counts as a "Walkin"
- Proactive tests are normally done on specific high risk locations
- Sources: [CCSA Daily Briefing](#cases-by-type-and-province),
  [MOPH daily situation report](#cases-types-and-pui-counts-daily-situation-reports)

## Cases by Age
![Source of Confirmed Cases](https://github.com/djay/covidthailand/wiki/cases_ages.png)

# Downloads

## Disclaimer
- Some data sources are missing days or numbers (e.g. situation reports and tests)
- Some are scraped from text and the code to do this might not be perfect
- Some are translations where mistypings have happened
- I take no responsibility for the accuracy of this data.

## Daily Tests Private+Public

![Private and Public Positive Test Results](https://github.com/djay/covidthailand/wiki/cases.png)

- Source: 
   - [DMSC: Thailand Laboratory testing data - weekly summary reports](https://service.dmsc.moph.go.th/labscovid19/indexen.php#rtpcr) (link at bottom) (updated weekly but sparodic)
   -  Latest [Thailand_COVID-19_testing_data-update.xlsx](https://service.dmsc.moph.go.th/labscovid19/indexen.php#rtpcr) (link at bottom of page) (updated weekly but sparodic)
- Downloads: [JSON](https://github.com/djay/covidthailand/wiki/tests_pubpriv), [CSV](https://github.com/djay/covidthailand/wiki/tests_pubpriv.csv)
   - Date: e.g "2021-04-06"
   - Tests Public: PCR tests
   - Tests Private: PCR tests
   - Pos Public: Positive result
   - Pos Private: Positive result
   - Pos XLS: Public tests positive results (includes corrected dataless data)
   - Tests XLS: Public tests conducted (includes corrected dataless data)
- Notes:
  - Uses case history graphs from latest PPTX
  - data seems to exclude private tests and non-PCR tests (likely used in some proactive testing)
  - Public data matches the XLS file contained in the same shared folder marked as raw testing data.
  - The Test XLS data includes a number tests and results for which the date is unknown. This has been redistributed into the Pos XLS and Tests XLS numbers. Other than this it
  should be the same numbers as Pos Public and Tests Public. 

## Public Tests by Health District  

![Positive Test Results by health area](https://github.com/djay/covidthailand/wiki/pos_area_daily.png)

![Positive Rate by Health District in overall positive rate (public tests ex. some proactive tests)](https://github.com/djay/covidthailand/wiki/positivity_area_2.png)


- Source: 
   - [DMSC: Thailand Laboratory testing data - weekly summary reports](https://service.dmsc.moph.go.th/labscovid19/indexen.php#rtpcr) (link at bottom) (updated weekly but sparodic)
- Downloads: [JSON](https://github.com/djay/covidthailand/wiki/tests_by_area), [CSV](https://github.com/djay/covidthailand/wiki/tests_by_area.csv)
   - Start: e.g "2021-04-06"
   - End: e.g "2021-04-13"
   - Pos Area {1-13} - Positive public test results
   - Tests Area {1-13} - Total public tests (PCR)
- Notes:
  - not all periods are a week
  - data seems to exclude private tests and non-PCR tests (likely used in some proactive testing)
  - There is missing file so 1 weeks data is not shown
  - The example graphs shown have been extrapolated using daily totals from the Public test daily data below

## Cases Types and PUI counts (Daily Situation Reports)

![PUI from situation reports](https://github.com/djay/covidthailand/wiki/tested_pui.png)
![Case Types](https://github.com/djay/covidthailand/wiki/cases_types_all.png)


- Source: 
  - [MOPH daily situation report PDFs](https://ddc.moph.go.th/viralpneumonia/situation.php) (Updated daily in the evening)
  - [MOPH daily situation report PDFs (english translations)](https://ddc.moph.go.th/viralpneumonia/eng/situation.php)) (Updated a few days later)
  - [DDC Website](https://ddc.moph.go.th/viralpneumonia/index.php) - Todays PUI count
- API: [Download JSON](https://github.com/djay/covidthailand/wiki/situation_reports) [Download CSV](https://github.com/djay/covidthailand/wiki/situation_reports.csv)
  - Date: e.g "2021-04-06"
  - Cases: Total cases that day. Cases Imported + Cases Local Transmission
  - Cases In Quarantine: "Cases found in quarantine facilities/centers"
  - Cases Imported: Cases In Quarantine + Cases outside quarantine
  - Cases Proavtive: Local transmissions that aren't walkins
  - Cases Local Transmission: "Cases infected in Thailand". Cases Walkins + Cases Proactive
  - Tested: says "Total number of laboratory tests" but is mislabeled. ~PUI + 30%
  - Tested PUI: People Classified as Person Under Infestigation. Qualifies for free test.
  - Tested Quarantine: "Returnees in quarantine facilities/centers". Stopped getting updated
  - Tested Proactive: Tested from "active case finding". Stopped getting updated
  - Tested Not PUI: "People who did not meet the PUI criteria". Stopped getting updated
- Notes:
  - The only useful Tested number is "Tested PUI".
  - All the daily numbers have cumulative raw data columns (ending in "Cum") from which the daily numbers are calculated
     - except for all the Cases numbers from 2020-11-02 where daily numbers are taken from the reports
     - to calculate daily numbers missing data is interpolated
  - There are some figures in these reports not included
    - Screened Ports: Screened at "Ports of entry"
    - Screened Immigration: "People renewing their passports at the Immigration
Bureau, Chaeng Watthana"
    - Breakdown of PUI source - Almost all at hospitals
        - Could extract public vs private PUI
    - Data found in other places e.g.
        - Deaths
        - Recovered
        - Hospitized

# Cases by province by case type
![Cases by Health Area](https://github.com/djay/covidthailand/wiki/cases_areas_all.png)
![Walkin Cases by Health Area](https://github.com/djay/covidthailand/wiki/cases_areas_walkins.png)
![Proactive Cases by Health Area](https://github.com/djay/covidthailand/wiki/cases_areas_proactive.png)

- Source: 
   - [CCSA Daily Briefing ](https://www.facebook.com/informationcovid19) - 12pm each day
   - [API: Daily reports of COVID-19 infections](https://data.go.th/dataset/covid-19-daily) - 1-2 days delayed
   - [Daily infographics translated and tweeted](https://twitter.com/search?q=%22%F0%9F%91%89%22%20%F0%9F%93%8D%20(from%3ARichardBarrow)&src=typed_query&f=live) Updated daily around midday (after gov briefing) - *No Longer updated*

- Source: 
   - [Daily CCSA Covid Briefings](https://www.facebook.com/ThaigovSpokesman) 
   - [Daily infographics translated and tweeted](https://twitter.com/search?q=%22%F0%9F%91%89%22%20%F0%9F%93%8D%20(from%3ARichardBarrow)&src=typed_query&f=live) Updated daily around midday (after gov briefing) *No longer updated*
- Download by Province: [JSON](https://github.com/djay/covidthailand/wiki/cases_by_province) [CSV](https://github.com/djay/covidthailand/wiki/cases_by_province.csv)
  - "Date": e.g "2021-04-06"
  - "Province": e.g "Samut Sakhon"
  - "Cases": Confirmed cases in this province
  - "Cases Walkin": Confirmed cases found those requestings tests or asked to from contact tracing or the media. Paid or having met the PUI criteria. *No longer updated*
  - "Cases Proactive": Confirmed cases found goverment has gone to specific areas to mass test due to high risk of covid. *No longer updated*
  - "Health District Number": 1-13 - see [Thailand Health Areas](https://mophgis.maps.arcgis.com/apps/opsdashboard/index.html#/bcd61791c8b441fa9224d129f28e8be0)  
- Download summary by health district [Download JSON](https://github.com/djay/covidthailand/wiki/cases_by_area), [Download CSV](https://github.com/djay/covidthailand/wiki/cases_by_area.csv)
  - Date: e.g "2021-04-06"
  - Cases Area {1-13}: Confirmed cases in a given [Health Area](https://mophgis.maps.arcgis.com/apps/opsdashboard/index.html#/bcd61791c8b441fa9224d129f28e8be0)  
  - Cases Proactive Area {1-13}: Cases found by people requesting tests in a given [Health Area](https://mophgis.maps.arcgis.com/apps/opsdashboard/index.html#/bcd61791c8b441fa9224d129f28e8be0)  
  - Cases Walkin Area {1-13}: Cases found by government testing in specific location with in a given [Health Area](https://mophgis.maps.arcgis.com/apps/opsdashboard/index.html#/bcd61791c8b441fa9224d129f28e8be0)  
- Notes:
  - [Thailand Health Areas](https://mophgis.maps.arcgis.com/apps/opsdashboard/index.html#/bcd61791c8b441fa9224d129f28e8be0)  


# Combined

- Source: 
  - All of the above
  - plus [COVID-19 report, periodic summary](https://data.go.th/dataset/covid-19-daily)
- API: [Download JSON](https://github.com/djay/covidthailand/wiki/combined) [Download CSV](https://github.com/djay/covidthailand/wiki/combined.csv)
  - See all of the above for data definitions

# Other sources of data
## Thailand
  - [Pete Scully: COVID-19 Thailand Public Data](https://petescully.co.uk/research/covid19-thailand-dashboards/) for added visulisations and comparisons
  - [Stefano Starita](https://twitter.com/DrSteStarita) - more excellent analysis and visualisations
  - [Thai Gov Press: FB](https://www.facebook.com/ThaigovSpokesman), [Ministry of Healt: Twitter](https://twitter.com/thaimoph), [Thai Government PR](https://www.facebook.com/thailandprd)
  - [MOPH GIS](https://mophgis.maps.arcgis.com/apps/opsdashboard/index.html#/210413ebb5ff49bb8914808af6473322)
  - [Our World in Data: Thailand Profile](https://ourworldindata.org/coronavirus/country/thailand?country=~THA#what-is-the-daily-number-of-confirmed-cases)

# Change log
- 2021-04-21 - Added clearer positive rate by district plot and made overall positive rate clearer
- 2021-04-15 - Quicker province case type breakdowns from daily briefing reports
- 2021-04-13 - get quicker PUI count from https://ddc.moph.go.th/viralpneumonia/index.php
- 2021-04-12 - Put in "unknown area" for tests and cases by district so totals are correct
- 2021-04-05 - add tweets with province/type break down to get more up to date stats

# TODO
- switch to plotly or seaborn 
  - https://towardsdatascience.com/how-to-create-a-plotly-visualization-and-embed-it-on-websites-517c1a78568b
- put in plots of 
  - risk/source e.g entertainment vs factory vs prison vs friend 
  - case age groups over time
  - severe cases vs mild vs deaths?

- get data source for antigen and antibody tests
  - historical info out of https://mophgis.maps.arcgis.com/apps/opsdashboard/index.html#/bcd61791c8b441fa9224d129f28e8be0?
- get data source for walkins or proactive investigated/tested
- put in badges for date of last record per dataset
- get data from older briefing reports to improve cases by area data