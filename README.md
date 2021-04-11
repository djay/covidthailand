# Covidthailand: Thailand Covid testing stats

Thailand testing and case data gathered and combined from various sources.

The data is updated twice daily at 13:00 UTC+7 and 01:00 UTC+7. [![last update was](https://github.com/djay/covidthailand/actions/workflows/main.yml/badge.svg)](https://github.com/djay/covidthailand/actions/workflows/main.yml))


For more in depth analysis on what some of this means see [An analysis of testing in thailand](https://github.com/djay/covidthailand/wiki)


# Cases by type and province

![Walkin Cases by Health Area](https://github.com/djay/covidthailand/wiki/cases_areas_walkins.png)
![Proactive Cases by Health Area](https://github.com/djay/covidthailand/wiki/cases_areas_proactive.png)

- Source: 
   - [Daily Covid Briefings](https://www.facebook.com/ThaigovSpokesman) - *No longer updated*
   - [Daily infographics translated and tweeted](https://twitter.com/search?q=%22%F0%9F%91%89%22%20%F0%9F%93%8D%20(from%3ARichardBarrow)&src=typed_query&f=live) Updated daily around midday (after gov briefing) *No longer updated*
- API: [Download JSON](https://github.com/djay/covidthailand/wiki/cases_by_province) [Download CSV](https://github.com/djay/covidthailand/wiki/cases_by_province.csv)
  - "Date": e.g "2021-04-06"
  - "Province": e.g "Samut Sakhon"
  - "Cases Walkin": Confirmed cases found those requestings tests or asked to from contact tracing or the media. Paid or having met the PUI criteria
  - "Cases Proactive": Confirmed cases found goverment has gone to specific areas to mass test due to high risk of covid
  - "Health District Number": 1-13 - see [Thailand Health Areas](https://mophgis.maps.arcgis.com/apps/opsdashboard/index.html#/bcd61791c8b441fa9224d129f28e8be0)  

## Cases by Area

![Cases by Health Area](https://github.com/djay/covidthailand/wiki/cases_areas_2.png)

- Source: 
   - [Daily reports of COVID-19 infections](https://data.go.th/dataset/covid-19-daily) - 1-2 days delayed
   - [Daily infographics translated and tweeted](https://twitter.com/search?q=%22%F0%9F%91%89%22%20%F0%9F%93%8D%20(from%3ARichardBarrow)&src=typed_query&f=live) Updated daily around midday (after gov briefing) - *No Longer updated*
- API: [Download JSON](https://github.com/djay/covidthailand/wiki/cases_by_area) [Download CSV](https://github.com/djay/covidthailand/wiki/cases_by_area.csv)
  - Date: e.g "2021-04-06"
  - Cases Area {1-13}: Confirmed cases in a given [Health Area](https://mophgis.maps.arcgis.com/apps/opsdashboard/index.html#/bcd61791c8b441fa9224d129f28e8be0)  
  - Cases Proactive Area {1-13}: Cases found by people requesting tests in a given [Health Area](https://mophgis.maps.arcgis.com/apps/opsdashboard/index.html#/bcd61791c8b441fa9224d129f28e8be0)  
  - Cases Walkin Area {1-13}: Cases found by government testing in specific location with in a given [Health Area](https://mophgis.maps.arcgis.com/apps/opsdashboard/index.html#/bcd61791c8b441fa9224d129f28e8be0)  
- Notes:
  - [Thailand Health Areas](https://mophgis.maps.arcgis.com/apps/opsdashboard/index.html#/bcd61791c8b441fa9224d129f28e8be0)  
## Cases Types and PUI counts (Daily Situation Reports)

![Source of Confirmed Cases](https://github.com/djay/covidthailand/wiki/cases_types.png)
![PUI from situation reports](https://github.com/djay/covidthailand/wiki/tested_pui.png)


- Source: 
  - [MOPH daily situation report PDFs](https://ddc.moph.go.th/viralpneumonia/situation.php) (Updated daily in the evening)
  - [MOPH daily situation report PDFs (english translations)](https://ddc.moph.go.th/viralpneumonia/eng/situation.php)) (Updated a few days later)
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


## Tests by Health Area  

![Tests by health area](https://github.com/djay/covidthailand/wiki/tests_area_daily.png)
![Positive Test Results by health area](https://github.com/djay/covidthailand/wiki/pos_area_daily.png)


- Source: 
   - [DMSC: Thailand Laboratory testing data - weekly summary reports](https://service.dmsc.moph.go.th/labscovid19/indexen.php#rtpcr) (link at bottom) (updated weekly but sparodic)
- API: [Download JSON](https://github.com/djay/covidthailand/wiki/tests_by_area) [Download CSV](https://github.com/djay/covidthailand/wiki/tests_by_area.csv)
   - Start: e.g "2021-04-06"
   - End: e.g "2021-04-13"
   - Pos Area {1-13} - Positive public test results
   - Tests Area {1-13} - Total public tests (PCR)
- Notes:
  - not all periods are a week
  - data seems to exclude private tests and non-PCR tests (likely used in some proactive testing)
  - There is missing file so 1 weeks data is not shown
  - The example graphs shown have been extrapolated using daily totals from the Public test daily data below

## Tests Private+Public

![Private and Public Positive Test Results](https://github.com/djay/covidthailand/wiki/cases.png)
![Private and Public Tests](https://github.com/djay/covidthailand/wiki/tests.png)


- Source: 
   - [DMSC: Thailand Laboratory testing data - weekly summary reports](https://service.dmsc.moph.go.th/labscovid19/indexen.php#rtpcr) (link at bottom) (updated weekly but sparodic)
   -  Latest [Thailand_COVID-19_testing_data-update.xlsx](https://service.dmsc.moph.go.th/labscovid19/indexen.php#rtpcr) (link at bottom of page) (updated weekly but sparodic)
- API: [Download JSON](https://github.com/djay/covidthailand/wiki/tests_pubpriv) [Download CSV](https://github.com/djay/covidthailand/wiki/tests_pubpriv.csv)
   - Date: e.g "2021-04-06"
   - Tests Public: PCR tests for free (PUI Criteria)
   - Tests Private: PCR tests paid for
   - Pos Public: Positive result of free PCR test
   - Pos Private: Positive result of paid PCR test
   - Pos XLS: Public tests positive results (includes corrected dataless data)
   - Tests XLS: Public tests conducted (includes corrected dataless data)
- Notes:
  - Uses case history graphs from latest PPTX
  - data seems to exclude private tests and non-PCR tests (likely used in some proactive testing)
  - Public data matches the XLS file contained in the same shared folder marked as raw testing data.
  - The Test XLS data includes a number tests and results for which the date is unknown. This has been redistributed into the Pos XLS and Tests XLS numbers. Other than this it
  should be the same numbers as Pos Public and Tests Public. 

# Combined

![Positive Rate by Health Area in proportion to Thailand positive rate (public tests ex. some proactive tests)](https://github.com/djay/covidthailand/wiki/positivity_area_2.png)


- Source: 
  - All of the above
  - plus [COVID-19 report, periodic summary](https://data.go.th/dataset/covid-19-daily)
- API: [Download JSON](https://github.com/djay/covidthailand/wiki/combined) [Download CSV](https://github.com/djay/covidthailand/wiki/combined.csv)
  - See all of the above for data definitions


# Change log
- 2021-04-05 - add tweets with province/type break down to get more up to date stats

# TODO
- get timely data source for cases by type and cases by area
  - e.g. https://www.google.com/search?q=%E0%B8%AA%E0%B8%96%E0%B8%B2%E0%B8%99%E0%B8%81%E0%B8%B2%E0%B8%A3%E0%B8%93%E0%B9%8C%E0%B9%82%E0%B8%A3%E0%B8%84%E0%B8%95%E0%B8%B4%E0%B8%94%E0%B9%80%E0%B8%8A%E0%B8%B7%E0%B9%89%E0%B8%AD%E0%B9%84%E0%B8%A7%E0%B8%A3%E0%B8%B1%E0%B8%AA%E0%B9%82%E0%B8%84%E0%B9%82%E0%B8%A3%E0%B8%99%E0%B8%B2+site:thaigov.go.th+filetype:pdf
  - https://www.facebook.com/ThaigovSpokesman
  - http://media.thaigov.go.th/uploads/public_img/source/300364.pdf (can guess date)
  - https://twitter.com/thaimoph - infographic which richardbarrow translates
  - https://www.facebook.com/thailandprd
  - https://mophgis.maps.arcgis.com/apps/opsdashboard/index.html#/210413ebb5ff49bb8914808af6473322
- get data source for antigen and antibody tests
  - historical info out of https://mophgis.maps.arcgis.com/apps/opsdashboard/index.html#/bcd61791c8b441fa9224d129f28e8be0?
- get data source for walkins or proactive investigated/tested
- put in badges for date of last record per dataset