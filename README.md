# Covidthailand: Thailand Covid testing stats

An api for data extracted from various sources is available.

In addition there is [An analysis of testing in thailand](https://github.com/djay/covidthailand/wiki)

Runs each night. [![daily update is currently](https://github.com/djay/covidthailand/actions/workflows/main.yml/badge.svg)](https://github.com/djay/covidthailand/actions/workflows/main.yml))

## Tests by Health Area  
- Source: [DMSC: Thailand Laboratory testing data - weekly summary reports](https://service.dmsc.moph.go.th/labscovid19/indexen.php) (link at bottom)
- API: [Download JSON](https://github.com/djay/covidthailand/wiki/tests_by_area) [Download CSV](https://github.com/djay/covidthailand/wiki/tests_by_area.csv)
   - Start: Date
   - End: Date
   - Pos Area {1-13} - Positive public test results
   - Tests Area {1-13} - Total public tests (PCR)
- Notes:
  - not all periods are a week
  - data seems to exclude private tests and non-PCR tests (likely used in some proactive testing)
  - There is no data for 1 week


![Positive Test Results by health area](https://github.com/djay/covidthailand/wiki/pos_area.png)
![Tests by health area](https://github.com/djay/covidthailand/wiki/tests_area.png)


## Tests Private+Public
- Source: [DMSC: Thailand Laboratory testing data - weekly summary reports](https://service.dmsc.moph.go.th/labscovid19/indexen.php) (link at bottom)
- API: [Download JSON](https://github.com/djay/covidthailand/wiki/tests_pubpriv) [Download CSV](https://github.com/djay/covidthailand/wiki/tests_pubpriv.csv)
   - Date
   - Tests Public
   - Tests Private
   - Pos Public
   - Pos Private
   - % Detection Public (raw figure Pos Public was derived from)
   - % Detection Private (raw figure Pos Private was derived from)
- Notes:
  - Uses case history graphs from latest PPTX
  - data seems to exclude private tests and non-PCR tests (likely used in some proactive testing)
  - Public data matches the XLS file contained in the same shared folder marked as raw testing data.

![Private and Public Positive Test Results](https://github.com/djay/covidthailand/wiki/cases.png)
![Private and Public Tests](https://github.com/djay/covidthailand/wiki/tests.png)

## Cases by Area
- Source: [Report COVID-19, individual case information](https://data.go.th/dataset/covid-19-daily)
- API: [Download JSON](https://github.com/djay/covidthailand/wiki/cases_by_area) [Download CSV](https://github.com/djay/covidthailand/wiki/cases_by_area.csv)
  - Date
  - Cases Area {1-13}: The health district where the case was confirmed
- Notes:
  - Not updated after 2021-01-14

![Cases by Health Area](https://github.com/djay/covidthailand/wiki/cases_areas_1.png)
![Cases by Health Area](https://github.com/djay/covidthailand/wiki/cases_areas_2.png)

## Cases Types and PUI counts
- Source: [MOPH daily situation report PDFs](https://ddc.moph.go.th/viralpneumonia/situation.php) (inc also [english translated situation reports](https://ddc.moph.go.th/viralpneumonia/eng/situation.php))
- API: [Download JSON](https://github.com/djay/covidthailand/wiki/situation_reports) [Download CSV](https://github.com/djay/covidthailand/wiki/situation_reports.csv)
  - Date:
  - Cases In Quarantine: "Cases found in quarantine facilities/centers"
  - Cases Imported: All imported cases including those found outside quarantine
  - Cases Proavtive: Local transmissions that aren't walkins
  - Cases Local Transmission: "Cases infected in Thailand". Walkins + Active case finding
  - Tested: "Total number of laboratory tests": Same as PUI + some added occasionally
  - Tested PUI: Cumulative People Classified as PUI
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

![Source of Confirmed Cases](https://github.com/djay/covidthailand/wiki/cases_types.png)
![PUI from situation reports](https://github.com/djay/covidthailand/wiki/tested_pui.png)

# Combined
- Source: 
  - All of the above
  - + Latest [Thailand_COVID-19_testing_data-update.xlsx](https://service.dmsc.moph.go.th/labscovid19/indexen.php) (link at bottom of page)
  - + [COVID-19 report, periodic summary](https://data.go.th/dataset/covid-19-daily)
- API: [Download JSON](https://github.com/djay/covidthailand/wiki/combined) [Download CSV](https://github.com/djay/covidthailand/wiki/combined.csv)
  - See all of the above for data definitions
  - Pos XLS: Public tests positive results
  - Tests XLS: Public tests
- Notes:
  - The Test XLS data includes a number tests and results for which the date is unknown. This has been redistributed into the Pos XLS and Tests XLS numbers. Other than this it
  should be the same numbers as Pos Public and Tests Public. 
