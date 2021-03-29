# Covidthailand: Thailand Covid testing stats

An api for data extracted from various sources is available.

In addition there is [An analysis of testing in thailand](https://github.com/djay/covidthailand/wiki)

Runs each night. [![daily update is currently](https://github.com/djay/covidthailand/actions/workflows/main.yml/badge.svg)](https://github.com/djay/covidthailand/actions/workflows/main.yml))

## Tests by Health Area  
- Source: [DMSC: Thailand Laboratory testing data - weekly summary reports](https://service.dmsc.moph.go.th/labscovid19/indexen.php) (link at bottom)
- API: [Download JSON](https://github.com/djay/covidthailand/wiki/tests_by_area)
   - Start: Date
   - End: Date
   - Pos Area {1-13} - Positive public test results
   - Tests Area {1-13} - Total public tests (PCR)
- Notes:
  - not all periods are a week
  - data seems to exclude private tests and non-PCR tests (likely used in some proactive testing)
  - There is no data for 1 week

## Tests Private+Public
- Source: [DMSC: Thailand Laboratory testing data - weekly summary reports](https://service.dmsc.moph.go.th/labscovid19/indexen.php) (link at bottom)
- API: [Download JSON](https://github.com/djay/covidthailand/wiki/tests_pubpriv)
   - Date
   - Pos Public
   - Tests Public
   - Pos Private
   - Tests Private
- Notes:
  - Uses case history graphs + raw data XLS
  - data seems to exclude private tests and non-PCR tests (likely used in some proactive testing)

## Cases by Area and Demographics
- Source: [Report COVID-19, individual case information](https://data.go.th/dataset/covid-19-daily)
- API: [Download JSON](https://github.com/djay/covidthailand/wiki/cases_by_area)
  - Date
  - Cases Area {1-13}: 
- Notes:
  - Not updated after 2021-01-14

## Cases Types and PUI counts
- Source: [MOPH daily situation report PDFs](https://ddc.moph.go.th/viralpneumonia/situation.php) (inc also english translations)
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



- Thailand COVID-19 Testing Data (Raw Data)  
  - Tests/Cases (excluding proactive cases)
  - https://service.dmsc.moph.go.th/labscovid19/indexen.php

