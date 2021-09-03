
# Downloads


## Daily CCSA Briefings <a name="dl-briefings">

- Sources 
   - [CCSA Daily Briefing](https://www.facebook.com/ThaigovSpokesman) - Uploaded ~1-2pm each day
   - [MOPH COVID 19 Dashboard](https://ddc.moph.go.th/covid19-dashboard/index.php?dashboard=main)
   - [API: Details of all confirmed COVID-19 infections](https://data.go.th/dataset/covid-19-daily) - 1-2 days delayed
   - [API: Daily Summary of Cases/Deaths/Recovered](https://covid19.th-stat.com/json/covid19v2/getTimeline.json)
   - [Daily infographics translated and tweeted](https://twitter.com/search?q=%22%F0%9F%91%89%22%20%F0%9F%93%8D%20(from%3ARichardBarrow)&src=typed_query&f=live) Updated daily around midday (after gov briefing) - *No Longer updated*

### [cases_briefings.json](https://github.com/djay/covidthailand/wiki/cases_briefings) | [cases_briefings.csv](https://github.com/djay/covidthailand/wiki/cases_briefings.csv)

- Schema: 
  - Date: e.g "2021-04-06"
  - Cases: Total cases that day. (Cases Imported + Cases Local Transmission)
  - Cases In Quarantine: "Cases found in quarantine facilities/centers"
  - Cases Imported: Cases In Quarantine + Cases outside quarantine
  - Cases Proactive: Local transmissions that aren't walk-ins
  - Cases Local Transmission: "Cases infected in Thailand". Cases Walkins + Cases Proactive
  - Cases Area Prison: Cases reported in prison on this date
  - Hospitalized: Total currently in isolation in hospital or field hospital
  - Hospitalized Field: Total currently in isolation in field hospitals
  - Hospitalized Hospital: total current active cases - anyone confirmed is considered hospitalized currently
  - Hospitalized Severe": Currently hospitalised in a severe condition. Unclear what kind of beds this entails.
  - Hospitalized Respirator: Current number in severe condition requiring ICU and mechanical ventilator
  - Recovered: Number released from hospital/field hospital on this date
  - Deaths: Number of deaths annouced that day
  - Deaths Age (Min,Max): Range of ages of those who died
  - Deaths Age Median": Median age of those who died
  - Deaths Comorbidity None: Deaths where there wasn't a disease that increased risk
  - Deaths {Female,Male}: Deaths for 2 of the genders
  - Deaths Risk Family: Deaths who likely cause of transmission was via family member
  - Source Cases:  Tweet, api or briefing the primary information came from
  - Fields no longer updated
    - Cases (Asymptomatic,Symptomatic): - No longer reported in briefing reports

### Cases/Deaths per province

#### [cases_by_province.json](https://github.com/djay/covidthailand/wiki/cases_by_province) | [cases_by_province.csv](https://github.com/djay/covidthailand/wiki/cases_by_province.csv)

- Schema cases_by_province: 
  - "Date": e.g "2021-04-06"
  - "Province": e.g "Samut Sakhon"
  - "Cases": Confirmed cases in this province
  - "Health District Number": 1-13 - see [Thailand Health Areas](https://mophgis.maps.arcgis.com/apps/opsdashboard/index.html#/bcd61791c8b441fa9224d129f28e8be0)  
  - The following is no longer updated
     - "Cases Walkin": Confirmed cases found those requesting tests or asked to from contact tracing or the media. Paid or having met the PUI criteria. *No longer updated*
     - "Cases Proactive": Confirmed cases found government has gone to specific areas to mass test due to high risk of COVID-19. *No longer updated*
     - "Deaths": 31.0

### Cases/Deaths per Health District
#### [cases_by_area.json](https://github.com/djay/covidthailand/wiki/cases_by_area), [cases_by_area.csv](https://github.com/djay/covidthailand/wiki/cases_by_area.csv)
- Schema cases_by_area: 
  - "Date": e.g "2021-04-06"
  - "Cases Area {1-13}": Confirmed cases in a given [Health Area](https://mophgis.maps.arcgis.com/apps/opsdashboard/index.html#/bcd61791c8b441fa9224d129f28e8be0)  
  - "Deaths Area {1-13}": Deaths that day in the health district
  - "Cases Risk: {Group} Area {1-13}": Categorisation of Risk field from the covid-19-daily dataset  
  - The following are no longer updated but have some historical data
    - "Cases {Proactive,Walkin} Area {1-13}": Cases found by people where tested

- Notes:
  - [Thailand Health Areas](https://mophgis.maps.arcgis.com/apps/opsdashboard/index.html#/bcd61791c8b441fa9224d129f28e8be0)  


<img alt="Cases by Health Area" src="https://github.com/djay/covidthailand/wiki/cases_areas_all.png"  width=200>
<img alt="Walk-in Cases by Health Area" src="https://github.com/djay/covidthailand/wiki/cases_areas_walkins.png" width=200>
<img alt="Proactive Cases by Health Area" src="https://github.com/djay/covidthailand/wiki/cases_areas_proactive.png" width=200>
<img alt="Cases by symptoms by Health Area" src="https://github.com/djay/covidthailand/wiki/cases_sym.png"  width=200>

### Deaths by Province
#### [deaths.json](https://github.com/djay/covidthailand/wiki/deaths), [deaths.csv](https://github.com/djay/covidthailand/wiki/deaths.csv)
- Schema: 
   - e.g
      - "Date":"2021-04-27"
      - "death_num":149.0,
      - "gender":"Male",
      - "nationality":"ไทย",
      - "age":47.0,
      - "Province":"Bangkok"
    - Following information is extracted by not properly parsed yet
      - "congenital_disease":
      - "case_history":
      - "risk_factor_sickness":
      - "risk_factor_death":
- Notes:
  - Stopped being published 2021-04-28. Only summary data in cases_by_area is continuing

## MOPH Covid-19 Dashboard <a name="dl-moph-dashboard">
- Sources [MOPH Covid-19 Dashboard](https://ddc.moph.go.th/covid19-dashboard/?dashboard=main)
### [moph_dashboad.json](https://github.com/djay/covidthailand/wiki/moph_dashboard) | [moph_dashboard.csv](https://github.com/djay/covidthailand/wiki/moph_dashboard.csv)

- Schema
  - Date
  - ATK
  - Cases
  - Cases Area Prison
  - Cases Imported
  - Cases Proactive
  - Cases Walkin
  - Deaths
  - Hospitalized
  - Hospitalized Field
  - Hospitalized Field HICI
  - Hospitalized Field Hospitel
  - Hospitalized Field Other
  - Hospitalized Hospital
  - Hospitalized Respirator
  - Hospitalized Severe
  - Recovered
  - Source Cases
  - Tests
  - Vac Given {1-3} Cum

### [moph_dashboad_prov.json](https://github.com/djay/covidthailand/wiki/moph_dashboard_prov) | [moph_dashboard_prov.csv](https://github.com/djay/covidthailand/wiki/moph_dashboard_prov.csv)

- Schema
  - Date
  - ATK
  - Cases
  - Cases Area Prison
  - Cases Imported
  - Cases Proactive
  - Cases Walkin
  - Deaths
  - Tests
  - Vac Given {1-3} Cum

### [moph_dashboad_ages.json](https://github.com/djay/covidthailand/wiki/moph_dashboard_ages) | [moph_dashboard_ages.csv](https://github.com/djay/covidthailand/wiki/moph_dashboard_ages.csv)

- Schema
  - Date
  - Cases Age {'0-9', '10-19', '20-29', '30-39', '40-49', '50-59', '60-69', '70+'}
  - Deaths Age {'0-9', '10-19', '20-29', '30-39', '40-49', '50-59', '60-69', '70+'}
  - Hospitalized Severe Age {'0-9', '10-19', '20-29', '30-39', '40-49', '50-59', '60-69', '70+'}
## Daily Situation Reports <a name="dl-situation-reports">
Case Types and PUI counts

### [situation_reports.json](https://github.com/djay/covidthailand/wiki/situation_reports) | [situation_reports.csv](https://github.com/djay/covidthailand/wiki/situation_reports.csv)

- Sources: 
  - [MOPH daily situation report PDFs](https://ddc.moph.go.th/viralpneumonia/situation.php) (Updated daily in the evening)
  - [MOPH daily situation report PDFs (english translations)](https://ddc.moph.go.th/viralpneumonia/eng/situation.php) (Updated a few days later)
  - [DDC Website](https://ddc.moph.go.th/viralpneumonia/index.php) - Today's PUI count

<img alt="PUI from situation reports" src="https://github.com/djay/covidthailand/wiki/tested_pui_all.png" width=200>
<img alt="Case Types" src="https://github.com/djay/covidthailand/wiki/cases_types_all.png" width=200>

- Schema
  - Date: e.g "2021-04-06"
  - Cases: Total cases that day. Cases Imported + Cases Local Transmission
  - Cases In Quarantine: "Cases found in quarantine facilities/centers"
  - Cases Imported: Cases In Quarantine + Cases outside quarantine
  - Cases Proactive: Local transmissions that aren't walk-ins
  - Cases Local Transmission: "Cases infected in Thailand". Cases Walkins + Cases Proactive
  - Tested PUI: People Classified as Person Under Investigation.
  - Tested PUI Walkin Public: "Sought medical services on their own at hospitals"/Public
  - Tested PUI Walkin Private: "Sought medical services on their own at hospitals"/Private
- The following are included but are *not useful data since 2020-08*.
  - Tested: *Not different from PUI since 2020-08* says "Total number of laboratory tests" but is mislabeled.
  - Tested Quarantine: *Not changed since 2020-08*. "Returnees in quarantine facilities/centers".
  - Tested Proactive: *Not changed since 2020-08*.Tested from "active case finding".
  - Tested Not PUI: *Not changed since 2020-08*. "People who did not meet the PUI criteria".
- The following aren't yet included
  - Screened Ports: "Type of Screened People and PUI / Ports of entry (Airports, ground ports, and seaports)"
  - Screened Immigration "Type of Screened People and PUI / People renewing their passports at the Immigration Bureau, Chaeng Watthana"

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
        - Hospitalized

## Testing Data <a name="dl-testing"> 

- Source: 
   - [DMSC: Thailand Laboratory testing data - latest report](https://service.dmsc.moph.go.th/labscovid19/indexen.php#rtpcr) (link at bottom) (updated weekly but sporadic)
   - [DMSC: Thailand Laboratory testing data - weekly summary reports](http://nextcloud.dmsc.moph.go.th/index.php/s/wbioWZAQfManokc)
   - also available via [data.go.th testing data](https://data.go.th/dataset/covid-19-testing-data)
   - This datasource is incomplete as not all tests go via this [DMSc’co-lab database](https://www3.dmsc.moph.go.th/post-view/974). In particular times with large amounts of proactive testing
     sometimes result in more cases reported than positive results.

<img src="https://github.com/djay/covidthailand/wiki/cases_all.png" width=200 alt="Private and Public Positive Test Results">
<img alt="Private and Public Positive Tests" src="https://github.com/djay/covidthailand/wiki/tests_all.png" width=200>
<img alt="Positive Test Results by health area" src="https://github.com/djay/covidthailand/wiki/pos_area_daily_all.png" width=200>
<img alt="PCR Tests by health area" src="https://github.com/djay/covidthailand/wiki/tests_area_daily_all.png" width=200>
<img alt="Positive Rate by Health District in overall positive rate (ex. some proactive tests)" src="https://github.com/djay/covidthailand/wiki/positivity_area_all.png" width=200>

### Daily Tests Private+Public
#### [tests_pubpriv.json](https://github.com/djay/covidthailand/wiki/tests_pubpriv) | [tests_pubpriv.csv](https://github.com/djay/covidthailand/wiki/tests_pubpriv.csv)
- Schema: 
   - Date: e.g "2021-04-06"
   - Tests: PCR tests
   - Tests Private: PCR tests from private labs
   - Pos: Positive result
   - Pos Private: Positive result from private labs
   - Pos XLS: Tests positive results (includes corrected date-less data)
   - Tests XLS: Tests conducted (includes corrected date-less data)
- Notes:
  - Uses case history graphs from the latest PPTX
  - data seems to exclude some non-PCR tests (likely used in some proactive testing)
  - The Test XLS data includes a number of tests and results for which the date is unknown. This has been redistributed into the Pos XLS and Tests XLS numbers. Other than this it
  should be the same numbers as ```Pos``` and ```Tests```. 

### Tests by Health District  
#### [tests_by_area.json](https://github.com/djay/covidthailand/wiki/tests_by_area) [tests_by_area.csv](https://github.com/djay/covidthailand/wiki/tests_by_area.csv)

- Schema: 
   - Start: e.g "2021-04-06"
   - End: e.g "2021-04-13"
   - Pos Area {1-13} - Positive test results
   - Tests Area {1-13} - Total tests (PCR)
- Notes:
  - not all periods are a week
  - data seems to exclude non-PCR tests (likely used in some proactive testing)
  - There are missing files, so some weeks' data are not shown
  - The example graphs shown have been extrapolated using daily totals from the test daily data

## Vaccination Downloads <a name="dl-vac">
## Daily DDC Vaccination Reports
### [vac_timeline.csv](https://github.com/djay/covidthailand/wiki/vac_timeline.csv)

- Source: 
   - [DDC Daily Vaccination Reports](https://ddc.moph.go.th/dcd/pagecontent.php?page=643&dept=dcd)
   - [COVID-19 Vaccines Track and Traceability Platform for Cold Chain and Patient Safety](https://datastudio.google.com/u/0/reporting/731713b6-a3c4-4766-ab9d-a6502a4e7dd6/page/SpZGC)
- Schema e.g.
```
      "Date":"2021-04-25",
      "Vac Allocated Sinovac {1,2}":3840.0,
      "Vac Allocated AstraZeneca {1,2}":0.0,
      "Vac Delivered": 200.00
      "Vac Given {1,2} Cum":3189.0,
      "Vac Given {1,2} %":83.0,
      "Vac Group Medical Staff {1,2} Cum":1939.0,
      "Vac Group Other Frontline Staff {1,2} Cum":1081.0,
      "Vac Group Over 60 {1,2} Cum":0.0,
      "Vac Group Risk: Disease {1,2} Cum":54.0,
      "Vac Group Risk: Location {1,2} Cum":115.0,
```
- Note
   - The previous data per province is no longer updated in the reports so this download has been removed.
   - "Vaccinations Given 1/2 %" refers to the % of allocation, not against population.
   - 1/2 refers to shot 1 or shot 2.
   - Some days some tables are images so there is missing data. 
   - Summary vaccination data included in the combine download
   - Delivered Vaccines comes from [Track and Traceability Platform]((https://datastudio.google.com/u/0/reporting/731713b6-a3c4-4766-ab9d-a6502a4e7dd6/page/SpZGC)
   - #TODO: put in thai group explanations.

## COVID-19 Vaccines Track and Traceability Platform for Cold Chain and Patient Safety <a name="dl-vac-prov">
### [vaccinations.csv](https://github.com/djay/covidthailand/wiki/vaccinations.csv)
- Source: 
   - [DDC Daily Vaccination Reports](https://ddc.moph.go.th/dcd/pagecontent.php?page=643&dept=dcd)
   - [COVID-19 Vaccines Track and Traceability Platform for Cold Chain and Patient Safety](https://datastudio.google.com/u/0/reporting/731713b6-a3c4-4766-ab9d-a6502a4e7dd6/page/SpZGC)

- Schema:
```
      "Date":"2021-04-25",
      "Province": "Bangkok",
      "Vac Given Cum":3189.0,
      "Vac Given ":83.0,
      "Vac Given {vaccine} Cum":3189.0,
      "Vac Given {vaccine}":83.0,
      "Vac Allocated {vaccine} {1-2}:
      "Vac Group {group} {1-2} Cum: Cumulative vaccines given to particular risk group (dose 1 and 2)
```

## BORA Monthly Deaths (Excess Deaths) <a name="dl-deaths-all">

### [deaths_all.csv](https://github.com/djay/covidthailand/wiki/deaths_all.csv)

- Source: 
   - [Office of Registration Administration, Department of Provincial Administration](https://stat.bora.dopa.go.th/stat/statnew/statMONTH/statmonth/#/mainpage)
- Schema:
  - Year: 2012-2021
  - Month: 1-12
  - Province:
  - Gender: Male|Female
  - Age: 0-101
  - Deaths: 



## Combined <a name="dl-combined">
### [combined.csv](https://github.com/djay/covidthailand/wiki/combined.csv)

- Source: 
  - All the above daily sources combined
  - plus [COVID-19 report, periodic summary](https://data.go.th/dataset/covid-19-daily)
- Schema: 
  - Cases Age {'0-9', '10-19', '20-29', '30-39', '40-49', '50-59', '60-69', '70+'}
  - Cases Risk {Group}: Categorisation of Risk field from the covid-19-daily dataset  
  - + See all the above for data definitions
