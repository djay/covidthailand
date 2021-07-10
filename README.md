# Thailand COVID-19 Data

*Note* Share via https://djay.github.io/covidthailand

Thailand COVID-19 case/test/vaccination data gathered and combined from various government sources for others to view or download. 
- Updated daily 8-9am summary info, 1-3pm from full briefing. Testing data is updated every 1-3 weeks.

[![last update was](https://github.com/djay/covidthailand/actions/workflows/main.yml/badge.svg)](https://github.com/djay/covidthailand/actions/workflows/main.yml). 

## [Cases](#cases) |  [Active Cases](#active-cases) |  [Deaths](#deaths) | [Testing](#testing) | [Vaccinations](#vaccinations) | [Downloads](#downloads) | [About](#about)


# Cases

## Cases by Where Tested
![Source of Confirmed Cases](https://github.com/djay/covidthailand/wiki/cases_types_2.png)
 - [Source of Confirmed Cases: 2020-2021](https://github.com/djay/covidthailand/wiki/cases_types_all.png)
- Contact tracing normally counts as a "Walk-in"
- Proactive tests are normally done at specific high risk locations or places of known cases, rather than random sampling (but it's possible random sampling may also be included).
- Sources: [CCSA Daily Briefing](#dl-briefings),
  [MOPH daily situation report](#dl-situation-reports)

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

![Provinces with Most Cases](https://github.com/djay/covidthailand/wiki/cases_prov_top_30d.png)

- Sources: [CCSA Daily Briefing](#dl-briefings), [API: Daily reports of COVID-19 infections](https://data.go.th/dataset/covid-19-daily)

- see also
   [Trending Down Confirmed Cases](https://github.com/djay/covidthailand/wiki/cases_prov_decreasing_30d.png), 
   [Trending Up Contact Cases](https://github.com/djay/covidthailand/wiki/cases_contact_increasing_30d.png)
   [Trending Up Community Cases](https://github.com/djay/covidthailand/wiki/cases_community_increasing_30d.png),
   [Trending Up Work Cases](https://github.com/djay/covidthailand/wiki/cases_work_increasing_30d.png) and
   [Trending Up Proactive Cases](https://github.com/djay/covidthailand/wiki/cases_proactive_increasing_30d.png)


## Cases by Health District

![Cases by Health District](https://github.com/djay/covidthailand/wiki/cases_areas_2.png)

- To see cases for every province go to [The Researcher Covid Tracker](https://covid-19.researcherth.co/)
- [Cases by Health District: Full Year](https://github.com/djay/covidthailand/wiki/cases_areas_all.png)
- [Thailand Health Districts](https://mophgis.maps.arcgis.com/apps/opsdashboard/index.html#/bcd61791c8b441fa9224d129f28e8be0)
- You can also see [Cases by District broken down by walk-in vs proactive](#dl-situation-reports) but there is no longer a data source to keep this updated.
- Sources: [CCSA Daily Briefing](#dl-briefings)

## Cases by Age
![Ages of Confirmed cases](https://github.com/djay/covidthailand/wiki/cases_ages_2.png)

- see [Ages of confirmed cases 2020-2021]((https://github.com/djay/covidthailand/wiki/cases_ages_all.png))
- Source: [API: Daily reports of COVID-19 infections](https://data.go.th/dataset/covid-19-daily)

## Unoffcial Estimated Infections based on Deaths/IFR

![Estimated Infections Thailand](https://github.com/djay/covidthailand/wiki/cases_infections_estimate_2.png)

- Due to the Asymptomatic nature of Covid all countries have more infections than can be confirmed via testing.
- Research has been done to work out how many real infections there are in many countries to work out an [estimated global Infection Fatality Rate of the virus](http://epimonitor.net/Covid-IFR-Analysis.htm) for each age group. A simple estimate based on reported deaths using a per province IFR back-dated 11 days (median reported time till death for thailand) gives an estimate of infections, however there are [many assumptions](https://github.com/djay/covidthailand/wiki#are-there-a-lot-more-infections-than-confirmed-cases), that if wrong, could make this estimate higher e.g. uncounted covid deaths.
- This doesn't mean there is not enough testing being done in Thailand. [Positive rate](#positive-rate) is another indication of testing effectiveness.
- More detail models with predictions that take into account factors like [Goggle mobility data](https://ourworldindata.org/grapher/changes-visitors-covid?time=2021-04-01..latest&country=~THA) to predict infections based on adherence to social distancing measures.
   - [ICL Covid Model](https://mrc-ide.github.io/global-lmic-reports/THA/) ([OWID ICL](https://ourworldindata.org/grapher/daily-new-estimated-covid-19-infections-icl-model?country=~THA)), 
   - [IHME Covid Model](https://covid19.healthdata.org/thailand) 
([OWID IHME](https://ourworldindata.org/grapher/daily-new-estimated-covid-19-infections-ihme-model?country=~THA)) 
   - [LSHTM Model](https://epiforecasts.io/covid/posts/national/thailand/).
   - [OWID Covid Models for Thailand](https://ourworldindata.org/grapher/daily-new-estimated-infections-of-covid-19?country=~THA) lets you compare these infection estimates.
- Sources: [CCSA Daily Briefing](#dl-briefings), [Covid IFR Analysis](http://epimonitor.net/Covid-IFR-Analysis.htm), [Thailand population by Age](http://statbbi.nso.go.th/staticreport/Page/sector/EN/report/sector_01_11101_EN_.xlsx)


# Active Cases Since April 1st <a name="active-cases">

![Thailand Active Cases](https://github.com/djay/covidthailand/wiki/cases_cumulative_3.png)

![Thailand Cases in ICU](https://github.com/djay/covidthailand/wiki/active_severe_3.png)

- Break down of active case status only available from 2020-04-24 onwards.
- Other Active Cases + ICU + Ventilator + Field hospitals = Hospitalised, which is everyone who is 
  confirmed (for 14days at least)
- see [Thailand Active Cases 2020-2021]((https://github.com/djay/covidthailand/wiki/cases_active_all.png))
- Source: [CCSA Daily Briefing ](#dl-briefings)


# Deaths

## COVID-19 Deaths

![Thailand Covid Deaths by Reason](https://github.com/djay/covidthailand/wiki/deaths_reason_3.png)
- source: [CCSA Daily Briefing ](#dl-briefings)

## COVID-19 Deaths by Health District

![Thailand Covid Deaths by Health District](https://github.com/djay/covidthailand/wiki/deaths_by_area_3.png)
- source: [CCSA Daily Briefing ](#dl-briefings)


## COVID-19 Deaths Age Range

![Thailand Covid Death Age Range](https://github.com/djay/covidthailand/wiki/deaths_age_3.png)
- Source: [CCSA Daily Briefing ](#dl-briefings) 

# Testing

## Positive Rate

![Positive Rate](https://github.com/djay/covidthailand/wiki/positivity_2.png)

- *NOTE* Walkin Cases/3*PUI seems to give an estimate of positive rate (when cases are high), so it is included for when testing data is delayed. *Note* it is not the actual positive rate.
- Positive rate is little like fishing in a lake. If you get few nibbles each time you put your line in you can guess that there is few fish in the lake. Less positives per test, less infections likely in the population.
- [WHO considers enough testing is happening if positive rate is < %4](https://www.jhsph.edu/covid-19/articles/covid-19-testing-understanding-the-percent-positive.html) rather than tests per population. Note this works only if everyone who might have COVID-19 is equally likely to get tested and there are reasons why this might not be the case in Thailand.
- It's likely [Thailand excludes some proactive test data](https://github.com/djay/covidthailand/wiki#more-cases-than-positive-results) so there could be more tests than this data shows. Excluding
proactive tests from positive rate is perhaps better for [comparison with other countries](https://ourworldindata.org/grapher/positive-rate-daily-smoothed) they are less random and more likely to be positive as its testing known clusters.
- This positive rate is based on [DMSC: Thailand Laboratory testing data](#dl-testing). In the [Daily MOPH Situation Reports](#dl-situation-reports) is a number labelled ```Total number of laboratory tests```.  [```Total number of laboratory tests``` is mislabelled and is exactly the same as the PUI number](https://github.com/djay/covidthailand/wiki). 
- see also [Positive Rate: Full year](https://github.com/djay/covidthailand/wiki/positivity_all.png), [Tests per Case Graph](https://github.com/djay/covidthailand/wiki/tests_per_case.png) (Positive rate inverted) could be easier to understand.
- Sources: [DMSC: Thailand Laboratory testing data](#dl-testing), [Daily situation Reports](#dl-situation-reports)

## PCR Tests in Thailand by day

![Private and Public Tests](https://github.com/djay/covidthailand/wiki/tests_all.png)

- Note, In the past Thailand seems to have confirmed some cases from proactive testing without using PCR tests which makes the number of tests published lower than it might be.  see [Understanding Thailand COVID-19 Positive Rate](https://github.com/djay/covidthailand/wiki)
- Sources: [Daily situation Reports](#dl-situation-reports), [DMSC: Thailand Laboratory testing data](#dl-testing)

## PCR Tests by Health District

![Tests by health area](https://github.com/djay/covidthailand/wiki/tests_area_daily_2.png)

- [Tests by health area: Full Year](https://github.com/djay/covidthailand/wiki/tests_area_daily_all.png)
- *NOTE* Excludes some proactive tests (non-PCR) so actual tests is higher
- Source: [DMSC: Thailand Laboratory testing data](#dl-testing)
## Positive Rate by Health District

![Proportion of positive rate contributed by health districts](https://raw.githubusercontent.com/wiki/djay/covidthailand/positivity_area_2.png)
- Shows if all health districts are testing similarly

![Health Districts with high Positive Rate (ex. some proactive tests)](https://github.com/djay/covidthailand/wiki/positivity_area_unstacked_2.png)
- Shows which districts have the highest positive rate

- see also [Positive Rate by Health District: Full Year](https://github.com/djay/covidthailand/wiki/positivity_area_unstacked_all.png)

- *NOTE* Excludes some proactive tests (non-PCR) so actual rate would be lower
- Source: [DMSC: Thailand Laboratory testing data](#dl-testing)

# Vaccinations

## Vaccinations by Priority Groups

![Daily Vaccinations in Thailand](https://github.com/djay/covidthailand/wiki/vac_groups_daily_30d.png)
- Source: [DDC Daily Vaccination Reports](#dl-vac), [COVID-19 Vaccines Track and Traceability Platform for Cold Chain and Patient Safety](#dl-vac-prov)

![Vaccinations in Thailand](https://github.com/djay/covidthailand/wiki/vac_groups_3.png)
- Source: [DDC Daily Vaccination Reports](#dl-vac), [COVID-19 Vaccines Track and Traceability Platform for Cold Chain and Patient Safety](#dl-vac-prov)

![Vaccinations Groups by Progress towards goals](https://github.com/djay/covidthailand/wiki/vac_groups_goals_3.png)
- Source: [DDC Daily Vaccination Reports](#dl-vac)

![Top Provinces by Vaccination Doses Given](https://github.com/djay/covidthailand/wiki/vac_top5_doses_3.png)
- Source: [COVID-19 Vaccines Track and Traceability Platform for Cold Chain and Patient Safety](#dl-vac-prov)

![Vaccine Doses given by Heath District](https://github.com/djay/covidthailand/wiki/vac_areas_3.png)
- Source: [COVID-19 Vaccines Track and Traceability Platform for Cold Chain and Patient Safety](#dl-vac-prov)

- see also [Map of Vaccinations: The Researcher Covid Tracker](https://covid-19.researcherth.co/vaccination)

# Downloads

## Disclaimer
- Some data sources are missing days or numbers (e.g. situation reports and tests)
- Some are scraped from text, and the code to do this might not be perfect
- Some are translations where mistypings have happened. Some manual corrections are included
- I take no responsibility for the accuracy of this data.


## Daily CCSA Briefings <a name="dl-briefings">
- Sources 
   - [CCSA Daily Briefing ](https://www.facebook.com/ThaigovSpokesman) - Uploaded ~1-2pm each day
   - [API: Details of all confirmed COVID-19 infections](https://data.go.th/dataset/covid-19-daily) - 1-2 days delayed
   - [API: Daily Summary of Cases/Deaths/Recovered](https://covid19.th-stat.com/json/covid19v2/getTimeline.json)
   - [Daily infographics translated and tweeted](https://twitter.com/search?q=%22%F0%9F%91%89%22%20%F0%9F%93%8D%20(from%3ARichardBarrow)&src=typed_query&f=live) Updated daily around midday (after gov briefing) - *No Longer updated*

<img alt="Cases by Health Area" src="https://github.com/djay/covidthailand/wiki/cases_areas_all.png"  width=200>
<img alt="Walk-in Cases by Health Area" src="https://github.com/djay/covidthailand/wiki/cases_areas_walkins.png" width=200>
<img alt="Proactive Cases by Health Area" src="https://github.com/djay/covidthailand/wiki/cases_areas_proactive.png" width=200>
<img alt="Cases by symptoms by Health Area" src="https://github.com/djay/covidthailand/wiki/cases_sym.png"  width=200>

- Downloads: [Download JSON](https://github.com/djay/covidthailand/wiki/cases_briefings), [Download CSV](https://github.com/djay/covidthailand/wiki/cases_briefings.csv)
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
- Downloads by Province: [Download JSON](https://github.com/djay/covidthailand/wiki/cases_by_province), [Download CSV](https://github.com/djay/covidthailand/wiki/cases_by_province.csv)
  - "Date": e.g "2021-04-06"
  - "Province": e.g "Samut Sakhon"
  - "Cases": Confirmed cases in this province
  - "Health District Number": 1-13 - see [Thailand Health Areas](https://mophgis.maps.arcgis.com/apps/opsdashboard/index.html#/bcd61791c8b441fa9224d129f28e8be0)  
  - The following is no longer updated
     - "Cases Walkin": Confirmed cases found those requesting tests or asked to from contact tracing or the media. Paid or having met the PUI criteria. *No longer updated*
     - "Cases Proactive": Confirmed cases found government has gone to specific areas to mass test due to high risk of COVID-19. *No longer updated*
     - "Deaths": 31.0

### Cases/Deaths per Health District
- Downloads: [Download JSON](https://github.com/djay/covidthailand/wiki/cases_by_area), [Download CSV](https://github.com/djay/covidthailand/wiki/cases_by_area.csv)
  - "Date": e.g "2021-04-06"
  - "Cases Area {1-13}": Confirmed cases in a given [Health Area](https://mophgis.maps.arcgis.com/apps/opsdashboard/index.html#/bcd61791c8b441fa9224d129f28e8be0)  
  - "Deaths Area {1-13}": Deaths that day in the health district
  - "Cases Risk: {Group} Area {1-13}": Categorisation of Risk field from the covid-19-daily dataset  
  - The following are no longer updated but have some historical data
    - "Cases {Proactive,Walkin} Area {1-13}": Cases found by people where tested

- Notes:
  - [Thailand Health Areas](https://mophgis.maps.arcgis.com/apps/opsdashboard/index.html#/bcd61791c8b441fa9224d129f28e8be0)  

### Deaths by Province
- Downloads: [Download JSON](https://github.com/djay/covidthailand/wiki/deaths), [Download CSV](https://github.com/djay/covidthailand/wiki/deaths.csv)
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


## Daily Situation Reports <a name="dl-situation-reports">
Case Types and PUI counts

- Sources: 
  - [MOPH daily situation report PDFs](https://ddc.moph.go.th/viralpneumonia/situation.php) (Updated daily in the evening)
  - [MOPH daily situation report PDFs (english translations)](https://ddc.moph.go.th/viralpneumonia/eng/situation.php) (Updated a few days later)
  - [DDC Website](https://ddc.moph.go.th/viralpneumonia/index.php) - Today's PUI count

<img alt="PUI from situation reports" src="https://github.com/djay/covidthailand/wiki/tested_pui_all.png" width=200>
<img alt="Case Types" src="https://github.com/djay/covidthailand/wiki/cases_types_all.png" width=200>


- Downloads: [Download JSON](https://github.com/djay/covidthailand/wiki/situation_reports), [Download CSV](https://github.com/djay/covidthailand/wiki/situation_reports.csv)
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
   - [DMSC: Thailand Laboratory testing data - weekly summary reports](https://service.dmsc.moph.go.th/labscovid19/indexen.php#rtpcr) (link at bottom) (updated weekly but sporadic)

<img src="https://github.com/djay/covidthailand/wiki/cases_all.png" width=200 alt="Private and Public Positive Test Results">
<img alt="Private and Public Positive Tests" src="https://github.com/djay/covidthailand/wiki/tests_all.png" width=200>
<img alt="Positive Test Results by health area" src="https://github.com/djay/covidthailand/wiki/pos_area_daily_all.png" width=200>
<img alt="PCR Tests by health area" src="https://github.com/djay/covidthailand/wiki/tests_area_daily_all.png" width=200>
<img alt="Positive Rate by Health District in overall positive rate (ex. some proactive tests)" src="https://github.com/djay/covidthailand/wiki/positivity_area_all.png" width=200>

### Daily Tests Private+Public
- Downloads: [Download JSON](https://github.com/djay/covidthailand/wiki/tests_pubpriv), [Download CSV](https://github.com/djay/covidthailand/wiki/tests_pubpriv.csv)
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

- Downloads: [Download JSON](https://github.com/djay/covidthailand/wiki/tests_by_area), [Download CSV](https://github.com/djay/covidthailand/wiki/tests_by_area.csv)
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
- Source: 
   - [DDC Daily Vaccination Reports](https://ddc.moph.go.th/dcd/pagecontent.php?page=643&dept=dcd)
   - [COVID-19 Vaccines Track and Traceability Platform for Cold Chain and Patient Safety](https://datastudio.google.com/u/0/reporting/731713b6-a3c4-4766-ab9d-a6502a4e7dd6/page/SpZGC)
- [Download CSV](https://github.com/djay/covidthailand/wiki/vac_timeline.csv)
- e.g.
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
- Source: 
   - [COVID-19 Vaccines Track and Traceability Platform for Cold Chain and Patient Safety](https://datastudio.google.com/u/0/reporting/731713b6-a3c4-4766-ab9d-a6502a4e7dd6/page/SpZGC)
- [Download CSV](https://github.com/djay/covidthailand/wiki/vaccinations.csv)
- e.g.
```
      "Date":"2021-04-25",
      "Province": "Bangkok",
      "Vac Given Cum":3189.0,
      "Vac Given ":83.0,
      "Vac Given {vaccine} Cum":3189.0,
      "Vac Given {vaccine}":83.0,
```


## Combined <a name="dl-combined">
- Source: 
  - All the above
  - plus [COVID-19 report, periodic summary](https://data.go.th/dataset/covid-19-daily)
- Downloads: [Download CSV](https://github.com/djay/covidthailand/wiki/combined.csv)
  - See all the above for data definitions

# License

<a rel="license" href="http://creativecommons.org/licenses/by/4.0/"><img alt="Creative Commons License" style="border-width:0" src="https://i.creativecommons.org/l/by/4.0/88x31.png" /></a><br />This work is licensed under a <a rel="license" href="http://creativecommons.org/licenses/by/4.0/">Creative Commons Attribution 4.0 International License</a>.
# About

Made with python/pandas/matplotlib. Dylan Jay gave a talk on how easy it is to extract data from PDFs
and powerpoints and plot data at [Bangkok's Monthly ThaiPy Event](https://www.meetup.com/en-AU/ThaiPy-Bangkok-Python-Meetup) [Video: "How I scraped Thailand's covid data" (1h mark)](https://www.facebook.com/watch/live/?v=2890282134582879)

Why do this? Originally to answer the question ["Was Thailand doing enough testing?"](https://github.com/djay/covidthailand/wiki) for myself and others. Shorter answer: to slow down jumping to conclusions.

## Contributors
- [Dylan Jay](https://github.com/djay)
- [Vincent Casagrande](https://github.com/flyingvince)
- [Peter Scully](https://github.com/pmdscully)
- Help us? - [Submit issue or contribute a fix: Github](https://github.com/djay/covidthailand/issues)

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
- 2021-06-29 - Use coldchain data to plot deliveries and province vac data
- 2021-06-22 - Add trending provinces for contact cases
- 2021-06-12 - Add vacination daily and improve cumulative vaccinations
- 2021-06-05 - update vaccination reports to parse summary timeline data only (missing source)
- 2021-06-30 - death reasons and hospitalisation critical plots
- 2021-05-21 - Estimate of Infections from Deaths
- 2021-05-18 - Include prisons as seperate province/health district (because briefings do)
- 2021-05-15 - improve highest positive rate plot to show top 5 only.
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

## TODO (looking for contributors!)
- get death age brackets from CFR in situation reports
  - also has intersection of deaths and disease deaths
- estimate median age of death from population. 
  - could help show if cases or deaths are underreported or if elderly are more protected in thailand
  - potentially could adjust the IFR to get a better infeciton estimtate.
- excess deaths adjusted for road accidents and suicides
  - https://github.com/TheEconomist/covid-19-excess-deaths-tracker/blob/master/output-data/excess-deaths/thailand_excess_deaths.csv
- plot nationality of cases over time, thai, neighbours, others. Perhaps compare against known populations?
- do some graphs showing north, south, east, central, bangkok.
  - same breakdown as briefing infographic
  - these? https://www.facebook.com/informationcovid19/posts/322313232720341
- think of a metric that shows test capacity bottleneck or reluctance to test
- Fix unknowns to make more clear
  - active cases looks like severe etc disappear for a day. Maybe need "unknown condition" category? or just don't show the data?
- export csv of dated sources
- find historical source for mild vs severe hospitalisations
- get source for sym/asym for latest cases
  - stopped being put in briefings
- plot % subplots to get closer detail
  - esp for the by district plots
- switch to plotly to interactively inspect values
  - https://towardsdatascience.com/how-to-create-a-plotly-visualization-and-embed-it-on-websites-517c1a78568b
  - or another way to show values at an x position. such as SVG, or css imgmap?
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

<a href="https://github.com/djay/covidthailand" class="github-corner" aria-label="View source on GitHub"><svg width="80" height="80" viewBox="0 0 250 250" style="fill:#151513; color:#fff; position: absolute; top: 0; border: 0; right: 0;" aria-hidden="true"><path d="M0,0 L115,115 L130,115 L142,142 L250,250 L250,0 Z"></path><path d="M128.3,109.0 C113.8,99.7 119.0,89.6 119.0,89.6 C122.0,82.7 120.5,78.6 120.5,78.6 C119.2,72.0 123.4,76.3 123.4,76.3 C127.3,80.9 125.5,87.3 125.5,87.3 C122.9,97.6 130.6,101.9 134.4,103.2" fill="currentColor" style="transform-origin: 130px 106px;" class="octo-arm"></path><path d="M115.0,115.0 C114.9,115.1 118.7,116.5 119.8,115.4 L133.7,101.6 C136.9,99.2 139.9,98.4 142.2,98.6 C133.8,88.0 127.5,74.4 143.8,58.0 C148.5,53.4 154.0,51.2 159.7,51.0 C160.3,49.4 163.2,43.6 171.4,40.1 C171.4,40.1 176.1,42.5 178.8,56.2 C183.1,58.6 187.2,61.8 190.9,65.4 C194.5,69.0 197.7,73.2 200.1,77.6 C213.8,80.2 216.3,84.9 216.3,84.9 C212.7,93.1 206.9,96.0 205.4,96.6 C205.1,102.4 203.0,107.8 198.3,112.5 C181.9,128.9 168.3,122.5 157.7,114.1 C157.9,116.9 156.7,120.9 152.7,124.9 L141.0,136.5 C139.8,137.7 141.6,141.9 141.8,141.8 Z" fill="currentColor" class="octo-body"></path></svg></a><style>.github-corner:hover .octo-arm{animation:octocat-wave 560ms ease-in-out}@keyframes octocat-wave{0%,100%{transform:rotate(0)}20%,60%{transform:rotate(-25deg)}40%,80%{transform:rotate(10deg)}}@media (max-width:500px){.github-corner:hover .octo-arm{animation:none}.github-corner .octo-arm{animation:octocat-wave 560ms ease-in-out}}</style>