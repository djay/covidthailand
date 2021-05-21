# Thailand COVID-19 Data

Thailand COVID-19 case/test/vaccination data gathered and combined from various government sources for others to view or download. Updated between 11:30-12:30 daily [![last update was](https://github.com/djay/covidthailand/actions/workflows/main.yml/badge.svg)](https://github.com/djay/covidthailand/actions/workflows/main.yml). 

## [Active Cases](#active-cases) | [Cases](#cases) |  [Deaths](#deaths) | [Testing](#testing) | [Vaccinations](#vaccinations) | [Downloads](#downloads) | [About](#about)

*Note* Now available full page at https://djay.github.io/covidthailand

# Active Cases 3rd Wave <a name="active-cases">

![Thailand Active Cases](https://github.com/djay/covidthailand/wiki/cases_cumulative_3.png)

- Break down of active case status only available from 2020-04-24 onwards.
- Other Active Cases + severe + ventilator + field hospitals = Hospitalised, which is everyone who is 
  confirmed (for 14days at least)
- see [Thailand Active Cases 2020-2021]((https://github.com/djay/covidthailand/wiki/cases_active_all.png))
- Source: [CCSA Daily Briefing ](#dl-briefings)

# Cases

## Cases vs Estimated Infections based on Deaths

![Cases vs Estimated Infections](https://github.com/djay/covidthailand/wiki/cases_infections_estimate_2.png)

- Uses [estimated global Infection Fatality Rate](http://epimonitor.net/Covid-IFR-Analysis.htm) 
  and applies it do Thailand province demographics to get an Infection Fatality Rate (IFR) per province and then applies this
  to each death (-14 days) / Province IFR to appoximate the number of infections that would lead to the recorded deaths.
- Some assumptions in this model include:
   - All covid deaths are accounted for. Since reported deaths will be lower than actual deaths
     this estimate is likely a lower bound on the real infections.
   - Elderly in thailand are as protected/cautious as global average, i.e. everyone has an equal chance to catch it.
   - Diseases that increase the chance of death from covid (co-morbidities) have the same prevelence in thailand as globally.
   - Age demographics of a province match those exposed to covid (clusters in factories, prisons etc have different age demographics from the province for example)
   - Everyone sick has equal access to good healthcare (e.g. health system is not overloaded) 
- [ICL Covid Model](https://mrc-ide.github.io/global-lmic-reports/THA/) does a more detailed
  estimate with upper and lower bounds and includes future case/death/hospitalisation predictions. [OWID Covid Models](https://ourworldindata.org/covid-models) has more models for thailand to compare.
- Sources: [CCSA Daily Briefing](#dl-briefings), [Covid IFR Analysis](http://epimonitor.net/Covid-IFR-Analysis.htm), [Thailand population by Age](http://statbbi.nso.go.th/staticreport/Page/sector/EN/report/sector_01_11101_EN_.xlsx)

## Cases by Health District

![Cases by Health District](https://github.com/djay/covidthailand/wiki/cases_areas_2.png)

- [Cases by Health District: Full Year](https://github.com/djay/covidthailand/wiki/cases_areas_all.png)
- [Thailand Health Districts](https://mophgis.maps.arcgis.com/apps/opsdashboard/index.html#/bcd61791c8b441fa9224d129f28e8be0)
- You can also see [Cases by District broken down by walk-in vs proactive](#dl-situation-reports) but there is no longer a data source to keep this updated.
- Sources: [CCSA Daily Briefing](#dl-briefings)

## Provinces with Cases Trending Up (last 3 days)

![Provinces with Cases Trending Up](https://github.com/djay/covidthailand/wiki/cases_prov_increasing_30d.png)

![Provinces with Most Cases](https://github.com/djay/covidthailand/wiki/cases_prov_top_30d.png)

- see also [Provinces with Cases Trending Down](https://github.com/djay/covidthailand/wiki/cases_prov_decreasing_30d.png)
- Sources: [CCSA Daily Briefing](#dl-briefings)

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

## Cases by Age
![Ages of Confirmed cases](https://github.com/djay/covidthailand/wiki/cases_ages_2.png)

- see [Ages of confirmed cases 2020-2021]((https://github.com/djay/covidthailand/wiki/cases_ages_all.png))
- Source: [API: Daily reports of COVID-19 infections](https://data.go.th/dataset/covid-19-daily)


# Deaths

## COVID-19 Deaths by Health District

![Thailand Covid Deaths by Health District](https://github.com/djay/covidthailand/wiki/deaths_by_area_3.png)
- source: [CCSA Daily Briefing ](#dl-briefings)


## COVID-19 Deaths Age Range

![Thailand Covid Death Age Range](https://github.com/djay/covidthailand/wiki/deaths_age_3.png)
- Source: [CCSA Daily Briefing ](#dl-briefings) 

# Testing

## Positive Rate

![Positive Rate](https://github.com/djay/covidthailand/wiki/positivity_2.png)

- [Enough testing is happening if positive rate is < %3](https://www.jhsph.edu/covid-19/articles/covid-19-testing-understanding-the-percent-positive.html) (not tests per population), however this works only if everyone who might have COVID-19 is equally likely to get tested. This has changed over time in thailand.
- *NOTE* Cases/3*PUI seems to give an estimate of positive rate (when proactive testing is low), so it is included for when testing data is delayed. *Note* it is not the actual positive rate.
- [Positive Rate: Full year](https://github.com/djay/covidthailand/wiki/positivity_all.png) 
- This positive rate is based on [DMSC: Thailand Laboratory testing data](#dl-testing). In the [Daily MOPH Situation Reports](#dl-situation-reports) is a number labelled ```Total number of laboratory tests```.  [```Total number of laboratory tests``` is mislabelled and is exactly the same as the PUI number](https://github.com/djay/covidthailand/wiki). 
- [In appears not all proactive cases have been confirmed with PCR tests in the past](https://github.com/djay/covidthailand/wiki) which has previously made Thailand positive rate lower [compared to other countries](https://ourworldindata.org/grapher/positive-rate-daily-smoothed) during times of high proactive testing. However, excluding proactive testing maybe give a better indication of how many more cases might be found if you tested more since proactive testing is normally done in a high risk specific area, ie it's less of a random sampling of the general population.
- [Tests per Case Graph](https://github.com/djay/covidthailand/wiki/tests_per_case.png) (Positive rate inverted) could be easier to understand.
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

![Vaccinations in Thailand](https://github.com/djay/covidthailand/wiki/vac_groups_3.png)
- Source: [DDC Daily Vaccination Reports](#dl-vac)

## Provinces most fully vaccinated

![Vaccinations in Thailand](https://github.com/djay/covidthailand/wiki/vac_top5_full_3.png)

## Vaccinations by Health District
![Vaccinations in Thailand](https://github.com/djay/covidthailand/wiki/vac_areas_s2_3.png)
![Vaccinations in Thailand](https://github.com/djay/covidthailand/wiki/vac_areas_s1_3.png)
- Source: [DDC Daily Vaccination Reports](#dl-vac)

# Downloads

## Disclaimer
- Some data sources are missing days or numbers (e.g. situation reports and tests)
- Some are scraped from text, and the code to do this might not be perfect
- Some are translations where mistypings have happened. Some manual corrections are included
- I take no responsibility for the accuracy of this data.

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

## Daily Situation Reports <a name="dl-situation-reports">
Case Types and PUI counts

- Sources: 
  - [MOPH daily situation report PDFs](https://ddc.moph.go.th/viralpneumonia/situation.php) (Updated daily in the evening)
  - [MOPH daily situation report PDFs (english translations)](https://ddc.moph.go.th/viralpneumonia/eng/situation.php) (Updated a few days later)
  - [DDC Website](https://ddc.moph.go.th/viralpneumonia/index.php) - Today's PUI count

<img alt="PUI from situation reports" src="https://github.com/djay/covidthailand/wiki/tested_pui.png" width=200>
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

## Daily CCSA Briefings <a name="dl-briefings">
- Sources 
   - [CCSA Daily Briefing ](https://www.facebook.com/informationcovid19) - 12pm each day
   - [API: Daily reports of COVID-19 infections](https://data.go.th/dataset/covid-19-daily) - 1-2 days delayed
   - [Daily infographics translated and tweeted](https://twitter.com/search?q=%22%F0%9F%91%89%22%20%F0%9F%93%8D%20(from%3ARichardBarrow)&src=typed_query&f=live) Updated daily around midday (after gov briefing) - *No Longer updated*

<img alt="Cases by Health Area" src="https://github.com/djay/covidthailand/wiki/cases_areas_all.png"  width=200>
<img alt="Walk-in Cases by Health Area" src="https://github.com/djay/covidthailand/wiki/cases_areas_walkins.png" width=200>
<img alt="Proactive Cases by Health Area" src="https://github.com/djay/covidthailand/wiki/cases_areas_proactive.png" width=200>
<img alt="Cases by symptoms by Health Area" src="https://github.com/djay/covidthailand/wiki/cases_sym.png"  width=200>

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
  - "Deaths":27.0,
  - "Deaths Age (Min|Max)":92.0,
  - "Deaths Age Median":66.0,
  - "Deaths Area {1-13}":3.0,  
  - "Hospitalized":30011.0,
  - "Hospitalized Field":8558.0,
  - "Hospitalized Hospital":21453.0, - total current active cases - anyone confirmed is considered hospitalized
  - "Hospitalized Respirator":311.0,
  - "Hospitalized Severe":1009.0  
  - The following are no longer updated but have some historical data
    - "Cases Proactive Area {1-13}": Cases found by people requesting tests in a given [Health Area](https://mophgis.maps.arcgis.com/apps/opsdashboard/index.html#/bcd61791c8b441fa9224d129f28e8be0)  
    - "Cases Walkin Area {1-13}": Cases found by government testing in specific location with in a given [Health Area](https://mophgis.maps.arcgis.com/apps/opsdashboard/index.html#/bcd61791c8b441fa9224d129f28e8be0)
    - "Cases (Asymptomatic|Symptomatic)":null, - No longer reported in briefing reports

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

## Daily DDC Vaccination Reports <a name="dl-vac">
- Source: [DDC Daily Vaccination Reports](https://ddc.moph.go.th/dcd/pagecontent.php?page=643&dept=dcd)
- [Download CSV](https://github.com/djay/covidthailand/wiki/vaccinations.csv)
- e.g.
```
      "Date":"2021-04-25",
      "Province":"Mae Hong Son",
      "Vaccinations Allocated Sinovac {1|2}":3840.0,
      "Vaccinations Allocated AstraZeneca {1|2}":0.0,
      "Vaccinations Given {1|2} Cum":3189.0,
      "Vaccinations Given {1|2} %":83.0,
      "Vaccinations Group Medical Staff {1|2} Cum":1939.0,
      "Vaccinations Group Other Frontline Staff {1|2} Cum":1081.0,
      "Vaccinations Group Over 60 {1|2} Cum":0.0,
      "Vaccinations Group Risk: Disease {1|2} Cum":54.0,
      "Vaccinations Group Risk: Location {1|2} Cum":115.0,
```
- Note
   - "Vaccinations Given 1/2 %" refers to the % of allocation, not against population.
   - 1/2 refers to shot 1 or shot 2.
   - Some days some tables are images so there is missing data. 
   - Summary vaccination data included in the combine download
   - #TODO: put in thai group explanations.


## Combined <a name="dl-combined">
- Source: 
  - All the above
  - plus [COVID-19 report, periodic summary](https://data.go.th/dataset/covid-19-daily)
- Downloads: [Download CSV](https://github.com/djay/covidthailand/wiki/combined.csv)
  - See all the above for data definitions

# About

Made with python/pandas/matplotlib. Dylan Jay gave a talk on how easy it is to extract data from PDFs
and powerpoints and plot data at [Bangkok's Monthly ThaiPy Event](https://www.meetup.com/en-AU/ThaiPy-Bangkok-Python-Meetup) [Video: "How I scraped Thailand's covid data" (1h mark)](https://www.facebook.com/watch/live/?v=2890282134582879)

Why do this? Originally to answer the question ["Was Thailand doing enough testing?"](https://github.com/djay/covidthailand/wiki) for myself and others.

## Contributors
- [Dylan Jay](https://github.com/djay)
- [Vincent Casagrande](https://github.com/flyingvince)
- [Submit or contribute: Github](https://github.com/djay/covidthailand/issues)

## Other sources of visualisations/Data for Thailand

- [Our World in Data: Thailand Profile](https://ourworldindata.org/coronavirus/country/thailand?country=~THA#what-is-the-daily-number-of-confirmed-cases) - the best way to compare against other countries
- [Pete Scully: COVID-19 Thailand Public Data](https://petescully.co.uk/research/covid19-thailand-dashboards/) for added visualisations and comparisons
- [Stefano Starita](https://twitter.com/DrSteStarita) - more excellent analysis and visualisations
- [Richard Barrow](https://www.facebook.com/richardbarrowthailand) - maybe the fastest way to get COVID-19 updates in English
- Thai Gov news feeds
  - [Thai Gov Spokesman: FB](https://www.facebook.com/ThaigovSpokesman), 
  - [Thai Government PR: FB](https://www.facebook.com/thailandprd),
  - [Ministry of Health: Twitter](https://twitter.com/thaimoph), 
  - [DMSC PR: FB](https://www.facebook.com/DMSc.PR.Network)
- [MOPH ArcGIS](https://mophgis.maps.arcgis.com/apps/opsdashboard/index.html#/210413ebb5ff49bb8914808af6473322) - PUI + worldwide covid stats
- [MOPH OPS Dashboard: ArcGIS](https://mophgis.maps.arcgis.com/apps/opsdashboard/index.html#/bcd61791c8b441fa9224d129f28e8be0?) - current usage of hospital resource but seems no longer updated (since mid last year?)
  
## Change log
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
- trending province vaccinations
- estimate median age of death from population. 
  - could help show if cases or deaths are underreported or if elderly are more protected in thailand
  - potentially could adjust the IFR to get a better infeciton estimtate.
- put non MA lines on some area graphs e.g. deaths, cases 
- fix vaccinations by parsing daily numbers table
- plot nationality of cases over time, thai, neighbours, others. Perhaps compare against known populations?
- do some graphs showing north, south, east, central, bangkok.
  - same breakdown as briefing infographic
  - these? https://www.facebook.com/informationcovid19/posts/322313232720341
- think of a metric that shows test capacity bottleneck or reluctance to test
- Fix unknowns to make more clear
  - active cases looks like severe etc disappear for a day. Maybe need "unknown condition" category? or just don't show the data?
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