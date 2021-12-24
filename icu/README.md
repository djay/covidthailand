# Availiable ICU Bed per province

number of ICU could directly translate to number of ventalators, which is essential number to understand the regional situation.
Mojority of the exceed death might be cause by a hospital cannot accept more emergency cases due to oppupency of bed for COVID patients.
We could explain criticality of healthcare capacity use the ratio of hospitalized patients per availiable ICU.
We could also understand the node strength of a province by calculating adjecent sum of hospitalized patient in surrounded province since the patients could easily transfer to.

## Source
1. Strategist Dept, TH Health Misitry - **PDF** [year 2554](http://thcc.or.th/download/gishealth/report-gis54.pdf) up to [year 2557](http://thcc.or.th/download/gishealth/report-gis57.pdf)
2. [Report 2553, System Capacity](http://hrm.moph.go.th/res53/res-rep2553.html) - [XLS](http://hrm.moph.go.th/res53/report53/res53-tb09.xls)

Note for each source
1. try to bruteforce url by change the year part
2. try with the same step, but not find anymore year

## Output
[`icu\n_icu_bed_2553.csv`](icu\n_icu_bed_2553.csv)  is the **clean data** in **csv** format.
