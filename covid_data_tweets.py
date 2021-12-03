import re

import pandas as pd
from dateutil.parser import parse as d

from utils_pandas import cum2daily
from utils_scraping import any_in
from utils_scraping import get_next_number
from utils_scraping import get_next_numbers
from utils_scraping import get_tweets_from
from utils_scraping import logger
from utils_scraping import toint


##################################
# RB Tweet Parsing
##################################

UNOFFICIAL_TWEET = re.compile("Full details at 12:30pm.*#COVID19")
OFFICIAL_TWEET = re.compile("#COVID19 update")
MOPH_TWEET = re.compile("🇹🇭 ยอดผู้ติดเชื้อโควิด-19")


def parse_official_tweet(df, date, text, url):
    imported, _ = get_next_number(text, "imported", before=True, default=0)
    local, _ = get_next_number(text, "local", before=True, default=0)
    cases = imported + local
    # cases_cum, _ = get_next_number(text, "Since Jan(?:uary)? 2020")
    deaths, _ = get_next_number(text, "dead +", "deaths +")
    serious, _ = get_next_number(text, "in serious condition", "in ICU", before=True)
    recovered, _ = get_next_number(text, "discharged", "left care", before=True)
    hospitalised, _ = get_next_number(text, "in care", before=True)
    vent, _ = get_next_number(text, "on ventilators", before=True)
    cols = [
        "Date",
        "Cases Imported",
        "Cases Local Transmission",
        "Cases",
        "Deaths",
        "Hospitalized",
        "Recovered",
        "Hospitalized Severe",
        "Hospitalized Respirator",
        "Source Cases"
    ]
    row = [date, imported, local, cases, deaths]
    row2 = row + [hospitalised, recovered]
    if date >= d("2021-10-14").date():
        # there is a problem but we no longer need this so just skip
        return df
    elif date <= d("2021-05-01").date():
        assert not any_in(row, None), f"{date} Missing data in Official Tweet {row}"
    else:
        assert not any_in(row2, None), f"{date} Missing data in Official Tweet {row}"
    row_opt = row2 + [serious, vent, url]
    tdf = pd.DataFrame([row_opt], columns=cols).set_index("Date")
    logger.info("{} Official: {}", date, tdf.to_string(index=False, header=False))
    return df.combine_first(tdf)


def parse_unofficial_tweet(df, date, text, url):
    if not UNOFFICIAL_TWEET.search(text):
        return df
    deaths, _ = get_next_number(text, "deaths", before=True)
    cases, _ = get_next_number(text, "cases", before=True)
    prisons, _ = get_next_number(text, "prisons", before=True)
    if any_in([None], deaths, cases):
        # raise Exception(f"Can't parse tweet {date} {text}")
        return df
    cols = ["Date", "Deaths", "Cases", "Cases Area Prison", "Source Cases"]
    row = [date, deaths, cases, prisons, url]
    tdf = pd.DataFrame([row], columns=cols).set_index("Date")
    logger.info("{} Breaking: {}", date, tdf.to_string(index=False, header=False))
    return df.combine_first(tdf)


def parse_moph_tweet(df, date, text, url):
    """https://twitter.com/thaimoph"""
    cases, _ = get_next_number(text, "รวม", "ติดเชื้อใหม่", until="ราย")
    prisons, _ = get_next_number(text, "ที่ต้องขัง", "ในเรือนจำ", until="ราย")
    recovered, _ = get_next_number(text, "หายป่วย", "หายป่วยกลับบ้าน", until="ราย")
    deaths, _ = get_next_number(text, "เสียชีวิต", "เสียชีวิต", until="ราย")

    if any_in([None], cases):
        # https://twitter.com/thaimoph/status/1460412804880424963 no deaths
        raise Exception(f"Can't parse tweet {date} {text}")
    numbers, _ = get_next_numbers(text, "ราย", until="ตั้งแต่")  # TODO: test len to make sure we didn't miss something

    if any_in([None], prisons, recovered):
        pass
    cols = ["Date", "Deaths", "Cases", "Cases Area Prison", "Recovered", "Source Cases"]
    row = [date, deaths, cases, prisons, recovered, url]
    tdf = pd.DataFrame([row], columns=cols).set_index("Date")
    logger.info("{} Moph: {}", date, tdf.to_string(index=False, header=False))
    return df.combine_first(tdf)


def parse_case_prov_tweet(walkins, proactive, date, text, url):
    if "📍" not in text:
        return walkins, proactive
    if "ventilators" in text:  # after 2021-05-11 start using "👉" for hospitalisation
        return walkins, proactive
    start, *lines = text.split("👉", 2)
    if len(lines) < 2:
        raise Exception()
    for line in lines:
        prov_matches = re.findall(r"📍([\s\w,&;]+) ([0-9]+)", line)
        prov = dict((p.strip(), toint(v)) for ps, v in prov_matches for p in re.split("(?:,|&amp;)", ps))
        if d("2021-04-08").date() == date:
            if prov["Bangkok"] == 147:  # proactive
                prov["Bangkok"] = 47
            elif prov["Phuket"] == 3:  # Walkins
                prov["Chumphon"] = 3
                prov['Khon Kaen'] = 3
                prov["Ubon Thani"] = 7
                prov["Nakhon Pathom"] = 6
                prov["Phitsanulok"] = 4

        label = re.findall(r'^ *([0-9]+)([^📍👉👇\[]*)', line)
        if label:
            total, label = label[0]
            # label = label.split("👉").pop() # Just in case tweets get muddled 2020-04-07
            total = toint(total)
        else:
            raise Exception(f"Couldn't find case type in: {date} {line}")
        if total is None:
            raise Exception(f"Couldn't parse number of cases in: {date} {line}")
        elif total != sum(prov.values()):
            raise Exception(f"bad parse of {date} {total}!={sum(prov.values())}: {text}")
        if "proactive" in label:
            proactive.update(dict(((date, k), v) for k, v in prov.items()))
            logger.info("{} Proactive: {}", date, len(prov))
            # proactive[(date,"All")] = total
        elif "walk-in" in label:
            walkins.update(dict(((date, k), v) for k, v in prov.items()))
            logger.info("{} Walkins: {}", date, len(prov))
            # walkins[(date,"All")] = total
        else:
            raise Exception()
    return walkins, proactive


def get_cases_by_prov_tweets():
    logger.info("========RB Tweets==========")
    # These are published early so quickest way to get data
    # previously also used to get per province case stats but no longer published

    # Get tweets
    # 2021-03-01 and 2021-03-05 are missing
    # new = get_tweets_from(531202184, d("2021-04-03"), None, OFFICIAL_TWEET, "📍")
    new = get_tweets_from(531202184, d("2021-06-06"), None, OFFICIAL_TWEET, "📍")
    # old = get_tweets_from(72888855, d("2021-01-14"), d("2021-04-02"), "Official #COVID19 update", "📍")
    # old = get_tweets_from(72888855, d("2021-02-21"), None, OFFICIAL_TWEET, "📍")
    old = get_tweets_from(72888855, d("2021-05-21"), None, OFFICIAL_TWEET, "📍")
    # unofficial = get_tweets_from(531202184, d("2021-04-03"), None, UNOFFICIAL_TWEET)
    unofficial = get_tweets_from(531202184, d("2021-06-06"), None, UNOFFICIAL_TWEET)
    thaimoph = get_tweets_from(2789900497, d("2021-06-18"), None, MOPH_TWEET)
    officials = {}
    provs = {}
    breaking = {}
    for date, tweets in list(new.items()) + list(old.items()):
        for tweet, url in tweets:
            if "RT @RichardBarrow" in tweet:
                continue
            if OFFICIAL_TWEET.search(tweet):
                officials[date] = tweet, url
            elif "👉" in tweet and "📍" in tweet:
                if tweet in provs.get(date, ""):
                    continue
                provs[date] = provs.get(date, "") + " " + tweet
    for date, tweets in unofficial.items():
        for tweet, url in tweets:
            if UNOFFICIAL_TWEET.search(tweet):
                breaking[date] = tweet, url

    # Get imported vs walkin totals
    df = pd.DataFrame()

    for date, tweets in sorted(thaimoph.items(), reverse=True):
        for tweet, url in tweets:
            df = df.pipe(parse_moph_tweet, date, tweet, url)

    for date, tweet in sorted(officials.items(), reverse=True):
        text, url = tweet
        df = df.pipe(parse_official_tweet, date, text, url)

    for date, tweet in sorted(breaking.items(), reverse=True):
        text, url = tweet
        if date in officials:
            # do unofficial tweets if no official tweet
            continue
        df = df.pipe(parse_unofficial_tweet, date, text, url)

    # get walkin vs proactive by area
    walkins = {}
    proactive = {}
    for date, text in provs.items():
        walkins, proactive = parse_case_prov_tweet(walkins, proactive, date, text)

    # Add in missing data
    date = d("2021-03-01")
    p = {"Pathum Thani": 35, "Nonthaburi": 1}  # "All":36,
    proactive.update(((date, k), v) for k, v in p.items())
    w = {"Samut Sakhon": 19, "Tak": 3, "Nakhon Pathom": 2, "Bangkok": 2, "Chonburi": 1, "Ratchaburi": 1}  # "All":28,
    walkins.update(((date, k), v) for k, v in w.items())
    cols = ["Date", "Province", "Cases Walkin", "Cases Proactive"]
    rows = []
    for date, province in set(walkins.keys()).union(set(proactive.keys())):
        rows.append([date, province, walkins.get((date, province)), proactive.get((date, province))])
    dfprov = pd.DataFrame(rows, columns=cols)
    index = pd.MultiIndex.from_frame(dfprov[['Date', 'Province']])
    dfprov = dfprov.set_index(index)[["Cases Walkin", "Cases Proactive"]]
    df = df.combine_first(cum2daily(df))
    return dfprov, df
