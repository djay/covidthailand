import datetime
import dateutil
from io import StringIO
from itertools import compress, cycle
import os
import pickle
import re
import urllib.parse

from bs4 import BeautifulSoup
from pptx import Presentation
from pytwitterscraper import TwitterScraper
import requests
from requests.exceptions import Timeout, ConnectionError
from requests.adapters import HTTPAdapter, Retry
from tika import parser
from webdav3.client import Client


CHECK_NEWER = bool(os.environ.get("CHECK_NEWER", False))
USE_CACHE_DATA = os.environ.get('USE_CACHE_DATA', False) == 'True'
MAX_DAYS = int(os.environ.get("MAX_DAYS", 1 if USE_CACHE_DATA else 0))

NUM_RE = re.compile(r"\d+(?:\,\d+)*(?:\.\d+)?")
INT_RE = re.compile(r"\d+(?:\,\d+)*")
NUM_OR_DASH = re.compile(r"([0-9\,\.]+|-)-?")

requests.adapters.DEFAULT_RETRIES = 3  # for other tools that use requests internally
s = requests.Session()
RETRY = Retry(
    total=3, backoff_factor=1
)  # should make it more reliable as ddc.moph.go.th often fails
s.mount("http://", HTTPAdapter(max_retries=RETRY))
s.mount("https://", HTTPAdapter(max_retries=RETRY))


####################
# Extraction helpers
#####################
def parse_file(filename, html=False, paged=True):
    pages_txt = []

    # Read PDF file
    data = parser.from_file(filename, xmlContent=True)
    xhtml_data = BeautifulSoup(data["content"], features="lxml")
    if html and not paged:
        return xhtml_data
    pages = xhtml_data.find_all("div", attrs={"class": ["page", "slide-content"]})
    if not pages:
        if not paged:
            return repr(xhtml_data)
        else:
            return [repr(xhtml_data)]

    # TODO: slides are divided by slide-content and slide-master-content rather than being contained
    for i, content in enumerate(pages):
        # Parse PDF data using TIKA (xml/html)
        # It's faster and safer to create a new buffer than truncating it
        # https://stackoverflow.com/questions/4330812/how-do-i-clear-a-stringio-object
        _buffer = StringIO()
        _buffer.write(str(content))
        parsed_content = parser.from_buffer(_buffer.getvalue())
        if parsed_content["content"] is None:
            continue

        # Add pages
        text = parsed_content["content"].strip()
        if html:
            pages_txt.append(repr(content))
        else:
            pages_txt.append(text)
    if paged:
        return pages_txt
    else:
        return '\n\n\n'.join(pages_txt)


def get_next_numbers(content, *matches, debug=False, before=False, remove=0, ints=True, until=None, return_rest=True):
    if len(matches) == 0:
        matches = [""]
    for match in matches:
        if type(match) == str:
            match = re.compile(f"({match})")
        ahead, *behind = match.split(content, 1) if match else ("", "", content)
        if not behind:
            continue
        matched, *behind = behind
        behind = "".join(behind)
        found = ahead if before else behind
        if until is not None:
            found, *rest = re.split(until, found, 1)  # TODO: how to put it back togeather if behind=True?
            rest = until + (rest[0] if rest else "")
        else:
            rest = ""
        numbers = (INT_RE if ints else NUM_RE).findall(found)
        numbers = [n.replace(",", "") for n in numbers]
        numbers = [int(n) if ints else float(n) for n in numbers if n]
        numbers = numbers if not before else list(reversed(numbers))
        if remove:
            behind = (INT_RE if ints else NUM_RE).sub("", found, remove)
        if return_rest:
            return numbers, matched + " " + rest + behind
        else:
            return numbers
    if debug and matches:
        print("Couldn't find '{}'".format(match))
        print(content)
    if return_rest:
        return [], content
    else:
        return []


def get_next_number(content, *matches, default=None, remove=False, before=False, until=None):
    num, rest = get_next_numbers(content, *matches, remove=1 if remove else 0, before=before, until=until)
    return num[0] if num else default, rest


def toint(s):
    return int(s.replace(',', '')) if s else None


def slide2text(slide):
    text = ""
    if slide.shapes.title:
        text += slide.shapes.title.text
    for shape in slide.shapes:
        if shape.has_text_frame:
            # for p in shape.text_frame:
            text += "\n" + shape.text
    return text


def pptx2chartdata(file):
    prs = Presentation(file)
    for i, slide in enumerate(prs.slides):
        for shape in slide.shapes:
            if not shape.has_chart:
                continue
            chart = shape.chart
            if chart is None:
                continue
            title = chart.chart_title.text_frame.text if chart.has_title else ""
            series = dict([(s.name, s.values) for s in chart.series])

            yield chart, title, series, i


####################
# Download helpers
####################
def resume_from(file, remote_date, check=True, size=0, appending=False):
    if not os.path.exists(file):
        print(f"Missing: {file}")
        return 0
    elif not check:
        return -1
    elif remote_date is None:
        return 0  # TODO: should we always keep cached?
    if type(remote_date) == str:
        remote_date = dateutil.parser.parse(remote_date).astimezone()
    fdate = datetime.datetime.fromtimestamp(os.path.getmtime(file)).astimezone()
    resume_pos = os.stat(file).st_size
    if remote_date > fdate and not appending:
        # timestamp = fdate.strftime("%Y%m%d-%H%M%S")
        # os.rename(file, f"{file}.{timestamp}")
        return 0
    elif remote_date > fdate and resume_pos < size:
        return resume_pos
    elif resume_pos != size:
        return 0
    return -1


def is_cutshort(file, modified, check):

    if type(modified) == str:
        modified = dateutil.parser.parse(modified).astimezone()
    if not check and MAX_DAYS and modified and (datetime.datetime.today().astimezone()
                                                - modified).days > MAX_DAYS and os.path.exists(file):
        print(f"Reached MAX_DAYS={MAX_DAYS}")
        return True
    return False


def web_links(*index_urls, ext=".pdf", dir="html", match=None):
    def is_ext(a):
        return len(a.get("href").rsplit(ext)) == 2 if ext else True

    def is_match(a):
        return a.get("href") and is_ext(a) and (match.search(a.get_text(strip=True)) if match else True)

    for index_url in index_urls:
        for file, index, _ in web_files(index_url, dir=dir, check=True):
            soup = parse_file(file, html=True, paged=False)
            links = (urllib.parse.urljoin(index_url, a.get('href')) for a in soup.find_all('a') if is_match(a))
            for link in links:
                yield link


def web_files(*urls, dir=os.getcwd(), check=CHECK_NEWER, strip_version=False, appending=False):
    "if check is None, then always download"
    i = 0
    for url in urls:
        file = sanitize_filename(url.rsplit("/", 1)[-1])
        if strip_version and '.' in file:
            file = ".".join(file.split(".")[:2])
        file = os.path.join(dir, file)
        os.makedirs(os.path.dirname(file), exist_ok=True)
        resumable = False
        size = None

        if check or MAX_DAYS:
            try:
                r = s.head(url, timeout=1)
                modified = r.headers.get("Last-Modified")
                if r.headers.get("content-range"):
                    pre, size = r.headers.get("content-range").split("/")
                    size = int(size)
                    assert "bytes" in pre
                else:
                    size = int(r.headers.get("content-length", 0))
                resumable = r.headers.get('accept-ranges') == 'bytes' and check and size > 0
            except (Timeout, ConnectionError):
                modified = None
        else:
            modified = None
        if i > 0 and is_cutshort(file, modified, check):
            break
        if (resume_byte_pos := resume_from(file, modified, check, size, appending)) >= 0:
            resume_byte_pos = int(resume_byte_pos * 0.9) if resumable else 0  # go back 10% in case end of data changed (e.g csv)
            resume_header = {'Range': f'bytes={resume_byte_pos}-'} if resumable else {}

            try:
                # TODO: handle resuming based on range requests - https://stackoverflow.com/questions/22894211/how-to-resume-file-download-in-python
                # Will speed up covid-19 download a lot, but might have to jump back to make sure we don't miss data.
                r = s.get(url, timeout=2, stream=True, headers=resume_header, verify=False, allow_redirects=True)
            except (Timeout, ConnectionError):
                r = None
            if r is not None and r.status_code == 200:
                print(f"Download: {file} {modified}", end="")
                os.makedirs(os.path.dirname(file), exist_ok=True)
                mode = "w+b" if resume_byte_pos > 0 else "wb"
                with open(file, mode) as f:
                    f.seek(resume_byte_pos, 0)
                    # TODO: handle timeouts happening below since now switched to streaming
                    try:
                        for chunk in r.iter_content(chunk_size=2 * 1024 * 1024):
                            if chunk:  # filter out keep-alive new chunks
                                f.write(chunk)
                                print(".", end="")
                    except Timeout:
                        print(f"Error downloading: {file}: skipping")
                        continue
                print("")
            elif os.path.exists(file):
                print(f"Error downloading: {file}: using cache")
            else:
                print(f"Error downloading: {file}: skipping")
                continue
        with open(file, "rb") as f:
            content = f.read()
        i += 1
        yield file, content, url


def sanitize_filename(filename):
    return filename.translate(str.maketrans({"*": "_", "?": "_", ":": "_", "\\": "_", "<": "_", ">": "_", "|": "_"}))
    # Windows Filename Compatibility: '?*:<>|'


def dav_files(url, username=None, password=None,
              ext=".pdf .pptx", dir=os.getcwd()):

    options = {
        "webdav_hostname": url,
        "webdav_login": username,
        "webdav_password": password,
    }
    client = Client(options)
    client.session.mount("http://", HTTPAdapter(max_retries=RETRY))
    client.session.mount("https://", HTTPAdapter(max_retries=RETRY))
    # important we get them sorted newest files first as we only fill in NaN from each additional file
    files = sorted(
        client.list(get_info=True),
        key=lambda info: dateutil.parser.parse(info["modified"]),
        reverse=True,
    )
    i = 0
    for info in files:
        file = info["path"].split("/")[-1]
        if not any([ext == file[-len(ext):] for ext in ext.split()]):
            continue
        target = os.path.join(dir, file)
        os.makedirs(os.path.dirname(target), exist_ok=True)
        if i > 0 and is_cutshort(target, info["modified"], False):
            break
        if resume_from(target, info["modified"]) >= 0:
            client.download_file(file, target)
        i += 1
        yield target


#################
# Twitter helpers
#################
def parse_tweet(tw, tweet, found, *matches):
    """if tweet contains any of matches return its text joined with comments by the same person
    that also match (and contain [1/2] etc)"""
    if not any_in(tweet.get('text', tweet.get("comment", "")), *matches):
        return ""
    text = tw.get_tweetinfo(tweet['id']).contents['text']
    if any(text in t for t in found):
        return ""
    # TODO: ensure tweets are [1/2] etc not just "[" and by same person
    if "[" not in text:
        return text
    for t in sorted(tw.get_tweetcomments(tweet['id']).contents, key=lambda t: t['id']):
        rest = parse_tweet(tw, t, found + [text], *matches)
        if rest and rest not in text:
            text += " " + rest
    return text


def get_tweets_from(userid, datefrom, dateto, *matches):
    "return tweets from single person that match, merging in followups of the form [1/2]. Caches to speed up"

    tw = TwitterScraper()
    filename = os.path.join("tweets", f"tweets2_{userid}.pickle")
    os.makedirs("tweets", exist_ok=True)
    try:
        with open(filename, "rb") as fp:
            tweets = pickle.load(fp)
    except (IOError, EOFError, OSError, pickle.PickleError, pickle.UnpicklingError) as e:
        print(f'Error detected when attempting to load the pickle file: {e}, setting an empty \'tweets\' dictionary')
        tweets = {}
    for date, tweet_list in tweets.items():
        fixed = []
        for tweet in tweet_list:
            text, url = (tweet, None) if type(tweet) == str else tweet
            fixed.append((text, (url if url else None)))
        tweets[date] = fixed
    latest = max(tweets.keys()) if tweets else None
    if latest and dateto and latest >= (datetime.datetime.today() if not dateto else dateto).date():
        return tweets
    for limit in [50, 2000, 20000]:
        print(f"Getting {limit} tweets")
        try:
            resp = tw.get_tweets(userid, count=limit).contents
        except requests.exceptions.RequestException:
            resp = []
        for tweet in sorted(resp, key=lambda t: t['id']):
            date = tweet['created_at'].date()
            url = tweet['urls'][0]['url'] if tweet['urls'] else f"https://twitter.com/{userid}/status/{tweet['id']}"
            text = parse_tweet(tw, tweet, tweets.get(date, []), *matches)
            if text:
                tweets[date] = tweets.get(date, []) + [(text, url)]

        earliest = min(tweets.keys())
        latest = max(tweets.keys())
        print(f"got tweets {earliest} to {latest} {len(tweets)}")
        if earliest <= datefrom.date():  # TODO: ensure we have every tweet in sequence?
            break
        else:
            print(f"Retrying: Earliest {earliest}")
    with open(filename, "wb") as fp:
        pickle.dump(tweets, fp)
    return tweets


#################
# String helpers
#################
def remove_prefix(text: str, *prefixes: str) -> str:
    """Removes the prefix of a string"""
    for prefix in prefixes:
        if text.startswith(prefix):
            text = text[len(prefix):]
    return text


def remove_suffix(text: str, *suffixes: str) -> str:
    """Removes the suffix of a string"""
    for suffix in suffixes:
        if suffix and text.endswith(suffix):
            text = text[:-len(suffix)]
    return text


def seperate(seq, condition):
    a, b = [], []
    for item in seq:
        (a if condition(item) else b).append(item)
    return a, b


def split(seq, condition, maxsplit=0):
    "Similar to str.split except works on lists of lines. e.g. split([1,2,3,4], lambda x: x==2) -> [[1],[2],[3,4]]"
    run = []
    last = False
    splits = 0
    for i in seq:
        if (maxsplit and splits >= maxsplit) or bool(condition(i)) == last:
            run.append(i)
        else:
            splits += 1
            yield run
            run = [i]
            last = not last
    yield run

# def nwise(iterable, n=2):
#     iters = tee(iterable, n)
#     for i, it in enumerate(iters):
#         next(islice(it, i, i), None)
#     return zip(*iters)


def pairwise(lst):
    "Takes a list and turns them into pairs of tuples, e.g. [1,2,3,4] -> [[1,2],[3,4]]"
    lst = list(lst)
    return list(zip(compress(lst, cycle([1, 0])), compress(lst, cycle([0, 1]))))


def parse_numbers(lst):
    return [float(i.replace(",", "")) if i != "-" else 0 for i in lst]


def any_in(target, *matches):
    return any((m in target) if type(m) != re.Pattern else m.search(target) for m in matches)


def all_in(target, *matches):
    return all((m in target) if type(m) != re.Pattern else m.search(target) for m in matches)


def strip(lst):
    lst = [i.strip() for i in lst]
    return [i for i in lst if i]


def unique_values(iterable):
    it = iter(iterable)
    seen = set()
    for item in it:
        if item in seen:
            continue
        seen.add(item)
        yield item


def replace_matcher(matches, replacements=None):
    if replacements is None:
        replacements = matches

    def replace_match(item):
        for m, r in zip(matches, replacements):
            if re.search(m, item, re.IGNORECASE):
                return r
        return item
    return replace_match
