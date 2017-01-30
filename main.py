import time
from bs4 import BeautifulSoup
from urllib.request import urlopen
from db import get_new
from pymongo import TEXT

init_url = 'http://news.google.com'
database = get_new()
mongo_collection = 'full_url'


def store_meta(soup, url):
    body = " ".join(soup.stripped_strings)
    # body = [string for string in soup.stripped_strings]

    try:
        title = soup.find('title').text
    except AttributeError:
        title = "no title"

    meta_desc = soup.findAll('meta', attrs={"name": "description"})

    if not meta_desc:
        desc = "no description"
    else:
        desc = meta_desc[0].attrs.get('content')

    meta_keywords = soup.findAll('meta', attrs={"name": "keywords"})

    if not meta_keywords:
        keywords = "no keywords"
    else:
        keywords = meta_keywords[0].attrs.get('content')

    database.mongodb[mongo_collection].update_one(
        {'url': url},
        {'$set': {
            'title': title,
            'desc': desc,
            'keywords': keywords,
            'body': body
        }
        },
        upsert=False)


def store_links_in(soup, url):
    for link in soup.find_all('a'):
        href = link.get('href')
        if not href:
            continue
        if "://" not in href:
            continue
        if href and href.startswith('http'):
            domain_list = href.split('/')
            try:
                domain = domain_list[0]+'//'+domain_list[1]+domain_list[2]
            except IndexError:
                print(href, domain_list)
                continue

            a = database.mongodb[mongo_collection].find_one({'url': domain})

            if a:
                if domain != url:
                    database.mongodb[mongo_collection].update_one(
                        {'_id': a['_id']},
                        {'$inc': {
                            'hits': 1
                        }
                        },
                        upsert=False)
            else:
                database.mongodb[mongo_collection].insert({'url': domain,
                                                'hits': 1,
                                                "parsed": False})


def set_entry_parsed(entry):
    database.mongodb[mongo_collection].update_one(
        {'_id': entry['_id']},
        {"$set": {
            "parsed": True
        }
        },
        upsert=False)


def process(url):
    try:
        soup = BeautifulSoup(urlopen(url, timeout=8), 'html.parser')
    except Exception:
        return

    # drop scripts and css
    for script in soup(["script", "style"]):
        script.decompose()  # rip it out

    store_meta(soup, url)
    store_links_in(soup, url)


def run_engine():
    not_parsed = database.mongodb[mongo_collection].find_one({'parsed': False})
    if not not_parsed:
        return
    process(not_parsed['url'])
    set_entry_parsed(not_parsed)


def kick():
    process(init_url)
    database.mongodb[mongo_collection].create_index('parsed')
    database.mongodb[mongo_collection].create_index([('body', TEXT)])
    while True:
        run_engine()
        time.sleep(0.1)

kick()
