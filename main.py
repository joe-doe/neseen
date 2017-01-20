import time
from bs4 import BeautifulSoup
from urllib.request import urlopen
from db import get_new

init_url = 'http://news.google.com'
database = get_new()

def store_meta(soup, url):
    try:
        title = soup.find('title').text
    except AttributeError:
        title = "no title"

    meta = soup.findAll('meta', attrs={"name": "description"})

    if not meta:
        desc = "no description"
    else:
        desc = meta[0].attrs.get('content')

    database.mongodb['url'].update_one(
        {'url': url},
        {'$set': {
            'title': title,
            "desc": desc
        }
        },
        upsert=False)


def store_links_in(soup, url):
    for link in soup.find_all('a'):
        href = link.get('href')
        if href and href.startswith('http'):
            domain_list = href.split('/')
            try:
                domain = domain_list[0]+'//'+domain_list[1]+domain_list[2]
            except IndexError:
                print(href, domain_list)
                continue

            a = database.mongodb['url'].find_one({'url': domain})

            if a:
                database.mongodb['url'].update_one(
                    {'_id': a['_id']},
                    {'$inc': {
                        'hits': 1
                    }
                    },
                    upsert=False)
            else:
                database.mongodb['url'].insert({'url': domain,
                                                'hits': 1,
                                                "parsed": False})


def set_entry_parsed(entry):
    database.mongodb['url'].update_one(
        {'_id': entry['_id']},
        {"$set": {
            "parsed": True
        }
        },
        upsert=False)


def process(url):
    try:
        soup = BeautifulSoup(urlopen(url), 'html.parser')
    except Exception:
        return

    store_meta(soup, url)
    store_links_in(soup, url)


def run_engine():
    not_parsed = database.mongodb['url'].find_one({'parsed': False})
    process(not_parsed['url'])
    set_entry_parsed(not_parsed)


def kick():
    # process(init_url)
    database.mongodb['url'].create_index('parsed')
    while True:
        run_engine()
        time.sleep(0.1)

kick()