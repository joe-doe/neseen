import time
from bs4 import BeautifulSoup
from urllib.request import urlopen
from datastores.mongo.db import get_new
from multiprocessing import Process

init_url = 'http://news.google.com'
mongo_collection = 'murl'


def store_meta(soup, url, database):
    try:
        title = soup.find('title').text
    except AttributeError:
        title = "no title"

    meta = soup.findAll('meta', attrs={"name": "description"})

    if not meta:
        desc = "no description"
    else:
        desc = meta[0].attrs.get('content')

    database.mongodb[mongo_collection].update_one(
        {'url': url},
        {'$set': {
            'title': title,
            'desc': desc
        }
        },
        upsert=False)


def store_links_in(soup, database):
    for link in soup.find_all('a'):
        href = link.get('href')
        if href and href.startswith('http'):
            domain_list = href.split('/')
            try:
                domain = domain_list[0] + '//' + domain_list[1] + domain_list[
                    2]
            except IndexError:
                print(href, domain_list)
                continue

            a = database.mongodb[mongo_collection].find_one({'url': domain})

            if a:
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


def set_entry_parsed(url, database):
    database.mongodb[mongo_collection].update_one(
        {'url': url},
        {"$set": {
            "parsed": True
        }
        },
        upsert=False)


def my_process(url):
    database = get_new()
    try:
        soup = BeautifulSoup(urlopen(url), 'html.parser')
    except Exception:
        print("Really bad !")
        return

    store_meta(soup, url, database)
    store_links_in(soup, database)


def run_engine(database):
    procs = []
    po = Process(target=clean_up, args=(procs, ))
    po.start()
    while True:
        not_parsed = database.mongodb[mongo_collection].find_one(
            {'parsed': False})
        if not not_parsed:
            continue
        set_entry_parsed(not_parsed['url'], database)

        p = Process(target=my_process, args=(not_parsed['url'],))
        procs.append(p)
        p.start()
        time.sleep(0.1)


def kick():
    database = get_new()

    database.mongodb[mongo_collection].create_index('parsed')
    my_process(init_url)

    run_engine(database)


def clean_up(procs):
    print("OK")
    while True:
        for proc in procs:
            if not proc.is_alive():
                procs.remove(proc)
                print(len(procs))
        time.sleep(1)


if __name__ == "__main__":
    kick()
