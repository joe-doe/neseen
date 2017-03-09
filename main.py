import time
import signal
from bs4 import BeautifulSoup
from urllib.request import Request, urlopen
from datastores.mongo.db import get_new
from pymongo import TEXT

init_url = 'http://news.google.com'
database = get_new()
mongo_collection = 'full_url'
go_on = True


def signal_handler(signal, frame):
    """
    Catch Ctrl-C to gracefully end infinite loop

    :param signal: not used
    :param frame: not used
    :return: nothing
    """
    global go_on
    go_on = False
    print("Drop dead")


# register signal
signal.signal(signal.SIGINT, signal_handler)


def store_meta(soup, url):
    """
    Store url's html metadata

    :param soup: BeautifulSoup object which contains parsed url
    :param url: the actual url
    :return: nothing
    """
    body = " ".join(soup.stripped_strings)

    try:
        title = soup.find('title').text
    except AttributeError:
        title = "no title"
    except Exception:
        title = "Something went awfully bad"

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

    try:
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
    except UnicodeEncodeError as e:
        print(e)
        return


def store_links_in(soup, url):
    """
    Find hrefs in url and store them in db if they don't exist. If they
    do exist, increase hits.

    :param soup: BeautifulSoup object which contains parsed url
    :param url: the actual url
    :return: nothing
    """
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
    """
    Mark an entry as parsed.

    :param entry: mongodb JSON document

    :return: nothing
    """
    database.mongodb[mongo_collection].update_one(
        {'_id': entry['_id']},
        {"$set": {
            "parsed": True
        }
        },
        upsert=False)


def process(url):
    """
    Main process function.
    Make it seems that it's firefox, get BeautifulSoup object and run
    store_meta and store_links functions.

    :param url: url to process
    :return: nothing
    """
    req = Request(url)
    req.add_header('user-agent',
                   'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:51.0) '
                   'Gecko/20100101 Firefox/51.0')
    try:
        soup = BeautifulSoup(urlopen(req, timeout=8), 'html.parser')
    except Exception:
        return

    # drop scripts and css
    for script in soup(["script", "style"]):
        script.decompose()  # rip it out

    store_meta(soup, url)
    store_links_in(soup, url)


def run_engine():
    """
    Find a not processed url from mongodb and process it !

    :return: nothing
    """
    not_parsed = database.mongodb[mongo_collection].find_one({'parsed': False})
    if not not_parsed:
        return
    process(not_parsed['url'])
    set_entry_parsed(not_parsed)


def kick():
    """
    Run indefinitely until ctrl-c
    :return: nothing
    """
    process(init_url)
    database.mongodb[mongo_collection].create_index('parsed')
    database.mongodb[mongo_collection].create_index([('body', TEXT)])

    while go_on:
        run_engine()
        time.sleep(0.1)


if __name__ == "__main__":
    kick()
