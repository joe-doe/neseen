import time
from bs4 import BeautifulSoup
from db import MongoDB
import asyncio
from aiohttp import ClientSession

database = MongoDB('localhost', 'urls')
init_url = 'http://news.google.com'


async def read_url(url, session):
    async with session.get(url) as response:
        return await response.read()


async def soup_it(url, session):
    html = await read_url(url, session)
    try:
        soup = BeautifulSoup(html, 'html.parser')
    except Exception:
        return None

    store_meta(soup, url)
    store_links_in(soup)
    set_entry_parsed(url)
    print("done with: ", url)


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

    database.mongodb['aurl'].update_one(
        {'url': url},
        {'$set': {
            'title': title,
            "desc": desc
        }
        },
        upsert=False)


def store_links_in(soup):
    for link in soup.find_all('a'):
        href = link.get('href')
        if href and href.startswith('http'):
            domain_list = href.split('/')
            domain = domain_list[0] + '//' + domain_list[1] + domain_list[2]

            a = database.mongodb['aurl'].find_one({'url': domain})

            if a:
                database.mongodb['aurl'].update_one(
                    {'_id': a['_id']},
                    {'$inc': {
                        'hits': 1
                    }
                    },
                    upsert=False)
            else:
                database.mongodb['aurl'].insert({'url': domain,
                                                 'hits': 1,
                                                 'parsed': False})


def set_entry_parsed(url):
    database.mongodb['aurl'].update_one(
        {'url': url},
        {"$set": {
            "parsed": True
        }
        },
        upsert=False)


def process():
    database.mongodb['aurl'].insert({'url': init_url,
                                     'hits': 1,
                                     'parsed': False})


async def run_engine():

    async with ClientSession() as session:
        tasks = []
        while True:
            not_parsed = database.mongodb['aurl'].find({'parsed': False})

            for entry in not_parsed:
                task = asyncio.ensure_future(soup_it(entry['url'], session))
                tasks.append(task)
            await asyncio.gather(*tasks)

if __name__ == "__main__":
    process()
    loop = asyncio.get_event_loop()
    future = asyncio.ensure_future(run_engine())
    loop.run_until_complete(future)
