import time
from bs4 import BeautifulSoup
from urllib.request import urlopen
from elastic import es
from multiprocessing import Pool

init_url = 'http://news.google.com'
es_index = 'neseen'
es_type = 'urls'


def get_meta(soup):
    return_dict = {}

    try:
        title = soup.find('title').text
    except AttributeError:
        title = "no title"

    return_dict['title'] = title

    meta = soup.findAll('meta', attrs={"name": "description"})

    if not meta:
        desc = "no description"
    else:
        desc = meta[0].attrs.get('content')

    return_dict['desc'] = desc

    return return_dict


def store_meta(soup, doc_id):
    # print("store meta for: ", url)

    meta = get_meta(soup)

    es.update(index=es_index,
              doc_type=es_type,
              id=doc_id,
              body={
                  'doc': {
                      'title': meta.get('title'),
                      'desc': meta.get('desc')
                  }
              })


def store_links_in(soup, doc_id):
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

            a = es.search(index=es_index,
                          doc_type=es_type,
                          id=doc_id
                          )

            if a.get('hits').get('hits'):
                continue
                # database.mongodb[mongo_collection].update_one(
                #     {'_id': a['_id']},
                #     {'$inc': {
                #         'hits': 1
                #     }
                #     },
                #     upsert=False)
            else:
                # print("store url: ", domain)
                es.index(index=es_index,
                         doc_type=es_type,
                         body={'url': domain,
                               'hits': 1,
                               "parsed": False})


def set_entry_parsed(doc_id):
    # print("set parsed to True for: ", url)
    es.update(index=es_index,
              doc_type=es_type,
              id=doc_id,
              body={
                  'doc': {
                      "parsed": True
                  }
              })


def my_process(url, doc_id):
    # print("process: ", url)
    try:
        soup = BeautifulSoup(urlopen(url), 'html.parser')
    except Exception:
        return

    store_meta(soup, doc_id)
    store_links_in(soup, doc_id)


def run_engine():
    pool = Pool(processes=2)

    while True:
        not_parsed = es.search(index=es_index,
                               doc_type=es_type,
                               body={
                                   'query': {
                                       'match': {
                                           'parsed': False
                                       }
                                   }
                               }
                               )
        if not not_parsed:
            continue
        if not not_parsed.get('hits'):
            continue
        if not not_parsed.get('hits').get('hits'):
            continue
        next_not_parsed = not_parsed.get('hits').get('hits')[0] \
            .get('_source').get('url')
        doc_id = not_parsed.get('hits').get('hits')[0].get('_id')

        set_entry_parsed(doc_id)

        pool.apply_async(my_process, args=(next_not_parsed, doc_id))
        time.sleep(0.2)


def init(url):
    soup = BeautifulSoup(urlopen(url), 'html.parser')
    meta = get_meta(soup)

    es.index(index=es_index,
             doc_type=es_type,
             body={'url': url,
                   'hits': 1,
                   'parsed': False,
                   'title': meta.get('title'),
                   'desc': meta.get('desc')
                   })


def kick():
    init(init_url)
    run_engine()


if __name__ == "__main__":
    kick()
