import time
from bs4 import BeautifulSoup
from urllib.request import urlopen
from datastores.elastic.elastic import get_es
from multiprocessing import Pool, current_process

init_url = {"protocol": "http",
            "domain": ["news", "google", "com"]}
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

    meta = get_meta(soup)

    es = get_es()
    es.update(index=es_index,
              doc_type=es_type,
              id=doc_id,
              body={
                  'doc': {
                      'title': meta.get('title'),
                      'desc': meta.get('desc')
                  }
              })


def store_links_in(soup, url):
    print("7. " + current_process().name + " store links for: ", url)

    for link in soup.find_all('a'):
        href = link.get('href')
        # print("found: ", href)
        if href and href.startswith('http'):
            url_list = href.split('/')

            try:
                protocol = url_list[0]
                domain_list = url_list[2].split('.')
            except IndexError:
                print(href, domain_list)
                continue

            es = get_es()
            a = es.search(index=es_index,
                          doc_type=es_type,
                          body={
                              'query': {
                                  'term': {
                                      'protocol': protocol
                                  },
                                  'term': {
                                      'domain': domain_list
                                  }
                              }
                          },
                          request_timeout=100
                          )

            if a.get('hits').get('hits'):
                print(current_process().name + " einai mesa: ", domain_list)
                continue
                # database.mongodb[mongo_collection].update_one(
                #     {'_id': a['_id']},
                #     {'$inc': {
                #         'hits': 1
                #     }
                #     },
                #     upsert=False)
            else:
                print(current_process().name + "store url: " + domain_list)
                # print(current_process().name+domain+" not in: list")
                es.index(index=es_index,
                         doc_type=es_type,
                         body={'protocol': protocol,
                               'domain': domain_list,
                               'hits': 1,
                               "parsed": False})


def set_entry_parsed(doc_id):
    es = get_es()
    es.update(index=es_index,
              doc_type=es_type,
              id=doc_id,
              body={
                  'doc': {
                      "parsed": True
                  }
              })


def my_process(not_parsed_yet, doc_id):
    # print("5. " + current_process().name + "making soup for: ", url)
    url = not_parsed_yet.get("protocol")+"://"+"."\
        .join(not_parsed_yet.get("domain"))
    try:
        soup = BeautifulSoup(urlopen(url), 'html.parser')
    except Exception as e:
        print("WTF: ", url, e)
        return

    store_meta(soup, doc_id)
    store_links_in(soup, url)
    # print("done: ", url)


def run_engine():
    pool = Pool(processes=4)

    while True:
        es = get_es()
        not_parsed = es.search(index=es_index,
                               doc_type=es_type,
                               body={
                                   'query': {
                                       'match': {
                                           'parsed': False
                                       }
                                   }
                               },
                               request_timeout=100
                               )
        if not not_parsed:
            continue
        if not not_parsed.get('hits'):
            continue
        if not_parsed.get('hits').get('total') < 1:
            continue

        next_not_parsed = not_parsed.get('hits').get('hits')[0] \
            .get('_source')
        doc_id = not_parsed.get('hits').get('hits')[0].get('_id')

        set_entry_parsed(doc_id)

        pool.apply_async(my_process, args=(next_not_parsed, doc_id))
        time.sleep(0.7)


def init():
    es = get_es()
    es.index(index=es_index,
             doc_type=es_type,
             body={'protocol': init_url.get('protocol'),
                   'domain': init_url.get('domain'),
                   'hits': 1,
                   'parsed': False
                   })


def kick():
    init()
    run_engine()


if __name__ == "__main__":
    kick()
