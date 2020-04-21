import logging
import os
import requests
import time
from functools import lru_cache

import elasticsearch
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)
logger.addHandler(logging.StreamHandler())
logger.setLevel(logging.INFO)

ES_HOST = 'elasticsearch:9200'
INDEX_NAME = 'wikis'
URL = 'https://en.wikipedia.org/wiki/Wikipedia:Multiyear_ranking_of_most_viewed_pages'
WIKI_API = 'https://en.wikipedia.org/w/api.php'
REPO_NAME = 'wikisrepo'
SNAPSHOT_NAME = 'wikis'

LIST_INDEX_TO_NAME = {
    0: 'Top-100 list',
    1: 'Countries',
    2: 'Cities',
    3: 'People',
    4: 'Singers',
    5: 'Actors',
    6: 'Athletes',
    7: 'Modern Political Leaders',
    8: 'Pre-modern people',
    9: '3rd-millennium people'
}

INDEX_BODY = {
    "settings": {
      "index": {
        "number_of_shards": 8,
        "number_of_replicas": 1
        }
    },
    "mappings": {
      "_meta": {
        "index_type": "wikis",
        "version": "1.0",
      },
      "_source": {
        "enabled": True
      },
      "properties": {
        "link": {
          "type": "keyword"
        },
        "title": {
          "type": "text",
        },
        "contents": {
          "type": "text"
        },
        "list": {
            "type": "text"
        },
        "rank": {
            "type": "integer"
        }
      }
    }
}


@lru_cache()
def _client_for_host(host):
    return elasticsearch.Elasticsearch(host)


@lru_cache()
def _indices_client_for_host(host):
    return elasticsearch.client.IndicesClient(_client_for_host(host))


def contents_from_wiki(title):
    response = requests.get(WIKI_API,
                 params={'action': 'parse', 'page': title, 'format': 'json'}).json()
    return response['parse']['text']['*']


def gen_docs_from_table(table):
    rows = table.find_all('tr')
    for row in rows[1:]:
        cols = row.find_all('td')
        rank = cols[0].text.strip()
        if rank.isdigit():
            link_td = cols[1].find_all('a')[0]
            title = link_td['title']
            link = link_td['href']
            contents = contents_from_wiki(title)
            yield {'rank': rank, 'title': title, 'contents': contents, 'link': link}


def gen_documents():
    r = requests.get(URL)
    soup = BeautifulSoup(r.content, 'lxml')
    tables = soup.find_all('table')
    for table_index, list_name in LIST_INDEX_TO_NAME.items():
        for doc in gen_docs_from_table(tables[table_index]):
            doc['list'] = list_name
            yield doc


def load_data():
    for doc in gen_documents():
        _client_for_host(ES_HOST).index(index=INDEX_NAME, body=doc)


def create_index():
    _indices_client_for_host(ES_HOST).create(index=INDEX_NAME, body=INDEX_BODY)


def create_repo():
    _client_for_host(ES_HOST).snapshot.create_repository(REPO_NAME, {'type': 'fs', 'settings':
        {'location': '/snapshot'}})


def create_snapshot():
    _client_for_host(ES_HOST).snapshot.create(REPO_NAME, SNAPSHOT_NAME)
    state = None
    while state != 'SUCCESS':
        logger.info('waiting for snapshot to complete')
        statuses = _client_for_host(ES_HOST).snapshot.status(repository=REPO_NAME, snapshot=SNAPSHOT_NAME)
        if len(statuses.get('snapshots', [])):
            state = statuses['snapshots'][0].get('state')
        time.sleep(1)


def prep_index():
    snapshot_contents = [f for f in os.listdir('/snapshot') if f != '.gitignore']
    if snapshot_contents:
        logger.info('Restoring wikis index')
        create_repo()
        try:
            _client_for_host(ES_HOST).snapshot.restore(REPO_NAME, SNAPSHOT_NAME)
        except elasticsearch.exceptions.TransportError as e:
            if 'an open index with same name already exists' in str(e):
                logger.info('Index already exists, nothing to do')
            else:
                raise e
    else:
        logger.info('Initializing wikis index...')
        create_index()
        logger.info('Created')
        logger.info('Loading Data...')
        load_data()
        logger.info('Loaded')
        logger.info('Creating backup')
        create_repo()
        create_snapshot()
    logger.info('Done')


if __name__ == '__main__':
    prep_index()
