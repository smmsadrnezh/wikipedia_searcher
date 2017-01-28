from elasticsearch import Elasticsearch
from WikipediaCrawler.settings import CLOSESPIDER_ITEMCOUNT

docs = {}


def vector_build(title):
    es = Elasticsearch()
    doc = es.search(index="wikipedia", body={"query": {"match": {"title": title}}})
    item = doc['hits']['hits'][0]
    doc_posting_list = es.termvectors(index=item["_index"], id=item['_id'],
                                      doc_type=item["_type"],
                                      fields=["title", "tsext", "brief"],
                                      offsets=False,
                                      payloads=False,
                                      positions=False,
                                      field_statistics=False,
                                      term_statistics=True, ignore=[400, 404])


def init():
    es = Elasticsearch()
    res = es.search(index="wikipedia", size=CLOSESPIDER_ITEMCOUNT, body={"query": {"match_all": {}}},
                    filter_path=['hits.total', 'hits.hits._source.title'])
    for hit in res['hits']['hits']:
        vector_build(hit["_source"]["title"])


init()
