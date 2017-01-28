from elasticsearch import Elasticsearch
from WikipediaCrawler.settings import CLOSESPIDER_ITEMCOUNT
from scipy.cluster.vq import kmeans, kmeans2, whiten, vq
from numpy import array, shape

docs = {}
all_terms = set()
coefficient = {"title": 5, "brief": 2, "text": 1}


def vector_build(doc_posting_list):
    vector = []
    for term in all_terms:
        count = 0
        if term in doc_posting_list["term_vectors"]["title"]["terms"]:
            count += coefficient["title"] * doc_posting_list["term_vectors"]["title"]["terms"][term]["term_freq"]
        if "brief" in doc_posting_list["term_vectors"] and term in doc_posting_list["term_vectors"]["brief"]["terms"]:
            count += coefficient["brief"] * doc_posting_list["term_vectors"]["brief"]["terms"][term]["term_freq"]

        if term in doc_posting_list["term_vectors"]["text"]["terms"]:
            count += coefficient["text"] * doc_posting_list["term_vectors"]["text"]["terms"][term]["term_freq"]
        vector.append(count)
    return vector


def add_words(doc_posting_list):
    for field in doc_posting_list['term_vectors'].values():
        for term in field["terms"].keys():
            all_terms.add(term)


def posting_list_build(title):
    es = Elasticsearch()
    doc = es.search(index="wikipedia", body={"query": {"match": {"title": title}}})
    item = doc['hits']['hits'][0]
    return es.termvectors(index=item["_index"], id=item['_id'],
                          doc_type=item["_type"],
                          fields=["title", "text", "brief"],
                          offsets=False,
                          payloads=False,
                          positions=False,
                          field_statistics=False,
                          term_statistics=True, ignore=[400, 404])


def init(l):
    es = Elasticsearch()
    res = es.search(index="wikipedia", size=CLOSESPIDER_ITEMCOUNT, body={"query": {"match_all": {}}},
                    filter_path=['hits.total', 'hits.hits._source.title'])

    for hit in res['hits']['hits']:
        doc_posting_list = posting_list_build(hit["_source"]["title"])

        docs.update({hit["_source"]["title"]: doc_posting_list})

    for value in docs.values():
        add_words(value)

    features = []
    for doc in docs.keys():
        features.append(vector_build(docs[doc]))

    whitened = whiten(array(features))
    centroids_matrix = None
    if l == -1:
        centroids_matrix, _ = kmeans(whitened, int(CLOSESPIDER_ITEMCOUNT / 10), iter=20, thresh=1e-5, check_finite=True)
    else:
        centroids_matrix, _ = kmeans(whitened, l, iter=20, thresh=1e-5, check_finite=True)
    print(vq(whitened, centroids_matrix)[0])
