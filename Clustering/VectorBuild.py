import colorsys
import os
import random

import numpy as np
from elasticsearch import Elasticsearch
from matplotlib import pyplot as plt
from matplotlib.mlab import PCA as mlabPCA
from scipy.cluster.vq import kmeans, whiten, vq
from scipy.sparse import coo_matrix

from WikipediaCrawler.pipelines import get_MAX_ITEMCOUNT

docs = {}
all_terms = set()
coefficient = {"title": 5, "brief": 2, "text": 1}


def set_coefficient(new_coefficient):
    coefficient["title"] = new_coefficient["title"]
    coefficient["brief"] = new_coefficient["brief"]
    coefficient["text"] = new_coefficient["text"]


def vector_build(doc_posting_list, doc_number):
    val = []
    col = []
    row = []
    for i, term in enumerate(all_terms):
        count = 0
        if term in doc_posting_list["term_vectors"]["title"]["terms"]:
            count += coefficient["title"] * doc_posting_list["term_vectors"]["title"]["terms"][term]["term_freq"]
        if "brief" in doc_posting_list["term_vectors"] and term in doc_posting_list["term_vectors"]["brief"]["terms"]:
            count += coefficient["brief"] * doc_posting_list["term_vectors"]["brief"]["terms"][term]["term_freq"]

        if term in doc_posting_list["term_vectors"]["text"]["terms"]:
            count += coefficient["text"] * doc_posting_list["term_vectors"]["text"]["terms"][term]["term_freq"]

        val.append(count)
        col.append(i)
        row.append(doc_number)

    width = len(all_terms)
    height = get_MAX_ITEMCOUNT()
    vector = coo_matrix((val, (col, row)), shape=(width, height))
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


def add_cluster_to_elasticsearch(title, cluster):
    es = Elasticsearch()
    doc = es.search(index="wikipedia", body={"query": {"match": {"title": title}}})
    item = doc['hits']['hits'][0]
    return es.update(index=item["_index"], id=item['_id'],
                     doc_type=item["_type"],
                     body={"doc": {"cluster": int(cluster)}})


def get_colors(num_colors):
    colors = []
    random_colors = []
    for i in np.arange(0., 360., 360. / 256.):
        hue = i / 360.
        lightness = (50 + np.random.rand() * 10) / 100.
        saturation = (90 + np.random.rand() * 10) / 100.
        colors.append(colorsys.hls_to_rgb(hue, lightness, saturation))

    for i in range(0, num_colors):
        random_colors.append(colors[random.randint(0, len(colors) - 1)])
    return random_colors


def visualize(kmeansdata, centroid_list, label_list, num_cluster):
    mlab_pca = mlabPCA(kmeansdata)
    cutoff = mlab_pca.fracs[1]
    users_2d = mlab_pca.project(kmeansdata, minfrac=cutoff)
    centroids_2d = mlab_pca.project(centroid_list, minfrac=cutoff)

    colors = get_colors(num_cluster)
    plt.figure()
    plt.xlim([users_2d[:, 0].min() - 3, users_2d[:, 0].max() + 3])
    plt.ylim([users_2d[:, 1].min() - 3, users_2d[:, 1].max() + 3])
    for i, position in enumerate(centroids_2d):
        plt.scatter(centroids_2d[i, 0], centroids_2d[i, 1], marker='o', c=colors[i], s=100)

    for i, position in enumerate(label_list):
        plt.scatter(users_2d[i, 0], users_2d[i, 1], marker='+', c=colors[position])

    filename = "Clustering/clusters"
    i = 0
    while True:
        path = filename + str(i) + ".png"
        if os.path.isfile(path) == False:
            plt.savefig(path)
            print("file saved as " + path)
            break
        else:
            i = i + 1

    return


def init(l, visualize_selection):
    np.seterr(divide='ignore', invalid='ignore')
    es = Elasticsearch()
    res = es.search(index="wikipedia", size=get_MAX_ITEMCOUNT(), body={"query": {"match_all": {}}},
                    filter_path=['hits.total', 'hits.hits._source.title'])

    for hit in res['hits']['hits']:
        doc_posting_list = posting_list_build(hit["_source"]["title"])

        docs.update({hit["_source"]["title"]: doc_posting_list})

    for value in docs.values():
        add_words(value)

    features = coo_matrix(([], ([], [])), shape=(len(all_terms), get_MAX_ITEMCOUNT()))
    for i, doc in enumerate(docs.keys()):
        features += vector_build(docs[doc], i)

    centroids_matrix = None
    whitened = whiten(features.toarray())
    if l == -1:
        centroids_matrix, _ = kmeans(whitened, int(get_MAX_ITEMCOUNT() / 10), iter=20, thresh=1e-5, check_finite=True)
    else:
        centroids_matrix, _ = kmeans(whitened, l, iter=20, thresh=1e-5, check_finite=True)

    clusters = vq(whitened, centroids_matrix)[0]
    for i, title in enumerate(docs.keys()):
        add_cluster_to_elasticsearch(title, clusters[i])

    if visualize_selection == "y":
        print("\tStart Visualizing...")
        visualize(whitened, centroids_matrix, clusters, l)
