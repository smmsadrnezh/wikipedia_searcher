import colorsys
import os
import random

import numpy
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
    """
    Function to generate a list of randomly generated colors
    The function first generates 256 different colors and then
    we randomly select the number of colors required from it
    num_colors        -> Number of colors to generate
    colors            -> Consists of 256 different colors
    random_colors     -> Randomly returns required(num_color) colors
    """
    colors = []
    random_colors = []
    # Generate 256 different colors and choose num_clors randomly
    for i in numpy.arange(0., 360., 360. / 256.):
        hue = i / 360.
        lightness = (50 + numpy.random.rand() * 10) / 100.
        saturation = (90 + numpy.random.rand() * 10) / 100.
        colors.append(colorsys.hls_to_rgb(hue, lightness, saturation))

    for i in range(0, num_colors):
        random_colors.append(colors[random.randint(0, len(colors) - 1)])
    return random_colors


def random_centroid_selector(total_clusters, clusters_plotted):
    """
    Function to generate a list of randomly selected
    centroids to plot on the output png
    total_clusters        -> Total number of clusters
    clusters_plotted      -> Number of clusters to plot
    random_list           -> Contains the index of clusters
                             to be plotted
    """
    random_list = []
    for i in range(0, clusters_plotted):
        random_list.append(random.randint(0, total_clusters - 1))
    return random_list


def visualize(kmeansdata, centroid_list, label_list, num_cluster):
    mlab_pca = mlabPCA(kmeansdata)
    cutoff = mlab_pca.fracs[1]
    users_2d = mlab_pca.project(kmeansdata, minfrac=cutoff)
    centroids_2d = mlab_pca.project(centroid_list, minfrac=cutoff)

    colors = get_colors(num_cluster)
    plt.figure()
    plt.xlim([users_2d[:, 0].min() - 3, users_2d[:, 0].max() + 3])
    plt.ylim([users_2d[:, 1].min() - 3, users_2d[:, 1].max() + 3])

    # Plotting 50 clusters only for now
    # random_list = random_centroid_selector(num_cluster, 50)

    # Plotting only the centroids which were randomly_selected
    # Centroids are represented as a large 'o' marker
    for i, position in enumerate(centroids_2d):
        # if i in random_list:
        plt.scatter(centroids_2d[i, 0], centroids_2d[i, 1], marker='o', c=colors[i], s=100)

    # Plotting only the points whose centers were plotted
    # Points are represented as a small '+' marker
    for i, position in enumerate(label_list):
        # if position in random_list:
        plt.scatter(users_2d[i, 0], users_2d[i, 1], marker='+', c=colors[position])

    filename = "Clustering/clusters"
    i = 0
    while True:
        if os.path.isfile(filename + str(i) + ".png") == False:
            # new index found write file and return
            plt.savefig(filename + str(i) + ".png")
            break
        else:
            # Changing index to next number
            i = i + 1
    return


# def visualize(whitened, centroids_matrix):
#     plot(whitened[0, 0], whitened[0, 1], 'ob',
#          whitened[1, 0], whitened[1, 1], 'or')
#     plot(centroids_matrix[:, 0], centroids_matrix[:, 1], 'sg', markersize=8)
#     show()


def init(l):
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
    print("start")
    visualize(whitened, centroids_matrix, clusters, l)
    print("end")
    for i, title in enumerate(docs.keys()):
        add_cluster_to_elasticsearch(title, clusters[i])
