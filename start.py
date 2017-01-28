import os
import sys

from elasticsearch import Elasticsearch

from Clustering import VectorBuild

search_config = {"title": None, "brief": None, "text": None, "cluster": None}


def init():
    print(
        """
        Welcome.
        Select task?
        1) Crawl
        2) Index Operations
        3) Data Clustering
        4) PageRank Calculation
        5) Search
        0) Exit
        """)

    selected_task = input()
    if selected_task in init_options:
        init_options[selected_task]()
    else:
        init()


def close():
    sys.exit()


def crawl():
    print(
        """
        Select task?
        1) Run Crawler with default configuration
        2) Run Crawler with manual configuration
        3) Back
        0) Exit
        """)
    selected_task = input()
    if selected_task in crawl_options:
        crawl_options[selected_task]()
    else:
        crawl()


def default_crawl():
    command = "rm DataStore/*; `which scrapy` crawl wikipedia 2> logs"
    os.system(command)


def manual_crawl():
    config = {}
    print(
        """
        Enter start URLs seperated with comma:
        """)
    config["start_urls"] = input()
    print(
        """
        Enter out degree:
        """)
    config["out_degree"] = input()
    print(
        """
        Enter item count:
        """)
    config["item_count"] = input()
    command = "rm DataStore/*; `which scrapy` crawl -a start_urls=" + config["start_urls"] + " -a out_degree=" + config[
        "out_degree"] + " -a item_count=" + config["item_count"] + " wikipedia 2> logs"
    os.system(command)


def index_operations():
    print(
        """
        Select task?
        1) Create index
        2) Delete index
        3) Show sample URLs
        4) Back
        0) Exit
        """)
    selected_task = input()
    if selected_task in index_operations_options:
        index_operations_options[selected_task]()
    else:
        index_operations()


def create_index():
    es = Elasticsearch()
    path = "DataStore/"

    es.indices.create(index='wikipedia')

    for file in os.listdir(path):
        if file.endswith(".json"):
            with open(path + file) as f:
                es.index(index="wikipedia", doc_type="JSON", body=f.read())


def delete_index():
    es = Elasticsearch()
    es.indices.delete(index='wikipedia')


def show_sample_urls():
    print("http://localhost:9200/wikipedia/_search?pretty=true")
    print("http://localhost:9200/wikipedia/_search?pretty=true&q=*:*")
    print("http://localhost:9200/wikipedia/_search?pretty=true&q=title:سعدی")
    print("http://localhost:9200/wikipedia/_search?pretty=true&size=10&q=*:*")


def data_clustering():
    print(
        """
        Select task?
        1) Run K-Means and Add Clusters to ElasticSearch
        2) Show sample URLs
        3) Back
        0) Exit
        """)
    selected_task = input()
    if selected_task in data_clustering_options:
        data_clustering_options[selected_task]()
    else:
        data_clustering()


def start_kmeans():
    print(
        """
        Enter maximum limit for K (-1 for no limit)
        """)
    l = int(input())
    VectorBuild.init(l)


def pagerank_calculation():
    pass


def search():
    print(
        """
        Select task?
        1) Advanced search
        2) Change coefficient for different doc fields
        3) Back
        0) Exit
        """)
    selected_task = input()
    if selected_task in search_options:
        search_options[selected_task]()
    else:
        search()


def advanced_search():
    print(
        """
        Set your search configuration?
        1) Search...
        2) Set title
        3) Set brief
        4) Set text
        5) Set cluster
        6) PageRank Effective
        7) Back
        0) Exit
        """)
    selected_task = input()
    if selected_task in advanced_search_options:
        advanced_search_options[selected_task]()
    else:
        advanced_search()


def full_search():
    es = Elasticsearch()

    title_dict = brief_dict = text_dict = cluster_dict = {}

    if search_config["title"]:
        title_dict = {"match": {"title": search_config["title"]}}
    if search_config["brief"]:
        brief_dict = {"match": {"brief": search_config["brief"]}}
    if search_config["text"]:
        text_dict = {"match": {"text": search_config["text"]}}
    if search_config["cluster"]:
        cluster_dict = {"match": {"cluster": search_config["cluster"]}}

    search_results = es.search(index="wikipedia", size=VectorBuild.get_MAX_ITEMCOUNT(),
                               body={"query": {"bool": {"must": [title_dict, brief_dict, text_dict, cluster_dict]}}},
                               filter_path=['hits.hits._source.title'])

    if len(search_results) > 0:
        for search_result in search_results['hits']['hits']:
            print("\t" + search_result['_source']['title'])
    else:
        print("No Result")

def set_title():
    print(
        """
        Enter search:
        """)
    search_config["title"] = input()
    print(
        """
        Search term configured successfully.
        """)
    search_options['1']()


def set_brief():
    print(
        """
        Enter search:
        """)
    search_config["brief"] = input()
    print(
        """
        Search term configured successfully.
        """)
    search_options['1']()


def set_text():
    print(
        """
        Enter search:
        """)
    search_config["text"] = input()
    print(
        """
        Search term configured successfully.
        """)
    search_options['1']()


def set_cluster():
    print("""
    Select the cluster to search in:
    """)

    es = Elasticsearch()
    res = es.search(index="wikipedia", size=VectorBuild.get_MAX_ITEMCOUNT(), body={"query": {"match_all": {}}},
                    filter_path=['hits.hits._source.cluster'])

    clusters = set()
    for cluster in res['hits']['hits']:
        clusters.add(cluster['_source']['cluster'])

    for cluster in clusters:
        print(cluster)

    search_config["cluster"] = int(input())
    print(
        """
        Search term configured successfully.
        """)
    search_options['1']()


def pagerank_effective():
    print("""
        Is it necessary to affect PageRanks:
        1) Back
        0) Exit
        """)
    selected_task = input()
    if selected_task in coefficient_options:
        coefficient_options[selected_task]()
    else:
        coefficient_options['0']()


def change_coefficient():
    coefficient = {}

    print(
        """
        Coefficient for page title?
        """)
    coefficient["title"] = int(input())
    print(
        """
        Coefficient for page brief?
        """)
    coefficient["brief"] = int(input())
    print(
        """
        Coefficient for page text?
        """)
    coefficient["text"] = int(input())

    VectorBuild.set_coefficient(coefficient)

    print(
        """
        Coefficients changed successfully.
        """)

    coefficient_options['1']()


advanced_search_options = {'0': close,
                           '1': full_search,
                           '2': set_title,
                           '3': set_brief,
                           '4': set_text,
                           '5': set_cluster,
                           '6': pagerank_effective,
                           '7': search,
                           }

coefficient_options = {'0': close,
                       '1': search,
                       }

search_options = {'0': close,
                  '1': advanced_search,
                  '2': change_coefficient,
                  '3': init,
                  }

data_clustering_options = {'0': close,
                           '1': start_kmeans,
                           '2': show_sample_urls,
                           '3': init,
                           }

crawl_options = {'0': close,
                 '1': default_crawl,
                 '2': manual_crawl,
                 '3': init,
                 }

init_options = {'0': close,
                '1': crawl,
                '2': index_operations,
                '3': data_clustering,
                '4': pagerank_calculation,
                '5': search,
                }

index_operations_options = {'0': close,
                            '1': create_index,
                            '2': delete_index,
                            '3': show_sample_urls,
                            '4': init,
                            }

init()
