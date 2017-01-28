import sys
from elasticsearch import Elasticsearch
from Clustering import VectorBuild
import os


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
        index_operations()


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
    pass


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
