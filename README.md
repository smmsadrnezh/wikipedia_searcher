This program is a simple web crawler, indexer and searcher for [Persian Wikipedia](http://fa.wikipedia.org/) based on Python3.5 and elasticsearch.

How to run
=========

This plugin is based on Python3.5 and some Python packages. You can install all of them with the following command:

`pip3 install -r requirements.txt`

How to use
=========

Run elasticsearch service on your computer:

`service elasticsearch start`

Run the program on your console:

`python3.5 start.py` 

You have to collect your data corpse by choosing the first given option in the console. Next, you do index operations and data clustering. Finally, you have all necessary data to search for it.

TODO
=========

1. Frontier Queue Prioritization

2. Using Mutual Information to identify cluster names

3. Progress Bar for K-means

4. Finding best K with the given maximum L
