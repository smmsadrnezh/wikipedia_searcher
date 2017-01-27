# -*- coding: utf-8 -*-
import json
import codecs
from WikipediaCrawler.settings import CLOSESPIDER_ITEMCOUNT


class WikipediacrawlerPipeline(object):
    progress_count = 0

    def printProgressBar(self,iteration, total, prefix='', suffix='', decimals=1, length=100, fill='█'):
        percent = ("{0:." + str(decimals) + "f}").format(100 * (iteration / float(total)))
        filledLength = int(length * iteration // total)
        bar = fill * filledLength + '-' * (length - filledLength)
        print('\r%s |%s| %s%% %s' % (prefix, bar, percent, suffix), end='\r')
        if iteration == total:
            print()

    def process_item(self, item, spider):
        file = codecs.open('DataStore/' + item["title"] + '.json', 'w', encoding='utf-8')
        line = json.dumps(dict(item), ensure_ascii=False) + "\n"
        file.write(line)
        file.close()
        self.progress_count += 1
        self.printProgressBar(self.progress_count, CLOSESPIDER_ITEMCOUNT, prefix='Progress:', suffix='Complete', length=50)
        return item

    def spider_closed(self, spider):
        print("Crawling finished successfully.")
