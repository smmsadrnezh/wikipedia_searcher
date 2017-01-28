# -*- coding: utf-8 -*-
import json
import codecs
import os

import subprocess

file_count = 0
MAX_ITEMCOUNT = 100


def set_MAX_ITEMCOUNT(value):
    global MAX_ITEMCOUNT
    MAX_ITEMCOUNT = value


def get_MAX_ITEMCOUNT():
    global MAX_ITEMCOUNT
    return MAX_ITEMCOUNT


def get_progress_count():
    global file_count
    return file_count


class WikipediacrawlerPipeline(object):
    def printProgressBar(self, iteration, total, prefix='', suffix='', decimals=1, length=100, fill='█'):
        percent = ("{0:." + str(decimals) + "f}").format(100 * (iteration / float(total)))
        filledLength = int(length * iteration // total)
        bar = fill * filledLength + '-' * (length - filledLength)
        print('\r%s |%s| %s%% %s' % (prefix, bar, percent, suffix), end='\r')
        if iteration == total:
            print()

    def process_item(self, item, spider):
        global file_count, MAX_ITEMCOUNT

        file_count = int(subprocess.check_output("ls DataStore | wc -l", shell=True))
        if file_count < MAX_ITEMCOUNT:
            if item["title"] is None:
                # print(item)
                pass
            else:
                file = codecs.open('DataStore/' + item["title"] + '.json', 'w', encoding='utf-8')
            line = json.dumps(dict(item), ensure_ascii=False) + "\n"
            file.write(line)

            file.close()
            self.printProgressBar(file_count + 1, MAX_ITEMCOUNT, prefix='Progress:', suffix='Complete',
                                  length=50)
        return item
