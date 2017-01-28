import scrapy
from bs4 import BeautifulSoup
import re
from urllib import parse

from scrapy.exceptions import CloseSpider

from WikipediaCrawler.pipelines import set_MAX_ITEMCOUNT, get_progress_count, get_MAX_ITEMCOUNT


class WikipediaSpider(scrapy.Spider):
    name = 'wikipedia'

    OUT_DEGREE = 10
    allowed_domains = ['fa.wikipedia.org']
    start_urls = [
        'https://fa.wikipedia.org/wiki/%D8%B3%D8%B9%D8%AF%DB%8C',
    ]

    def __init__(self, out_degree=OUT_DEGREE, start_urls=start_urls, item_count=get_MAX_ITEMCOUNT()):
        self.OUT_DEGREE = int(out_degree)
        if isinstance(start_urls, str):
            self.start_urls = start_urls.split(",")
        set_MAX_ITEMCOUNT(int(item_count))

    def parse(self, response):

        page = BeautifulSoup(response.text, "lxml")
        content = page.find(id="mw-content-text")
        item = self.scrap_content(page, content)
        yield item

        if get_progress_count() < get_MAX_ITEMCOUNT():
            for i in range(min(self.OUT_DEGREE, len(item["out_links"]))):
                yield scrapy.Request(response.urljoin(item["out_links"][i]), callback=self.parse)
        else:
            raise CloseSpider()

    def scrap_content(self, page, content):

        item = {"text": "", "out_links": [], "info": {}}

        brief = content.find_all("p", recursive=False)
        if len(brief) > 0:
            item["brief"] = brief[0].get_text()
        else:
            item["brief"] = ""

        for text_block in content.find_all(["h1", "h2", "h3", "h4", "p", "table", "ul", "ol", "blockquote"]):
            item["text"] += text_block.get_text()

        item["title"] = page.find("h1", id="firstHeading").string

        infobox = content.find_all("table", {"class": "infobox vcard"}, recursive=False)
        if len(infobox) > 0:
            fields = infobox[0].find_all("tr")
            for field in fields:
                if field.find("th") is not None and field.find("td") is not None:
                    item["info"][field.find("th").get_text()] = field.find("td").get_text()

        for link in content.find_all('a'):
            url = parse.unquote(link.get("href"))
            if not bool(re.search("action=edit|[\d۱۲۳۴۵۶۷۸۹۰#:]+", url)):
                item["out_links"].append(url)

        item["cluster"] = -1
        item["pageRank"] = -1
        return item
