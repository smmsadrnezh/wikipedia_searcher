import scrapy
from bs4 import BeautifulSoup
import re
from urllib import parse

class WikipediaSpider(scrapy.Spider):
    name = 'wikipedia'

    scrape_count = 0
    OUT_DEGREE = 10
    allowed_domains = ['fa.wikipedia.org']
    start_urls = [
        'https://fa.wikipedia.org/wiki/%D8%B3%D8%B9%D8%AF%DB%8C',
    ]

    def parse(self, response):

        page = BeautifulSoup(response.text, "lxml")
        content = page.find(id="mw-content-text")
        item = self.scrap_content(page, content)
        yield item

        for link in content.find_all('a', limit=self.OUT_DEGREE):
            url = parse.unquote(link.get("href"))
            if not bool(re.search("action=edit|[\d۱۲۳۴۵۶۷۸۹۰#:]+", url)):
                yield scrapy.Request(response.urljoin(link.get("href")), callback=self.parse)

    def scrap_content(self, page, content):

        item = {"text": ""}

        item["brief"] = content.find_all("p", recursive=False)[0].get_text()

        for text_block in content.find_all(["h1", "h2", "h3", "h4", "p", "table", "ul", "ol", "blockquote"]):
            item["text"] += text_block.get_text()

        item["title"] = page.find("h1",id="firstHeading").string

        infobox = content.find_all("table", {"class": "infobox vcard"}, recursive=False)
        if len(infobox) > 0:
            item["infobox"] = infobox[0].get_text()

        return item
