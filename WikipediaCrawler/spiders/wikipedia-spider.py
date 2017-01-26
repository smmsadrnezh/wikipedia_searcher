import scrapy
from bs4 import BeautifulSoup


class WikipediaSpider(scrapy.Spider):
    name = 'wikipedia'

    number_of_pages = 1000
    i = 0
    out_degree = 10
    allowed_domains = ['fa.wikipedia.org']
    start_urls = [
        'https://fa.wikipedia.org/wiki/%D8%B3%D8%B9%D8%AF%DB%8C',
    ]

    def parse(self, response):

        page = BeautifulSoup(response.text, "lxml")
        content = page.find(id="mw-content-text")
        item = self.scrap_content(page, content)

        yield {
            'title': item["title"],
            'infobox': item["infobox"],
            'brief': item["brief"],
            'text': item["text"]
        }

        for link in content.find_all('a'):
            if (self.i == self.number_of_pages):
                exit()
            else:
                print(self.i)
                self.i += 1
                yield scrapy.Request(response.urljoin(link.get("href")), callback=self.parse)

    def scrap_content(self, page, content):

        item = {"brief": "", "text": ""}

        if content.find(id="toc") is not None:
            for paragraph in content.find(id="toc").find_previous_siblings("p"):
                item["brief"] += paragraph.get_text()
        else:
            for paragraph in content.find_all("p", recursive=False):
                item["brief"] += paragraph.get_text()

        for text_block in content.find_all(["h1", "h2", "h3", "h4", "p", "table", "ul", "ol", "blockquote"]):
            item["text"] += text_block.get_text()

        item["title"] = page.h1.string
        item["infobox"] = content.find_all("table", recursive=False)[0].get_text()

        return item
