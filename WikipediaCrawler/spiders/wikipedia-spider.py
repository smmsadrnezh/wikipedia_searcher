import scrapy
from bs4 import BeautifulSoup

class WikipediaSpider(scrapy.Spider):
    name = 'wikipedia'

    number_of_pages = 1000
    out_degree = 10
    allowed_domains = ['fa.wikipedia.org']
    start_urls = [
        'https://fa.wikipedia.org/wiki/%D8%B3%D8%B9%D8%AF%DB%8C',
    ]

    def parse(self, response):
        content = BeautifulSoup(response)
        yield {
            'title': content.h1.string,
            'brief': content.p.string.get_text(),
            'text': content.find(id="toc").next_siblings.get_text()
        }

        for link in content.find_all('a'):
            next_page = link.get("href")
            next_page = response.urljoin(next_page)
            yield scrapy.Request(next_page, callback=self.parse)