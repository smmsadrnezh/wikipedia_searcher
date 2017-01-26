import scrapy

class WikipediaSpider(scrapy.Spider):
    name = 'wikipedia'

    number_of_pages = 1000
    out_degree = 10
    start_urls = [
        'https://fa.wikipedia.org/wiki/%D8%B3%D8%B9%D8%AF%DB%8C',
    ]

    def parse(self, response):
        page = response.url.split("/")[-2]
        with open(page, 'wb') as f:
            f.write(response.body)
        self.log('Saved file %s' % page)
