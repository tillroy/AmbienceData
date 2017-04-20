# coding: utf-8

from scrapy import Spider


class EpubSpider(Spider):
    name = "epub"
    start_urls = ("https://www.packtpub.com/packt/offers/free-learning",)

    def parse(self, response):
        book_name = response.xpath('//*[@id="deal-of-the-day"]/div/div/div[2]/div[2]/h2/text()').extract_first()
        book_name = book_name.replace("\t", "")
        book_name = book_name.replace("\n", "")
        print(book_name)