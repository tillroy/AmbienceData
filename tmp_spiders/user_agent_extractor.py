# coding: utf-8

from datetime import datetime

from scrapy import Spider, Request
from dateutil import parser
from pytz import timezone
import ujson


class UserAgentExtractor(Spider):
    name = u"user_agent_extractor"

    def start_requests(self):

        href = u"http://www.useragentstring.com/pages/useragentstring.php"
        # url = href.format(codes_value)

        open("user_agents.txt", "w").close()

        yield Request(
            url=href,
            # headers={'User-Agent': 'Mozilla/5.0'},
            callback=self.get_base_links,
            # meta={u"code": codes_value}
        )

    def get_base_links(self, response):
        base_links = response.xpath('//*[@id="auswahl"]//tr/td[2]/a[@class = "unterMenuName"]/@href').extract()
        base_links = ["".join("/".join(("http://www.useragentstring.com", link)).split()) for link in base_links]

        for link in base_links:
            yield Request(
                url=link,
                callback=self.get_user_agent_string
            )
            # break

    def get_user_agent_string(self, resp):

        user_agent_string = resp.xpath('//*[@id="liste"]//a[parent::li]/text()').extract()
        for el in user_agent_string:
            open("user_agents.txt", "a").write(el + "\n")
