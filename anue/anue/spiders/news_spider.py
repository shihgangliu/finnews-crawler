import json
import scrapy
from datetime import datetime

from elasticsearch_dsl import Date, Document, Text
from elasticsearch_dsl.connections import connections

connections.create_connection(hosts=['localhost'], timeout=20)


class Anue(Document):
    title = Text(analyzer='my_cjk')
    content = Text(analyzer='my_cjk')
    created_at = Date()

    class Index:
        name = 'anue-001'

    def save(self, ** kwargs):
        self.created_at = datetime.now()
        return super().save(** kwargs)


class NewsSpider(scrapy.Spider):
    name = 'news'
    page = 1
    url = 'https://news.cnyes.com/api/v3/news/category/tw_stock?page={}'

    def start_requests(self):
        Anue.init()
        yield scrapy.Request(url=self.url.format(self.page), callback=self.parse_news_list)

    def parse_news_list(self, response):
        news_url_template = 'https://news.cnyes.com/news/id/{}?exp=a'
        response_body = json.loads(response.text)
        news_items = response_body['items']['data']
        for news_item in news_items:
            news_url = news_url_template.format(news_item['newsId'])
            yield scrapy.Request(url=news_url, callback=self.parse_news_content)

        if response_body['items']['next_page_url']:
            self.page += 1
            yield scrapy.Request(url=self.url.format(self.page), callback=self.parse_news_list)

    def parse_news_content(self, response):
        anue_doc = Anue()
        news_title = response.xpath('//h1[@itemprop="headline"]/text()').get()
        if news_title is not None:
            print('---{}---'.format(news_title))
            anue_doc.title = news_title

        news_content = response.xpath('//div[@itemprop="articleBody"]/div/p/text()').getall()
        if news_content:
            print(''.join(news_content))
            anue_doc.content = ''.join(news_content)

        anue_doc.save()
