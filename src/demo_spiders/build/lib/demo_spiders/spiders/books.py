# -*- coding: utf-8 -*-
import scrapy
from demo_spiders.items import BooksItem, BooksItemLoader


class BooksSpider(scrapy.Spider):
    name = "books"
    allowed_domains = ["books.toscrape.com"]
    start_urls = [
        'http://books.toscrape.com/',
    ]

    custom_settings = {
        'CRAWLERA_ENABLED': False,
    }

    def parse(self, response):
        for book_url in response.css("article.product_pod > h3 > a ::attr(href)").getall():
            yield scrapy.Request(response.urljoin(book_url), callback=self.parse_book_page)
        next_page = response.css("li.next > a ::attr(href)").get()
        if next_page:
            yield scrapy.Request(response.urljoin(next_page), callback=self.parse)

    def parse_book_page(self, response):
        item = {}
        product = response.css("div.product_main")
        item["title"] = product.css("h1 ::text").get()
        item['price'] = product.css('p.price_color ::text').get()
        item['category'] = response.xpath(
            "//ul[@class='breadcrumb']/li[@class='active']/preceding-sibling::li[1]/a/text()"
        ).get()
        item['description'] = response.xpath(
            "//div[@id='product_description']/following-sibling::p/text()"
        ).get()
        book_item = BooksItemLoader(item=item)
        yield book_item.load_item()
