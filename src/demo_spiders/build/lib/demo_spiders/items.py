# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://doc.scrapy.org/en/latest/topics/items.html
import re
from enum import Enum

from scrapy import Item, Field
from scrapy.selector import Selector, SelectorList
from scrapy.loader import ItemLoader
from scrapy.loader.processors import TakeFirst, Compose, Identity
from slugify import slugify

from .processors import clean_output, only_alphanumeric, get_money, boolean_output


class Availability(Enum):
    UNKNOWN = 0
    IN_STOCK = 1
    OUT_OF_STOCK = 2
    IN_STORE_ONLY = 3
    BACKORDERED = 4
    SHIP_FROM_STORE = 5
    LIMITED_DELIVERY = 6

    @classmethod
    def from_string(cls, value):
        value = value.lower().strip()
        value = slugify(value, separator='_')
        if value in {'instock', 'in_stock', 'limited_stock', 'onlineonly', 'available', 'lowinstock'}:
            return cls.IN_STOCK
        elif value in {'discontinued', 'outofstock', 'out_of_stock', 'soldout', 'unavailable'}:
            return cls.OUT_OF_STOCK
        elif value in {'instoreonly'}:
            return cls.IN_STORE_ONLY
        elif value in {'limitedavailability'}:
            return cls.LIMITED_DELIVERY
        else:
            return cls.UNKNOWN


class ProductItem(Item):
    url = Field()
    name = Field()
    part_number = Field()
    availability = Field()
    price = Field()
    list_price = Field()
    category = Field()
    thirdparty = Field()
    seller = Field()
    rating = Field()
    rating_count = Field()

    subcategory = Field()
    quantity = Field()
    uom = Field()
    coupon = Field()
    subscription_price = Field()
    altpartnumber = Field()
    gtin = Field()
    manufacturer = Field()
    mfpartnumber = Field()
    qtylimit = Field()
    min_purchase_quantity = Field()


class ProductItemLoader(ItemLoader):
    default_item_class = ProductItem
    default_input_processor = Identity()
    default_output_processor = Compose(TakeFirst(), clean_output)

    name_out = Compose(default_output_processor, only_alphanumeric)
    seller_out = Compose(default_output_processor, only_alphanumeric)
    rating_out = Compose(default_output_processor, float)
    rating_count_out = Compose(default_output_processor, int)
    quantity_out = Compose(default_output_processor, float)
    qtylimit_out = Compose(default_output_processor, int)
    min_purchase_quantity_out = Compose(default_output_processor, int)
    thirdparty_out = Compose(TakeFirst(), boolean_output)
    availability_out = Compose(TakeFirst(), lambda a: a.value if a else None)
    price_out = Compose(default_output_processor, get_money)
    list_price_out = Compose(default_output_processor, get_money)
    subscription_price_out = Compose(default_output_processor, get_money)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        response = kwargs.get('response')
        if not response:
            return

        # self.add_value('url', response.url)

    def add_value(self, field, value, *args, **kwargs):
        if isinstance(value, (Selector, SelectorList)):
            value = value.extract()
        super().add_value(field, value, *args, **kwargs)

    @classmethod
    def apply_regex(cls, pattern, value):
        match = re.findall(pattern, value)
        if match:
            return match[0]
        raise ValueError(f'Could not parse rating in "{value}"')


class DiscoveryItem(Item):
    url = Field()


class BooksItem(Item):
    title = Field()
    category = Field()
    price = Field()
    description = Field()


class BooksItemLoader(ItemLoader):
    default_item_class = BooksItem
    default_input_processor = Identity()
