import json
from functools import partial
from urllib.parse import urljoin
import os

import jmespath
from scrapy import Request, Spider
from scrapy.loader.processors import Compose, TakeFirst, MapCompose, SelectJmes
from scrapy.settings.default_settings import RETRY_HTTP_CODES
from scrapy.downloadermiddlewares.retry import RetryMiddleware
from w3lib.url import add_or_replace_parameter

from scrapinghub import ScrapinghubClient

from demo_spiders.items import ProductItemLoader, ProductItem, Availability, DiscoveryItem
from demo_spiders.processors import boolean_output, parse_quantity_uom, standardize_uom
from .. import settings


def take_category(nth, category):
    try:
        return category.split('/')[nth]
    except IndexError:
        pass


def get_sds_collection(name):
    client = ScrapinghubClient()
    project = client.get_project(381798)
    collections = project.collections
    return collections.get_store(name)


class BadResponse(Exception):

    def __init__(self, reason, *args, **kwargs):
        self.reason = reason
        super().__init__(args, kwargs)


class RetryBadResponse(RetryMiddleware):
    '''Some requests return without some product info (price, offers).
    Noticed that when retrying them, they seem to work as expected.
    Probably, this might be related to some Crawlera config, however I coundn't identify it.'''

    def process_spider_exception(self, response, exception, spider):
        if isinstance(exception, BadResponse):
            item = response.meta.get('item')
            reason = f'bad_request_{exception.reason}'
            yield self._retry(response.request, reason, spider) or item


class WalmartProductItemLoader(ProductItemLoader):
    default_item_class = ProductItem
    availability_in = MapCompose(Availability.from_string)
    # to exclude 0 value
    rating_count_in = MapCompose(lambda x: x if x else None)
    thirdparty_out = Compose(TakeFirst(), boolean_output)
    category_in = MapCompose(partial(take_category, 2))
    subcategory_in = MapCompose(partial(take_category, 3))


class WalmartComSpider(Spider):
    name = 'walmart.com'
    allowed_domains = ['walmart.com']
    custom_settings = {
        'RETRY_HTTP_CODES': RETRY_HTTP_CODES + [444, 520],
        'SPIDER_MIDDLEWARES': dict(
            getattr(settings, 'SPIDER_MIDDLEWARES', {}),
            **{
                'amazon_arche.spiders.walmart_com.RetryBadResponse': 99
            }
        )
    }

    def __init__(self, *args, **kwargs):
        super(WalmartComSpider, self).__init__(*args, **kwargs)
        spider_name = self.name.replace('.', '_')
        spider_name = f'{spider_name}_discovery'
        self.collection = get_sds_collection(spider_name)

    def start_requests(self):
        for item in list(self.collection.iter()):
            request = self.make_request(item['value'], item['_key'])
            request.meta.update({
                'partnumber': item['_key'],
                'input_url': item['value'],
            })
            yield request

    def make_request(self, url, part_number):
        return Request(
            f'https://www.walmart.com/product/{part_number}/sellers',
            callback=self.parse,
            dont_filter=True,
            meta={'url': url}
        )

    def parse(self, response):
        partnumber = response.meta['partnumber']
        item_info = self._parse_item_json(response)
        product = self._get_product_by_partnumber(item_info, partnumber)
        if not product:
            return

        base_item = self._parse_base_product(product, response, item_info)
        if product.get('productPublishStatus', 'published').lower() != 'published':
            base_item['availability'] = Availability.OUT_OF_STOCK.value
            base_item['thirdparty'] = True
            base_item['seller'] = 'unknown'
            return base_item

        offers = list(self._parse_offers(item_info, product, base_item, response))

        if not offers:
            self.crawler.stats.inc_value('chewy/walmart/product_without_offer')
            self.logger.warning(f'Could not load offer for product {partnumber} in {response.url}.')
            raise BadResponse('no_offers')

        return offers

    def _parse_item_json(self, response):
        json_txt = response.css('script#item::text').extract_first()
        return json.loads(json_txt)['item']

    def _get_product_by_partnumber(self, item_info, part_number):
        products = item_info.get('product', {}).get('products', {}).values()
        products = filter(lambda p: p['usItemId'] == part_number, products)
        products = filter(lambda p: p['status'].lower() == 'fetched', products)
        return next(products, None)

    def _parse_base_product(self, product, response, item_info):
        loader = WalmartProductItemLoader(response=response)

        product_id = product['productId']
        attributes = product['productAttributes']
        loader.add_value('name', attributes['productName'])
        loader.add_value('part_number', product['usItemId'])

        self._add_quantity_uom(loader, item_info, attributes, product_id)
        self._add_category_and_subcategory(loader, attributes)
        self._add_rating_and_rating_count(loader, item_info, product_id)

        return loader.load_item()

    def _add_quantity_uom(self, loader, item_info, attributes, product_id):
        product_info = item_info['productBasicInfo'].get(product_id, {})
        variants = product_info.get('selectedVariant', [])
        if variants:
            (quantity, uom) = parse_quantity_uom(variants[0]['name'])
        else:
            quantity = attributes.get('pricePerUnitQuantity', None)
            uom = attributes.get('pricePerUnitUom', '')
            uom = standardize_uom(uom)

        if not (quantity and uom):
            (quantity, uom) = parse_quantity_uom(attributes['productName'])

        if uom and quantity:
            loader.add_value('uom', uom)
            loader.add_value('quantity', quantity)

    def _add_category_and_subcategory(self, loader, attributes):
        category_subcategory = jmespath.search('productCategory.categoryPath', attributes)
        loader.add_value('category', category_subcategory)
        loader.add_value('subcategory', category_subcategory)

    def _add_rating_and_rating_count(self, loader, item_info, product_id):
        loader.add_value(
            'rating',
            item_info,
            MapCompose(
                SelectJmes(f'product.reviews."{product_id}".averageOverallRating')
            ),
        )
        loader.add_value(
            'rating_count',
            item_info,
            MapCompose(SelectJmes(f'product.reviews."{product_id}".totalReviewCount')),
        )

    def _parse_offers(self, item_info, product, base_item, response):
        offers = item_info['product']['offers']
        sellers = item_info['product']['sellers']
        offers_ids = product.get('offers', [])

        if self._only_one_offer_or_all_offers_out_of_stock(product, offers_ids, offers):
            product_id = product['productId']
            first_offer = item_info['offersOrder'].get(product_id)
            if first_offer:
                self.logger.info(f'Loading first offer for product {base_item["part_number"]}.')
                offer_id = first_offer[0]['id']
                yield self._parse_offer(offers[offer_id], sellers, base_item, response)
                return

        for offer_id in offers_ids:
            item = self._parse_offer(offers[offer_id], sellers, base_item, response)
            if item and item['availability'] == Availability.IN_STOCK.value:
                yield item

    def _only_one_offer_or_all_offers_out_of_stock(self, product, offers_ids, offers):
        only_one_offer = product.get('transactableOfferCount', 0) == 1
        only_one_offer = only_one_offer or product.get('offerCount', 0) == 1
        only_one_offer = only_one_offer or len(offers_ids) == 1

        out_of_stock = map(lambda x: offers[x], offers_ids)
        out_of_stock = map(lambda x: x.get('productAvailability', {}), out_of_stock)
        out_of_stock = map(lambda x: x.get('availabilityStatus', ''), out_of_stock)
        out_of_stock = map(lambda x: Availability.from_string(x), out_of_stock)
        out_of_stock = map(lambda x: x != Availability.IN_STOCK, out_of_stock)

        return only_one_offer or all(out_of_stock)

    def _parse_offer(self, offer_info, sellers, base_item, response):
        part_number = base_item['part_number']

        loader = WalmartProductItemLoader(item=base_item.copy())
        jmes_offer = partial(jmespath.search, data=offer_info)

        if offer_info.get('status', 'NOT_FETCHED').upper() == 'NOT_FETCHED':
            self.logger.warning(f'Skipping offer {offer_info["id"]} because it is not fetched.')
            return None

        availability_status = jmes_offer('productAvailability.availabilityStatus')
        loader.add_value('availability', availability_status)

        loader.add_value('list_price', jmes_offer('pricesInfo.priceMap.WAS.price'))
        loader.add_value('list_price', jmes_offer('pricesInfo.priceMap.LIST.price'))

        seller_id = offer_info.get('sellerId', '')
        seller_name = sellers.get(seller_id, {}).get('sellerDisplayName', '')

        loader.add_value('seller', seller_name.replace('.com', ''))
        loader.add_value('thirdparty', 'walmart' not in seller_name.lower())

        price = jmes_offer('pricesInfo.priceMap.CURRENT.price')
        loader.add_value('price', price)
        if not loader.get_output_value('price'):
            msg = f'Price not parsed for offer {offer_info["id"]} for product {part_number}.'
            self.logger.warning(msg)
            loader.add_value('price', 0)
            if loader.get_output_value('availability') == Availability.IN_STOCK.value:
                response.meta['item'] = loader.load_item()
                raise BadResponse('price_not_fetched')

        return loader.load_item()


class WalmartComDiscoverySpider(Spider):
    name = 'walmart.com-discovery'
    _product_base_url = 'https://www.walmart.com'
    _department_url = 'https://api.mobile.walmart.com/taxonomy/departments/{}'
    _shelf_url = ('https://www.walmart.com/preso/search'
                  '?prg=mWeb&cat_id={}&sort=best_seller&page=1&pref_store=')

    start_urls = [_department_url.format('')]

    def __init__(self, *args, **kwargs):
        super(WalmartComDiscoverySpider, self).__init__(*args, **kwargs)
        spider_name = self.name.replace('.', '_').replace('-discovery', '_discovery')
        self.collection = get_sds_collection(spider_name)

    def parse(self, response):
        data = json.loads(response.text)
        for child in data['children']:
            if child['name'].lower() == 'pets':
                yield Request(
                    self._department_url.format(child['id']),
                    callback=self.parse_sub_department
                )

    def parse_sub_department(self, response):
        data = json.loads(response.text)
        for child in data['children']:
            if 'browseToken' in child:
                yield Request(
                    self._shelf_url.format(child['category']),
                    callback=self.parse_shelf
                )
            else:
                yield Request(
                    self._department_url.format(child['id']),
                    callback=self.parse_sub_department
                )

    def parse_shelf(self, response):
        data = json.loads(response.text)
        yield from self._parse_products(data['items'])
        yield self._maybe_request_next_page(response, data)

    def _parse_products(self, products):
        for product in products:
            url = urljoin(self._product_base_url, product['productPageUrl'])
            self.collection.set({'_key': product['usItemId'], 'value': url})
            yield DiscoveryItem(url=url)

    def _maybe_request_next_page(self, response, data):
        pagination = data['paginationV2']
        current_page = pagination['currentPage']
        max_page = pagination['maxPage']
        if current_page < max_page:
            return Request(
                add_or_replace_parameter(response.url, 'page', current_page + 1),
                callback=self.parse_shelf
            )
