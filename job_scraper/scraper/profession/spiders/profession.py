from pathlib import Path
from scrapy.linkextractors import LinkExtractor

import re
import scrapy
import json

class ProfessionSpider(scrapy.Spider):
    name = "profession"
    counter = 0
    page_link_extractor = LinkExtractor(r"https://www\.profession\.hu/allasok/[0-9]+$")
    # job_link_extractor = LinkExtractor(r"https://www\.profession\.hu/allas/[A-Za-z0-9]+")
    async def start(self):
        urls = [
            "https://www.profession.hu/allasok"
        ]

        for url in urls:
            yield scrapy.Request(url = url, callback=self.parse)
    

    def parse(self, response):
        self.counter = self.counter + 1
        
        if self.counter > 1:
            for li in response.css('li.advertisement-result-list-item'):
                yield {
                    'prof_id': li.attrib.get('data-prof-id'),
                    'link': li.attrib.get('data-link'),
                    'prof_category': li.attrib.get('data-prof-category'),
                    'prof_position': li.attrib.get('data-prof-position'),
                    'prof_name': li.attrib.get('data-prof-name'),
                    'row_number': li.attrib.get('data-row-number'),
                    'item_name': li.attrib.get('data-item-name'),
                    'item_id': li.attrib.get('data-item-id'),
                    'item_brand': li.attrib.get('data-item-brand'),
                    'category1': li.attrib.get('data-category1'),
                    'category2': li.attrib.get('data-category2'),
                    'category3': li.attrib.get('data-category3'),
                    'category4': li.attrib.get('data-category4'),
                    'category5': li.attrib.get('data-category5'),
                    'category6': li.attrib.get('data-category6'),
                    'list_name': li.attrib.get('data-list-name'),
                    'list_id': li.attrib.get('data-list-id'),
                    'location_id': li.attrib.get('data-location-id'),
                    'list_index': li.attrib.get('data-list-index'),
                    'variant': li.attrib.get('data-variant'),
                    'currency': li.attrib.get('data-currency'),
                    'value': li.attrib.get('data-value'),
                    'affiliation': li.attrib.get('data-affiliation'),
                    'quantity': li.attrib.get('data-quantity'),
                    'price': li.attrib.get('data-price'),
                    'application_type': li.attrib.get('data-application_type'),
                    'prof_product_name': li.attrib.get('data-prof_product_name'),
                }
        
        if self.counter == 1:
            last_page = int(self.page_link_extractor.extract_links(response)[-1].text)
            for i in range(1, 100):
                yield scrapy.Request(url = f"https://www.profession.hu/allasok/{i}", callback=self.parse)
        