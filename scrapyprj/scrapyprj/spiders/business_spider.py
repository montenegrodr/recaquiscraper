import time
import scrapy
import datetime

from itertools import count
from scrapy_splash import SplashRequest
from .orm import DataController
from .commons import URL, MAX_PAGE, SLEEP_TIME


def business_start():
    with DataController() as ds:
        return ds.business_max_id() or 1


class BusinessSpider(scrapy.Spider):
    name = 'business'

    def start_requests(self):
        for id in count(start=business_start()):
            yield SplashRequest(url=URL.format(id, MAX_PAGE),
                                callback=self.parse,
                                errback=self.error,
                                meta={'id': id},
                                args={'wait': 20}
            )
            # Sleeping because we're not using proxy here
            time.sleep(SLEEP_TIME)

    def error(self, response):
        company_id = response.meta.get('id')
        with DataController() as ds:
            ds.insert(obj={
                'store_id': company_id,
                'created_at': datetime.datetime.now(),
                'error': response.status
            })

    def parse(self, response):
        s = response.css

        company_id = response.meta.get('id')
        company_name = None
        company_nb_pages = None

        try:
            company_name = s('p.company-name a::text').extract_first()
            company_nb_pages = int(s('ul.pagination li a::text').extract()[-2])

            with DataController() as ds:
                company = ds.insert(obj={
                    'name': company_name,
                    'store_id': company_id,
                    'nb_pages': company_nb_pages,
                    'created_at': datetime.datetime.now()
                },
                    commit=False)

                for page in range(1, company_nb_pages + 1):
                    ds.insert(obj={
                        'page': page,
                        'processed': False,
                        'id_business': company_id
                    },
                        commit=False)
                company.processed = True
        except Exception as exp:
            with DataController() as ds:
                ds.insert(obj={
                    'name': company_name,
                    'store_id': company_id,
                    'nb_pages': company_nb_pages,
                    'created_at': datetime.datetime.now(),
                    'error': exp
                })
