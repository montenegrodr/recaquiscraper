import time
import scrapy
import logging

from scrapy_splash import SplashRequest
from letmecrawl import letmecrawl
from .orm import DataController, NoPageToProcessException
from .commons import URL, NO_PAGES_SLEEP

logger = logging.Logger(__name__)
logging.getLogger('letmecrawl.models').setLevel(logging.INFO)
logger.setLevel(logging.INFO)


class ProxyController(object):
    gen = letmecrawl()
    proxy = None

    @staticmethod
    def get_proxy(new=False):
        try:
            if not ProxyController.proxy or new:
                proxy = next(ProxyController.gen)
        except Exception as exp:
            logger.error("Proxy error. {}".format(exp))
            return None
        return 'http://{}:{}'.format(proxy.ip, proxy.port)


class ComplaintSpider(scrapy.Spider):
    name = 'complaint'

    def start_requests(self):
        with DataController() as ds:
            while True:
                try:
                    page = ds.next_page()
                    yield SplashRequest(url=URL.format(page.id_business, page.page),
                                        callback=self.parse,
                                        errback=self.error,
                                        meta={'page_id': page.id},
                                        args={'wait': 20,
                                              'proxy': ProxyController.get_proxy()})
                except NoPageToProcessException:
                    logger.info('No pages foudn to process')
                    time.sleep(NO_PAGES_SLEEP)

    def error(self, response):
        logger.info("error")
        pass

    def parse(self, response):
        page_id = response.meta.get('page_id')
        try:
            complaint_ls = response.css('div.complain-status-title')

            if not complaint_ls:
                raise Exception("complaint_ls is empty")

            with DataController() as ds:
                page = ds.get_page(page_id)
                for complaint_item in complaint_ls:
                    complaint_item_href = complaint_item.css('a::attr(href)').extract_first()
                    if complaint_item_href:
                        next_item = response.urljoin(complaint_item_href)
                        ds.insert(obj={
                            'url': next_item,
                            'id_business': page.id_business,
                            'id_page': page.id
                        }, commit=False)
                page.processed=True
        except Exception as err:
            with DataController() as ds:
                ds.unlock(page, err)

