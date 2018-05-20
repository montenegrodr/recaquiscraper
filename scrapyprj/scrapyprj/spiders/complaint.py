import time
import scrapy
import logging
import datetime

from letmecrawl import letmecrawl
from scrapy_splash import SplashRequest
from .orm import DataController, NoPageToProcessException
from .commons import NO_PAGES_SLEEP

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


class Complaint(scrapy.Spider):
    name = 'complaint'

    def start_requests(self):
        with DataController() as ds:
            while True:
                try:
                    complaint = ds.next_complaint()
                    yield SplashRequest(url=complaint.url,
                                        callback=self.parse,
                                        errback=self.error,
                                        meta={'complaint_id': complaint.id},
                                        args={'wait': 20,
                                              'proxy': ProxyController.get_proxy()})
                except NoPageToProcessException:
                    logger.info('No pages found to process')
                    time.sleep(NO_PAGES_SLEEP)
                    ds.commit()

    def error(self, response):
        complaint_id = response.value.response.meta.get('complaint_id')
        with DataController() as ds:
            page = ds.get_complaint_page(complaint_id)
            page.error = response.value.response.text
            page.locked = False

    def parse(self, response):
        def parse_chunks(selector):
            chunks = response.css(selector).extract()
            return '\n'.join(chunks)

        def solved():
            img_src = rates[0].css('img::attr(src)').extract_first()
            if 'nao-resolvida' in img_src:
                return 'nao-resolvida'
            elif 'resolvida' in img_src:
                return 'resolvida'
            else:
                logger.warning(
                    "Couldn't find solved status for {}".format(img_src))
                return None

        complaint_id = response.meta.get('complaint_id')
        try:
            css = response.css

            business = css('a.business::text')[0].extract()
            location = css('ul.local-date li::text')[0].extract()
            date = css('ul.local-date li::text')[1].extract()
            title = css('h1::text').extract_first()
            complaint_body = parse_chunks('div.complain-body p::text')
            final_answer = css('div.reply-content p::text')[-1].extract()
            rates = css('div.score-seal')
            again = rates[1].css('p::text')[2].extract()
            rate = rates[2].css('p::text')[2].extract()

            with DataController() as ds:
                page = ds.get_complaint_page(complaint_id)
                ds.insert({
                    'business': business,
                    'location': location,
                    'date': date,
                    'title': title,
                    'complaint_body': complaint_body,
                    'final_answer': final_answer,
                    'solved': solved(),
                    'again': again,
                    'rate': rate,
                    'url': response.url,
                    'created_at': datetime.datetime.now(),
                    'id': response.meta.get('id'),
                    'complaint_page_id': page.id
                },
                commit=False)
                page.processed = True
        except Exception as err:
            with DataController() as ds:
                page = ds.get_complaint_page(complaint_id)
                ds.unlock(page, err)