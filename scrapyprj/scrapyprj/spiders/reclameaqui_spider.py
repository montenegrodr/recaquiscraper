import re
import sys
import scrapy
import logging
import datetime
import itertools

from letmecrawl import letmecrawl
from .orm import DataController
from scrapy_splash import SplashRequest

logger = logging.Logger(__name__)
logging.getLogger('letmecrawl.models').setLevel(logging.INFO)
logger.setLevel(logging.INFO)

wait_time = 5
window    = 10000
pattern   = 'page=(\d+)'
base_url  = 'https://www.reclameaqui.com.br/indices/lista_reclamacoes/?id={}&size=10&page=1&status=EVALUATED'


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


class ReclameAquiSpider(scrapy.Spider):
    name = "reclame_aqui"

    def start_requests(self):
        id0 = int(self.begin)
        idf = id0 + window
        for id in range(id0, idf):
            url = base_url.format(id)
            yield SplashRequest(url, self.parse_menu, errback=self.err_back,
                                args={
                                    'wait': wait_time,
                                    'proxy': ProxyController.get_proxy()
                                },
                                meta={'id': id})

    def err_back(self, response):
        ProxyController.get_proxy(new=True)
        with DataController() as ds:
            ds.insert_error({
                'description': response.value.response.data.get('description'),
                'error': response.value.response.data.get('error'),
                'timeout': response.value.response.data.get('info', {}).get('timeout'),
                'type': response.value.response.data.get('type'),
                'url': response.value.response.url,
                'created_at': datetime.datetime.now()
            })

    def parse_menu(self, response):
        def last_page():
            if next_pages.extract()[-2] == '...':
                return sys.maxsize
            else:
                return int(next_pages.extract()[-2])

        complaint_ls = response.css('div.complain-status-title')
        if not complaint_ls:
            logging.info("Not found content for {}. Status {}.".format(
                response.url, response.status))
            return

        for complaint_item in complaint_ls:
            complaint_item_href = complaint_item.css('a::attr(href)').extract_first()
            if complaint_item_href:
                next_item = response.urljoin(complaint_item_href)
                yield SplashRequest(next_item, self.parse_item,  args={'wait': 5,},
                                    meta={'id': response.meta.get('id')})

        next_pages = response.css('ul.pagination li a::text')

        # It means it has at least on page to go forward
        has_pagination = len(next_pages) >= 4
        if has_pagination:
            m = re.search(pattern, response.url)
            if m:
                next_page = int(m.group(1)) + 1
                if next_page > last_page():
                    return
                next_page_href = response.url.replace(m.group(0), 'page={}'.format(next_page))
                yield SplashRequest(next_page_href, self.parse_menu,
                                    args={
                                        'wait': wait_time,
                                        'proxy': ProxyController.get_proxy()
                                    },
                                    meta={'id': response.meta.get('id')})

    def parse_item(self, response):
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

        css = response.css

        business        = css('a.business::text')[0].extract()
        location        = css('ul.local-date li::text')[0].extract()
        date            = css('ul.local-date li::text')[1].extract()
        title           = css('h1::text').extract_first()
        complaint_body  = parse_chunks('div.complain-body p::text')
        final_answer    = css('div.reply-content p::text')[-1].extract()
        rates           = css('div.score-seal')
        again           = rates[1].css('p::text')[2].extract()
        rate            = rates[2].css('p::text')[2].extract()

        with DataController() as ds:
            ds.insert({
                'business':       business,
                'location':       location,
                'date':           date,
                'title':          title,
                'complaint_body': complaint_body,
                'final_answer':   final_answer,
                'solved':         solved(),
                'again':          again,
                'rate':           rate,
                'url':            response.url,
                'created_at':     datetime.datetime.now(),
                'id':             response.meta.get('id'),
            })
