import scrapy
from scrapy_splash import SplashRequest
import logging
import itertools

logger = logging.Logger(__name__)

base_url = 'https://www.reclameaqui.com.br/indices/lista_reclamacoes/' \
           '?id={}&size=10&page=1&status=EVALUATED'


class ReclameAquiSpider(scrapy.Spider):
    name = "reclame_aqui"

    def start_requests(self):
        for id in itertools.count():
            id = 616
            url = base_url.format(id)
            yield SplashRequest(url, self.parse_menu, args={'wait': 5,},)

    def parse_menu(self, response):
        for complaint_item in response.css('div.complain-status-title'):
            complaint_item_href = complaint_item.css('a::attr(href)').extract_first()
            if complaint_item_href:
                next_page = response.urljoin(complaint_item_href)
                yield SplashRequest(next_page, self.parse_item,  args={'wait': 5,},)
        # TODO go to the next page

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

        business = response.css('a.business::text')[0].extract()
        location = response.css('ul.local-date li::text')[0].extract()
        date = response.css('ul.local-date li::text')[1].extract()
        title = response.css('h1::text').extract_first()
        complaint_body = parse_chunks('div.complain-body p::text')
        final_answer = response.css('div.reply-content p::text')[-1].extract()

        rates = response.css('div.score-seal')
        solved = solved()
        again = rates[1].css('p::text')[2].extract()
        rate = rates[2].css('p::text')[2].extract()
