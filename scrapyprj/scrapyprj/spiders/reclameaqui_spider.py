import scrapy
from scrapy_splash import SplashRequest
from letmecrawl import letmecrawl

class ReclameAquiSpider(scrapy.Spider):
    name = "reclame_aqui"

    def start_requests(self):
        for id in range(1, 100000):
            id = 616
            url = 'https://www.reclameaqui.com.br/indices/lista_reclamacoes/?id={}&size=10&page=3&status=EVALUATED'.format(id)
            # yield scrapy.Request(
            #     url=url,
            #     callback=self.parse
            # )
            # proxy = next(letmecrawl())
            yield SplashRequest(url, self.parse,
                                args={
                                    'wait': 2,
                                },
                                )

    def parse(self, response):
        x = 1
        pass