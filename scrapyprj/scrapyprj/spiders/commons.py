import os

# urljoin sucks!
URL_HOST   = 'www.reclameaqui.com.br'
URL_PATH   = 'indices/lista_reclamacoes'
URL_PARAMS = 'id={}&size=10&page={}&status=EVALUATED'
URL        = 'https://{}/{}/?{}'.format(URL_HOST, URL_PATH, URL_PARAMS)

SLEEP_TIME     = 1
MAX_PAGE       = 10000
NO_PAGES_SLEEP = 60
ENABLE_PROXY   = os.getenv('ENABLE_PROXY') == "1"
