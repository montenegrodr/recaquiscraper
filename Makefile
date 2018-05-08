SPLASH_PORT ?= 8050
SLICE_BEGIN ?= 0

export

.PHONY: run-splash
run-splash:
	docker run -p ${SPLASH_PORT}:${SPLASH_PORT} scrapinghub/splash

.PHONY: crawl
crawl:
	$(MAKE) -C scrapyprj crawl

.PHONY: build
build:
    pip install -r requirements.txt