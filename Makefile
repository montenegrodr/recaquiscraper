SPLASH_PORT  ?= 8050
SLICE_BEGIN  ?= 0
ENABLE_PROXY ?= 1

export

.PHONY: run-splash
run-splash:
	docker run -p ${SPLASH_PORT}:${SPLASH_PORT} scrapinghub/splash

.PHONY: business
business:
	$(MAKE) -C scrapyprj business

.PHONY: complaint_page
complaint_page:
	$(MAKE) -C scrapyprj complaint_page

.PHONY: complaint
complaint:
	$(MAKE) -C scrapyprj complaint

.PHONY: build
build:
	pip install -r requirements.txt
