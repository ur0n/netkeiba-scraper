SCRAPER_LOGIN_MAIL = mail
SCRAPER_LOGIN_PASSWORD = password
SCRAPER_SCRAPE_INTERVAL = 500
SCRAPER_OUTPUT_DIR = ${PWD}/output

DOCKER_BASE_COMMAND = docker run -v ${SCRAPER_OUTPUT_DIR}:/output \
                                  -e SCRAPER_LOGIN_MAIL=${SCRAPER_LOGIN_MAIL} \
                                  -e SCRAPER_LOGIN_PASSWORD=${SCRAPER_LOGIN_PASSWORD} \
                                  -e SCRAPER_SCRAPE_INTERVAL=${SCRAPER_SCRAPE_INTERVAL} \
                                  netkeiba-scraper

## create image by sbt-docker
create-image:
	sbt docker

## save as tar.gz
save-image:
	docker save netkeiba-scraper > netkeiba-scraper.tar
	gzip netkeiba-scraper.tar

## load image to local
load-image:
	gzip -d netkeiba-scraper.tar.gz
	docker load < netkeiba-scraper.tar

## run collecturl
collecturl:
	${DOCKER_BASE_COMMAND} collecturl

## run scrapehtml
scrapehtml:
	${DOCKER_BASE_COMMAND} scrapehtml

## run extract
extract:
	${DOCKER_BASE_COMMAND} extract

## run genfeature
genfeature:
	${DOCKER_BASE_COMMAND} genfeature


#################################################################################
# Self Documenting Commands                                                     #
#################################################################################

.DEFAULT_GOAL := show_help

# Inspired by <http://marmelab.com/blog/2016/02/29/auto-documented-makefile.html>
# sed script explained:
# /^##/:
# 	* save line in hold space
# 	* purge line
# 	* Loop:
# 		* append newline + line to hold space
# 		* go to next line
# 		* if line starts with doc comment, strip comment character off and loop
# 	* remove target prerequisites
# 	* append hold space (+ newline) to line
# 	* replace newline plus comments by `---`
# 	* print line
# Separate expressions are necessary because labels cannot be delimited by
# semicolon; see <http://stackoverflow.com/a/11799865/1968>
.PHONY: show_help
show_help:
	@echo "$$(tput bold)Available rules:$$(tput sgr0)"
	@echo
	@sed -n -e "/^## / { \
		h; \
		s/.*//; \
		:doc" \
		-e "H; \
		n; \
		s/^## //; \
		t doc" \
		-e "s/:.*//; \
		G; \
		s/\\n## /---/; \
		s/\\n/ /g; \
		p; \
	}" ${MAKEFILE_LIST} \
	| LC_ALL='C' sort --ignore-case \
	| awk -F '---' \
		-v ncol=$$(tput cols) \
		-v indent=19 \
		-v col_on="$$(tput setaf 6)" \
		-v col_off="$$(tput sgr0)" \
	'{ \
		printf "%s%*s%s ", col_on, -indent, $$1, col_off; \
		n = split($$2, words, " "); \
		line_length = ncol - indent; \
		for (i = 1; i <= n; i++) { \
			line_length -= length(words[i]) + 1; \
			if (line_length <= 0) { \
				line_length = ncol - indent - length(words[i]) - 1; \
				printf "\n%*s ", -indent, " "; \
			} \
			printf "%s ", words[i]; \
		} \
		printf "\n"; \
	}' \
	| more $(shell test $(shell uname) = Darwin && echo '--no-init --raw-control-chars')
