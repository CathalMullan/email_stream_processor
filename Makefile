.PHONY: hook
hook:
	pre-commit autoupdate
	pre-commit install

.PHONY: lint
lint:
	pre-commit run --all-files

.PHONY: test
test:
	pytest tests/

.PHONY: validate
validate:
	make lint
	make test

.PHONY: crawl
crawl:
	scrapy crawl email_spider

.PHONY: docker_image
docker_image:
	docker build -t email_stream_processor -f worker.Dockerfile .
	docker build -t stream_submit -f submit.Dockerfile .

.PHONY: docker_publish
docker_publish:
	make docker_image

	docker tag email_stream_processor:latest gcr.io/distributed-email-pipeline/email_stream_processor:latest
	docker push gcr.io/distributed-email-pipeline/email_stream_processor:latest

	docker tag stream_submit:latest gcr.io/distributed-email-pipeline/stream_submit:latest
	docker push gcr.io/distributed-email-pipeline/stream_submit:latest
