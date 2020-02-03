.PHONY: venv
venv:
	./scripts/init_environment.sh

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

.PHONY: cluster
cluster:
	./scripts/create_cluster.sh

.PHONY: cluster-up
cluster-up:
	minikube start

.PHONY: cluster-down
cluster-down:
	minikube stop
