.PHONY: build test audit clean

build:
	docker build -t async-agent-dispatcher:latest .

test:
	docker compose run --rm test

audit:
	bash ./scripts/audit_image.sh

clean:
	docker compose down --rmi all --volumes --remove-orphans
