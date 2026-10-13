.PHONY: help build test check-blacklist audit clean

help:
	@echo "Available commands:"
	@echo "  make build           - Build local Docker container image"
	@echo "  make test            - Run complete test suite in isolated Redis container"
	@echo "  make check-blacklist - Verify zero secret or internal IP leaks in codebase"
	@echo "  make audit           - Run image layer and Trivy vulnerability audit"
	@echo "  make clean           - Tear down containers, networks, and orphaned volumes"

build:
	docker build -t async-agent-dispatcher:latest .

test:
	docker compose run --rm test

check-blacklist:
	bash ./scripts/check_blacklist.sh .

audit: check-blacklist
	bash ./scripts/audit_image.sh

clean:
	docker compose down --rmi all --volumes --remove-orphans
