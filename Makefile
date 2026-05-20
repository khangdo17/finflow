.PHONY: install up down restart logs status verify clean

install:
	pip3 install -r requirements.txt

up:
	docker compose -f infrastructure/docker-compose.yml up -d
	sleep 30
	$(MAKE) status

down:
	docker compose -f infrastructure/docker-compose.yml down

restart:
	$(MAKE) down
	$(MAKE) up

logs:
	docker compose -f infrastructure/docker-compose.yml logs -f

status:
	docker compose -f infrastructure/docker-compose.yml ps

verify:
	python3 scripts/verify_connections.py

clean:
	docker compose -f infrastructure/docker-compose.yml down -v
