.PHONY: check tidy image demo stop

all:	check tidy

check:
	pylint *.py

tidy:
	black --diff *.py

image:
	docker build --tag kafka-project .

demo:	image
	docker compose up -d

stop:
	docker compose down
