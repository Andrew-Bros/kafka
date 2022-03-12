.PHONY: check tidy

all:	check tidy

check:
	pylint *.py

tidy:
	black --diff *.py

image:
	docker build --tag kafka-project .
