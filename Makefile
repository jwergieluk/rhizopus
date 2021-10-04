export PYTHONPATH = .

black-format:
	black -t py38 -S -l 100 rhizopus rhizopus_tests example.py setup.py

black: black-format

test:
	pytest -v rhizopus_tests

wheel:
	rm dist/*
	python setup.py bdist_wheel
