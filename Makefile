export PYTHONPATH = .

test:
	pytest -v rhizopus_tests

wheel:
	rm dist/*
	python setup.py bdist_wheel
