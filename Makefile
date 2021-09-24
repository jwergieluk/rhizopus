export PYTHONPATH = .

test:
	pytest -v rhizopus_tests

wheel:
	python setup.py bdist_wheel
