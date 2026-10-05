PY ?= python
export PYTHONPATH := src:.

.PHONY: install demo run test lint validate serve clean dashboard

install:            ## install runtime + dev dependencies
	$(PY) -m pip install -r requirements.txt

demo:               ## full two-day demo: generate -> run -> incremental -> run -> evaluate
	$(PY) -m mdm demo

demo-break:         ## same, but day-2 CRM file is "truncated" -> reconciliation break + alert
	$(PY) -m mdm demo --simulate-truncated-file

run:
	$(PY) -m mdm run

validate:           ## lint all YAML configuration (CI gate)
	$(PY) -m mdm validate-config

test:
	$(PY) -m pytest -q || $(PY) -m unittest discover -s tests -t .

lint:
	ruff check src tests data_generator

serve:              ## Golden Record REST API on :8000
	$(PY) -m mdm serve

clean:
	$(PY) -m mdm reset
