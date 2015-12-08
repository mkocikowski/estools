ifneq  "$(shell basename $(VIRTUAL_ENV))" "virtualenv-estools"
$(error "virtualenv must be set to 'virtualenv-estools'")
endif

.PHONY: install
install: 
	pip uninstall -y estools ; true                                      
	rm -f $(VIRTUAL_ENV)/bin/es*
	find . -name "*.pyc" -delete
	pip install --upgrade -e ./

.PHONY: test
test:
	find . -name "*.pyc" -delete
	python estools/test/units.py
