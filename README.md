# Arche

[![PyPI](https://img.shields.io/pypi/v/arche.svg)](https://pypi.org/project/arche)
[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/arche.svg)](https://pypi.org/project/arche)
![GitHub](https://img.shields.io/github/license/scrapinghub/arche.svg)
[![Build Status](https://travis-ci.com/scrapinghub/arche.svg?branch=master)](https://travis-ci.com/scrapinghub/arche)
[![Codecov](https://img.shields.io/codecov/c/github/scrapinghub/arche.svg)](https://codecov.io/gh/scrapinghub/arche)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/ambv/black)
[![GitHub commit activity](https://img.shields.io/github/commit-activity/m/scrapinghub/arche.svg)](https://github.com/scrapinghub/arche/commits/master)

    pip install arche

Arche (pronounced *Arkey*) helps to verify scraped data using set of defined rules, for example:
  * Validation with [JSON schema](https://json-schema.org/)
  * Coverage (items, fields, categorical data, including booleans and enums)
  * Duplicates
  * Garbage symbols
  * Comparison of two jobs
  
_We use it in Scrapinghub, among the other tools, to ensure quality of scraped data_

## Installation

Arche requires [Jupyter](https://jupyter.org/install) environment, supporting both [JupyterLab](https://github.com/jupyterlab/jupyterlab#installation) and [Notebook](https://github.com/jupyter/notebook) UI

For JupyterLab, you will need to properly install [plotly extensions](https://github.com/plotly/plotly.py#jupyterlab-support-python-35)

Then just `pip install arche`

## Why
To check the quality of scraped data continuously. For example, if you scraped a website, a typical approach would be to validate the data with Arche. You can also create a schema and then set up [Spidermon](https://spidermon.readthedocs.io/en/latest/item-validation.html#with-json-schema)

## Developer Setup

	pipenv install --dev
	pipenv shell
	tox

## Contribution
Any contributions are welcome! See https://github.com/scrapinghub/arche/issues if you want to take on something or suggest an improvement/report a bug.
