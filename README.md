# Arche

[![PyPI](https://img.shields.io/pypi/v/arche.svg)](https://pypi.org/project/arche)
[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/arche.svg)](https://pypi.org/project/arche)
![GitHub](https://img.shields.io/github/license/scrapinghub/arche.svg)
[![Build Status](https://travis-ci.com/scrapinghub/arche.svg?branch=master)](https://travis-ci.com/scrapinghub/arche)
[![Codecov](https://img.shields.io/codecov/c/github/scrapinghub/arche.svg)](https://codecov.io/gh/scrapinghub/arche)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/ambv/black)
[![GitHub commit activity](https://img.shields.io/github/commit-activity/m/scrapinghub/arche.svg)](https://github.com/scrapinghub/arche/commits/master)
[![Join the chat at https://gitter.im/scrapinghub/arche](https://badges.gitter.im/scrapinghub/arche.svg)](https://gitter.im/scrapinghub/arche?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

    pip install arche

Arche (pronounced as Arkey) helps to verify data using set of defined rules, for example:
  * Validation with JSON schema
  * Coverage
  * Duplicates
  * Garbage symbols
  * Comparison of two jobs
  
_We use it in Scrapinghub to ensure quality of scraped data_

## Installation

Arche requires [Jupyter](https://jupyter.org/install) environment, supporting both [JupyterLab](https://github.com/jupyterlab/jupyterlab#installation) and [Notebook](https://github.com/jupyter/notebook) UI

For JupyterLab, you will need to properly install [plotly extensions](https://github.com/plotly/plotly.py#jupyterlab-support-python-35)

Then just `pip install arche`

## Use case
* You need to check the quality of data from Scrapy Cloud jobs continuously.

  Say, you scraped some website and have the data ready in the cloud. A typical approach would be:
    * Create a JSON schema and validate the data with it
    * Use the created schema in [Spidermon Validation](https://spidermon.readthedocs.io/en/latest/item-validation.html#with-json-schema)
* You want to use it in your application to verify Scrapy Cloud data

## Developer Setup

	pipenv install --dev
	pipenv shell
	tox

## Contribution
Any contributions are welcome!

* Fork or create a new branch
* Make desired changes
* Open a pull request
