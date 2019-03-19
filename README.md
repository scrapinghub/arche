# Arche

[![Build Status](https://travis-ci.org/scrapinghub/arche.svg?branch=master)](https://travis-ci.org/scrapinghub/arche)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/ambv/black)

The library helps to verify data using set of defined rules, for example:
  * Validation with JSON schema
  * Coverage
  * Duplicates
  * Garbage symbols
  * Comparison of two jobs
  
_We use it in Scrapinghub_

At the moment the tool supports only [Scrapy Cloud](https://scrapinghub.com/scrapy-cloud) jobs data as an input. The core libraries are **pandas**, **plotly** and **jsonschema**.

## Use case
* You need to perform QA on Scrapy Cloud jobs continuously.

  Say, you scraped some website and have the data ready in the cloud. A typical approach would be:
    * Create a JSON schema and validate the scrapped data with it
    * Use the created schema in [Spidermon Validation](https://spidermon.readthedocs.io/en/latest/item-validation.html#with-json-schema)
* You want to use it in your application to verify Scrapy Cloud data

## Usage
The library is intented to work in [Jupyter](https://jupyter.org/) environment and has its own plain text report module. It's assumed that:
* You have the library installed there with all dependencies
* [SH_APIKEY](https://github.com/scrapinghub/arche/wiki/Quickstart#environment-variables) is set up

A simple example will look like this:

	from arche import Arche
	g = Arche(source="112358/13/21")
	g.report_all()
	g.data_quality_report()

The outcome of executed rules will be printed, along with some fancy graphs

## Developer Setup

	pipenv install --dev
  	pipenv shell
	tox

##  Developer Usage

The library consists of two core modules - `arche.rules` and `arche.report`. If you wish to just use the rules and implement reporting yourself, here's one example of usage:

	import arche.rules.duplicates as dup_rules
	result = dup_rules.check_uniqueness(df, tagged_fields)

Each rule returns `arche.rules.result.Result` object which can be parsed however you like.

## Documentation

* [wiki](https://github.com/scrapinghub/arche/wiki)
    
## Contribution
Any contributions, no matter how minor, are welcome!

* Fork or create a new branch
* Make desired changes
* Open a pull request

To update docs, better check `tox.ini` docs section.
