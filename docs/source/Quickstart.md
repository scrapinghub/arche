# Quickstart

## Basic Usage

* make sure required [environment variables](https://github.com/scrapinghub/arche/wiki/Quickstart#environment-variables) are set

* create a schema with `arche.basic_json_schema(job_key)`

* create an Arche instance `g = Arche(source=job_key, schema=temp_schema)`

* run and report all tests with `g.report_all()`

* run DQR with `g.data_quality_report()`

`job_key` can be either a usual job key, e.g. `000001/1/1`, or a collection key - `00001/collections/s/reviews`

`schema` argument accepts either a dict or a s3 bucket link to a schema.

## Schema Validation

Arche allows to use custom rules, available with `tag` keyword. The value contains tag names, and could be either a string or a list or strings.

For a complete list of tags, check the code https://github.com/scrapinghub/arche/blob/master/src/arche/schema_tools.py#L18

For example:

     "name": {
       "type": "string",
       "tag": ["unique"]
     },

## Environment variables
Next env variables are required:

* SH_APIKEY - This key should have read permissions for the project you want to get items from.

If you also wish access your schemas from S3, set AWS credentials

* AWS_ACCESS_KEY_ID

* AWS_SECRET_ACCESS_KEY
