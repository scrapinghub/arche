# Handling items of different types

A data source could contain items of different types, which can be handled with some preparation:

* Create one schema per item type choosing homogeneous items  `basic_json_schema('job_key', items_number=[0,1)`
* Pass a filter argument to choose items of one type - `Arche(source='000000/000/0', schema=schema, filter=[("_type", "=", ["ItemType"])])`
* Repeat the previous steps for each item type

The library also supports other API arguments, such as `count, start`. The complete list of arguments is available in Scrapinghub Python API documentation:

* https://python-scrapinghub.readthedocs.io/en/latest/client/apidocs.html#scrapinghub.client.items.Items.iter

* https://python-scrapinghub.readthedocs.io/en/latest/client/apidocs.html#scrapinghub.client.collections.Collection.iter