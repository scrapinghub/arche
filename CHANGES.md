# Changes
Most recent releases are shown at the top. Each release shows:

- **Added**: New classes, methods, functions, etc
- **Changed**: Additional parameters, changes to inputs or outputs, etc
- **Fixed**: Bug fixes that don't change documented behaviour

Note that the top-most release is changes in the unreleased master branch on Github. Parentheses after an item show the name or github id of the contributor of that change.

[Keep a Changelog](https://keepachangelog.com/en/1.0.0/), [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.3.6dev] (Work In Progress)
### Added
### Changed
### Fixed
### Removed


## [0.3.5] (2019-05-14)
### Added
- `Arche()` supports any iterables with item dicts, fixing jsonschema consistency, #83
- `Items.from_array` to read raw data from iterables, #83
### Changed
- If reading from pandas df directly, store raw data in numpy array. See gotchas http://pandas.pydata.org/pandas-docs/stable/user_guide/gotchas.html#support-for-integer-na
### Fixed
### Removed


## [0.3.4] (2019-05-06)
### Fixed
- basic_json_schema() fails with long `1.0` types, #80


## [0.3.3] (2019-05-03)
### Added
- Accept dataframes as source or target, #69
### Changed
- data_quality_report plots the same "Fields Coverage" instead of green "Scraped Fields Coverage"
- Plot theme changed from ggplot2 to seaborn, #62
- Same target and source raise an error, was a warning before
- Passed rules marked with green PASSED.
### Fixed
- Online documentation now renders graphs https://arche.readthedocs.io/en/latest/, #41
- Error colours are back in `report_all()`. 
### Removed
- Deprecated `Arche.basic_json_schema()`, use `basic_json_schema()`
- Removed Quickstart.md as redundant - documentation lives in notebooks


## [0.3.2] (2019-04-18)
### Added
- Allow reading private raw schemas directly from bitbucket, #58
### Changed
- Progress widgets are removed before printing graphs
- New plotly v4 API
### Fixed
- Failing `Compare Prices For Same Urls` when url is `nan`, #67
- Empty graphs in Jupyter Notebook, #63
### Removed
- Scraped Items History graphs


## [0.3.1] (2019-04-12)
### Fixed
- Empty graphs due to lack of plotlyjs, #61


## [0.3.0] (2019-04-12)
### Fixed
- Big notebook size, replaced cufflinks with plotly and ipython, #39
### Changed
- *Fields Coverage* now is printed as a bar plot, #9
- *Fields Counts* renamed to *Coverage Difference* and results in 2 bar plots, #9, #51:
   * *Coverage from job stats fields counts* which reflects coverage for each field for both jobs
   * *Coverage difference more than 5%* which prints >5% difference between the coverages (was ratio difference before)
- *Compare Scraped Categories* renamed to *Category Coverage Difference* and results in 2 bar plots for each category, #52:
   * *Coverage for `field`* which reflects value counts (categories) coverage for the field for both jobs
   * *Coverage difference more than 10% for `field`* which shows >10% differences between the category coverages
- *Boolean Fields* plots *Coverage for boolean fields* graph which reflects normalized value counts for boolean fields for both jobs, #53
### Removed
- `cufflinks` dependency
- Deprecated `category_field` tag


## [2019.03.25]
### Added
- CHANGES.md
- new `arche.rules.duplicates.find_by()` to find duplicates by chosen columns
```
import arche
from arche.readers.items import JobItems
df = JobItems(0, "235801/1/15").df
arche.rules.duplicates.find_by(df, ["title", "category"]).show()
```
- `basic_json_schema().json()` prints a schema in JSON format
- `Result.show()` to print a rule result, e.g.
```
from arche.rules.garbage_symbols import garbage_symbols
from arche.readers.items import JobItems
items = JobItems(0, "235801/1/15")
garbage_symbols(items).show()
```
- notebooks to documentation
### Changed
- Tags rule returns unused tags, #2
- `basic_json_schema()` prints a schema as a python dict
### Deprecated
- `Arche().basic_json_schema()` deprecated in favor of `arche.basic_json_schema()`
### Removed
### Fixed
- `Arche().basic_json_schema()` not using `items_numbers` argument


## 2019.03.18
- Last release without CHANGES updates
