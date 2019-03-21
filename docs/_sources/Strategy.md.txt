# Strategy

The purpose of this page is to highlight the key points which shape the library, targeting both:

1. Stakeholders (e.g. QA teams and developers), to help making it clear why things are done in such way

2. Maintainers - to not deviate from the main purpose

While the library is an ongoing job and a lot has been changed the underlying principles should stay more or less the same.

By no means the information listed here should limit any work for the sake of the rules or processes. Instead it should be an additional instrument along with stakeholders' needs to evolve the framework.

At the time **pandas**, **Jupyter** and **Python** are the framework cornerstones. We can use other tools, but the choice was made because they nicely align with the library purpose.

[pandas](https://pandas.pydata.org/) is an open source, BSD-licensed library providing high-performance, easy-to-use data structures and data analysis tools for the Python programming language.

## Main purpose

Help people to test homogeneous data. It could be done by analyzing homogeneous data and flagging problem areas to help users detect them. Homogeneous means that all units of data (datums) share common property (or properties).

## Short-term

Up to 6 months, the current [releases](https://github.com/scrapinghub/arche/releases) reflect the short-term strategy

## Long-term

6 months and more

* **Choose tools wisely.** Any libraries and even the language should be chosen because it’s one of the most suitable tools for a given task keeping in mind the following points, not just because it works.
* **Add unit tests.** They are cheap and bring efficient results. All new features should be covered, old features should be covered and refactored if needed, in other words test coverage should strive for 100%.
* **Manage data of any sane size with size growth in mind.** I.e. the framework should allow to test required data in a sensible time.
* **Support the most common input data sources.** A good reference is [pandas](https://pandas.pydata.org/) which allows a wide number of way to read and transform data to DataFrame with simply API `pd.DataFrame()`.
* **UX. The framework is used by people, so it should be simple.** Take pains to keep it backwards compatible with at least the previous version, deprecate code noticeably, keep it simple. Another point is integration, for example in Scrapy Cloud.
* **Do not depend on custom rules, e.g. JSON schema.** Data should be enough by itself to verify it, custom rules should clarify verification and be optional.
