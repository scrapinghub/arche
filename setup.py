from setuptools import find_packages, setup


install_requires = (
    "pandas",
    "scrapinghub[msgpack]",
    "plotly",
    "genson",
    "boto3",
    "jsonschema[format]>=3.0.0",
    "fastjsonschema",
    "perfect-jsonschema",
    "colorama",
    "tqdm",
    "cufflinks",
)
dependency_links = ["https://github.com/ambv/black.git#egg=black"]


tests_require = ["pytest", "pytest-mock", "pytest-cov", "pytest-pythonpath"]


setup(
    name="arche",
    description="Analyze Scrapy Cloud data",
    author="Scrapinghub",
    license="MIT",
    url="http://github.com/scrapinghub/arche",
    platforms=["Any"],
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    zip_safe=True,
    install_requires=install_requires,
    classifiers=[
        "Development Status :: 4 - Beta",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3.7",
    ],
    python_requires=">=3.7",
    extras_require={
        "tests": tests_require,
        "pep8tests": ["flake8", "flake8-import-order", "flake8-bugbear", "black"],
        "docs": ["sphinx", "recommonmark"],
    },
    tests_require=tests_require,
)
