import re
from os import path

from setuptools import setup

# read the contents of README file
this_directory = path.abspath(path.dirname(__file__))
with open(path.join(this_directory, "README.md"), encoding="utf-8") as f:
    long_description = f.read()

version_str = "v1.0.0"


setup(
    name="firefly-automate",
    version=version_str,
    description="Automate Firefly management.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    author="Tin Lai (@soraxas)",
    author_email="oscar@tinyiu.com",
    license="MIT",
    url="https://github.com/soraxas/firefly-automate",
    keywords="tui",
    python_requires=">=3.6",
    packages=[
        "firefly_automate",
        "firefly_automate.rules",
    ],
    install_requires=[
        "Firefly-III-API-Client==1.5.6",
        "pandas>=1.3.1",
        "pyyaml>=5.4.1",
        "schema>=0.7.4",
        "tqdm~=4.62.0",
        "python-dateutil~=2.8.2",
        "humanize~=3.11.0",
        "argcomplete>=1.12.3",
        "tabulate",
    ],
    entry_points={
        "console_scripts": [
            "firefly-automate=firefly_automate.run:main",
            # "firefly-automate=firefly_automate.run_transform_transactions:main",
            "firefly-import-csv=firefly_automate.run_import_csv:main",
        ]
    },
    classifiers=[
        "Environment :: Console",
        "Framework :: Matplotlib",
        "Intended Audience :: Developers",
        "Intended Audience :: End Users/Desktop",
        "Intended Audience :: System Administrators",
        "License :: OSI Approved :: MIT License",
        "Operating System :: MacOS",
        "Operating System :: POSIX",
        "Operating System :: Unix",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Topic :: Desktop Environment",
        "Topic :: Terminals",
        "Topic :: Utilities",
    ],
)
