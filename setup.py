import os
from pathlib import Path

from setuptools import find_packages, setup


def read_long_description() -> str:
    readme_path = Path(__file__).parent / "README.md"
    with readme_path.open(encoding="utf-8") as fh:
        return fh.read()


setup(
    name="daplug-ddb",
    version=os.getenv("CIRCLE_TAG", "0.1.0"),
    url="https://github.com/dual/daplug-ddb",
    author="Paul Cruse III",
    author_email="paulcruse3@gmail.com",
    description="Schema-aware DynamoDB adapter with prefixing, SNS hooks, and batching.",
    long_description=read_long_description(),
    long_description_content_type="text/markdown",
    packages=find_packages(),
    python_requires=">=3.9",
    install_requires=[
        "boto3==1.40.69; python_version >= '3.9'",
        "botocore==1.40.69; python_version >= '3.9'",
        "daplug-core==1.0.0b5; python_version >= '3.9'",
        "jmespath==1.0.1; python_version >= '3.7'",
        "jsonref==1.1.0; python_version >= '3.7'",
        "python-dateutil==2.9.0.post0; python_version >= '2.7' and python_version not in '3.0, 3.1, 3.2'",
        "pyyaml==6.0.3; python_version >= '3.8'",
        "s3transfer==0.14.0; python_version >= '3.9'",
        "simplejson==3.20.2; python_version >= '2.5' and python_version not in '3.0, 3.1, 3.2'",
        "six==1.17.0; python_version >= '2.7' and python_version not in '3.0, 3.1, 3.2'",
        "urllib3==1.26.20; python_version >= '2.7' and python_version not in '3.0, 3.1, 3.2, 3.3, 3.4, 3.5'"
    ],
    keywords=[
        "dynamodb",
        "aws",
        "ddb",
        "normalizer",
        "schema",
        "sns",
        "event-driven",
        "database",
        "adapter",
        "python-library",
    ],
    project_urls={
        "Homepage": "https://github.com/dual/daplug-ddb",
        "Documentation": "https://github.com/dual/daplug-ddb#readme",
        "Source Code": "https://github.com/dual/daplug-ddb",
        "Bug Reports": "https://github.com/dual/daplug-ddb/issues",
        "CI/CD": "https://circleci.com/gh/dual/daplug-ddb",
    },
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Environment :: Web Environment",
        "Environment :: Console",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: OS Independent",
        "Topic :: Database :: Front-Ends",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3 :: Only",
    ],
    license="Apache License 2.0",
    platforms=["any"],
    zip_safe=False,
)
