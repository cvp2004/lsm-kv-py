"""
Setup script for LSM KV Store.
"""
from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name="lsm-kv-store",
    version="1.0.0",
    author="LSM KV Store Contributors",
    description="A simple LSM tree-based key-value store implementation",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/yourusername/lsm-kv-py",
    packages=find_packages(),
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "Topic :: Database",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
    ],
    python_requires=">=3.7",
    install_requires=[
        "skiplistcollections>=0.0.6",
    ],
    entry_points={
        "console_scripts": [
            "lsmkv=scripts.cli:main",
        ],
    },
    include_package_data=True,
)
