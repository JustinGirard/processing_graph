from pathlib import Path
from setuptools import setup

import os
import sys
import subprocess


long_description = Path("README.md").read_text(encoding="utf-8")

setup(
    name="processing_graph",
    version="0.1.0",
    description="The Decelium Graph Processor",
    url="https://github.com/JustinGirard/processing_graph.git",
    author="Justin Girard",
    author_email="justingirard@decelium.com",
    packages=["processing_graph"],
    entry_points={},
    long_description=long_description,
    long_description_content_type="text/markdown",
    install_requires=[
        "nodejobs>=0.3.0",
    ],
    python_requires=">=3.7",
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: Other/Proprietary License",
    ],
)
