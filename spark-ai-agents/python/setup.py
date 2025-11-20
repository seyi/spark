"""
Setup script for Spark AI Agents Python package
"""

from setuptools import setup, find_packages
from pathlib import Path

# Read README for long description
readme_file = Path(__file__).parent.parent / "README.md"
long_description = readme_file.read_text() if readme_file.exists() else ""

setup(
    name="spark-agents",
    version="0.1.0",
    author="Apache Spark Community",
    author_email="dev@spark.apache.org",
    description="Distributed AI agent framework inspired by Apache Spark",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/apache/spark/tree/master/spark-ai-agents",
    packages=find_packages(),
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "Intended Audience :: Science/Research",
        "License :: OSI Approved :: Apache Software License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Topic :: Scientific/Engineering :: Artificial Intelligence",
        "Topic :: Software Development :: Libraries :: Python Modules",
    ],
    python_requires=">=3.8",
    install_requires=[
        # Minimal dependencies - only standard library used
    ],
    extras_require={
        "dev": [
            "pytest>=7.0.0",
            "pytest-cov>=4.0.0",
            "black>=22.0.0",
            "isort>=5.0.0",
            "mypy>=0.990",
            "pylint>=2.15.0",
        ],
    },
    package_data={
        "spark_agents": ["py.typed"],
    },
    include_package_data=True,
    zip_safe=False,
)
