"""
Setup script for spark-ai-agents Python package.

Install with: pip install -e .
"""

from setuptools import setup, find_packages

setup(
    name="spark-ai-agents",
    version="0.1.0",
    author="Apache Spark AI Agents Contributors",
    author_email="dev@spark.apache.org",
    description="PySpark integration for distributed AI agent execution",
    url="https://github.com/apache/spark/spark-ai-agents",
    packages=find_packages(),
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "License :: OSI Approved :: Apache Software License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
    ],
    python_requires=">=3.8",
    install_requires=[
        "pyspark>=3.0.0",
        "requests>=2.25.0",
    ],
    extras_require={
        "dev": [
            "pytest>=7.0.0",
            "pytest-cov>=3.0.0",
            "black>=22.0.0",
            "flake8>=4.0.0",
        ],
        "grpc": [
            "grpcio>=1.50.0",
            "grpcio-tools>=1.50.0",
        ],
    },
)
