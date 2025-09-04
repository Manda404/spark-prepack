# spark-prepkit

`spark-prepkit` is a Python package designed to simplify preprocessing workflows for PySpark.  
It provides utilities to:

- Clean and standardize Spark DataFrames (trimming, casting, feature engineering).  
- Align multiple DataFrames into a consistent schema ("DataFrame pack").  
- Ensure reproducibility with unit tests and CI/CD integration (e.g. GitHub Actions).  

The goal is to make Spark preprocessing **testable, modular, and CI-friendly**, so data teams can build reliable pipelines from raw data to machine learning models.  

## Key Features
- 🚀 Easy integration with PySpark  
- 📦 `DataFramesPack` abstraction to handle pairs or groups of DataFrames  
- 🧪 Built-in pytest support for testing preprocessing logic  
- 🔄 CI/CD ready (tested with GitHub Actions + PySpark + Java 11)  
- ⚡ Lightweight, dependency-minimal design