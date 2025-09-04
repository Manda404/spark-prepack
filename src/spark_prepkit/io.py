from pathlib import Path
from pyspark.sql import DataFrame, SparkSession

def read_csv(spark: SparkSession, path: str | Path, header: bool = True, delimiter: str = ",") -> DataFrame:
    """
    Read a CSV into a Spark DataFrame.
    For production, consider providing an explicit schema instead of inferSchema.
    """
    return (
        spark.read
        .option("header", str(header).lower())
        .option("inferSchema", "true")
        .option("delimiter", delimiter)
        .csv(str(path))
    )
