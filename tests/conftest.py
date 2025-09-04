import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    spark = (
        SparkSession.builder.master("local[*]")
        .appName("unit-tests")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .getOrCreate()
    )
    yield spark
    spark.stop()


@pytest.fixture
def tiny_obesity_rows():
    # Mini dataset stable pour tester chaque transfo isolément
    return [
        (
            21.0,
            "  Female  ",
            1.62,
            64.0,
            "no",
            2.0,
            3.0,
            "no",
            "no",
            2.0,
            "no",
            2.0,
            1.0,
            " Sometimes ",
            0.0,
            " Public_Transportation ",
            " Normal_Weight ",
        ),
        (
            27.0,
            "Male",
            1.80,
            87.0,
            "Frequently",
            3.0,
            3.0,
            "no",
            "no",
            2.0,
            "no",
            2.0,
            0.0,
            "Sometimes",
            2.0,
            "Walking",
            "Overweight_Level_I",
        ),
    ]


@pytest.fixture
def tiny_obesity_cols():
    return [
        "Age",
        "Gender",
        "Height",
        "Weight",
        "FAVC",
        "FCVC",
        "NCP",
        "SCC",
        "SMOKE",
        "CH2O",
        "family_history_with_overweight",
        "FAF",
        "TUE",
        "CAEC",
        "CALC",
        "MTRANS",
        "NObeyesdad",
    ]


@pytest.fixture
def tiny_df(spark, tiny_obesity_rows, tiny_obesity_cols):
    return spark.createDataFrame(tiny_obesity_rows, tiny_obesity_cols)
