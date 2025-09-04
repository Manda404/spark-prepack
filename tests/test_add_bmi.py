from spark_prepkit.transform import add_bmi


def test_add_bmi_creates_bmi_when_cols_exist(tiny_df):
    out = add_bmi(tiny_df, height_col="Height", weight_col="Weight")
    assert "BMI" in out.columns
    row = out.orderBy("Age").first()
    expected = row["Weight"] / (row["Height"] ** 2)
    assert abs(row["BMI"] - expected) < 1e-9


def test_add_bmi_noop_if_columns_missing(spark):
    df = spark.createDataFrame([(1.75,)], ["Height"])
    out = add_bmi(df, "Height", "Weight")
    assert "BMI" not in out.columns
