from spark_prepkit.transform import preprocess_obesity


def test_preprocess_smoke(tiny_df):
    out = preprocess_obesity(tiny_df)
    assert "BMI" in out.columns
