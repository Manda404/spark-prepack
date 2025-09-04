from spark_prepkit.transform import normalize_yes_no

def test_normalize_yes_no_converts_to_booleans(tiny_df):
    out = normalize_yes_no(tiny_df, ["SCC","SMOKE","family_history_with_overweight","FAVC"])
    r1 = out.orderBy("Age").first()
    assert r1["SCC"] is False
    assert r1["SMOKE"] is False
    assert r1["family_history_with_overweight"] is False
    assert r1["FAVC"] is False  # "no" -> False pour la 1ère ligne

def test_normalize_yes_no_handles_unexpected_tokens(spark):
    df = spark.createDataFrame([("MAYBE",), ("YES",), ("0",), (None,)], ["SCC"])
    out = normalize_yes_no(df, ["SCC"]).orderBy("SCC")
    vals = [row["SCC"] for row in out.collect()]
    assert set(vals) == {True, False, None}
