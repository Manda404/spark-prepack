from spark_prepkit.transform import trim_strings


def test_trim_strings_only_trims_string_columns(tiny_df):
    out = trim_strings(tiny_df)
    r = out.orderBy("Age").first()
    assert r["Gender"] == "Female"
    assert r["CAEC"] == "Sometimes"
    assert r["MTRANS"] == "Public_Transportation"
    assert r["NObeyesdad"] == "Normal_Weight"
    assert r["Age"] == 21.0  # numérique inchangé
