from spark_prepkit.transform import preprocess_obesity

def test_preprocess_basic(spark):
    rows = [
        (21.0, "Female", 1.62, 64.0, "no", 2.0, 3.0, "no", "no", 2.0, "no", 2.0, 1.0, "Sometimes", 0.0, "Public_Transportation", "Normal_Weight"),
        (27.0, "Male",   1.80, 87.0, "Frequently", 3.0, 3.0, "no", "no", 2.0, "no", 2.0, 0.0, "Sometimes", 2.0, "Walking", "Overweight_Level_I"),
    ]
    cols = ["Age","Gender","Height","Weight","FAVC","FCVC","NCP","SCC","SMOKE","CH2O","family_history_with_overweight","FAF","TUE","CAEC","CALC","MTRANS","NObeyesdad"]
    df = spark.createDataFrame(rows, cols)

    out = preprocess_obesity(df)

    # Feature engineering
    assert "BMI" in out.columns

    # yes/no -> bool
    row = out.orderBy("Age").first()
    assert isinstance(row["SCC"], (bool, type(None)))
    assert isinstance(row["SMOKE"], (bool, type(None)))
    assert isinstance(row["family_history_with_overweight"], (bool, type(None)))

    # types numériques présents + BMI
    for c in ["Age","Height","Weight","FCVC","NCP","CH2O","FAF","TUE","CALC","BMI"]:
        assert c in out.columns

    # normalisation de catégories
    assert row["NObeyesdad"] in {"normal_weight", "overweight_level_i", "overweight_level_ii"}
