from spark_prepkit.transform import cast_numerics

def test_cast_numerics_casts_selected_columns(tiny_df):
    out = cast_numerics(tiny_df, ["Age","Height","Weight","FCVC","NCP","CH2O","FAF","TUE","CALC"])
    dtypes = dict(out.dtypes)
    for c in ["Age","Height","Weight","FCVC","NCP","CH2O","FAF","TUE","CALC"]:
        assert dtypes[c] in ("double", "float")

def test_cast_numerics_ignores_missing_columns(tiny_df):
    out = cast_numerics(tiny_df, ["NotAColumn"])
    assert "NotAColumn" not in out.columns
