from pyspark.sql import DataFrame, functions as F, types as T

YES_VALUES = {"yes", "y", "true", "1"}
NO_VALUES = {"no", "n", "false", "0"}


def trim_strings(df: DataFrame) -> DataFrame:
    """Trim all string columns."""
    out = df
    for f in df.schema.fields:
        if isinstance(f.dataType, T.StringType):
            out = out.withColumn(f.name, F.trim(F.col(f.name)))
    return out


def normalize_yes_no(df: DataFrame, cols: list[str]) -> DataFrame:
    """Convert various 'yes/no' tokens to booleans; unexpected tokens -> None."""

    def yn_to_bool(col):
        c = F.lower(F.col(col))
        return (
            F.when(c.isin(list(YES_VALUES)), F.lit(True))
            .when(c.isin(list(NO_VALUES)), F.lit(False))
            .otherwise(F.lit(None).cast("boolean"))
        )

    out = df
    for c in cols:
        if c in out.columns:
            out = out.withColumn(c, yn_to_bool(c))
    return out


def cast_numerics(df: DataFrame, numeric_cols: list[str]) -> DataFrame:
    """Cast selected columns to DoubleType if present."""
    out = df
    for c in numeric_cols:
        if c in out.columns:
            out = out.withColumn(c, F.col(c).cast(T.DoubleType()))
    return out


def add_bmi(
    df: DataFrame, height_col: str = "Height", weight_col: str = "Weight"
) -> DataFrame:
    """Add BMI = Weight / Height^2 when both columns exist."""
    if height_col in df.columns and weight_col in df.columns:
        return df.withColumn("BMI", F.col(weight_col) / (F.col(height_col) ** 2))
    return df


# (Facultatif) pipeline pour un test d’intégration léger
def preprocess_obesity(df: DataFrame) -> DataFrame:
    df1 = trim_strings(df)
    df2 = normalize_yes_no(
        df1, ["SCC", "SMOKE", "family_history_with_overweight", "FAVC"]
    )
    df3 = cast_numerics(
        df2, ["Age", "Height", "Weight", "FCVC", "NCP", "CH2O", "FAF", "TUE", "CALC"]
    )
    df4 = add_bmi(df3, "Height", "Weight")
    for c in ["CAEC", "MTRANS", "NObeyesdad", "Gender"]:
        if c in df4.columns:
            df4 = df4.withColumn(c, F.lower(F.regexp_replace(F.col(c), r"\s+", "_")))
    return df4
