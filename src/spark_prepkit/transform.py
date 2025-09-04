from pyspark.sql import DataFrame, functions as F, types as T

YES_VALUES = {"yes", "y", "true", "1"}
NO_VALUES  = {"no", "n", "false", "0"}

def _trim_strings(df: DataFrame) -> DataFrame:
    out = df
    for f in df.schema.fields:
        if isinstance(f.dataType, T.StringType):
            out = out.withColumn(f.name, F.trim(F.col(f.name)))
    return out

def _normalize_yes_no(df: DataFrame, cols: list[str]) -> DataFrame:
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

def _cast_numerics(df: DataFrame, numeric_cols: list[str]) -> DataFrame:
    out = df
    for c in numeric_cols:
        if c in out.columns:
            out = out.withColumn(c, F.col(c).cast(T.DoubleType()))
    return out

def _add_bmi(df: DataFrame, height_col: str = "Height", weight_col: str = "Weight") -> DataFrame:
    if height_col in df.columns and weight_col in df.columns:
        return df.withColumn("BMI", F.col(weight_col) / (F.col(height_col) ** 2))
    return df

def preprocess_obesity(df: DataFrame) -> DataFrame:
    """
    Basic, testable pipeline for an obesity-like dataset:
    - trim strings
    - yes/no normalization for selected columns
    - numeric casts
    - BMI feature
    - normalize some categorical text to lower/underscored
    """
    df1 = _trim_strings(df)

    yes_no_cols = ["SCC", "SMOKE", "family_history_with_overweight", "FAVC"]
    df2 = _normalize_yes_no(df1, yes_no_cols)

    numeric_cols = ["Age", "Height", "Weight", "FCVC", "NCP", "CH2O", "FAF", "TUE", "CALC"]
    df3 = _cast_numerics(df2, numeric_cols)

    df4 = _add_bmi(df3, height_col="Height", weight_col="Weight")

    def norm_cat(col):
        return F.lower(F.regexp_replace(F.col(col), r"\s+", "_"))

    for c in ["CAEC", "MTRANS", "NObeyesdad", "Gender"]:
        if c in df4.columns:
            df4 = df4.withColumn(c, norm_cat(c))

    return df4
