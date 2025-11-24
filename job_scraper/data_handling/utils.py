"""Utils for the code"""
import polars as pl
from polars.dataframe import DataFrame


def clean_null_values(df: DataFrame) -> DataFrame:
    """Clean null values in columns"""
    column_type_mapping = dict(zip(df.columns, df.dtypes))

    for column in df.columns:
        if column_type_mapping[column] == pl.String:
            df = df.with_columns(
                pl.col(column).replace(
                    ["", "None", "Null", " ", "none", "null"],
                    None
                )
            )

    return df