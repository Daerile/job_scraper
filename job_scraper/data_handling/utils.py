import polars as pl
from polars.dataframe import DataFrame


def clean_null_values(df: pl.DataFrame) -> pl.DataFrame:
    pass
    # str_cols = [col for col, dtype in zip(df.columns, df.dtypes) if dtype == pl.Utf8]
    
    # exprs = [
    #     pl.col(col).replace_strict(
    #         old=['', 'None', 'Null'],
    #         new=[None],
    #         default=
    #         return_dtype=df[col].dtype
    #     )
    #     for col in str_cols
    # ]
    
    # return df.with_columns(exprs)