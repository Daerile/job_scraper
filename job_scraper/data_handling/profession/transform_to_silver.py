import polars as pl
import os
import job_scraper.data_handling.utils as utils
from polars.dataframe import DataFrame
from dotenv import load_dotenv

def clean_columns(df: DataFrame) -> DataFrame:
    only_needed_columns_df = df.select([
            "link",
            "prof_name",
            "item_brand",
            "category3",
            "category4",
            "category6",
            "variant",
        ]
    )
    renamed_columns_df = only_needed_columns_df.rename({
            "link": "job_url",
            "prof_name": "job_title",
            "item_brand": "company_name",
            "category3": "job_type",
            "category4": "experience",
            "category6": "job_location",
            "variant": "salary_public",
        }
    )

    result_df = renamed_columns_df.with_columns(
        pl.col("salary_public").replace_strict(
            {"salary publicised": True, "salary confidential": False}
        )
    )

    return result_df

def numeric_experience(df: DataFrame) -> DataFrame:
    possible_experiences = {
        "1-3 years experience": (1, 3),
        ">10 years experience": (10, None),
        "3-5 years experience": (3, 5),
        "5-10 years experience": (5, 10),
        "Career starter/freshly graduated": (0, None),
        "professional experience is not required": (0, None)
    }

    df = df.with_columns([
        pl.col("experience").replace(
            old=list(possible_experiences.keys()),
            new=list(map(lambda x: x[0],list(possible_experiences.values())))
        ).alias("experience_low"),

        pl.col("experience").replace(
            old=list(possible_experiences.keys()),
            new=list(map(lambda x: x[1],list(possible_experiences.values())))
        ).alias("experience_high"),
    ])

    df = df.drop('experience')
    df = df.cast({
        'experience_low': pl.Int16,
        'experience_high': pl.Int16
    })

    return df



def main():
    load_dotenv(".env")
    POSTGRES_USERNAME: str = os.environ["POSTGRES_USERNAME"]
    POSTGRES_PASSWORD: str = os.environ["POSTGRES_PASSWORD"]

    uri: str = (
        f"postgresql://{POSTGRES_USERNAME}:{POSTGRES_PASSWORD}@localhost:5432/job_scraper"
    )
    query: str = "SELECT * FROM profession_data_bronze"
    bronze_df: DataFrame = pl.read_database_uri(query, uri)

    clean_columns_df = clean_columns(bronze_df)

    clean_df = utils.clean_null_values(clean_columns_df)

    experience_fixed_df = numeric_experience(clean_df)
    
    experience_fixed_df.write_database(
        table_name="profession_data_silver", connection=uri, if_table_exists="replace"
    )


if __name__ == "__main__":
    main()
