from dotenv import load_dotenv
import polars as pl
import os

def main():
    load_dotenv('.env')
    POSTGRES_USERNAME = os.environ['POSTGRES_USERNAME']
    POSTGRES_PASSWORD = os.environ['POSTGRES_PASSWORD']
    
    df = pl.read_json(os.path.abspath('./scraper/output.json'))

    uri = f"postgresql://{POSTGRES_USERNAME}:{POSTGRES_PASSWORD}@localhost:5432/job_scraper"

    df.write_database(table_name = "profession_data_bronze", connection=uri)

if __name__ == '__main__':
    main()