#!/usr/bin/env python
# coding: utf-8

import click
import pyarrow.parquet as pq
import pandas as pd
import requests
import tempfile
from sqlalchemy import create_engine
from tqdm.auto import tqdm

dtype = {
    "VendorID": "Int64",
    "passenger_count": "Int64",
    "trip_distance": "float64",
    "RatecodeID": "Int64",
    "store_and_fwd_flag": "string",
    "PULocationID": "Int64",
    "DOLocationID": "Int64",
    "payment_type": "Int64",
    "fare_amount": "float64",
    "extra": "float64",
    "mta_tax": "float64",
    "tip_amount": "float64",
    "tolls_amount": "float64",
    "improvement_surcharge": "float64",
    "total_amount": "float64",
    "congestion_surcharge": "float64"
}

dtype3 = {
    "LocationID": "Int64",
    "Borough": "string",
    "Zone": "string",
    "service_zone": "string",
}

parse_dates = [
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime"
]
@click.command()
@click.option('--pg-user', default='root', show_default=True, help='Postgres user name')
@click.option('--pg-password', default='root', show_default=True, help='Postgres password')
@click.option('--pg-host', default='localhost', show_default=True, help='Postgres host')
@click.option('--pg-port', default='5432', show_default=True, help='Postgres port')
@click.option('--pg-db', default='ny_taxi', show_default=True, help='Postgres database name')
@click.option('--year', default=2021, show_default=True, type=int, help='Year for the data file')
@click.option('--year2', default=2025, show_default=True, type=int, help='Year for the data file')
@click.option('--month', default=1, show_default=True, type=int, help='Month for the data file')
@click.option('--month2', default=11, show_default=True, type=int, help='Month for the data file')
@click.option('--target-table', default='yellow_taxi_data', show_default=True, help='Target Postgres table')
@click.option('--target-table2', default='green_taxi_data', show_default=True, help='Target Postgres table')
@click.option('--target-table3', default='taxi_zone_data', show_default=True, help='Target Postgres table')
@click.option('--chunksize', default=100000, show_default=True, type=int, help='CSV read chunksize')
def main(pg_user, pg_password, pg_host, pg_port, pg_db, year, year2, month, month2, target_table, target_table2, target_table3, chunksize):

    prefix = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/'
    url = f'{prefix}yellow_tripdata_{year}-{month:02d}.csv.gz'

    prefix2 = 'https://d37ci6vzurychx.cloudfront.net/trip-data/'
    url2 = f'{prefix2}green_tripdata_{year2}-{month2:02d}.parquet'

    prefix3 = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/'
    url3 = f'{prefix3}taxi_zone_lookup.csv'

    engine = create_engine(f'postgresql://{pg_user}:{pg_password}@{pg_host}:{pg_port}/{pg_db}')

    #read in yellow taxi data
    df_iter = pd.read_csv(
        url,
        dtype=dtype,
        parse_dates=parse_dates,
        iterator=True,
        chunksize=chunksize,
    )

    first_chunk = next(df_iter)

    first_chunk.head(0).to_sql(
        name=target_table,
        con=engine,
        if_exists="replace"
    )

    print("Table created")

    first_chunk.to_sql(
        name=target_table,
        con=engine,
        if_exists="append"
    )

    print("Inserted first chunk:", len(first_chunk))

    for df_chunk in tqdm(df_iter):
        df_chunk.to_sql(
            name=target_table,
            con=engine,
            if_exists="append"
        )
        print("Inserted chunk:", len(df_chunk))

    #read in green taxi data
    # Download the file
    response = requests.get(url2)
    response.raise_for_status()

    # Save to a temporary local file
    with tempfile.NamedTemporaryFile(delete=False, suffix=".parquet") as tmp_file:
        tmp_file.write(response.content)
        local_path = tmp_file.name

    parquet_file = pq.ParquetFile(tmp_file.name)
    batch_size = 10000  # Number of rows per batch

    first_batch = next(parquet_file.iter_batches(batch_size=batch_size))
    first_df = first_batch.to_pandas()
    first_df.head(0).to_sql(
    name=target_table2,
    con=engine,
    if_exists="replace"
    )

    print("Table created")

    for batch in tqdm(parquet_file.iter_batches(batch_size=batch_size)):

    # Convert the pyarrow RecordBatch to a pandas DataFrame
        df_batch = batch.to_pandas()

        df_batch.to_sql(
        name=target_table2,
        con=engine,
        if_exists="append"
    )

    print(f"Processed batch, {df_batch.shape}")

    #read in taxi zone data
    df_iter3 = pd.read_csv(
    url3,
    dtype=dtype3,
    iterator=True,
    chunksize=chunksize,
    )

    first_chunk3 = next(df_iter3)

    first_chunk3.head(0).to_sql(
        name=target_table3,
        con=engine,
        if_exists="replace"
    )

    print("Table created")

    first_chunk3.to_sql(
        name=target_table3,
        con=engine,
        if_exists="append"
    )

    print("Inserted first chunk:", len(first_chunk3))

    for df_chunk3 in tqdm(df_iter3):
        df_chunk3.to_sql(
            name=target_table3,
            con=engine,
            if_exists="append"
        )
        print("Inserted chunk:", len(df_chunk3))

if __name__ == "__main__":
    main()




