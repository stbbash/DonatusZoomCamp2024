import pandas as pd
from sqlalchemy import create_engine, MetaData, Table
from time import time
import argparse, os

def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    #url = params.url
    csv_name = "green_tripdata_2019-01.csv"
    
    # DOWNLOAD THE CSV
    # print(pd.io.sql.get_schema(df, name=table_name, con=engine))
    #os.systems(f"wget {url} -o {csv_name}")
    
    print(f"postgresql://{user}:{password}@{host}:{port}/{db}")
    engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{db}")


    df.head(n=0).to_sql(name=table_name, con=engine, if_exists="replace")

    df_iter = pd.read_csv(csv_name, iterator=True, chunksize=10000, low_memory=False)

    df = next(df_iter)

    while True:
        t_start = time()
        df = next(df_iter)

        df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
        df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)

        df.to_sql(name=table_name, con=engine, if_exists="append")

        t_end = time()
        #print(f"inserted another chunck..., took {t_end - t_start}")
        print("inserted another chunck..., took %.3f seconds" % (t_end - t_start))
        
        
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description ='Ingest CSV dat to postgres')

    # user
    # password
    # host
    # port
    # database
    # table name
    # url of csv

    parser.add_argument('--user', help ='user name for postgres')
    parser.add_argument('--password', help ='password for postgres')
    parser.add_argument('--host', help ='host for postgres')
    parser.add_argument('--port', help ='port for postgres')
    parser.add_argument('--db', help ='database name for postgres')
    parser.add_argument('--table_name', help ='name of the table where we will write the results to')
    #parser.add_argument('--ulr', help ='url of the csv file')

    args = parser.parse_args()

    main(args)




"""
# Reflect existing tables

metadata = MetaData()
metadata.reflect(bind=engine)

#### Drop all tables.

for table in reversed(metadata.sorted_tables):
    table.drop(engine)

#### Confirm that all tables have been dropped

print("All tables dropped from the database.")
"""