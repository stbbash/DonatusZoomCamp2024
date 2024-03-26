
import pandas as pd
from sqlalchemy import create_engine
from time import time


df = pd.read_csv("yellow_tripdata_2021-07.csv", nrows=100)

print(pd.io.sql.get_schema(df, name="yellow_taxi_data"))


df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

print(pd.io.sql.get_schema(df, name="yellow_taxi_data"))

engine = create_engine("postgresql://root:root@localhost/ny_taxi")

print(pd.io.sql.get_schema(df, name="yellow_taxi_data", con=engine))

df_iter = pd.read_csv("yellow_tripdata_2021-07.csv", iterator=True, chunksize=100000)

df = next(df_iter)

df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

df.head(n=0).to_sql(name="yellow_taxi_data", con=engine, if_exists="replace")

get_ipython().run_line_magic('time', 'df.to_sql(name="green_taxi_data", con=engine, if_exists="append")')



while True:
    t_start = time()
    df = next(df_iter)

    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

    df.to_sql(name="yellow_taxi_data", con=engine, if_exists="append")

    t_end = time()
    #print(f"inserted another chunck..., took {t_end - t_start}")
    print("inserted another chunck..., took %.3f seconds" % (t_end - t_start))
    

