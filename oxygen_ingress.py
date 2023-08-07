import argparse
import asyncio
import json
import os
import dotenv
import multiprocessing
import pandas as pd
from multiprocessing import Pool
from dotenv import load_dotenv
from memphis import Memphis, Headers, MemphisError, MemphisConnectError, MemphisHeaderError, MemphisSchemaError
from sqlalchemy import create_engine, text as sql_text
from sqlalchemy import create_engine, text, Table, Column, Integer, String, MetaData
from psycopg2.extensions import register_adapter, AsIs
import numpy
def addapt_numpy_float64(numpy_float64):
    return AsIs(numpy_float64)
def addapt_numpy_int64(numpy_int64):
    return AsIs(numpy_int64)
register_adapter(numpy.float64, addapt_numpy_float64)
register_adapter(numpy.int64, addapt_numpy_int64)

#Load env vars
load_dotenv()

conn = create_engine(
    f"postgresql://{os.getenv('PG_USER')}:{os.getenv('PG_PASSWORD')}@{os.getenv('PG_HOST')}/{os.getenv('PG_DBNAME')}"
)




async def main(station_name):
    try:
        load_dotenv()
        host = os.getenv("MEMPHIS_HOSTNAME")  
        username = os.getenv("MEMPHIS_USERNAME")
        password = os.getenv("MEMPHIS_PASSWORD")
        account_id = os.getenv("MEMPHIS_ACCOUNT_ID")
        #Get last data record
        last_day_record = pd.read_sql_query(sql_text("select * from temp_readings order by id desc limit 1;"), conn)['day'].values[0]

        last_id_record = pd.read_sql_query(sql_text("select * from temp_readings order by id desc limit 1;"), conn)['id'].values[0]
        memphis = Memphis()
        await memphis.connect(host=host, username=username, password=password, account_id=account_id)
        print(f"Memphis actualized and listening to {station_name}!")
        consumer = await memphis.consumer(station_name=f"{station_name}", consumer_name=f"{station_name}-consumer", consumer_group="")
        metadata = MetaData()
        #This code is a mess.
        temp_readings_duplicate = Table(
            'temp_readings_duplicate',
            metadata,
            Column('id', Integer, primary_key=True),
            Column('day', Integer),
            Column('xy', String),
            Column('temperature', Integer)
        )
        while True:
            batch = await consumer.fetch()
            if batch is not None:
                for msg in batch:
                    serialized_record = msg.get_data()
                    record = json.loads(serialized_record)
                    if "temperature" in record:
                        with conn.connect() as connection:
                            insert_statement = temp_readings_duplicate.insert().values(
                                id=last_id_record+1,
                                day=record["day"],
                                xy=f'{record["geospatial_x"]},{record["geospatial_y"]}',
                                temperature=record["temperature"]
                            )
                            connection.execute(insert_statement)
                    print(f"Record {last_id_record} inserted!")
                    last_id_record+=1
                        
                        
            
        
    except (MemphisError, MemphisConnectError) as e:
        print(e)
        
    finally:
        await memphis.close()
    

async def run_ingress():
    await asyncio.gather(
        main("zakar-fire-alerts"),
        main("zakar-temperature-readings")
    )

#You can call this function from your main.py file(should work)
if __name__ == "__main__":
    asyncio.run(run_ingress())

    
    