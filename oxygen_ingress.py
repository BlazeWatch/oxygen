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

#Load env vars
load_dotenv()

conn = create_engine(
    f"postgresql://{os.getenv('PG_USER')}:{os.getenv('PG_PASSWORD')}@{os.getenv('PG_HOST')}/{os.getenv('PG_DBNAME')}"
).connect()




async def main(station_name):
    try:
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
        #This code is a mess.
        temp_readings_duplicate = Table(
            'temp_readings_duplicate'
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
                    print(record)
                    if "temperature" in record:
                        insert_statement = temp_readings_duplicate.insert().values(
                        id=last_day_record+1,
                        day=record[0],
                        xy=f'{record[1]},{record[2]}',
                        temperature=record[3]
                    )
                    conn.execute(insert_statement) #Please work :3
                    conn.commit()
                    last_id_record+=1
                conn.close()
                        
            
        
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

    
    