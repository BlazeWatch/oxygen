
import asyncio
import json
import os
import threading
import numpy
from dotenv import load_dotenv
from memphis import Memphis, MemphisError, MemphisConnectError
from psycopg2.extensions import register_adapter, AsIs
from sqlalchemy import create_engine, Table, Column, Integer, String, MetaData, func
from sqlalchemy.sql.type_api import UserDefinedType


# hacky solution for numpy64
def addapt_numpy_float64(numpy_float64):
    return AsIs(numpy_float64)
 
 
def addapt_numpy_int64(numpy_int64):
    return AsIs(numpy_int64)
 
 
register_adapter(numpy.float64, addapt_numpy_float64)
register_adapter(numpy.int64, addapt_numpy_int64)
 
# Load env vars
load_dotenv()
 
conn = create_engine(
    f"postgresql://{os.getenv('PG_USER')}:{os.getenv('PG_PASSWORD')}@{os.getenv('PG_HOST')}/{os.getenv('PG_DBNAME')}"
)
 
 
class Point(UserDefinedType):
    def get_col_spec(self):
        return "POINT"
 
    def bind_processor(self, dialect):
        def process(value):
            return value
 
        return process
 
    def result_processor(self, dialect, coltype):
        def process(value):
            x, y = map(float, value[6:-1].split())
            return x, y
 
        return process
 
 
metadata = MetaData()
 
# This code is a mess.
temp_readings_production = Table(
    'temp_readings_production',
    metadata,
    Column('id', Integer, autoincrement=True, primary_key=True),
    Column('day', Integer),
    Column('xy', Point),
    Column('temperature', Integer)
)
 
tweets_production = Table('tweets_production',
                          metadata,
                          Column('id', Integer, autoincrement=True, primary_key=True),
                          Column('day', Integer), Column('xy', Point), Column('score', Integer),
                          Column('content', String))
 
firealerts_production = Table('fire_alerts_production', metadata,
                              Column('id', Integer, autoincrement=True, primary_key=True),
                              Column('event_day', Integer), Column('notification_day', Integer),
                              Column('xy', Point))
ai_firealerts_production = Table('ai_fire_alerts_production', metadata,
                                 Column('id', Integer, autoincrement=True, primary_key=True),
                                 Column('event_day', Integer), Column('notification_day', Integer),
                                 Column('xy', Point))
 
metadata.create_all(conn)

def insert(station, records):
    values_list = []
    if station == "zakar-tweets":
        insert_statement = tweets_production.insert()
        for record in records:
            values_list.append({
                'day': record["day"],
                'content': record["tweet"],
                'xy': func.point(record["geospatial_x"], record["geospatial_y"]),
            })
    elif station == "zakar-fire-alerts":
        insert_statement = firealerts_production.insert()
        for record in records:
            if record.get("event_day") is None:
                continue

            values_list.append({
                'event_day': record['event_day'],
                'notification_day': record['notification_day'],
                'xy': func.point(record["geospatial_x"], record["geospatial_y"]),
            })
    elif station == "zakar-temperature-readings":
        insert_statement = temp_readings_production.insert()
        for record in records:
            values_list.append({
                'day': record["day"],
                'temperature': record["temperature"],
                'xy': func.point(record["geospatial_x"], record["geospatial_y"]),
            })
    else:
        insert_statement = []
    if len(values_list) > 0:
        with conn.connect() as connection:
            connection.execute(insert_statement.values(values_list))
 
async def station_loop(station_name):
    try:
        load_dotenv()
        host = os.getenv("MEMPHIS_HOSTNAME")
        username = os.getenv("MEMPHIS_USERNAME")
        password = os.getenv("MEMPHIS_PASSWORD")
        account_id = os.getenv("MEMPHIS_ACCOUNT_ID")
 
        memphis = Memphis()
        await memphis.connect(host=host, username=username, password=password, account_id=account_id)
        print(f"Memphis initialized and listening to {station_name}!")
        consumer = await memphis.consumer(station_name=f"{station_name}", consumer_name=f"{station_name}-consumer",
                                          consumer_group="")
 
        while True:
            batch = await consumer.fetch(5000)

            if batch is not None:
                records = []
                for msg in batch:
                    try:
                        serialized_record = msg.get_data()
                        record = json.loads(serialized_record)
                        if record["geospatial_x"] is None:
                            # warp trolling
                            continue
                        records.append(record)
                    except Exception:
                        print(f"Failed parsing json in {station_name}")
                    finally:
                        await msg.ack()
                insert(station_name, records)
                if len(records) > 0:
                    print(f"Row from {station_name}: {len(records)}")
                else:
                    break



    except (MemphisError, MemphisConnectError) as e:
        print(e)
 
    finally:
        await memphis.close()
 
 
def run_ingress(task: str):
    asyncio.run(station_loop(task))
 
 
# You can call this function from your main.py file(should work)
def main():
    print("Starting Memphis Ingress")
    threads = []

    tasks = ["zakar-fire-alerts", "zakar-temperature-readings", "zakar-tweets"]
    # tasks = ["zakar-fire-alerts"]

    for task in tasks:
        thread = threading.Thread(target=run_ingress, args=(task,))
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()
