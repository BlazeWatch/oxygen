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
    'temp_readings_production_test',
    metadata,
    Column('id', Integer, autoincrement=True, primary_key=True),
    Column('day', Integer),
    Column('xy', Point),
    Column('temperature', Integer)
)

tweets_production = Table('tweets_production_test',
                          metadata,
                          Column('id', Integer, autoincrement=True, primary_key=True),
                          Column('day', Integer), Column('xy', Point), Column('score', Integer),
                          Column('content', String))

firealerts_production = Table('fire_alerts_production_test', metadata,
                              Column('id', Integer, autoincrement=True, primary_key=True),
                              Column('event_day', Integer), Column('notification_day', Integer),
                              Column('xy', Point))
ai_firealerts_production = Table('ai_fire_alerts_production_test', metadata,
                                 Column('id', Integer, autoincrement=True, primary_key=True),
                                 Column('event_day', Integer), Column('notification_day', Integer),
                                 Column('xy', Point))

metadata.create_all(conn)


async def main(station_name):
    try:
        load_dotenv()
        host = os.getenv("MEMPHIS_HOSTNAME")
        username = os.getenv("MEMPHIS_USERNAME")
        password = os.getenv("MEMPHIS_PASSWORD")
        account_id = os.getenv("MEMPHIS_ACCOUNT_ID")
        print(host, username, password, account_id)

        memphis = Memphis()
        await memphis.connect(host=host, username=username, password=password, account_id=account_id)
        print(f"Memphis actualized and listening to {station_name}!")
        consumer = await memphis.consumer(station_name=f"{station_name}", consumer_name=f"{station_name}-consumer-4",
                                          consumer_group="")

        while True:
            batch = await consumer.fetch()
            if batch is not None:
                for msg in batch:
                    serialized_record = msg.get_data()
                    print(serialized_record)
                    record = json.loads(serialized_record)

                    if "temperature" in record:
                        with conn.connect() as connection:
                            insert_statement = temp_readings_production.insert().values(
                                day=record["day"],
                                xy=func.point(record["geospatial_x"], record["geospatial_y"]),
                                temperature=record["temperature"]
                            )
                            print("Inserting temperature")
                            connection.execute(insert_statement)
                            connection.commit()
                    elif "tweet" in record:
                        with conn.connect() as connection:
                            insert_statement = tweets_production.insert().values(
                                day=record["day"],
                                xy=func.point(record["geospatial_x"], record["geospatial_y"]),
                                score=0, content=record["tweet"])
                            print("Inserting tweets")
                            connection.execute(insert_statement)
                            connection.commit()
                    elif "event_day" in record:
                        with conn.connect() as connection:
                            insert_statement = firealerts_production.insert().values(
                                event_day=record['event_day'],
                                xy=func.point(record["geospatial_x"], record["geospatial_y"]),
                                notification_day=record[
                                    'notification_day'],
                            )
                            print("Inserting firealerts")
                            connection.execute(insert_statement)
                            connection.commit()

    except (MemphisError, MemphisConnectError) as e:
        print(e)

    finally:
        await memphis.close()


def run_ingress(task:str):
    asyncio.run(main(task))


# You can call this function from your main.py file(should work)
if __name__ == "__main__":
    threads = []

    tasks = ["zakar-fire-alerts", "zakar-temperature-readings", "zakar-tweets"]

    for task in tasks:
        thread = threading.Thread(target=run_ingress, args=(task,))
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()
