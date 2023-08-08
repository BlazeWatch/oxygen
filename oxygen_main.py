#import oxygen_ingress
#import oxygen_egress
import asyncio
import os
import json
import time
import numpy
from dotenv import load_dotenv
from sqlalchemy import create_engine, text as sql_text
from sqlalchemy import create_engine, text, Table, Column, Integer, String, MetaData
from psycopg2.extensions import register_adapter, AsIs
from sqlalchemy_aio import ASYNCIO_STRATEGY

load_dotenv()

#hacky solution for numpy64
def addapt_numpy_float64(numpy_float64):
    return AsIs(numpy_float64)
def addapt_numpy_int64(numpy_int64):
    return AsIs(numpy_int64)
register_adapter(numpy.float64, addapt_numpy_float64)
register_adapter(numpy.int64, addapt_numpy_int64)


async def monitor(currentDay):
    engine = create_engine(
        f"postgresql://{os.getenv('PG_USER')}:{os.getenv('PG_PASSWORD')}@{os.getenv('PG_HOST')}/{os.getenv('PG_DBNAME')}",
        strategy=ASYNCIO_STRATEGY
    )
    conn = await engine.connect()
    
    records = await conn.execute(text('SELECT * FROM temp_readings WHERE day >= :startDay AND day <= :endDay ORDER BY day ASC'), {'startDay': currentDay - 7, 'endDay': currentDay})
    records_list = [dict(record) for record in await records.fetchall()]
    #print(records_list)
    #print(len(records_list))
    #time.sleep(8)
    currentDay += 7
    await asyncio.sleep(1)
    await monitor(currentDay)
    await conn.close()

currentDay = 7
asyncio.run(monitor(currentDay))


#Run all.
'''    
async def run_all():
    await asyncio.gather(
        oxygen_ingress.run_ingress(),
        monitor(currentDay)
    )
'''

#Runs Ingress + Egress + Monitor Function
#asyncio.run(run_all())