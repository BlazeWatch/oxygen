import asyncio
import os
import numpy
import oxygen_ingress
import do_crazy_ai_things
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from psycopg2.extensions import register_adapter, AsIs
from sqlalchemy_aio import ASYNCIO_STRATEGY

load_dotenv()


# hacky solution for numpy64
def addapt_numpy_float64(numpy_float64):
    return AsIs(numpy_float64)


def addapt_numpy_int64(numpy_int64):
    return AsIs(numpy_int64)


register_adapter(numpy.float64, addapt_numpy_float64)
register_adapter(numpy.int64, addapt_numpy_int64)


# Run all.   
async def run_all():
    await asyncio.gather(
        oxygen_ingress.station_loop(),
        do_crazy_ai_things.main()
    )

while True:
    asyncio.run(run_all())
