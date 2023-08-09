import asyncio

import numpy
from dotenv import load_dotenv
from psycopg2.extensions import register_adapter, AsIs

import do_crazy_ai_things
import oxygen_ingress

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
        oxygen_ingress.main(),
        do_crazy_ai_things.main()
    )

while True:
    asyncio.run(run_all())
