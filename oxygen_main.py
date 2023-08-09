import numpy
from dotenv import load_dotenv
from psycopg2.extensions import register_adapter, AsIs
from multiprocessing import Process
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
def run_all():
    p1 = Process(target=oxygen_ingress.main)
    p2 = Process(target=do_crazy_ai_things.main)

    p1.start()
    p2.start()

    p1.join()
    p2.join()

while True:
    run_all()

