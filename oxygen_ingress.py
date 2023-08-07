import argparse
import asyncio
import json
import os
import dotenv
import multiprocessing
from multiprocessing import Pool
from dotenv import load_dotenv
from memphis import Memphis, Headers, MemphisError, MemphisConnectError, MemphisHeaderError, MemphisSchemaError

#Load env vars
load_dotenv()


async def main(station_name):
    try:
        host = os.getenv("MEMPHIS_HOSTNAME")  
        username = os.getenv("MEMPHIS_USERNAME")
        password = os.getenv("MEMPHIS_PASSWORD")
        account_id = os.getenv("MEMPHIS_ACCOUNT_ID")
        memphis = Memphis()
        await memphis.connect(host=host, username=username, password=password, account_id=account_id)
        print(f"Memphis actualized and listening to {station_name}!")
        consumer = await memphis.consumer(station_name=f"{station_name}", consumer_name=f"{station_name}-consumer", consumer_group="")
        
        while True:
            batch = await consumer.fetch()
            if batch is not None:
                for msg in batch:
                    serialized_record = msg.get_data()
                    record = json.loads(serialized_record)
                    if "tweet" in record:
                        print("tweet key found!")
                    elif "temperature" in record:
                        print("temp key found")
                    else:
                        print("fire alert found!")
            
        
    except (MemphisError, MemphisConnectError) as e:
        print(e)
        
    finally:
        await memphis.close()
    

async def run_ingress():
    await asyncio.gather(
        main("zakar-fire-alerts"),
        main("zakar-tweets"),
        main("zakar-temperature-readings")
    )

#You can call this function from your main.py file(should work)
if __name__ == "__main__":
    asyncio.run(run_ingress())

    
    