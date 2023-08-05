import argparse
import asyncio
import json
import os
import dotenv
from memphis import Memphis, Headers, MemphisError, MemphisConnectError, MemphisHeaderError, MemphisSchemaError


#Define environemtn variables 
host = os.getenv("MEMPHIS_HOST")  
username = os.getenv("MEMPHIS_USERNAME")
password = os.getenv("MEMPHIS_PASSWORD")
account_id = os.getenv("MEMPHIS_ACCOUNT_ID")

async def socialmedia_engress(message): 
    try:
        memphis = Memphis()
        await memphis.connect(host="aws-eu-central-1.cloud.memphis.dev", username=username, password=password, account_id=account_id)
        
        await memphis.station(name="zakar-tweets-2")

        producer = await memphis.producer(station_name="zakar-tweets-2", producer_name="blazewatch-socialmedia", generate_random_suffix=False)
   
        for i in range(100):
            await producer.produce(bytearray(f"{message}", "utf-8"), async_produce=True)

    except (MemphisError, MemphisConnectError) as e:
        print(e)

    finally:
        producer.destroy
    


if __name__ == "__main__":
    msg = {
  "day": 69000,
  "geospatial_x": 260,
  "geospatial_y": 18,
}
    asyncio.run(socialmedia_engress(msg))