import asyncio
import json
import os
from dotenv import load_dotenv
from memphis import Memphis, MemphisError, MemphisConnectError, MemphisHeaderError, MemphisSchemaError

#Load env vars
load_dotenv()

#Define environement variables 
host = os.getenv("MEMPHIS_HOSTNAME")  
username = os.getenv("MEMPHIS_USERNAME")
password = os.getenv("MEMPHIS_PASSWORD")
account_id = os.getenv("MEMPHIS_ACCOUNT_ID")


async def egress(station_name,msg):
    try:
        memphis = Memphis()
        await memphis.connect(host=host, username=username, password=password, account_id=account_id)
        producer = await memphis.producer(station_name=f"{station_name}", producer_name=f"{station_name}-producer") # you can send the message parameter as dict as well
        await producer.produce(bytearray(json.dumps(msg),"utf-8"))
        
    except (MemphisError, MemphisConnectError, MemphisHeaderError, MemphisSchemaError) as e:
        print(e)
        
    finally:
        await memphis.close()


msg = {'day': 5023, 'geospatial_x': 261, 'geospatial_y': 18}
print(type(msg))
asyncio.run(egress("zakar-tweets-2",msg))
#Remember that u can call and set a task queue.





    
    