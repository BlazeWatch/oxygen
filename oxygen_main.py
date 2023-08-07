import oxygen_ingress
import oxygen_egress
import asyncio
import os
from dotenv import load_dotenv

#Init Creds
load_dotenv()
asyncio.run(oxygen_ingress.run_ingress())