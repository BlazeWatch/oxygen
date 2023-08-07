import oxygen_ingress
import oxygen_egress
import asyncio
import os
import json

#Sets the current day to default 0.
currentDay = 0


async def egress(currentDay):
    return currentDay
    
async def run_all():
    await asyncio.gather(
        oxygen_ingress.run_ingress(),
        egress(0)
        
    )

#Runs Ingress + Egress + Monitor Function
asyncio.run(oxygen_ingress.all())