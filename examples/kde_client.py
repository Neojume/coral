#!/usr/bin/python

from httpMethods import *
import json
import time
import random

def randEvent() :
  account = random.randrange(1000)
  city = "Amsterdam"
  # Bimodal distribution, with sometimes a sample in the middle
  dice = random.randint(0,100)
  if dice <= 2:
      amount = random.gauss(150,1)
  elif dice < 50:
      amount = random.gauss(220, 5)
  else:
      amount = random.gauss(80, 5)

  return {'account': 'NL'+str(account), 'amount': amount, 'city': city}

while(1):
    event = randEvent()
    post('/api/actors/1/in', event)
    if (event['amount'] > 140 and event['amount'] < 160): break
    time.sleep(0.01)
