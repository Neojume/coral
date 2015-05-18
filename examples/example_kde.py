from httpMethods import *

# Create the actors
post('/api/actors', {"type":"httpbroadcast"})
post('/api/actors', {"type": "window", "params": {"method": "count", "number": 100, "sliding": 1}})
post('/api/actors', {"type":"memory"})
post('/api/actors', {"type":"kde", "params": {"by": "", "field": "amount", "kernel": "gaussian", "bandwidth": "silverman"}})
post('/api/actors', {"type":"memory"})

# Connect the actors
put('/api/actors/1',  {"input":{"trigger":{"in":{"type":"external"}}}})
put('/api/actors/2',  {"input":{"trigger":{"in":{"type":"actor", "source":1}}}})
put('/api/actors/3',  {"input":{"trigger":{"in":{"type":"actor", "source":2}}}})
put('/api/actors/4',  {"input":{"trigger":{"in":{"type":"actor", "source":1}},"collect":{"memory":{"type":"actor", "source":3}}}})
put('/api/actors/5',  {"input":{"trigger":{"in":{"type":"actor", "source":4}}}})

# providing a random event stream
# run:> python ./client.py
