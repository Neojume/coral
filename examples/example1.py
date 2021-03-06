from httpMethods import *

# Create the graph (profiling cities)
post('/api/actors', {"type":"httpbroadcast"})
post('/api/actors', {"type":"stats", "params":{"field": "amount"}, "group":{"by":"city"}})
post('/api/actors', {"type":"zscore",    "params":{"by":"city", "field": "amount","score" : 1.0}})

put('/api/actors/1',  {"input":{"trigger":{"in":{"type":"external"}}}})
put('/api/actors/2',  {"input":{"trigger":{"in":{"type":"actor", "source":1}}}})
put('/api/actors/3',  {"input":{"trigger":{"in":{"type":"actor", "source":1}},"collect":{"stats":{"type":"actor", "source":2}}}})