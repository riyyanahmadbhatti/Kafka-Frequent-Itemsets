from kafka import KafkaConsumer
import json
import time
from collections import defaultdict
from multiprocessing import Pool

bootstrap_servers = ['localhost:9092']
topic = 'firsttopic'

#mapping function 
def mapping(chunk):
    frequentitems = defaultdict(int)
    for basket in chunk:
        for item in basket:
            frequentitems[item] += 1
    return frequentitems

#reducing function to remove less frequent items
def reducing(chunkresults, supportthreshold):
    totalfrequentitems = defaultdict(int)
    for frequentitems in chunkresults:
        for item, count in frequentitems.items():#iterating over the dictionary of frequent items with the count and the item itself
            totalfrequentitems[item] += count#setting count of items 
    frequentitemsets = {item for item, count in totalfrequentitems.items() if count >= supportthreshold}#filtering frequent itemsets based on support threshold
    return frequentitemsets

def son(baskets, supportthreshold, numchunks, numprocesses):
    if len(baskets) < numchunks:
        numchunks = len(baskets)
    chunk_size = len(baskets) // numchunks
    chunks = [baskets[i:i+chunk_size] for i in range(0, len(baskets), chunk_size)]#dividing baskets of data into chunks

    #use multiprocessing pool to parallelize mapping phase
    #2 chunks 2 processes
    with Pool(processes=numprocesses) as pool:
        chunkresults = pool.map(mapping, chunks)#sending chunks of data
    
    frequentitemsets = reducing(chunkresults, supportthreshold)
    
    return frequentitemsets

consumer = KafkaConsumer(topic,
                         bootstrap_servers=bootstrap_servers,
                         value_deserializer=lambda x: json.loads(x.decode('utf-8')))

minsupport = 0.8
numchunks = 2
numprocesses = 2

try:
    for message in consumer:
        neworder = message.value['also_buy']
        frequentitemsets = son([neworder], minsupport, numchunks, numprocesses)
        print("Frequent Itemsets:", frequentitemsets)

except KeyboardInterrupt:
    print("Keyboard interrupt detected. Exiting...")
finally:
    consumer.close()
