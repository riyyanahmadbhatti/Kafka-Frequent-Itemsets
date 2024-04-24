from kafka import KafkaConsumer
import json
import time
from collections import defaultdict
import hashlib

bootstrap_servers = ['localhost:9092']
topic = 'firsttopic'

#function to generate products
def generateproducts(orders, k):
    products = set()
    for order in orders:
        for item in order:
            products.add(frozenset([item]))
    return products

#function to filter products according to the minimum support using PCY
def filterproducts(orders, products, minsupport, hashbucketsize):
    supportcount = defaultdict(int)
    hashtable = [0] * hashbucketsize
    for order in orders:
        #counting item occurrences and hash pairs
        orderitems = list(order)
        for i in range(len(orderitems)):
            for j in range(i + 1, len(orderitems)):
                #calculating hash value
                itempairhash = (hash(orderitems[i]) + hash(orderitems[j])) % hashbucketsize
                hashtable[itempairhash] += 1
                if hashtable[itempairhash] >= minsupport:
                    supportcount[frozenset([orderitems[i], orderitems[j]])] += 1
    numorders = float(len(orders))
    frequentproducts = {}
    for product, count in supportcount.items():
        support = count / numorders#dividing support count of each product with the total orders
        if support >= minsupport:#printing the product if the support count bigger than minsupport
            frequentproducts[product] = support
            print(f"{', '.join(product)}")
            time.sleep(0.05)
    return frequentproducts

#pcy function
def pcy(orders, minsupport, hashbucketsize):
    frequentitemsets = []
    k = 2 #size of itemsets
    products = generateproducts(orders, k)
    print("Frequent Itemsets:")
    while products:
        frequentproducts = filterproducts(orders, products, minsupport, hashbucketsize)
        frequentitemsets.extend(frequentproducts)
        products = generateproducts(frequentproducts.keys(), k)
    return frequentitemsets


consumer = KafkaConsumer(topic,
                         bootstrap_servers=bootstrap_servers,
                         value_deserializer=lambda x: json.loads(x.decode('utf-8')))

minsupport = 0.8
hashbucketsize = 1000 
try:
    for message in consumer:
        neworder = message.value['also_buy']
        frequentitemsets = pcy([neworder], minsupport, hashbucketsize)

except KeyboardInterrupt:
    print("Keyboard interrupt detected. Exiting...")
finally:
    consumer.close()
