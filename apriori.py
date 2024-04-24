from kafka import KafkaConsumer
import json
import time

bootstrap_servers = ['localhost:9092']
topic = 'firsttopic'

#function to generate products
def generateproducts(orders, k):
    products = set()
    for order in orders:
        for item in order:
            products.add(frozenset([item]))
    return products

#function to filter products according to the minimun support
def filterproducts(orders, products, minsupport):
    supportcount = {}
    for order in orders:
        for product in products:
            #checking if the product is a subset of the order
            if product.issubset(order):
                supportcount[product] = supportcount.get(product, 0) + 1

    numorders = float(len(orders))
    frequentproducts = {}
    for product, count in supportcount.items():
        support = count / numorders#dividing support count of each product with the total orders
        if support >= minsupport:#printing the product if the support count bigger then minsupport
            frequentproducts[product] = support
            print(f"{', '.join(product)}")
            time.sleep(0.05)
    return frequentproducts

def apriori(orders, minsupport):
    frequentitemsets = []
    k = 1#size of itemsets
    products = generateproducts(orders, k)
    print("Frequent Itemsets:")
    while products:
        frequentproducts = filterproducts(orders, products, minsupport)
        frequentitemsets.extend(frequentproducts)
        products = generateproducts(frequentproducts.keys(), k)
    return frequentitemsets


consumer = KafkaConsumer(topic,
                         bootstrap_servers=bootstrap_servers,
                         value_deserializer=lambda x: json.loads(x.decode('utf-8')))


minsupport = 0.8  
try:
    for message in consumer:
        neworder = message.value['also_buy']
        frequentitemsets = apriori([neworder], minsupport)

except KeyboardInterrupt:
    print("Keyboard interrupt detected. Exiting...")
finally:
    consumer.close()
