import json

filepath = 'meta_sampled.json'
outputfile = 'metasample.json'
chunksize = 10000

#function to process and filter chunks of the json file(data cleaning)
def processchunk(chunk):
    columnsdrop = ['title','also_view','category', 'image', 'description', 'similar_item','details', 'tech2', 'tech1', 'feature', 'fit', 'brand', 'rank', 'main_cat', 'date', 'price']
    filteredchunk = []
    for col in chunk:
        if 'also_buy' in col and 'asin' in col:
            for column in columnsdrop:
                col.pop(column, None)
            filteredchunk.append(col)
    print(filteredchunk[0])
    return filteredchunk

#opening the json file and reading the data 
with open(filepath, 'r') as input, open(outputfile, 'a') as output:
    while True:
        #reading data chunk by chunk
        chunk = []
        try:
            for i in range(chunksize):
                line = next(input)
                chunk.append(json.loads(line))
        except StopIteration:
            break  #reading 10000 lines per chunk until the end of file is reached
        
        processedchunk = processchunk(chunk)
        #cleaning the chunk with the help of function
        for item in processedchunk:
            output.write(json.dumps(item) + '\n')
