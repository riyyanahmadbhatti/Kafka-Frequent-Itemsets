This assignment done on apache kafka ubuntu 
The purpose of this assignment is to find frequent itemsets
Three ways done to find frequent itemsets using kafka
First using Apriori algorithm
Second using pcy algorithm
Third using son algorithm

To run all this first you will get the meta data from the link inside the pdf 
After downloading extract it 

Now on the extracted file run the preprocessing.py which will make a shorter sample of the file which would remove unwanted columns and rows
After making the new sample run kafka 

To run kafka on ubuntu go into the kafka folder and run the following commands but before that ensure you have kafka in ubuntu as well as hadoop
bin/zookeeper-server-start.sh config/zookeeper.properties
bin/kafka-server-start.sh config/server.properties

After the kafka zookeeper and server is running run the producer.py in the normal way in one tab
python3 producer.py 

Now open another tab and run one of the consumers you want to run
python3 apriori.py
python3 pcy.py
python son.py

I would suggest to run one consumer at a time 
and thats it
