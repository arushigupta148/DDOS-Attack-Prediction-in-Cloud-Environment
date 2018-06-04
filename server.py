import os
import socket
from threading import Thread
from pyspark import SparkContext
from operator import add
import math
import time
import sys
import random

def mapreduce():
    lines = sc.textFile('/Users/arushigupta148/Desktop/logfile.txt')
    counts = lines.flatMap(lambda x: x.split(' ')).filter(lambda x: "." in x).map(lambda x: (x, 1)).reduceByKey(add)
    print ("2")
    output = counts.collect()
    print ("3")
    for v in output:
        print ("4")
        print (v[0], v[1])
        
        

#Running multiple threads for multiple clients
class SThread(Thread):
    def __init__(self,address,connection):
        Thread.__init__(self)
        self.address = address
        self.s= connection
    def run(self):
            print ('new connection')
            global count
            alpha=0.0
            beta=0.0
            L=-float("inf")
            #table=[]
            # keep track of time inside each thread
            threadCount=1
            sampling_rate=60*1000
            
            while True:
                    current_time=int(cur_milli()-tim)
                    print current_time
                    #Operation to be performed by server
                    log=self.s.recv(1024).decode()
                    if not log: break
                
                    logFile=open("logfile.txt","a")
                    logFile.write(log+"\n")                   
                    arr=log.split(" ")
                    packets=0
                    freq=int(arr[2])
               
                    lines = sc.textFile('/Users/arushigupta148/Desktop/logfile.txt')
                    counts = lines.flatMap(lambda x: x.split(' ')).filter(lambda x: "." in x).map(lambda x: ((count,x), freq)).reduceByKey(add)
                    output = counts.collect()
                    
                    new=counts.filter(lambda x: x[1]>60)
                    
                    
                    for v in output:
                        print (v[0], v[1])
                        packets=packets+v[1]*freq
                        
                        
                    if current_time>(sampling_rate*count):
                        unique_ips=len(output)
                        total_packets=float(packets)
                        Y=math.floor(total_packets/unique_ips)
                        
                        if count>1:
                            alpha=math.log(Y/table[0][2])
                            beta=math.log(Y/table[count-2][2])
                            L=float(1/60.0)*math.log(total_packets/table[0][1])
                            
                        row=[]
                        row.append(unique_ips)
                        row.append(total_packets)
                        row.append(Y)
                        row.append(beta)
                        row.append(alpha)
                        row.append(L)
                        table.append(row)
                        count=count+1
                        threadCount=threadCount+1
                        
                        print table
                        
                        threshold=60
                        
                        if total_packets>threshold:
                            pack={}
                            for v in output:
                                packets=packets+v[1]*freq
                                pack[v[0]]=packets
                                
                            packetTable[count]=pack
                        print packetTable
                        
                        out.append(new.collect())
                        print out
                        
                        """
                        if count==6:
                            #report malicious ips
                            for item in out:
                                print ("item is",item)
                                for it in item:
                                    for i in it:
                                        print ("ip is",it[1])
                                        break
                          """  
                            
                        #calculate table (unique ips, total packets, Y, beta, alpha,L, DDoS suspected )
                                  

            
#Socket connection
s1 = socket.socket()
host = socket.gethostname()
s1.bind((host, 0))
port=s1.getsockname()[1]

logFile=open("logfile.txt","w")
sc = SparkContext(appName="inf551")
# keep track of time
count=1

#For client to connect to server
hostip = socket.gethostbyname(host)
print('host ip is: ',hostip)
print ('port is: ',port)
cur_milli = lambda: int(round(time.time() * 1000))
tim = cur_milli()
table=[]
packetTable={}
out=[]

#Connect to multiple clients using multithreading
while True:
    s1.listen(1)
    connection, address = s1.accept()
    
    newthread = SThread(address, connection)
    newthread.start()

