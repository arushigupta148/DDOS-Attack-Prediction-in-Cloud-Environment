import os
import socket
from threading import Thread
from pyspark import SparkContext
from operator import add
import math
import time
import sys
import random


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

            sampling_rate=60*1000
            threshold=2000

            while True:

                    current_time=int(cur_milli()-tim)
                    print (current_time)

                    log=self.s.recv(1024).decode()
                    if not log: break

                    logFile=open("logfile.txt","a")
                    logFile.write(log+"\n")
                    arr=log.split(" ")
                    packets=0
                    freq=int(arr[2])

                    lines = sc.textFile('logfile.txt')
                    counts = lines.flatMap(lambda x: x.split(' ')).filter(lambda x: "." in x).map(lambda x: (x, freq)).reduceByKey(add)
                    output = counts.collect()
                    new=counts.filter(lambda x: x[1]>threshold)
                    for v in output:
                        print (v[0], v[1])
                        packets=packets+v[1]


                    if current_time>(sampling_rate*count):
                        unique_ips=len(output)


                        ddos_attack="No"
                        total = float(packets)
                        total_packets=float(packets)
                        Y=math.floor(total_packets/unique_ips)

                        if count == 2:
                            total_packets= total_packets - table[count-2][1]
                            Y=math.floor(total_packets/unique_ips)

                        if count>2:
                            total_packets= total - table[count-2][3]
                            print ("ips", unique_ips)
                            Y=math.floor(total_packets/unique_ips)

                            if Y/table[1][2]>0:
                                alpha=math.log(Y/table[1][2])
                            else:
                                alpha=0.0

                            if Y/table[count-2][2]>0:
                                beta=math.log(Y/table[count-2][2])
                            else:
                                beta=0.0

                            if total_packets/table[0][1]>0:
                                L=float(1/60.0)*math.log(total_packets/table[0][1])
                            else:
                                L=-float("inf")

                            if (alpha < 1):
                                ddos_attack = "No"
                            elif (L>0.5):
                                L=1.0
                                ddos_attack = "Yes"
                            else:
                                ddos_attack = "Yes"
                                L=-float("inf")

                        row=[]
                        row.append(unique_ips)
                        row.append(total_packets)
                        row.append(Y)
                        row.append(total)
                        row.append(beta)
                        row.append(alpha)
                        row.append(L)
                        row.append(ddos_attack)

                        table.append(row)
                        count=count+1

                        print("table containing analysis results of mapreduce on log files")
                        print('\n'.join(' '.join(map(str,sl)) for sl in table))

                        diction={}
                        seen=[]
                        peakCount=0

                        if count == 6:

                            out.append(new.collect())
                            print("After analyzing the log files for 5 minutes, the ipadresses sending packets greater than the threshold every minute are:")
                            print('\n'.join(' '.join(map(str,sl)) for sl in out))

                            for item in out:
                              #if not item:
                                peakCount=peakCount+1
                                for tup in item:
                                    if tup[0] not in seen:
                                        seen.append(tup[0])
                                        diction[tup[0]]=1
                                    else:
                                        diction[tup[0]]=diction[tup[0]]+1
                            block=[]
                            for key in diction:
                                if diction[key]==peakCount:
                                    block.append(key)
                            print("Ip addresses found in all peaks are reported:")
                            print block


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
