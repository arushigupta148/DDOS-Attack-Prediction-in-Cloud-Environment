import time
import sys
import random
import subprocess
import pipes
import socket
import os

s1 = socket.socket()

while True:
    #Client command to connect to server
    command=raw_input('enter client command:(serverip clientip port startTime endTime noOfPackets)')

    #Extract host no. and port no.
    conn=command.split()
    host=conn[0]
    login=conn[1]
    port=int(conn[2])
    noOfPackets=conn[5]

    #Connect to server
    try:
        s1.connect((host,port))
        break
    except:
        print ('wrong command, try again!')



cur_milli = lambda: int(round(time.time() * 1000))
star=int(conn[3])
end=int(conn[4])


start = cur_milli()
while cur_milli() - start < 5*60*1000:

        wait = random.randint(star,end)
        time.sleep(float(wait)/1000.0)

        print "request sent at - ",cur_milli()-start,"\bms"

        log=login+" "+str(cur_milli()-start)+" "+noOfPackets
        s1.send(log.encode())

s1.close()
