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
    """
    if conn[3]=="ddos":
        s1.connect((host,port))
        os.system("sudo ping -f 10.0.0.104 1>>ping.txt")
        file2 = open("/Users/arushigupta148/Desktop/ping.txt","r")
        for line in file2:
            t=line.split(" ")[-1]
            print t
            log=login+" "+str(t)+" 1"
            s1.send(log.encode())
        
        file2.close()
        
    else:
    """
    #end=int(conn[3])
    noOfPackets=conn[5]

    #Connect to server
    try:
        s1.connect((host,port))
        break
    except:
        print ('wrong command, try again!')



cur_milli = lambda: int(round(time.time() * 1000))
#star = int(conn[3]) if int(conn[3]).isdigit() == True and len(sys.argv) > 1 else 1000
#end= int(conn[4]) if int(conn[4]).isdigit() == True and len(sys.argv) > 1 else 10000
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
