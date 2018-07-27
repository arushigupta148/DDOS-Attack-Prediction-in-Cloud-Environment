# DDoSAttack
The objective of this project is to detect and prevent DDoS attacks using time series analysis. We will be using the hadoop architecture to process the log files over a cluster node in parallel for faster processing.

A python script is used to convert a text file into a log file of random time series data. We get a stream of logs including a timestamp, client and server IP and the resource requested. 

Sample Apache log message:
155.156.168.116 - - [25/May/2015:23:11:15 +0000] "GET / HTTP/1.0" 200 3557 "-" "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; acc=baadshah; acc=none; freenet DSL 1.1; (none))"

In our project, we are trying to create a system on Apache Hadoop to detect abnormal high amount of traffic. These log files are map-reduced to predict a DDoS attack. This is done for fast prediction of attacks and blocking those IPs. It will also try to find IPs which are posing as legitimate by analysing the recurrent patterns and blocking them. 
