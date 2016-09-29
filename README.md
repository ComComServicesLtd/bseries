# bseries
C++ 11 High Performance Cached Binary Time Series


Useful for storing large fixed time interval data series for example

pinging of thousands of devices every 1 second.

Each data series has a header which contains the creation date, checksum and misc other information

each time a write is requsted a single data point is appended to the end of this file (datatypes currently supported are float and
unsiged char) 
Since each point represents a fixed time interval, querying by date is instantaneous.. I.e. last day = last 86400 bytes if datasize is 1 byte

Additionally the database utilizes caching to reduce harddisk throughput, a write ahead log is also used to prevent increasing filesize by small intervals

