<center>
<h1>iogp database code</h1>
</center>

## Two essential commands you need to build this system:

###  Clean
```
make clean
```
This will clean all the generated thrift interfaces(both in C++ and Java) and generated C++ library files.  Also, the java package built by Apache Maven will be cleaned.

### Compile and Install
```
make install
```
This will generate thrift interfaces(both in C++ and Java) and the C++ library files. Java packages are automatically built by Apache Maven.

## Thrift Generation
```
make clean-thrift
make thrift
```
Usually, to maintain stability of thrift schema and its corresponding generated artifacts, we seldom run these two targets.

## Run
IOGP is packaged with maven and shell script. You need to first unpack the binary distribution, then run the __server.sh__ in ```bin``` directory.
```
cd release
cd /bin
sh server.sh -help
```
By doing so, you'll get a detailed arg list.
IOGP provides several run modes. The easiest way is to run servers and clients in local machine using multiple threads by passing "-local" as a boot argument. To achieve this, simply run:
```
sh server.sh start -local -db [dbFilePrefix] -id [ThreadsNumber] -type [edgecut/iogp]
```
where "id" is the number of threads, and db is the data file prefix.
Also, you can run it in a distributed mode, in which case the "-local" parameter should not be passed and -id indicates the ID of the server node, and -srvlist must be provided to make sure each server node knows others.
```
sh server.sh start -db [dbFile] -id [ServerId] -type [edgecut/iogp] -srvlists [s1:p1, s2:p2, …]
```
As you can see from parameter "-type", GraphMeta provides multiple types of servers, including Edge-Cut and IOGP. You can choose one of them as server and use corresponding clients to interact with the servers.

You can view the output of the program by doing so:
```
cat ../logs/stdout.log
cat ../logs/stderr.log
```
