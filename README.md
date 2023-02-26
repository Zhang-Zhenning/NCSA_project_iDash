# Install Clickhouse
We use docker to build local database engine. The official docker website is [offcical link in docker hub](https://hub.docker.com/r/clickhouse/clickhouse-server/). The official website for Clickhouse is [Clickhouse offical website](https://clickhouse.com/).

We provide a bash script **install_docker.sh** for your convenience. 

~~~
bash install_docker.sh [PORT1] [PORT2]
~~~

The default version will be 

~~~
bash install_docker.sh 18123 19000
~~~

Because for those two ports usually we don't need root privilege.

After running **install_docker.sh**, the basic information of the clickhouse server (e.g. url and container id) will be printed for your reference.      

  
<br /><br />

# Insert and Query 
We provide two files, **manage_db_fast.py** and **manage_db.py**. For **manage_db.py**, it is just a simple implementation of insert. It only uses one CPU core to do serial jobs. For **manage_db_fast.py**, we provide an optimized multiprocessing implementation of insert and query operations.

<br />

>*We highly recommend you to only utilize **manage_db_fast.py**, and use Mode 0 (pieceMode) for insertion*.

<br />

For **manage_db_fast.py**,  below the line 

<br />

~~~
if __name__ == "__main__":
~~~
<br />

We have defined several user-modifed variables

<br />


~~~
    db = "test_local_table1"
    url = "http://localhost:18123/"
    Mode = 2
    queryCommand = None 
    filePaths2018_2020 = [r"./dataset/bro_notice.log-20180410"]
    filePaths2020_2022 = [r"./dataset/notice.00:00:00-01:00:00-20220105.log"]
~~~

1. *db* is the table name you want to manipulate (e.g. create, drop, insert or query)

2. *url* is the address of clickhouse database server, which should be local in most cases

3. *Mode* has three possible values. 
    1. 0 is the piece mode for insertion. This is the recommended insertion mode. pieceMode will create a process for each line of data. But this may cause the port to suffer from spam which may increase time cost a bit as the program needs to spin for a while to make one POST call reach the server.
    2. 1 is the chunk mode for insertion. We will divide all lines into n chunks evenly where n is the number of CPU cores. 
    3. 2 is for query mode. In this mode, you need to modify *queryCommand*, change it to your text query command. Then the program will retrieve the result for you in a well-structured format in stdout.

4. *queryCommand* is mentioned in 3, which will only be used in query mode.

5. *filePaths2018_2020* is a list of file names for files in 2018-2020 period. Please be very careful here since files in 2018-2020 and 2020-2022 have very different format. Each element in list should be a relative/absolute string path.

6. *filePaths2020_2022* is a list of file names for files in 2018-2020 period. Please be very careful here since files in 2018-2020 and 2020-2022 have very different format. Each element in list should be a relative/absolute string path.


<br /><br />

# Drop Table
We also provide a function **drop_db(dbName, curURL)** for you to drop table. 