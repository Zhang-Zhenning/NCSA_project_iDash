import sys
import datetime
import requests
import time
from tqdm import tqdm
import multiprocessing
import math


# --------------------------------------------specify columns of database----------------------------------------------
columns = "ts	uid	id.orig_h	id.orig_p	id.resp_h	id.resp_p	fuid	file_mime_type	file_desc	proto	note	msg	sub	src	dst	p	n	peer_descr	actions	suppress_for	dropped	remote_location.country_code	remote_location.region	remote_location.city	remote_location.latitude	remote_location.longitude"
columns = columns.replace(".", "_")
columns = columns.split("\t")
print(f"********************************************************************************************************")
print(f"columns: {columns}")
print(f"columns length: {len(columns)}")


# ------------------------------------------specify types for special columns-------------------------------------------
# 0:timestamp   3:port    5:port    15:port    19:sup time   11:msg    12:sub
types = ["VARCHAR(50)" for i in range(len(columns))]
# port(int)
types[3] = "INT"
# port(int)
types[5] = "INT"
# port(int)
types[15] = "INT"
# sup time(int)
types[19] = "INT"
# ts(CST timestamp)
types[0] = "TIMESTAMP"
# msg(too long)
types[11] = "VARCHAR(1000)"
# sub(too long)
types[12] = "VARCHAR(500)"
print(f"********************************************************************************************************")
print(f"types: {types}")
print(f"types length: {len(types)}")

# -------------------------------------------------helper function-------------------------------------------------------
# year 0 for 2018 - 2020 and year 1 for 2020 - 2022
def unix_to_cst(convertLine, year=0):
    try:
        if year == 0:
           unix_time = float(convertLine[0].split(": ")[-1])
        else:
           unix_time = float(convertLine[0])
    except:
        return 1

    # Convert to UTC datetime object
    utc_datetime = datetime.datetime.utcfromtimestamp(unix_time)
    temp_cdt_datetime = utc_datetime - datetime.timedelta(hours=5)

    start_dst = datetime.datetime(temp_cdt_datetime.year, 3, 8, 3, 0, 0) + datetime.timedelta(days=(6-datetime.datetime(temp_cdt_datetime.year, 3, 8).weekday()))
    end_dst = datetime.datetime(temp_cdt_datetime.year, 11, 1, 2, 0, 0) + datetime.timedelta(days=(6-datetime.datetime(temp_cdt_datetime.year, 11, 1).weekday()))

    if temp_cdt_datetime >= end_dst:
        temp_cdt_datetime = utc_datetime - datetime.timedelta(hours=6)
    elif temp_cdt_datetime < start_dst:
        temp_cdt_datetime = utc_datetime - datetime.timedelta(hours=6)

    temp_cdt_datetime = temp_cdt_datetime.strftime('%Y-%m-%d %H:%M:%S')
    cst_str = temp_cdt_datetime
    convertLine[0] = cst_str
    return 0


# convert port and sup time to int
def convert_to_int(convertLine):
    flag = 0
    try:
        convertLine[3] = str(int(convertLine[3])
                             ) if convertLine[3] != "-" else "NULL"
    except:
        convertLine[3] = "NULL"
        flag = 1

    try:
        convertLine[5] = str(int(convertLine[5])
                             ) if convertLine[5] != "-" else "NULL"
    except:
        convertLine[5] = "NULL"
        flag = 1

    try:
        convertLine[15] = str(int(convertLine[15])
                              ) if convertLine[15] != "-" else "NULL"
    except:
        convertLine[15] = "NULL"
        flag = 1

    try:
        convertLine[19] = str(int(convertLine[19].split(
            ".")[0])) if convertLine[19] != "-" else "NULL"
    except:
        convertLine[19] = "NULL"
        flag = 1

    if flag:
        return 1
    else:
        return 0


# convert "-" to NULL
def convert_to_null(convertLine):
    for i in range(len(convertLine)):
        if convertLine[i] == "-":
            convertLine[i] = "NULL"


# convert 2018 - 2020 data
def convert_2018_2020_data(file_path):
    ret = []
    max_length = [0 for i in range(len(columns))]

    with open(file_path, "r") as f:
        i = 0
        badLines = []
        for line in f:
            i += 1
            newLine = line.strip().split("#011")

            # check length
            if len(newLine) < len(columns):
                badLines.append(i)
                continue
            # check timestamp
            if unix_to_cst(newLine):
                badLines.append(i)
                continue
            # check port and sup time
            if convert_to_int(newLine):
                badLines.append(i)
                continue

            convert_to_null(newLine)
            ret.append(newLine)

        print(f"********************************************************************************************************")
        print(f"for file {file_path}")
        print(f"total line number: {i}")
        print(f"bad line number: {len(badLines)}")

        for item in ret:
            for i in range(len(columns)):
                max_length[i] = max(max_length[i], len(item[i]))

        print(f"max item length for each column in {file_path} is")
        print(max_length)

        return ret


# convert 2020 - 2022 data
def convert_2020_2022_data(file_path):
    ret = []
    max_length = [0 for i in range(len(columns))]

    with open(file_path, "r") as f:
        i = 0
        badLines = []
        for line in f:
            i += 1
            newLine = line.strip().split("\t")

            # check length
            if len(newLine) < len(columns):
                badLines.append(i)
                continue
            # check timestamp
            if unix_to_cst(newLine, 1):
                badLines.append(i)
                continue
            # check port and sup time
            if convert_to_int(newLine):
                badLines.append(i)
                continue

            convert_to_null(newLine)
            ret.append(newLine)

        print(f"********************************************************************************************************")
        print(f"for file {file_path}")
        print(f"total line number: {i}")
        print(f"bad line number: {len(badLines)}")

        for item in ret:
            for i in range(len(columns)):
                max_length[i] = max(max_length[i], len(item[i]))

        print(f"max item length for each column in {file_path} is")
        print(max_length)

        return ret


# -------------------------------------------------database operation------------------------------------------------------
# create database, return 1 for error and 0 for success. If table does exist, it will simply return 0
def create_db(dbName, curURL):
    typeName = ",".join([columns[i] + " " + types[i]
                        for i in range(len(columns))])
    rawText = f"CREATE TABLE IF NOT EXISTS {dbName} ({typeName}) ENGINE=File(Native)"
    r = requests.post(curURL, data=rawText)
    if r.status_code != 200:
        print(f"********************************************************************************************************")
        print(f"CREATE ERROR when executing this:\n{rawText}\n")
        print(f"ERROR MESSAGE from server:\n{r.content}")
        print(f"********************************************************************************************************")
        return 1
    return 0


# unpack the argument for piece mode
def insert_to_db_unpack(args):
    return insert_to_db(*args)


# unpack the argument for chunk mode
def insert_to_db_chunk_unpack(args):
    return insert_to_db_chunk(*args)


# insert data in the chunk to database, return 1 for error and 0 for success
def insert_to_db_chunk(dbName,curURL,singleLines):
    for singleLine in singleLines:
        if insert_to_db(dbName,curURL,singleLine):
            return 1
    return 0


# insert one line of data to database, return 1 for error and 0 for success
def insert_to_db(dbName, curURL, singleLine):
    # make sure '' won't confuse mysql in field "sub"
    # time.sleep(0.01)
    singleLine[12] = singleLine[12].replace("'", "_")
    singleLine[11] = singleLine[11].replace("'", "_")
    # insert command
    singleLineName = f"cast('{singleLine[0]}' as TIMESTAMP),'{singleLine[1]}','{singleLine[2]}','{singleLine[3]}','{singleLine[4]}','{singleLine[5]}','{singleLine[6]}','{singleLine[7]}','{singleLine[8]}','{singleLine[9]}','{singleLine[10]}','{singleLine[11]}','{singleLine[12]}','{singleLine[13]}','{singleLine[14]}','{singleLine[15]}','{singleLine[16]}','{singleLine[17]}','{singleLine[18]}','{singleLine[19]}','{singleLine[20]}','{singleLine[21]}','{singleLine[22]}','{singleLine[23]}','{singleLine[24]}','{singleLine[25]}'"
    singleLineName = singleLineName.replace("'NULL'", "NULL")
    columnName = ",".join(columns)
    rawText = f"INSERT INTO {dbName}({columnName}) VALUES({singleLineName})"
    
    while True:
        try:
           r = requests.post(curURL, data=rawText)
           break
        except:
           continue

    if r.status_code != 200:
        print(f"********************************************************************************************************")
        print(f"INSERT ERROR when executing this:\n{rawText}\n")
        print(f"ERROR MESSAGE from server:\n{r.content}")
        print(f"********************************************************************************************************")
        return 1
    return 0


# drop the database, return 1 for error and 0 for success. If table does not exist, it will simply return 0
def drop_db(dbName, curURL):
    rawText = f"DROP TABLE IF EXISTS {dbName}"
    r = requests.post(curURL, data=rawText)
    if r.status_code != 200:
        print(f"********************************************************************************************************")
        print(f"DROP ERROR when executing this:\n{rawText}\n")
        print(f"ERROR MESSAGE from server:\n{r.content}")
        print(f"********************************************************************************************************")
        return 1
    return 0


# query the database, return 1 for error and query result for success
def query_db(curURL, querySentence):
    rawText = querySentence
    if rawText == None:
        print(f"********************************************************************************************************")
        print(f"INVALID QUERY")
        return 1

    r = requests.post(curURL, data=rawText)
    if r.status_code != 200:
        print(f"********************************************************************************************************")
        print(f"QUERY ERROR when executing this:\n{rawText}\n")
        print(f"ERROR MESSAGE from server:\n{r.content}")
        print(f"********************************************************************************************************")
        return 1
    print(f"********************************************************************************************************")
    print(f"SUCCESS when executing this:\n{rawText}\n")
    print(f"QUERY FEEDBACK from server:")
    
    lst = str(r.content).split("\\n")
    for itm in lst:
        print(itm)
    return lst



if __name__ == "__main__":

    # -------------------------------------------------user-defined variables------------------------------------------------------
    # name of table
    db = "test_zhenning_local_jubilee"
    # url for clickhouse server, should be local in NCSA server
    url = "http://localhost:18123/"
    # 0: pieceMode(recommend for insert) 1: chunkMode(not recommend for insert) 2: queryMode(query the db, needs to modify queryCommand below)
    Mode = 0
    # query command, only needed in queryMode (Mode=2)                                          
    queryCommand = None 
    # file name list for 2018-2020 data from NCSA    
    filePaths2018_2020 = [r"./dataset/bro_notice.log-20180410"]
    # file name list for 2020-2022 data from NCSA
    filePaths2020_2022 = [r"./dataset/notice.00:00:00-01:00:00-20220105.log"]


    # ------------------------------------------------------start working----------------------------------------------------------
    print(f"********************************************************************************************************")
    print(f"using url {url} to connect to clickhouse server")
    print(f"target database name is {db}")
    if Mode == 1:
        print("In chunkMode now")
    elif Mode == 0:
        print("In pieceMode now")
    else:
        # query mode
        print("In queryMode now")
        queryResult = query_db(url,queryCommand)
        exit(1)
    
    
    # convert data into clean format
    totalData = []
    print(f"********************************************************************************************************")
    print("STARTING processing 2018-2020 data")
    for filePath2018_2020 in tqdm(filePaths2018_2020):
        totalData += convert_2018_2020_data(filePath2018_2020)

    print(f"********************************************************************************************************")
    print("STARTING processing 2020-2022 data")
    for filePath2020_2022 in tqdm(filePaths2020_2022):
        totalData += convert_2020_2022_data(filePath2020_2022)


    # drop the existing database
    if drop_db(db, url):
        exit(1)
    # create database
    if create_db(db, url):
        exit(1)
    

    if Mode == 1:
        chunkNum = multiprocessing.cpu_count()
        chunkLength = math.floor(len(totalData) / chunkNum)
        chunks = []
        for i in range(chunkNum):
            if i < (chunkNum-1):
                cur_chunk = totalData[i*chunkLength:(i+1)*chunkLength] 
            else:
                cur_chunk = totalData[i*chunkLength:]

            chunks.append(cur_chunk)
        argIteratorChunk = ((db,url,tempChunk) for tempChunk in chunks)
        
    elif Mode == 0:
        argIteratorPiece = ((db,url,tempPiece) for tempPiece in totalData)

    else:
        print(f"********************************************************************************************************")
        print("Wrong Insert Mode! Mode = 0 for pieceMode, 1 for chunkMode")
        print(f"********************************************************************************************************")
        exit(1)

    
    # using multiprocessing to accelerate
    pool = multiprocessing.Pool()
    startTime = time.time()


    # insert data from 2020-2022 to database
    if Mode == 1:
        for flag in tqdm(pool.imap_unordered(insert_to_db_chunk_unpack,argIteratorChunk),total=chunkNum):
            if flag:
                exit(1)
    else:
        for flag in tqdm(pool.imap_unordered(insert_to_db_unpack,argIteratorPiece),total=len(totalData)):
            if flag:
                exit(1)
   

    endTime = time.time()
    minutes, seconds = divmod(endTime-startTime, 60)


    print(f"********************************************************************************************************")
    print(f"SUCCESSFULLY INSERTED {len(totalData)} LINES OF DATA INTO DATABASE {db}")
    print("Time taken: %dm %ds" % (minutes, seconds))



    print(f"********************************************************************************************************")
