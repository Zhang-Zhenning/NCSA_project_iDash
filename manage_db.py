import sys
import datetime
import requests


# specify columns of database
columns = "ts	uid	id.orig_h	id.orig_p	id.resp_h	id.resp_p	fuid	file_mime_type	file_desc	proto	note	msg	sub	src	dst	p	n	peer_descr	actions	suppress_for	dropped	remote_location.country_code	remote_location.region	remote_location.city	remote_location.latitude	remote_location.longitude"
columns = columns.replace(".", "_")
columns = columns.split("\t")
print(f"********************************************************************************************************")
print(f"columns: {columns}")
print(f"columns length: {len(columns)}")


# specify types for special columns
# 0:timestamp   3:port    5:port    15:port    19:sup time   11:msg    12:sub 
types = ["VARCHAR(50)" for i in range(len(columns))]
types[3] = "INT"                                                     # port(int)
types[5] = "INT"                                                     # port(int)
types[15] = "INT"                                                    # port(int)
types[19] = "INT"                                                    # sup time(int)
types[0] = "TIMESTAMP"                                               # ts(CST timestamp)
types[11] = "VARCHAR(1000)"                                          # msg(too long)
types[12] = "VARCHAR(500)"                                           # sub(too long)
print(f"********************************************************************************************************")
print(f"types: {types}")
print(f"types length: {len(types)}")


# year 0 for 2018 - 2020 and year 1 for 2020 - 2022
def unix_to_cst(convertLine,year=0):
    try:
        if year == 0:
           unix_time = float(convertLine[0].split(": ")[-1])
        else:
           unix_time = float(convertLine[0])
    except:
        return 1

    utc_datetime = datetime.datetime.utcfromtimestamp(unix_time)
    cst_datetime = utc_datetime - datetime.timedelta(hours=6)
    cst_str = cst_datetime.strftime('%Y-%m-%d %H:%M:%S')
    convertLine[0] = cst_str
    return 0


# convert port and sup time to int
def convert_to_int(convertLine):
    flag = 0
    try:
        convertLine[3] = str(int(convertLine[3])) if convertLine[3] != "-" else "NULL"
    except:
        convertLine[3] = "NULL"
        flag = 1
    
    try:
        convertLine[5] = str(int(convertLine[5])) if convertLine[5] != "-" else "NULL"
    except:
        convertLine[5] = "NULL"
        flag = 1
    
    try:
        convertLine[15] = str(int(convertLine[15])) if convertLine[15] != "-" else "NULL"
    except:
        convertLine[15] = "NULL"
        flag = 1

    try:
        convertLine[19] = str(int(convertLine[19].split(".")[0])) if convertLine[19] != "-" else "NULL"
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

    with open(file_path,"r") as f:
        i = 0
        badLines = []
        for line in f:
            i += 1
            newLine = line.strip().split("#011")

            # check length 
            if len(newLine) < 26:
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

    with open(file_path,"r") as f:
        i = 0
        badLines = []
        for line in f:
            i += 1
            newLine = line.strip().split("\t")

            # check length 
            if len(newLine) < 26:
                badLines.append(i)
                continue
            # check timestamp
            if unix_to_cst(newLine,1):
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


# create database, return 1 for error and 0 for success
def create_db(dbName,curURL):
    typeName = ",".join([columns[i] + " " + types[i] for i in range(len(columns))])
    rawText =  f"CREATE TABLE {dbName} ({typeName}) ENGINE=File(Native)"
    r = requests.post(curURL,data=rawText)
    if r.status_code != 200:
        print(f"********************************************************************************************************")
        print(f"CREATE ERROR when executing this:\n{rawText}\n")
        print(f"ERROR MESSAGE from server:\n{r.content}")
        print(f"********************************************************************************************************")
        return 1
    return 0


# insert data to database, return 1 for error and 0 for success
def insert_to_db(dbName, curURL, singleLine):
    # make sure '' won't confuse mysql in field "sub"
    singleLine[12] = singleLine[12].replace("'","_")
    singleLineName = f"cast('{singleLine[0]}' as TIMESTAMP),'{singleLine[1]}','{singleLine[2]}','{singleLine[3]}','{singleLine[4]}','{singleLine[5]}','{singleLine[6]}','{singleLine[7]}','{singleLine[8]}','{singleLine[9]}','{singleLine[10]}','{singleLine[11]}','{singleLine[12]}','{singleLine[13]}','{singleLine[14]}','{singleLine[15]}','{singleLine[16]}','{singleLine[17]}','{singleLine[18]}','{singleLine[19]}','{singleLine[20]}','{singleLine[21]}','{singleLine[22]}','{singleLine[23]}','{singleLine[24]}','{singleLine[25]}'"
    singleLineName = singleLineName.replace("'NULL'","NULL")
    columnName = ",".join(columns)
    rawText = f"INSERT INTO {dbName}({columnName}) VALUES({singleLineName})"
    r = requests.post(curURL,data=rawText)
    if r.status_code != 200:
        print(f"********************************************************************************************************")
        print(f"INSERT ERROR when executing this:\n{rawText}\n")
        print(f"ERROR MESSAGE from server:\n{r.content}")
        print(f"********************************************************************************************************")
        return 1
    return 0


# drop the database, return 1 for error and 0 for success
def drop_db(dbName,curURL):
    rawText = f"DROP TABLE {dbName}"
    r = requests.post(curURL,data=rawText)
    if r.status_code != 200:
        print(f"********************************************************************************************************")
        print(f"DROP ERROR when executing this:\n{rawText}\n")
        print(f"ERROR MESSAGE from server:\n{r.content}")
        print(f"********************************************************************************************************")
        return 1
    return 0


# query the database, return 1 for error and 0 for success
def query_db(curURL,querySentence):
    rawText = querySentence
    r = requests.post(curURL,data=rawText)
    if r.status_code != 200:
        print(f"********************************************************************************************************")
        print(f"QUERY ERROR when executing this:\n{rawText}\n")
        print(f"ERROR MESSAGE from server:\n{r.content}")
        print(f"********************************************************************************************************")
        return 1
    print(f"********************************************************************************************************")
    print(f"QUERY SUCCESS when executing this:\n{rawText}\n")
    print(f"QUERY FEEDBACK from server:\n{r.content}")
    return 0

    
if __name__ == "__main__":
    # variables
    testNum = 70
    db = "test_zhenning_local_mac6"
    url = "http://localhost:18123/"

    if len(sys.argv) >= 2:
        url = f"http://localhost:{sys.argv[1]}/"
    
    print(f"********************************************************************************************************")
    print(f"using url {url} to connect to server")

    # convert data into clean format
    filePath = r"./dataset/bro_notice.log-20180410"
    a = convert_2018_2020_data(filePath)
    filePath1 = r"./dataset/notice.00:00:00-01:00:00-20220105.log"
    b = convert_2020_2022_data(filePath1)


    # create database
    if create_db(db,url):
        exit(1)
    

    # insert data from 2018-2020 to database
    j1 = 0
    for item in a:
        j1 += 1
        if j1 == testNum:
            break
        if insert_to_db(db,url,item):
            exit(1)
    

    # insert data from 2020-2022 to database
    j2 = 0
    for item in b:
        j2 += 1
        if j2 == testNum:
            break
        if insert_to_db(db,url,item):
            exit(1)
    

    print(f"********************************************************************************************************")
    print(f"SUCCESSFULLY INSERTED {j1+j2} LINES OF DATA INTO DATABASE {db}")


    # query database
    query = f"SELECT * FROM {db} LIMIT 3"
    if query_db(url,query):
        exit(1)  
    

    # drop database
    if drop_db(db,url):
        exit(1)

    
    print(f"********************************************************************************************************")
