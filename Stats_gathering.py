
from googleapiclient.discovery import build
import mysql.connector
import datetime
from googleapiclient.errors import HttpError as GoogleHttpError

### Keys for ETL Application 
lista_keys=["..DIFFERENT API KEYS..."]

##Setup of youtube servise 
api_service_name = 'youtube'
api_version = 'v3'

## Exports the VideoIds from the Database in order to search for the statistics
def export_stats(search_input):
    
    try:
        cnx = mysql.connector.connect(host="localhost", user="root", password="")
        cursor = cnx.cursor()
        cursor.execute("ALTER USER %(name)s@%(host)s IDENTIFIED BY %(passwd)s",{'name': 'root', 'host': 'localhost', 'passwd': 'xmas2020'})
        cursor.close()
        cnx.close()
        cnx = mysql.connector.connect(host="localhost", user="root", password="xmas2020")
    except mysql.connector.errors.ProgrammingError as e:
        cnx = mysql.connector.connect(host="localhost", user="root", password="xmas2020")

    cursor = cnx.cursor()
    dbname = search_input
    cursor.execute("CREATE DATABASE IF NOT EXISTS %s;" % (dbname))
    cursor.execute("USE %s;" % (dbname))
    cursor.execute("GRANT ALL PRIVILEGES ON %s.* TO '%s'@'%s'" % (dbname, 'root', 'localhost'))
    
    cursor.execute("SELECT _id FROM `yt_records` GROUP BY _id")
    videoids=cursor.fetchall()
    
    
    cursor.close()
    cnx.close()
    return videoids

## Search for the statics of a VideoId
def statistics(vidid,keyid):
    service = build(api_service_name, api_version, developerKey=lista_keys[keyid])  #service with youtube api to retrive stats for specific video id
    request = service.videos().list(
        part="statistics,contentDetails",
        id=vidid
        )
    response = request.execute()
    stat = dict()
    stat[response['items'][0]['id']] = {    #creation dictionary stat for with all that stats for the video id
        "duration": response['items'][0]['contentDetails']['duration'],
        "views": response['items'][0]['statistics']['viewCount'],
        "likes": response['items'][0]['statistics']['likeCount'],
        "dislikes": response['items'][0]['statistics']['dislikeCount']
    }

    return stat, keyid

## insert the statistics of videos in Database table yt_records
def database_insert(stats, search_input):
    
    try:
        cnx = mysql.connector.connect(host="localhost", user="root", password="")
        cursor = cnx.cursor()
        cursor.execute("ALTER USER %(name)s@%(host)s IDENTIFIED BY %(passwd)s",{'name': 'root', 'host': 'localhost', 'passwd': 'xmas2020'})
        cursor.close()
        cnx.close()
        cnx = mysql.connector.connect(host="localhost", user="root", password="xmas2020")
    except mysql.connector.errors.ProgrammingError as e:
        cnx = mysql.connector.connect(host="localhost", user="root", password="xmas2020")

    cursor = cnx.cursor()
    dbname = search_input
    cursor.execute("CREATE DATABASE IF NOT EXISTS %s;" % (dbname))
    cursor.execute("USE %s;" % (dbname))
    cursor.execute("GRANT ALL PRIVILEGES ON %s.* TO '%s'@'%s'" % (dbname, 'root', 'localhost'))

    
    tableDB = "yt_records"
    fieldtableDB1 = "date_hour"
    fieldtableDB2 = "_id"
    fieldtableDB3 = "view_count"
    fieldtableDB4 = "likes"
    fieldtableDB5 = "dislikes"
    cursor.execute(
        "CREATE TABLE IF NOT EXISTS %s (%s varchar(30),%s varchar(30) NOT NULL DEFAULT '',%s INT DEFAULT '0',%s INT DEFAULT '0',%s INT DEFAULT '0');" % (
        tableDB,fieldtableDB1, fieldtableDB2,fieldtableDB3, fieldtableDB4,fieldtableDB5))
    for key in stats.keys():
        cursor.execute("""INSERT INTO %s (%s,%s,%s,%s,%s) VALUES ("%s","%s",%d,%d,%d);""" % (
            tableDB, fieldtableDB1, fieldtableDB2, fieldtableDB3, fieldtableDB4, fieldtableDB5,str(datetime.datetime.now().strftime("%d-%m-%Y %H %M %S")),key, int(stats[key]["views"]), int(stats[key]["likes"]), int(stats[key]["dislikes"])))
        cnx.commit()
    print("yt_records Inserted!")
    cursor.close()
    cnx.close()
    
## Main excecution function for data gathering 
def main(search_input,keyid):

    stats = {}
    vidid=export_stats(search_input)    #expports all video ids insto a list of tuples
    for i in vidid:
        try:
            stat, keyid = statistics(i[0],keyid)
            stats = {**stats, **stat}
        except GoogleHttpError:
            keyid= keyid+1
            print("The key has changed",keyid)
            stat, keyid = statistics(i[0],keyid)
            stats = {**stats, **stat}

    database_insert(stats, search_input)