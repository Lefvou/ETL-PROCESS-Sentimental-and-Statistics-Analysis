
import re
from googleapiclient.discovery import build
from youtube_transcript_api import YouTubeTranscriptApi
from textblob import TextBlob
from textblob.sentiments import NaiveBayesAnalyzer
from textblob import Blobber
from sklearn import preprocessing
import numpy as np
import mysql.connector
import statistics as st
import datetime
import Stats_gathering
import Analytics
from googleapiclient.errors import HttpError as GoogleHttpError

### Keys for ETL Application 
lista_keys=["..DIFFERENT API KEYS .."]

##Setup of youtube servise
api_service_name = 'youtube'
api_version = 'v3'
    
### Scanning the search_input of user
def multiple_words(search_input):
    checker = False
    word_list = []
    if len(search_input.split()) > 1:
        checker = True
        for word in search_input.split():
            word_list.append(word)  #appeds word into word list
    else:
        word_list.append(search_input)
    return checker,word_list    #return a boolean value that specifies if there are multiple words or not also returns the world list

### Search videos 
def video_search(search_input, next_page_token,keyid):
    chk, wrdlst = multiple_words(search_input)
    try:
        service = build(api_service_name, api_version, developerKey=lista_keys[keyid])
        request = service.search().list(
        part="snippet",
        maxResults=50,
        q=search_input,
        type='video',
        videoCaption="closedCaption",
        pageToken=next_page_token
        )
        result = request.execute()
        next_page_token = result.get('nextPageToken')
    except GoogleHttpError:
        keyid+=1
        print("The key has changed",keyid)
        return video_search(search_input, next_page_token,keyid)
        
    banlist = ["episode", "compilation", "part"]    #bans videos when their title contains 1 or more of the words in balist
    vid_search_output = {}
    for i in range(len(result['items'])):
        for word in wrdlst:
            if word in result['items'][i]['snippet']['title'].lower():  #keeps videos if title has 1 or more of the words user inputs
                vid_search_output[result['items'][i]['id']['videoId']] = {
                       "title": result['items'][i]['snippet']['title']   }
                break
    for i in vid_search_output:
        for word in banlist:
            if word in vid_search_output[i]['title']:
                print("Found a video with valid Tittle")

    return vid_search_output, next_page_token,keyid

### Search playlist with videos
def playlist_search(search_input,keyid):
    try:
        service = build(api_service_name, api_version, developerKey=lista_keys[keyid])
        request = service.search().list(
        part="snippet",
        maxResults=3,
        q=search_input,
        type='playlist'
        )
        response = request.execute()
    except GoogleHttpError:
        keyid= keyid+1
        print("The key has changed",keyid)
        return playlist_search(search_input,keyid)
    plstId = []
    for i in range(len(response["items"])):
        plstId.append(response["items"][i]["id"]["playlistId"])

    return plstId,keyid

### Search the videos from the selected playlists
def playlist_videos_search(plstId, search_input, next_page_token_playlist,keyid):
    chk, wrdlst = multiple_words(search_input)
    vid_search_output = {}
    try:
        service = build(api_service_name, api_version, developerKey=lista_keys[keyid])
        res = service.playlistItems().list(
        playlistId=plstId,
        part="snippet",
        maxResults=50,
        pageToken=next_page_token_playlist)
        resp = res.execute()
        next_page_token = resp.get('nextPageToken')
    except GoogleHttpError:
        keyid= keyid+1
        print("The key has changed",keyid)
        return playlist_videos_search(plstId, search_input, next_page_token_playlist,keyid)
    for i in range(len(resp["items"])):
        for word in wrdlst:
            if word in resp["items"][i]["snippet"]["title"].lower():
                vid_search_output[resp["items"][i]["snippet"]["resourceId"]["videoId"]] = {
                    "title": resp["items"][i]["snippet"]["title"]}

    return vid_search_output, next_page_token,keyid

### Find the statistics of the total videos
def statistics(vidid,keyid):
    
    service = build(api_service_name, api_version, developerKey=lista_keys[keyid])
    request = service.videos().list(
    part="statistics,contentDetails",
    id=vidid
    )
    response = request.execute()
    stat = dict()
    stat[response['items'][0]['id']] = {
        "duration": response['items'][0]['contentDetails']['duration'],
        "views": response['items'][0]['statistics']['viewCount'],
        "likes": response['items'][0]['statistics']['likeCount'],
        "dislikes": response['items'][0]['statistics']['dislikeCount']
    }
    
    return stat,keyid

### Conversion of the duration to seconds
def YTDurationToSeconds(duration):
    match = re.match("PT(\d+H)?(\d+M)?(\d+S)?", duration).groups()
    hours = parseInt(match[0]) if match[0] else 0
    minutes = parseInt(match[1]) if match[1] else 0
    seconds = parseInt(match[2]) if match[2] else 0
    return hours * 3600 + minutes * 60 + seconds

### Convert string to integer 
def parseInt(string):
    return int("".join([x for x in string if x.isdigit()]))

### Check the videos for the duration in seconds
def check_duration(stats):
    mysum = 0
    for k in stats.keys():
        mysum += stats[k]["duration"]
        MO = mysum / len(stats)
    print("\nVideo average time= ", MO, "sec\n")

    details_dict = {}
    for k in stats.keys():
        if stats[k]["duration"] < (MO + 300) and stats[k]["duration"]> 60:  #keep video ids if their duration smaler range mean.value +300 and biiger than 60 seconds
            details_dict[k] = {
                "duration": stats[k]["duration"],
                "views": stats[k]["views"],
                "likes": stats[k]["likes"],
                "dislikes": stats[k]["dislikes"]}

    return details_dict

### Search for the available subtittles
def subtittles(stats):
    print("Subtittles are dowloading..!\n")
    subs = dict()
    drop_out = []
    indicator = {}
    for j in stats.keys():

        try:
            transcript_list = YouTubeTranscriptApi.list_transcripts(j)  #fetch asubbtitles for j video id
        except Exception as e:

            drop_out.append(j)
            continue

        for transcript in transcript_list:

            print(transcript.video_id, "___", transcript)
            if transcript.is_generated == True and transcript.language_code == 'en':
                sublist = []
                transcript = transcript_list.find_generated_transcript([transcript.language_code])
                capt = ""

                for txt in transcript.fetch():
                    capt = capt + txt['text']
                    sublist.append((re.sub("[\(\[].*?[\)\]]", "", txt['text']).replace("\n", "").replace("♪", ""))) #puts to list text of subtitles replacing some values
                    try:
                        while True:
                            sublist.remove("")

                    except ValueError:
                        pass

                indicator[j] = len(sublist) / stats[j]['duration']  #indicator for quality of subtitle
                if indicator[j] >= 0.1:
                    subs[transcript.video_id] = (re.sub("[\(\[].*?[\)\]]", "", capt)).replace("\n", "").replace("♪", "")
                break
            elif transcript.is_generated == False and transcript.language_code == 'en':
                sublist = []
                transcript = transcript_list.find_manually_created_transcript([transcript.language_code])
                capt = ""

                for txt in transcript.fetch():
                    capt = capt + txt['text']
                    sublist.append((re.sub("[\(\[].*?[\)\]]", "", txt['text']).replace("\n", "").replace("♪", "")))
                    try:
                        while True:
                            sublist.remove("")

                    except ValueError:
                        pass

                indicator[j] = len(sublist) / stats[j]['duration']
                if indicator[j] >= 0.1:
                    subs[transcript.video_id] = (re.sub("[\(\[].*?[\)\]]", "", capt)).replace("\n", "").replace("♪", "")
                break
    return subs, indicator

### NaiveBayes-Sentiment Algorithm 
def sent_analysis_naiveBayes(sub_dic):
    print("NaiveBayes is running...!\n")
    blobber = Blobber(analyzer=NaiveBayesAnalyzer())
    sent_bayes = {}
    for i in sub_dic.keys():
        sub = blobber(sub_dic[i])   #train naive bayes
        sent_bayes[i] = sub.sentiment[1]
    return sent_bayes

### PatternAnalyzer-Sentiment Algorithm 
def sent_analysis_patternAnalyzer(sub_dic):
    print("PatternAnalyzer is running...!\n")
    sent_pattrn = {}
    for i in sub_dic.keys():
        sub = TextBlob(sub_dic[i])
        polarities = [-1, 1, sub.polarity]
        standpol = standarize_pattern_analyzer_polarity(polarities) #standarize values [0,1]
        sent_pattrn[i] = standpol[0]
    return sent_pattrn

### Standarize the PatternAnalyzer's Results to [0,1]
def standarize_pattern_analyzer_polarity(polarities):
    polarities = np.array(polarities).reshape(-1, 1)
    min_max_scaler = preprocessing.MinMaxScaler()
    polarities = min_max_scaler.fit_transform(polarities)
    y = []
    for i in polarities:
        y.append(float(i))
    return y[2:]

### Creation of Database if not exists and insert the datas
def database_insert(details_dict,search_input):
    base_link = "https://www.youtube.com/watch?v="
    try:
        cnx = mysql.connector.connect(host="localhost", user="root", password="")
        cursor = cnx.cursor()
        cursor.execute("ALTER USER %(name)s@%(host)s IDENTIFIED BY %(passwd)s",
                       {'name': 'root', 'host': 'localhost', 'passwd': 'xmas2020'})
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

    tableDB = "db_link_titles"
    fieldtableDB1 = "link"
    fieldtableDB2 = "title"
    cursor.execute(
        "CREATE TABLE IF NOT EXISTS %s (%s varchar(60) NOT NULL DEFAULT '',%s varchar(80) NOT NULL DEFAULT '');" % (
        tableDB, fieldtableDB1, fieldtableDB2))
    for key in details_dict.keys():
        cursor.execute("""INSERT INTO %s (%s,%s) VALUES ("%s","%s");""" % (
        tableDB, fieldtableDB1, fieldtableDB2, base_link + key, details_dict[key]["title"]))
        cnx.commit()
    print("db_link_titles is ready!")

    tableDB = "yt_details"
    fieldtableDB1 = "link"
    fieldtableDB2 = "duration"
    fieldtableDB3 = "view_count"
    fieldtableDB4 = "likes"
    fieldtableDB5 = "dislikes"
    fieldtableDB6 = "indicator"
    fieldtableDB7 = "Bayes"
    fieldtableDB8 = "Pattern"
    fieldtableDB9 = "ci"
    cursor.execute(
        "CREATE TABLE IF NOT EXISTS %s (%s varchar(60) NOT NULL DEFAULT '',%s INT DEFAULT '0',%s INT DEFAULT '0',%s INT DEFAULT '0',%s INT DEFAULT '0', %s FLOAT DEFAULT '0', %s FLOAT DEFAULT '0', %s FLOAT DEFAULT '0', %s FLOAT DEFAULT '0');" % (
            tableDB, fieldtableDB1, fieldtableDB2, fieldtableDB3, fieldtableDB4, fieldtableDB5, fieldtableDB6,
            fieldtableDB7, fieldtableDB8, fieldtableDB9))
    for key in details_dict.keys():
        cursor.execute("""INSERT INTO %s (%s,%s,%s,%s,%s,%s,%s,%s,%s) VALUES ("%s",%d,%d,%d,%d,%f,%f,%f,%f);""" % (
            tableDB, fieldtableDB1, fieldtableDB2, fieldtableDB3, fieldtableDB4, fieldtableDB5, fieldtableDB6,
            fieldtableDB7, fieldtableDB8, fieldtableDB9, base_link + key, details_dict[key]["duration"],
            int(details_dict[key]["views"]), int(details_dict[key]["likes"]), int(details_dict[key]["dislikes"]),
            float(details_dict[key]["indicator"]), float(details_dict[key]["bayes"]),
            float(details_dict[key]["pattern"]), float(details_dict[key]["ci"])))
        cnx.commit()
    print("yt_details Inserted!")

    tableDB = "yt_records"
    fieldtableDB1 = "date_hour"
    fieldtableDB2 = "_id"
    fieldtableDB3 = "view_count"
    fieldtableDB4 = "likes"
    fieldtableDB5 = "dislikes"
    cursor.execute(
        "CREATE TABLE IF NOT EXISTS %s (%s varchar(30),%s varchar(30) NOT NULL DEFAULT '',%s INT DEFAULT '0',%s INT DEFAULT '0',%s INT DEFAULT '0');" % (
            tableDB, fieldtableDB1, fieldtableDB2, fieldtableDB3, fieldtableDB4, fieldtableDB5))
    for key in details_dict.keys():
        cursor.execute("""INSERT INTO %s (%s,%s,%s,%s,%s) VALUES ("%s","%s",%d,%d,%d);""" % (
            tableDB, fieldtableDB1, fieldtableDB2, fieldtableDB3, fieldtableDB4, fieldtableDB5,
            str(datetime.datetime.now().strftime("%d-%m-%Y %H %M %S")), key, int(details_dict[key]["views"]),
            int(details_dict[key]["likes"]), int(details_dict[key]["dislikes"])))
        cnx.commit()
    print("yt_records Inserted!")
    cursor.close()
    cnx.close()

### Main Appliccation Process covering: Search-First Input in Database-Sentiment Analysis  
def main():
    keyid=0 #handler for api keys
    next_page_token = None
    next_page_token_playlist = None

    search_input = input("search: ").lower()    #user input
    looper = int(input("For how long do you want to ask for stats?\n1--> By hour\n2--> Continuously\n"))
    while looper!=1 and looper!=2:    #checks for invalid loop input
        print("Incorrect input, choose the correct number\n")
        looper = int(input("For how long do you want to ask for stats?\n1--> By hour\n2--> Continuously\n"))
    start1 = datetime.datetime.now()
    videos = {}
    stats = {}
    videos_playlist = {}
    while True:
        video, next_page_token, keyid = video_search(search_input, next_page_token,keyid)   #search videos
        videos = {**videos, **video}

        if len(videos) >= 400 or (next_page_token is None): #breaks if more than 400 video ids or page token turn again to None
            break
    playlistId, keyid = playlist_search(search_input,keyid) #search videos playlist
    
    counter = 0
    while True:
        video_playlist, next_page_token_playlist, keyid = playlist_videos_search(playlistId[counter], search_input,
                                                                          next_page_token_playlist,keyid)
        videos_playlist = {**videos_playlist, **video_playlist}
        if next_page_token_playlist is not None:
            continue
        counter += 1
        if counter > 2 or len(videos_playlist) >= 100:  #breaks if searching playlist number 3 or more and if founnd more than 100 videos in a playlist
            print("breaked")
            break
    videos = {**videos, **videos_playlist}
    print("We have all the videos!" )
    stats_counter=0
    for i in videos.keys():
        try:
            stat, keyid = statistics(i,keyid)   #bringing stats for videos kept after subtitle check
            stats = {**stats, **stat}
        except KeyError :
            print(i, "____ Statistics are not available!")
        except GoogleHttpError:
            keyid= keyid+1
            print("The key has changed",keyid)
            stat, keyid = statistics(i,keyid)
            stats = {**stats, **stat}
        stats_counter+=1
    print("We have the videos with enabled Statistics! ")
    for i in stats.keys():
        stats[i]['duration'] = YTDurationToSeconds(stats[i]['duration'])
    stats = check_duration(stats)
    print("We have the total videos after the Check Duration!")
    
    subs, ind = subtittles(stats)
    print("We have the videos with enabled Subs!")
    print("\nSentiment Analysis..!\n")
    start5 = datetime.datetime.now()
    pattern = sent_analysis_patternAnalyzer(subs)
    print("Pattern time: ", (datetime.datetime.now() - start5), " Seconds")
    start6 = datetime.datetime.now()
    sent_bayes = sent_analysis_naiveBayes(subs)
    print("Bayes time: ", (datetime.datetime.now() - start6), " Seconds")
    db_dict = {}
    
    for i in subs.keys():
        db_dict[i] = {
            "title": videos[i]['title'],
            "duration": stats[i]['duration'],
            "likes": stats[i]['likes'],
            "dislikes": stats[i]['dislikes'],
            "views": stats[i]['views'],
            "indicator": ind[i],
            "bayes": sent_bayes[i],
            "pattern": pattern[i],
            "ci": st.mean([sent_bayes[i], pattern[i]])
        }
    print("We have the final result!")    
    
    chk,search=multiple_words(search_input)
    se=""
    for i in search:   #adding multiple words to insert to databse
        se=se+i
    database_insert(db_dict,se)
    print("\nThe Database is Ready!!\n")
    print("Execution time: ", (datetime.datetime.now() - start1), " Seconds")

    return looper,start1,se, keyid
try:
    looper, start1, search, keyid = main()
    loop_counter = 0
    looptime = datetime.datetime.now()
    print("Now we will start gathering the Statistical Details of Videos for 48 times..!")
    print("Depending on user's choice this process will take about 1:30 Hours or two Days")
    ## Extraction of number k linked to user's choice in order to calcuate in a right way the pointers in Analytics.py
    while loop_counter <= 47:   #48 samples if looper = 1 every hour if 2 Continuously approx 1.30 hours
        if looper==1:
            if looptime.hour < datetime.datetime.now().hour:
                print(loop_counter)
                print(looptime)
                Stats_gathering.main(search,keyid)
                looptime = datetime.datetime.now()
                loop_counter += 1
            k=2
        elif looper==2:
                print(loop_counter)
                print(datetime.datetime.now())
                Stats_gathering.main(search,keyid)
                loop_counter += 1
                k=1
    print("We have gathered the necessary Statistics...! ")
    print("Let's calculate the pointers and plot the results..! ")
    Analytics.main(k, search)   #analytics and plots after collecting the saample
    print("It was nice to have you here with us for:",(datetime.datetime.now() - start1))
except IndexError:
    print("\nAll Keys are full..!!! We will see you tomorrow due to Google's Quotas Restrictions..!!")