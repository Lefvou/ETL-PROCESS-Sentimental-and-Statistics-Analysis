
import mysql.connector
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns
import datetime
import os
import pandas_profiling
import webbrowser

## Export stats from yt_records table of Database
def export_stats(q,search_input):
    
    try:    #try if first time else except
        cnx = mysql.connector.connect(host="localhost", user="root", password="")   #create connection with database
        cursor = cnx.cursor()
        cursor.execute("ALTER USER %(name)s@%(host)s IDENTIFIED BY %(passwd)s",{'name': 'root', 'host': 'localhost', 'passwd': 'xmas2020'}) #change user name and password
        cursor.close()
        cnx.close()
        cnx = mysql.connector.connect(host="localhost", user="root", password="xmas2020")   #create connection with the new credentials
    except mysql.connector.errors.ProgrammingError as e:    #   connection with new username password
        cnx = mysql.connector.connect(host="localhost", user="root", password="xmas2020")

    cursor = cnx.cursor()
    dbname = search_input   # name of database equals our search
    cursor.execute("CREATE DATABASE IF NOT EXISTS %s;" % (dbname))  #   Querry to create database if not listed
    cursor.execute("USE %s;" % (dbname))    # using our database
    cursor.execute("GRANT ALL PRIVILEGES ON %s.* TO '%s'@'%s'" % (dbname, 'root', 'localhost')) # all privileges assigned to the user
    
    cursor.execute(q)   #  executes queery that passed to the function
    videoids=cursor.fetchall()  # fetchall the results from the queery
      
    cursor.close()
    cnx.close()
    return videoids #   returns all the video ids from the database

## Insert pointers in yt_details table of Database
def database_insert(dic,p,r,LPV,VPD,DPV,search_input):
    
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
    cursor = cnx.cursor()
  
    tableDB = "yt_details"
    fieldtableDB1 = "p"
    fieldtableDB2 = "r"
    fieldtableDB3 = "LPV"
    fieldtableDB4 = "VPD"
    fieldtableDB5 = "DPV"
    cursor.execute("ALTER TABLE %s DROP COLUMN IF EXISTS %s ,DROP COLUMN IF EXISTS %s ,DROP COLUMN IF EXISTS %s,"   # changes the table to push new collumns and values
                   "DROP COLUMN IF EXISTS %s,DROP COLUMN IF EXISTS %s;" % ("yt_details","p","r","LPV","DPV","VPD"))
    cursor.execute("""ALTER TABLE %s ADD COLUMN %s FLOAT(25) DEFAULT null, ADD COLUMN %s FLOAT(25) DEFAULT null, 
    ADD COLUMN %s FLOAT(25) DEFAULT null, ADD COLUMN %s FLOAT(25) DEFAULT null, ADD COLUMN %s FLOAT(25) DEFAULT null""" # executes queery that changes the collumns
                   % (tableDB,fieldtableDB1, fieldtableDB2,fieldtableDB3, fieldtableDB4,fieldtableDB5))
    for key in dic.keys():
        identity='https://www.youtube.com/watch?v='+key
        try:
            cursor.execute("""UPDATE %s  
                       SET %s = %f                                 
                    where link='%s'""" % (
            tableDB, fieldtableDB1, p[key], identity))
        except KeyError:
                    pass
        try:
            cursor.execute("""UPDATE %s  
                       SET %s = %f                                 
                    where link='%s'""" % (
            tableDB,fieldtableDB2, r[key], identity))
        except KeyError:
                    pass
        try:
            cursor.execute("""UPDATE %s  
                       SET %s = %f 
                                
                    where link='%s'""" % (
            tableDB,fieldtableDB3,LPV[key],identity))
        except KeyError:
                    pass
        
        try:
            cursor.execute("""UPDATE %s  
                       SET %s = %f, 
                           %s = %f     
                    where link='%s'""" % (
            tableDB,fieldtableDB4,VPD[key], fieldtableDB5,DPV[key], identity))
        except KeyError:
                    pass
        cnx.commit()
    print("yt_records Inserted!")
    cursor.close()
    cnx.close()

## Standarize calculated pointers
def zscore(k):
    zresult={}
    for i in k.keys() :
        
        mean=np.mean(list(k.values()))  #mean of all the values of k dictionary
        std=np.std(list(k.values()))    #std of all values
       
        zresult[i]=((k[i]-mean)/std)    #new dictionar
       
    return zresult   

## Calculate Pointers-Plot Diagramms-Create final Excel file- Create the Report to html of Xlsx file  
def main(k, search_input):
    
    querry1="SELECT _id FROM `yt_records` GROUP BY _id" #querry that brings only th ids from databases
    querry2="SELECT * FROM yt_records"  # querry that brings all the database
    querry3="SELECT `yt_details`.`link`,`yt_details`.`duration`,`yt_details`.`indicator`,`yt_details`.`ci` FROM `yt_details` ORDER BY `yt_details`.`link`  ASC"
    querry4="Select link, ci from yt_details order by ci desc" # selects link and ci collumns ordered by ci descending
    
    
    ids=export_stats(querry1,search_input)
    vid=export_stats(querry2,search_input)  #executions of queery that gives back items in a list of tupples
    pointers=export_stats(querry3,search_input)
    c=export_stats(querry4,search_input)
    
    p={}
    r={}
    LPV={}
    VPD={}
    DPV={}
    duration={}
    indicator={}
    ci={}
    dic={}
    views={}
    
    for i in ids:
        dic[i[0]]={}    # creation of a dictionary with keys the video ids fetched with the querry1
    
    for i in dic.keys():
        for v in vid:
            if v[1]==i:
                
                dic[i][(datetime.datetime.strptime(v[0], '%d-%m-%Y %H %M %S'))]={
                    "views":v[2],
                    "likes":v[3],    # for every video_id(key1), and timestamp(key2) we insert the values of views,likes dislikes
                    "dislikes":v[4]          
                }
    
    for i in range(len(pointers)):
        duration[pointers[i][0][32:]]=pointers[i][1]    # with the [i,0,32:] we can take the video id from the link wich is stored into database
        indicator[pointers[i][0][32:]]=pointers[i][2]   # and we insert into 3 different dictionaries the values per id
        ci[pointers[i][0][32:]]=pointers[i][3]
    
    for i in dic.keys():    # calculation of p,r,LPV,VPD,DPV for every video id which is the keys of the dic dictionary
        dates=sorted(list(dic[i].keys()))   #sorted list from first date [0] to maximum date [length(dates)-1] per id=video (i)
        views[i]=dic[i][dates[len(dates)-1]]['views']   #insert into views dictionary in the field with key the video id (i) the last view count
        try:
                p[i]=((dic[i][dates[len(dates)-1]]['likes']-dic[i][dates[0]]['likes']))/(dic[i][dates[len(dates)-1]]['dislikes']-dic[i][dates[0]]['dislikes'])
        except ZeroDivisionError:
                        pass
        try:
                    
                r[i]=(183*((dic[i][dates[len(dates)-1]]['views']-dic[i][dates[0]]['views'])))/(dic[i][dates[len(dates)-1]]['views'])
        except ZeroDivisionError:
                        pass
        try:
            LPV[i]=((dic[i][dates[len(dates)-1]]['likes']-dic[i][dates[0]]['likes']))/((dic[i][dates[len(dates)-1]]['views']-dic[i][dates[0]]['views']))
        except ZeroDivisionError:
                        pass          
           
        try:    
                VPD[i] =(dic[i][dates[len(dates)-1]]['views']-dic[i][dates[0]]['views'])/k  #k is the number of days given by our Project.py
                DPV[i]=((dic[i][dates[len(dates)-1]]['dislikes']-dic[i][dates[0]]['dislikes']))/((dic[i][dates[len(dates)-1]]['views']-dic[i][dates[0]]['views']))
        except ZeroDivisionError:
                        pass  
            
         
    #%% Last Database Insert

    database_insert(dic,p,r,LPV,VPD,DPV,search_input)
    #%% Diagramms
    mylist=[]
    for i in c[:3]: # take the ids of the 3 top videos by ci
        mylist.append(i[0][32:])    
    for i in c[160:163]:
        mylist.append(i[0][32:])    # take the ids of the 3 last videos by ci

    counter=0
    leg=[]
    for key in dic.keys():
        if key in mylist:
            link="https://www.youtube.com/watch?v="+key
            querry5="select title from db_link_titles where link='%s'" % (link) # querry to retrieve the titles from the database
            title=export_stats(querry5,search_input)
            leg.append(title[0][0].replace('&#39;',"'").replace("&quot;",'"').replace("&amp;","&")) # list to use in the legends of our plots
            y1=[]
            y2=[]
            y3=[]
            
            colors=["black","red","green","blue","yellow","grey"]   # list of colors to apply in plots
            # input(dates)
            dates = sorted(list(dic[key].keys()))   #sorted list from first date [0] to maximum date [length(dates)-1] per id=video (key)
            # input(dates[0])
            for i in dic[key].keys():
                
                y1.append(dic[key][i]['views']-dic[key][dates[0]]['views']) #list with the difference of views between first and last (according to timestamp)
                y2.append(dic[key][i]['likes']-dic[key][dates[0]]['likes']) #list with the difference of likes between first and last (according to timestamp)
                y3.append(dic[key][i]['dislikes']-dic[key][dates[0]]['dislikes'])   #list with the difference of dislikes between first and last (according to timestamp)
    
            plt.figure(1)   #plot views over time
            plt.plot(range(len(y1)),y1,color=colors[counter])
            plt.xlabel("Hour")
            plt.ylabel("Number of views")
            plt.legend(leg, loc=2, prop={"size":7})
    
            
            plt.figure(2)   #plot likes over time
            plt.plot(range(len(y2)),y2,color=colors[counter])
            plt.xlabel("Hour")
            plt.ylabel("Number of likes")
            plt.legend(leg, loc=2, prop={"size":7})
            
            
            plt.figure(3)   #plot dislikes over time
            plt.plot(range(len(y3)),y3,color=colors[counter])
            plt.xlabel("Hour")
            plt.ylabel("Number of dislikes")
            plt.legend(leg, loc=2, prop={"size":7})
            
            
            counter+=1
    #standarize all metrics with zscore
    p=zscore(p)
    r=zscore(r)
    LPV=zscore(LPV)
    VPD=zscore(VPD)
    DPV=zscore(DPV)
    duration=zscore(duration)
    indicator=zscore(indicator)
    ci=zscore(ci)
    
       
     #creation of the dataframe df_deiktes to apply to correlations
    df_deiktes=pd.DataFrame({"p":p,"r":r,"LPV":LPV,"VPD":VPD,"DPV":DPV,"duration":duration,"indicator":indicator,"ci":ci},columns=["p","r","LPV","VPD","DPV","duration","indicator","ci"])
    cor1=df_deiktes.corr(method="pearson")
    cor2=df_deiktes.corr(method="kendall")
    cor3=df_deiktes.corr(method="spearman")
    
    plt.figure(4)
    sns.heatmap(cor1,annot=True, center=0)
    plt.title("Pearson Corellation")
    
    plt.figure(5)
    sns.heatmap(cor2,annot=True, center=0)
    plt.title("Kendall Corellation")
    
    plt.figure(6)
    sns.heatmap(cor3,annot=True, center=0)
    plt.title("Spearman Corellation")
    
    figure = plt.figure(7) 
    alphabets = ["p","r","LPV","VPD","DPV","dur","indi","ci"]
       
    axes = figure.add_subplot(111) 
         
    caxes = axes.matshow(cor1, interpolation ='nearest') 
    figure.colorbar(caxes) 
      
    axes.set_xticklabels(['']+alphabets) 
    axes.set_yticklabels(['']+alphabets) 
    plt.title("Pearson Corellation\n")  
    plt.show()
#%% Creation of excel table with results

    views=zscore(views)
    excel=pd.DataFrame({"views":views,"p":p,"r":r,"LPV":LPV,"VPD":VPD,"DPV":DPV,"duration":duration,"indicator":indicator,"ci":ci},
                       columns=["views","p","r","LPV","VPD","DPV","duration","indicator","ci"])
    excel=excel.sort_values(by='views', ascending=False)
    excel=excel.iloc[0:150,:]   #excel creation with the 150 descenting zscore of views
    excel.to_excel(search_input+'.xlsx')
    df = pd.read_excel(search_input+'.xlsx')
    os.system('start "excel" '+search_input+'.xlsx')
    report=df.profile_report(title='Details_by_Views' , progress_bar = False)
    report.to_file(search_input+'.html')
    webbrowser.open(search_input+'.html')   