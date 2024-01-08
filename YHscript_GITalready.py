import json
from googleapiclient.discovery import build
import pymongo
import mysql.connector
import pandas as pd
import streamlit as st
import re
#Connecting to Youtube API
def myapifunc():
    api_key = "AIzaSyCrPMRtpWN4YO3XiyFZ-qe_qa4-G226iN0"
    servicename="Youtube"
    apiversion="v3"
    ytbuild = build(servicename,apiversion,developerKey=api_key)
    return ytbuild

DatafromApi = myapifunc()
#==================Fetching channel and video details - Function definitions===================
def FetchDetails(ChannelId):
    # Fetching channel details
    detailrequest=DatafromApi.channels().list(
            part="snippet,contentDetails,statistics",
            id=ChannelId)
    CollectedData = detailrequest.execute()
    for i in CollectedData["items"]:
        Datacoll = dict(
            ChannelID=i["id"],
            ChannelTitle=i["snippet"]["title"],
            ChannelDescription = i["snippet"]["description"],
            TotalViews = i["statistics"]["viewCount"],
            SubscribersCount = i["statistics"]["subscriberCount"],
            TotalVideosPosted=i["statistics"]["videoCount"],
            UploadID=i["contentDetails"]["relatedPlaylists"]["uploads"]
        )
    return Datacoll

def FetchVideoDetails(channeldatabase):
    # Fetching video details
    nextpagetoken = None #Pagetoken parameter used to extract all videos in the channel - this is none for 1st and last video
    videoIDs=[]
    detailrequest = DatafromApi.channels().list(
        part="contentDetails",
        id=channeldatabase)
    CollectedData = detailrequest.execute()
    for i in CollectedData["items"]:
        while True:
            reqDetails_videolevl = (DatafromApi.playlistItems().list(
                part="snippet",
                playlistId=i["contentDetails"]["relatedPlaylists"]["uploads"],
                maxResults=50,
                pageToken=nextpagetoken)).execute()
            for j in (reqDetails_videolevl['items']):
                eachchannelvideo=(j['snippet']['resourceId']['videoId'])
                nextpagetoken=reqDetails_videolevl.get('nextPageToken') #Using get here because if page token value not present(in last page), it will return none instead of error
                videoIDs.append(eachchannelvideo)
            if nextpagetoken is None:
                break

    return videoIDs
def Fetchinnervideo(videoIds):
    #fetching video inner details
    videoInnerDetList=[]
    Myvideodict={}
    i=1
    for k in videoIds:
        videoInnerDetails=(DatafromApi.videos().list(
            part="snippet, ContentDetails, statistics",
            id=k
        ))
        videoInnerDet=videoInnerDetails.execute()
        for items in videoInnerDet["items"]:
            vartempname = "Video_" + str(i)
            vartempdata=dict(
                channelTitle=items["snippet"]["channelTitle"],
                channelId=items["snippet"]["channelId"],
                videoId=items["id"],
                videoTitle=items["snippet"]["title"],
                videoTags=items["snippet"].get("tags"), # used get() because some videos doesn't have tags which will give error
                videoThumbnails=items["snippet"]["thumbnails"]["default"]["url"],
                videoDescription=items["snippet"]["description"],
                videoPublishedDate=items["snippet"]["publishedAt"],
                videoDuration=items["contentDetails"]["duration"],
                videoDefinition=items["contentDetails"]["definition"],
                videoCaption=items["contentDetails"]["caption"],
                videoViews=items["statistics"]["viewCount"],
                videoFavs=items["statistics"]["favoriteCount"],
                videoLikesCount=items["statistics"].get("likeCount"),#because some videos will not have likes count
                videoCommentsCount=items["statistics"].get("commentCount"), #because some videos will not have comments
                Comments=commentDetails(items["id"])
            )
            Myvideodict.update(
                {
                    vartempname:vartempdata
                }
            )
            i+=1
    return Myvideodict

def commentDetails(videoid):
    # Fetching comments details
    try:
        CommentDataset=[]
        commentreq=(DatafromApi.commentThreads().list(
            part="snippet",
            videoId=videoid,
            maxResults=50
        ))
        commentresp=commentreq.execute()
        CommentDataset={}
        i=1
        for items in commentresp["items"]:
            CommentData=dict(
                topLvlCommentid=items["snippet"]["topLevelComment"]["id"],
                topLvlVideoid=items["snippet"]["topLevelComment"]["snippet"]["videoId"],
                topLvltextDisplay=items["snippet"]["topLevelComment"]["snippet"]["textDisplay"],
                topLvlauthorName=items["snippet"]["topLevelComment"]["snippet"]["authorDisplayName"],
                topLvlCommentDate=items["snippet"]["topLevelComment"]["snippet"]["publishedAt"],
                topLvlCommentLikes=items["snippet"]["topLevelComment"]["snippet"]["likeCount"]
            )
            commentname="comment_" + str(i)
            CommentDataset.update({
                commentname: CommentData
            })
            i+=1
    except: # for ex: in case of disabled comment section
        pass
    return CommentDataset

def playlistDetails(channeldetails):
    #Fetching playlist details
    nextpagetoken = None #Pagetoken parameter used to extract all playlists in the channel - this is none for 1st and last video
    playlstDataset=[]
    while True:
        reqDetails_playlistlevl = (DatafromApi.playlists().list(
        part="snippet,contentDetails",
        channelId=channeldetails['ChannelID'],
        maxResults=50,
        pageToken=nextpagetoken)).execute()
        for j in reqDetails_playlistlevl['items']:
            playListcoll=dict(
                PlaylistID=j["id"],
                ChannelID=j["snippet"]["channelId"],
                ChannelTitle=j["snippet"]["channelTitle"],
                PlaylistTitle=j["snippet"]["title"],
                PlaylistPubDate=j["snippet"]["publishedAt"],
                PlaylistVideoCount=j["contentDetails"]["itemCount"]
                )
            nextpagetoken=reqDetails_playlistlevl.get('nextPageToken') #Using get here because if page token value not present(in last page), it will return none instead of error
            playlstDataset.append(playListcoll)
        if nextpagetoken is None:
            break
    return playlstDataset

#MG squad - UCY6KjrDBN_tIRFT_QNqQbRQ
#mahi UCgVpme-0L6W7ALlaT_Wzv5w
#Cinema ticket - UC7z3Odkn0pDTbZoLSbzmmHA
# DS tamil - UCuI5XcJYynHa5k_lqDzAgwQ
#===================================Import collected data to MongoDB===============================================
MongoConnect=pymongo.MongoClient("localhost:27017")
DatabaseMDB=MongoConnect["Youtube_Harvesting"]
NewCollect = DatabaseMDB["Collected_Data"]

def exportoMDB(channelInput):
    channelDetails=FetchDetails(channelInput) # This function returns channel details
    videoIDsList=FetchVideoDetails(channelInput) # This function returns video IDs of provided channel ids
    videoEntireDetails=Fetchinnervideo(videoIDsList) # This function returns required details of all videos in provided channel ids
    PlaylistDetails = playlistDetails(channelDetails)  # This function returns playlist details of provided channel IDs
    TotalData={
        "channelDetails":channelDetails,
        "videoEntireDetails": videoEntireDetails,
        "PlaylistDetails": PlaylistDetails
    }
    NewCollect.insert_one(TotalData)
    return "Data Exported to MongoDB"

#'======================================CHANNEL TABLE============================================================='
def channel_table():
    #**********************************Connecting to MySQL *********************************************
    mydb = mysql.connector.connect(host="localhost",user="root",password="Anthathi@2398",database="youtube_harvesting")
    Cursorsql=mydb.cursor(buffered=True)
    Cursorsql.execute("Show tables")
    dropping='''drop table if exists channels'''
    Cursorsql.execute(dropping)
    mydb.commit()
    try:
        channel_query="create table if not exists channels(ChannelID varchar(70) primary key,ChannelTitle varchar(80),ChannelDescription text," \
                      "TotalViews bigint, SubscribersCount bigint, TotalVideosPosted int, UploadID varchar(70))"
        Cursorsql.execute(channel_query)
        mydb.commit()

    except:
        print("CHANNEL TABLE ALREADY EXISTS")
    #*********************************************Getting data from MongoDB and converting it to dataframe*********************************************
    Mydatabase=[]
    DatabaseMDB=MongoConnect["Youtube_Harvesting"]
    NewCollect = DatabaseMDB["Collected_Data"]

    for data in NewCollect.find({},{"_id":0,"channelDetails":1}):
        Mydatabase.append(data)
    for i in range(len(Mydatabase)):
        df=pd.DataFrame(Mydatabase[i].values())
        for index,row in df.iterrows():
            insertdata='''insert into channels(
                                               ChannelID,
                                               ChannelTitle,
                                               ChannelDescription,
                                               TotalViews,
                                               SubscribersCount,
                                               TotalVideosPosted,
                                               UploadID ) values(%s,%s,%s,%s,%s,%s,%s)'''
            values=(row['ChannelID'],
                    row['ChannelTitle'],
                    row['ChannelDescription'],
                    row['TotalViews'],
                    row['SubscribersCount'],
                    row['TotalVideosPosted'],
                    row['UploadID'])

            try:
                Cursorsql.execute(insertdata, values)
                mydb.commit()
            except:
                print("Channel Data Already exported to SQL")
#'======================================PLAYLIST TABLE============================================================='
def playlist_table():
    #**********************************Connecting to MySQL *********************************************
    mydbPL = mysql.connector.connect(host="localhost",user="root",password="Anthathi@2398",database="youtube_harvesting")
    CursorsqlPL=mydbPL.cursor(buffered=True)
    CursorsqlPL.execute("Show tables")
    droppingPL='''drop table if exists playlist'''
    CursorsqlPL.execute(droppingPL)
    mydbPL.commit()
    try:
        Playlist_query="create table if not exists playlist(PlaylistID varchar(70) primary key,ChannelID varchar(70),ChannelTitle varchar(80),PlaylistTitle text," \
                      "PlaylistPubDate varchar(70), PlaylistVideoCount int)"
        CursorsqlPL.execute(Playlist_query)
        mydbPL.commit()
    except:
        print("PLAYLIST TABLE ALREADY EXISTS")
    #*********************************************Getting data from MongoDB and converting it to dataframe
    MyPlaylist=[]
    DatabaseMDB=MongoConnect["Youtube_Harvesting"]
    NewCollect = DatabaseMDB["Collected_Data"]

    for obj in NewCollect.find({},{"_id":0,"PlaylistDetails":1}):
        for details in range(len(obj["PlaylistDetails"])):
            MyPlaylist.append(obj["PlaylistDetails"][details])
    dfp=pd.DataFrame(MyPlaylist)
    pd.set_option('display.max_columns', None)
    for index,row in dfp.iterrows():
        insertdataPL='''insert into playlist(
                                           PlaylistID,
                                           ChannelID,
                                           ChannelTitle,
                                           PlaylistTitle,
                                           PlaylistPubDate,
                                           PlaylistVideoCount) values(%s,%s,%s,%s,%s,%s)'''
        values=(row['PlaylistID'],
                row['ChannelID'],
                row['ChannelTitle'],
                row['PlaylistTitle'],
                row['PlaylistPubDate'],
                row['PlaylistVideoCount'])
        try:
            CursorsqlPL.execute(insertdataPL, values)
            mydbPL.commit()
        except:
            print("Playlist Data Already exported to SQL")

    #'======================================VIDEO TABLE============================================================='
def video_table():
    #**********************************Connecting to MySQL *********************************************
    mydbVL = mysql.connector.connect(host="localhost",user="root",password="Anthathi@2398",database="youtube_harvesting")
    CursorsqlVL=mydbVL.cursor(buffered=True)
    CursorsqlVL.execute("Show tables")
    droppingVL='''drop table if exists videolists'''
    CursorsqlVL.execute(droppingVL)
    mydbVL.commit()
    try:
        Video_query='''create table if not exists videolists(
                        channelTitle varchar(100),
                        channelId varchar(100),
                        videoId varchar(30) primary key,
                        videoTitle varchar(100),
                        videoThumbnails varchar(200),
                        videoDescription text,
                        videoPublishedDate text,
                        videoDuration text,
                        videoDefinition varchar(20),
                        videoCaption varchar(70),
                        videoViews bigint,
                        videoFavs bigint,
                        videoLikesCount bigint,
                        videoCommentsCount bigint)'''
        CursorsqlVL.execute(Video_query)
        mydbVL.commit()
    except:
        print("VIDEOLIST TABLE ALREADY EXISTS")
    #*********************************************Getting data from MongoDB and converting it to dataframe
    Myvideolist=[]
    myvideo=""
    DatabaseMDB=MongoConnect["Youtube_Harvesting"]
    NewCollect = DatabaseMDB["Collected_Data"]

    for obj in NewCollect.find({},{"_id":0,"videoEntireDetails":1}):
        for details in range(len(obj["videoEntireDetails"])):
            myvideo = "Video_"+str(details+1)
            Myvideolist.append(obj["videoEntireDetails"][myvideo])
    dfv=pd.DataFrame(Myvideolist)
    pd.set_option('display.max_columns', None)
    for index,row in dfv.iterrows():
        insertdataVL='''insert into videolists(
                                            channelTitle,
                                            channelId,
                                            videoId,
                                            videoTitle,
                                            videoThumbnails,
                                            videoDescription,
                                            videoPublishedDate,
                                            videoDuration,
                                            videoDefinition,
                                            videoCaption,
                                            videoViews,
                                            videoFavs,
                                            videoLikesCount,
                                            videoCommentsCount) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
        txt=str(row['videoDuration'])
        Totalsecs = 0
        H=""
        M=""
        S=""

        if len(txt) > 0:
            H = (re.search(r"(\d+)H", txt))
            M = (re.search(r"(\d+)M", txt))
            S = (re.search(r"(\d+)S", txt))
            if H is not None:
                digaloneH = re.findall("\d+", str(H.group(0)))
                digaloneM = re.findall("\d+", str(M.group(0)))
                digaloneS = re.findall("\d+", str(S.group(0)))
                Totalsecs = int(digaloneH[0])*60*60 + int(digaloneM[0])*60 + int(digaloneS[0])
            elif M is not None:
                digaloneM = re.findall("\d+", str(M.group(0)))
                Totalsecs = int(digaloneM[0])*60
                if S is not None:
                    digaloneS = re.findall("\d+", str(S.group(0)))
                    Totalsecs = int(digaloneM[0])*60 + int(digaloneS[0])
            elif S is not None:
                digaloneS = re.findall("\d+",str(S.group(0)))
                Totalsecs = int(digaloneS[0])
        values=(row['channelTitle'],
                row['channelId'],
                row['videoId'],
                row['videoTitle'],
                row['videoThumbnails'],
                row['videoDescription'],
                row['videoPublishedDate'],
                Totalsecs,
                row['videoDefinition'],
                row['videoCaption'],
                row['videoViews'],
                row['videoFavs'],
                row['videoLikesCount'],
                row['videoCommentsCount'])
        try:
            CursorsqlVL.execute(insertdataVL, values)
            mydbVL.commit()
        except:
            print("Video List Data Already exported to SQL")
#'======================================COMMENTS TABLE============================================================='
def comments_table():
    #**********************************Connecting to MySQL *********************************************
    mydbCL = mysql.connector.connect(host="localhost",user="root",password="Anthathi@2398",database="youtube_harvesting")
    CursorsqlCL=mydbCL.cursor(buffered=True)
    CursorsqlCL.execute("Show tables")
    droppingCL='''drop table if exists commentlists'''
    CursorsqlCL.execute(droppingCL)
    mydbCL.commit()
    try:
        Comment_query='''create table if not exists commentlists(
                    comment_id varchar(255),
                    video_id varchar(255) ,
                    comment_text varchar(600),
                    comment_author varchar(255),
                    comment_published_date text,
                    comment_likes bigint)'''
        CursorsqlCL.execute(Comment_query)
        mydbCL.commit()
    except:
       print("COMMENTLIST TABLE ALREADY EXISTS")
    #*********************************************Getting data from MongoDB and converting it to dataframe
    Mycommentlist=[]
    myvid=""
    mycomment=""
    DatabaseMDB=MongoConnect["Youtube_Harvesting"]
    NewCollect = DatabaseMDB["Collected_Data"]

    for obj in NewCollect.find({},{"_id":0,"videoEntireDetails":1}):
        for details in range(len(obj["videoEntireDetails"])):
            myvid = "Video_"+str(details+1)
            for j in range(len(obj["videoEntireDetails"][myvid]['Comments'])):
                mycomment = "comment_"+str(j+1)
                Mycommentlist.append(obj["videoEntireDetails"][myvid]['Comments'][mycomment])
    dfc=pd.DataFrame(Mycommentlist)
    pd.set_option('display.max_columns', None)

    for index,row in dfc.iterrows():
        insertdataCL= '''insert into commentlists(
                    comment_id,
                    video_id,
                    comment_text,
                    comment_author,
                    comment_published_date,
                    comment_likes) values(%s,%s,%s,%s,%s,%s)'''
        values=(row['topLvlCommentid'],
                    row['topLvlVideoid'],
                    row['topLvltextDisplay'],
                    row['topLvlauthorName'],
                    row['topLvlCommentDate'],
                    row['topLvlCommentLikes'])
        try:
            CursorsqlCL.execute(insertdataCL, values)
            mydbCL.commit()
        except:
            print("Comment List Data Already exported to SQL")
def all_table():
    channel_table()
    playlist_table()
    video_table()
    comments_table()
    return "Migrated successfully!!"

#============================================DATA FRAME FOR STREAM LIT==================================================
def show_st_channels():
    mydb = mysql.connector.connect(host="localhost",user="root",password="Anthathi@2398",database="youtube_harvesting")
    Cursorsq=mydb.cursor(buffered=True)
    Cursorsq.execute('''select * from channels''')
    mydb.commit()
    channelfetch = Cursorsq.fetchall()
    df=pd.DataFrame(channelfetch,columns=["ChannelID","ChannelTitle","ChannelDescription","TotalViews","SubscribersCount","TotalVideosPosted","UploadID"])
    st.write(df)
def show_st_playlist():
    mydb = mysql.connector.connect(host="localhost",user="root",password="Anthathi@2398",database="youtube_harvesting")
    Cursorsq=mydb.cursor(buffered=True)
    Cursorsq.execute('''select * from playlist''')
    mydb.commit()
    channelfetch = Cursorsq.fetchall()
    df=pd.DataFrame(channelfetch,columns=["PlaylistID","ChannelID","ChannelTitle","PlaylistTitle","PlaylistPubDate","PlaylistVideoCount"])
    st.write(df)

def show_st_videolist():
    mydb = mysql.connector.connect(host="localhost",user="root",password="Anthathi@2398",database="youtube_harvesting")
    Cursorsq=mydb.cursor(buffered=True)
    Cursorsq.execute('''select * from videolists''')
    mydb.commit()
    channelfetch = Cursorsq.fetchall()
    df=pd.DataFrame(channelfetch,columns =["channelTitle","channelId","videoId","videoTitle","videoThumbnails","videoDescription","videoPublishedDate","videoDuration","videoDefinition","videoCaption","videoViews","videoFavs","videoLikesCount","videoCommentsCount"])
    st.write(df)

def show_st_commentlist():
    mydb = mysql.connector.connect(host="localhost",user="root",password="Anthathi@2398",database="youtube_harvesting")
    Cursorsq=mydb.cursor(buffered=True)
    Cursorsq.execute('''select * from commentlists''')
    mydb.commit()
    channelfetch = Cursorsq.fetchall()
    df=pd.DataFrame(channelfetch,columns =["comment_id","video_id","comment_text","comment_author","comment_published_date","comment_likesCount"])
    st.write(df)
#================================================STREAMLIT SECTION====================================================='
with st.sidebar:
    st.title(":blue[Youtube Data Harvesting and Warehousing]")
    st.header("Features:")
    st.caption("General Channel Details of any youtube channel")
    st.caption("Playlist Details")
    st.caption("Video Details")
    st.caption("Comment Details")
    st.caption("List of Q&As")
channelInput = st.text_input("Please Enter the Channel ID")
if st.button("Collect channel details and Store in MongoDB"): # Collecting data from youtube and storing in MongoDB
    chanIDList = []
    collection = DatabaseMDB["Collected_Data"]
    for data in collection.find({},{"_id":0,"channelDetails":1}):
        chanIDList.append(data["channelDetails"]["ChannelID"])
    if channelInput in chanIDList:
        st.success("Channel ID's data already exists in Database")
    else:
        insertData=exportoMDB(channelInput)
        st.success(insertData)
if st.button("Migrate collected data to SQL"): # Migrating collected data to SQL
    migrate = all_table()
    st.success(migrate)

if st.button("Select to view tables"): # Showing tables
    # ****************Showing required table to view****************
    Single_Select = st.radio("Select the table you wish to view",("Channels","PlayLists","VideoDetails","CommentDetails"),index=None)

    if Single_Select=="Channels":
        show_st_channels()
    elif Single_Select=="PlayLists":
        show_st_playlist()
    elif Single_Select=="VideoDetails":
        show_st_videolist()
    elif Single_Select=="CommentDetails":
        show_st_commentlist()
if st.checkbox("Select to view Q&A"): # Showing tables    # ****************SQL Query for questions****************
    mydbque = mysql.connector.connect(host="localhost",user="root",password="Anthathi@2398",database="youtube_harvesting")
    Cursorsqlque=mydbque.cursor()
    Selection = st.selectbox("Kindly select the question you wish to view answer for:",("What are the names of all the videos and their corresponding channels?",
                                                                                       "Which channels have the most number of videos, and how many videos do they have?",
                                                                                       "What are the top 10 most viewed videos and their respective channels?",
                                                                                       "How many comments were made on each video, and what are their corresponding video names?",
                                                                                       "Which videos have the highest number of likes, and what are their corresponding channel names?",
                                                                                       "What is the total number of likes for each video, and what are their corresponding video names?",
                                                                                       "What is the total number of views for each channel, and what are their corresponding channel names?",
                                                                                       "What are the names of all the channels that have published videos in the year 2022?",
                                                                                       "What is the average duration of all videos in each channel, and what are their corresponding channel names?",
                                                                                       "Which videos have the highest number of comments, and what are their corresponding channel names?"),index=None,placeholder="Please select an option")
    #QUESTION 1
    if Selection == "What are the names of all the videos and their corresponding channels?":
        mydb = mysql.connector.connect(host="localhost",user="root",password="Anthathi@2398",database="youtube_harvesting")
        Cursorsq=mydb.cursor(buffered=True)
        Cursorsq.execute('''select videoTitle, channelTitle from videolists order by channelTitle''')
        mydb.commit()
        channelfetch = Cursorsq.fetchall()
        df=pd.DataFrame(channelfetch,columns =["videoTitle","channelTitle"])
        st.write(df)
    #QUESTION 2
    elif Selection == "Which channels have the most number of videos, and how many videos do they have?":
        mydb = mysql.connector.connect(host="localhost",user="root",password="Anthathi@2398",database="youtube_harvesting")
        Cursorsq=mydb.cursor(buffered=True)
        Cursorsq.execute('''select channelTitle, TotalVideosPosted from channels order by TotalVideosPosted desc''')
        mydb.commit()
        channelfetch = Cursorsq.fetchall()
        df=pd.DataFrame(channelfetch,columns =["channelTitle","TotalVideosPosted"])
        st.write(df)
    #QUESTION 3
    elif Selection == "What are the top 10 most viewed videos and their respective channels?":
        mydb = mysql.connector.connect(host="localhost",user="root",password="Anthathi@2398",database="youtube_harvesting")
        Cursorsq=mydb.cursor(buffered=True)
        Cursorsq.execute('''select channelTitle, videoTitle, videoViews from videolists where videoViews is not null 
                            order by videoViews desc LIMIT 10''')
        mydb.commit()
        channelfetch = Cursorsq.fetchall()
        df=pd.DataFrame(channelfetch,columns =["channelTitle","videoTitle","videoViewsCount"])
        st.write(df)
    #QUESTION 4
    elif Selection == "How many comments were made on each video, and what are their corresponding video names?":
        mydb = mysql.connector.connect(host="localhost",user="root",password="Anthathi@2398",database="youtube_harvesting")
        Cursorsq=mydb.cursor(buffered=True)
        Cursorsq.execute('''select videoTitle, videoCommentsCount, channelTitle from videolists where videoCommentsCount is not null order by videoCommentsCount desc''')
        mydb.commit()
        channelfetch = Cursorsq.fetchall()
        df=pd.DataFrame(channelfetch,columns =["videoTitle","videoCommentsCount","channelTitle"])
        st.write(df)
    #QUESTION 5
    elif Selection == '''Which videos have the highest number of likes, and what are their corresponding channel names?''':
        mydb = mysql.connector.connect(host="localhost",user="root",password="Anthathi@2398",database="youtube_harvesting")
        Cursorsq=mydb.cursor(buffered=True)
        Cursorsq.execute('''select channelTitle,videoTitle,videoLikesCount from videolists where videoLikesCount is not null 
                            order by videoLikesCount desc''')
        mydb.commit()
        channelfetch = Cursorsq.fetchall()
        df=pd.DataFrame(channelfetch,columns =["channelTitle","videoTitle","videoLikesCount"])
        st.write(df)
    #QUESTION 6
    elif Selection == '''What is the total number of likes for each video, and what are their corresponding video names?''':
        mydb = mysql.connector.connect(host="localhost",user="root",password="Anthathi@2398",database="youtube_harvesting")
        Cursorsq=mydb.cursor(buffered=True)
        Cursorsq.execute('''select videoTitle, videoLikesCount from videolists where videoLikesCount is not Null''')
        mydb.commit()
        channelfetch = Cursorsq.fetchall()
        df=pd.DataFrame(channelfetch,columns =["videoTitle","videoLikesCount"])
        st.write(df)
    #QUESTION 7
    elif Selection == '''What is the total number of views for each channel, and what are their corresponding channel names?''':
        mydb = mysql.connector.connect(host="localhost",user="root",password="Anthathi@2398",database="youtube_harvesting")
        Cursorsq=mydb.cursor(buffered=True)
        Cursorsq.execute('''select ChannelTitle, TotalViews from channels''')
        mydb.commit()
        channelfetch = Cursorsq.fetchall()
        df=pd.DataFrame(channelfetch,columns =["ChannelTitle","TotalViews"])
        st.write(df)
    #QUESTION 8
    elif Selection == "What are the names of all the channels that have published videos in the year 2022?":
        mydb = mysql.connector.connect(host="localhost",user="root",password="Anthathi@2398",database="youtube_harvesting")
        Cursorsq=mydb.cursor(buffered=True)
        Cursorsq.execute('''select DISTINCT channelTitle from videolists where videoPublishedDate LIKE "2022%"''')
        mydb.commit()
        channelfetch = Cursorsq.fetchall()
        df=pd.DataFrame(channelfetch,columns =["Channels published videos in 2022"])
        st.write(df)
    #QUESTION 9
    elif Selection == "What is the average duration of all videos in each channel, and what are their corresponding channel names?":
        mydb = mysql.connector.connect(host="localhost",user="root",password="Anthathi@2398",database="youtube_harvesting")
        Cursorsq=mydb.cursor(buffered=True)
        Cursorsq.execute('''select channelTitle, avg((videoDuration))/60 from videolists group by channelTitle''')
        mydb.commit()
        channelfetch = Cursorsq.fetchall()
        df=pd.DataFrame(channelfetch,columns =["channelTitle","Average Duration in Minutes"])
        st.write(df)
    #QUESTION 10
    elif Selection == "Which videos have the highest number of comments, and what are their corresponding channel names?":
        mydb = mysql.connector.connect(host="localhost",user="root",password="Anthathi@2398",database="youtube_harvesting")
        Cursorsq=mydb.cursor(buffered=True)
        Cursorsq.execute('''select channelTitle, videoTitle, videoCommentsCount from videolists where videoCommentsCount is not null 
                            order by videoCommentsCount desc''')
        mydb.commit()
        channelfetch = Cursorsq.fetchall()
        df=pd.DataFrame(channelfetch,columns =["channelTitle","videoTitle","videoCommentsCount"])
        st.write(df)
