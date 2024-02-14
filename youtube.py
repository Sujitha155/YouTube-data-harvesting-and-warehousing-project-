from googleapiclient.discovery import build
import pymongo
import psycopg2
import pandas as pd
import streamlit as st


def api_connect():
    Api_id="AIzaSyBCKSEjqi3UilZwrhssxzjbj1luzBzF55Y"
    api_service_name="youtube"
    api_version="v3"
    youtube=build(api_service_name,api_version,developerKey=Api_id)
    return youtube

youtube=api_connect()

def channel_info(c_id):
    request=youtube.channels().list(
                            part="snippet,statistics,contentDetails",
                            id=c_id
    )
    response=request.execute()

    for i in response["items"]:
        data=dict(Channel_name=i["snippet"]["title"],
                Channel_id=i["id"],
                Subscribers=i["statistics"]["subscriberCount"],
                Channel_description=i["snippet"]["description"],
                Views=i["statistics"]["viewCount"],
                Total_videos=i["statistics"]["videoCount"],
                Playlist_id=i["contentDetails"]["relatedPlaylists"]["uploads"]
                )
    return data

def video_ids(channel_id):
    video_ids=[]
    next_page_token=None

    while True:
        response=youtube.channels().list(id=channel_id,
                                                part="contentDetails"
                                                ).execute()
        Playlist_Id=response["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"]
        response1=youtube.playlistItems().list(
                                                        part="snippet",
                                                        playlistId=Playlist_Id,
                                                        maxResults=50,
                                                        pageToken=next_page_token).execute()
        for i in range(len(response1["items"])):
                video_ids.append(response1["items"][i]["snippet"]["resourceId"]["videoId"])
        next_page_token=response1.get("nextPageToken")

        if next_page_token is None:
                break

    return video_ids


def video_info(Video_id):
    video_data=[]
    for video_id in Video_id:
        request=youtube.videos().list(
            part="snippet,contentDetails,statistics",
            id=video_id
        )
        response=request.execute()
        for item in response["items"]:
            data=dict(Channel_name=item["snippet"]["channelTitle"],
                      Channel_id=item["snippet"]["channelId"],
                      Video_Id=item["id"],
                      Title=item["snippet"]["title"],
                      Tags=item["snippet"].get("tags"),
                      Thumbnail=item["snippet"]["thumbnails"]["default"]["url"],
                      Description=item["snippet"].get("description"),
                      Published_date=item["snippet"]["publishedAt"],
                      Duration=item["contentDetails"]["duration"],
                      Views=item["statistics"].get("viewCount"),
                      Likes=item["statistics"].get("likeCount"),
                      Comments=item["statistics"].get("commentCount"),
                      Favorite_count=item["statistics"]["favoriteCount"],
                      Definition=item["contentDetails"]["definition"],
                      Caption_status=item["contentDetails"]["caption"]
                      )
            video_data.append(data)
    return video_data


def comment_info(video_ids):
    Comment_data=[]

    try:
        for video_id in video_ids :
            request=youtube.commentThreads().list(
                                        part="snippet",
                                        videoId=video_id,
                                        maxResults=50
                                        
                        )
                        
            response=request.execute()

            for item in response["items"]:
                            data=dict(Comment_id=item["snippet"]["topLevelComment"]["id"],
                                            Video_Id=item["snippet"]["topLevelComment"]["snippet"]["videoId"],
                                            Comment_text=item["snippet"]["topLevelComment"]["snippet"]["textDisplay"],
                                            Comment_Author=item["snippet"]["topLevelComment"]["snippet"]["authorDisplayName"],
                                            Comment_Published=item["snippet"]["topLevelComment"]["snippet"]["publishedAt"]
                                            )
                            Comment_data.append(data)
               
                        
    except:
            pass
    
    return Comment_data
            
        
        
def get_playlist_details(channel_id):
    playlist_data=[]
    next_page_token=None
    while True:
        request=youtube.playlists().list(
            part="snippet,contentDetails",
            channelId=channel_id,
            maxResults=50,
            pageToken=next_page_token
        )
        response=request.execute()
    
        for item in response["items"]:
            data=dict(Playlist_Id=item['id'],
                    Title=item['snippet']['title'],
                    Channel_id=item['snippet']['channelId'],
                    Channel_name=item['snippet']['channelTitle'],
                    PublishedAt=item['snippet']['publishedAt'],
                    Video_count=item['contentDetails']['itemCount']
                    )
            playlist_data.append(data)
        if next_page_token is None:
            break
    return playlist_data


client=pymongo.MongoClient("mongodb+srv://sujitharajaiah:rajaiah@cluster0.zetzf9x.mongodb.net/?retryWrites=true&w=majority")
database=client["Youtube_data"]


def CHANNEL_DETAILS(channel_id):
    ch_details=channel_info(channel_id)
    pl_details=get_playlist_details(channel_id)
    v_id=video_ids(channel_id)
    v_details=video_info(v_id)
    com_details=comment_info(v_id)

    coll=database["CHANNEL_DETAIL"]
    coll.insert_one({"channel_information": ch_details,"playlist_information": pl_details,
                     "video_information":  v_details,"comment_information":   com_details })
    
    return "completed successfully"


def channels_tables():
    mydb=psycopg2.connect(host="localhost",
                        user="postgres",
                        password="sujitha",
                        database="youtubedatas",
                        port="5433")
    cursor=mydb.cursor()
    drop_query='''drop table if exists channel'''
    cursor.execute(drop_query)
    mydb.commit()

    try:
        create_query='''create table if not exists channel(Channel_name varchar(100),
                                                            Channel_id varchar(80) primary key,
                                                            Subscribers bigint,
                                                            Channel_description text,
                                                            Views bigint,
                                                            Total_videos int,
                                                            Playlist_id varchar(80))'''
        
        cursor.execute(create_query)
        mydb.commit()
    except:
        print("channel table already created")
        
    ch_list=[]
    database=client["Youtube_data"]
    coll=database["CHANNEL_DETAIL"]
    for ch_data in coll.find({},{"_id":0,"channel_information":1}):
        ch_list.append(ch_data["channel_information"])
    df=pd.DataFrame(ch_list)

    for index,row in df.iterrows():
        insert_qu='''insert into channel(Channel_name,
                                        Channel_id,
                                        Subscribers,
                                        Channel_description,
                                        Views,
                                        Total_videos,
                                        Playlist_id)
                                        
                                        values(%s,%s,%s,%s,%s,%s,%s)'''

        values=(row['Channel_name'],
                row['Channel_id'],
                row['Subscribers'],
                row['Channel_description'],
                row['Views'],
                row['Total_videos'],
                row['Playlist_id'])

        try:
            cursor.execute(insert_qu,values)
            mydb.commit()
        except:
            print("channels values are already inserted")


def playlists_table():
    mydb=psycopg2.connect(host="localhost",
                        user="postgres",
                        password="sujitha",
                        database="youtubedatas",
                        port="5433")
    cursor=mydb.cursor()
    drop_query='''drop table if exists playlists'''
    cursor.execute(drop_query)
    mydb.commit()


    create_query='''create table if not exists playlists(Playlist_Id varchar(100) primary key,
                                                        Title varchar(80),
                                                        Channel_id varchar(100),
                                                        Channel_name varchar(100),
                                                        PublishedAt timestamp,
                                                        Video_count int
                                                        )'''

    cursor.execute(create_query)
    mydb.commit()

    pl_list=[]
    database=client["Youtube_data"]
    coll=database["CHANNEL_DETAIL"]
    for pl_data in coll.find({},{"_id":0,"playlist_information":1}):
       for i in range(len(pl_data['playlist_information'])):
         pl_list.append(pl_data['playlist_information'][i])
    df1=pd.DataFrame(pl_list)



    for index,row in df1.iterrows():
            insert_qu='''insert into playlists(Playlist_Id,
                                            Title,
                                            Channel_id,
                                            Channel_name,
                                            PublishedAt,
                                            Video_count)
                            
                                            values(%s,%s,%s,%s,%s,%s)'''
        

            values=(row['Playlist_Id'],
                    row['Title'],
                    row['Channel_id'],
                    row['Channel_name'],
                    row['PublishedAt'],
                    row['Video_count'])

       
            cursor.execute(insert_qu,values)
            mydb.commit()        

    

def videos_table():
    mydb=psycopg2.connect(host="localhost",
                        user="postgres",
                        password="sujitha",
                        database="youtubedatas",
                        port="5433")
    cursor=mydb.cursor()
    drop_query='''drop table if exists videos'''
    cursor.execute(drop_query)
    mydb.commit()

    create_query='''create table if not exists videos(Channel_name varchar(100),
                                                    Channel_id varchar(100),
                                                    Video_Id varchar(30) primary key,
                                                    Title varchar(150),
                                                    Tags text,
                                                    Thumbnail varchar(200),
                                                    Description text,
                                                    Published_date timestamp,
                                                    Duration interval,
                                                    Views bigint,
                                                    Likes bigint,
                                                    Comments int,
                                                    Favorite_count int,
                                                    Definition varchar(10),
                                                    Caption_status varchar(50)
                                                    )'''

    cursor.execute(create_query)
    mydb.commit()

    vi_list=[]
    database=client["Youtube_data"]
    coll=database["CHANNEL_DETAIL"]
    for vi_data in coll.find({},{"_id":0,"video_information":1}):
        for i in range(len(vi_data["video_information"])):
            vi_list.append(vi_data["video_information"][i])
    df2=pd.DataFrame(vi_list)


    for index,row in df2.iterrows():
            insert_qu='''insert into videos(Channel_name,
                                                    Channel_id,
                                                    Video_Id,
                                                    Title,
                                                    Tags,
                                                    Thumbnail,
                                                    Description,
                                                    Published_date,
                                                    Duration,
                                                    Views,
                                                    Likes,
                                                    Comments,
                                                    Favorite_count,
                                                    Definition,
                                                    Caption_status)
                                
                                                values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
        

            values=(row['Channel_name'],
                    row['Channel_id'],
                    row['Video_Id'],
                    row['Title'],
                    row['Tags'],
                    row['Thumbnail'],
                    row['Description'],
                    row['Published_date'],
                    row['Duration'],
                    row['Views'],
                    row['Likes'],
                    row['Comments'],
                    row['Favorite_count'],
                    row['Definition'],
                    row['Caption_status']
                    )

        
            cursor.execute(insert_qu,values)
            mydb.commit()    


def comments_table():
    mydb=psycopg2.connect(host="localhost",
                        user="postgres",
                        password="sujitha",
                        database="youtubedatas",
                        port="5433")
    cursor=mydb.cursor()
    drop_query='''drop table if exists comments'''
    cursor.execute(drop_query)
    mydb.commit()

    create_query='''create table if not exists comments(Comment_id varchar(100) primary key,
                                                        Video_Id varchar(50) ,
                                                        Comment_text text,
                                                        Comment_Author varchar(150) ,
                                                        Comment_Published timestamp
                                                        )'''

    cursor.execute(create_query)
    mydb.commit()


    com_list=[]
    database=client["Youtube_data"]
    coll=database["CHANNEL_DETAIL"]
    for com_data in coll.find({},{"_id":0,"comment_information":1}):
        for i in range(len(com_data["comment_information"])):
            com_list.append(com_data["comment_information"][i])
    df3=pd.DataFrame(com_list)


    for index,row in df3.iterrows():
            insert_qu='''insert into comments(Comment_id,
                                            Video_Id,
                                            Comment_text,
                                            Comment_Author,
                                            Comment_Published
                                            )
                                    
                                        values(%s,%s,%s,%s,%s)'''
                
                
            

            values=(row['Comment_id'],
                    row['Video_Id'],
                    row['Comment_text'],
                    row['Comment_Author'],
                    row['Comment_Published'],
                    )

            try:
                cursor.execute(insert_qu,values)
                mydb.commit()  

            except:
                print("comment detail is already existed")

        
def tables():
    channels_tables()
    playlists_table()
    videos_table()
    comments_table()

    return "tables created successfully"

def show_channels_table():
    ch_list=[]
    database=client["Youtube_data"]
    coll=database["CHANNEL_DETAIL"]
    for ch_data in coll.find({},{"_id":0,"channel_information":1}):
        ch_list.append(ch_data["channel_information"])
    df=st.dataframe(ch_list)

    return df

def show_playlists_table():
    pl_list=[]
    database=client["Youtube_data"]
    coll=database["CHANNEL_DETAIL"]
    for pl_data in coll.find({},{"_id":0,"playlist_information":1}):
        for i in range(len(pl_data['playlist_information'])):
            pl_list.append(pl_data['playlist_information'][i])
    df1=st.dataframe(pl_list)

    return df1

def show_videos_table():
    vi_list=[]
    database=client["Youtube_data"]
    coll=database["CHANNEL_DETAIL"]
    for vi_data in coll.find({},{"_id":0,"video_information":1}):
        for i in range(len(vi_data["video_information"])):
            vi_list.append(vi_data["video_information"][i])
    df2=st.dataframe(vi_list)

    return df2

def show_comments_table():
    com_list=[]
    database=client["Youtube_data"]
    coll=database["CHANNEL_DETAIL"]
    for com_data in coll.find({},{"_id":0,"comment_information":1}):
        for i in range(len(com_data["comment_information"])):
            com_list.append(com_data["comment_information"][i])
    df3=st.dataframe(com_list)

    return df3

with st.sidebar:
    st.title(":red[YOUTUBE DATA HARVESTING AND WAREHOUSING]")
    st.header("Skills Take Away")
    st.caption("Python")
    st.caption("API integration")
    st.caption("Data collection")
    st.caption("Monogdb")
    st.caption("Data management using mongodb and sql")

channel_id=st.text_input("Enter the Channel ID")

if st.button("collect and store data"):

    ch_ids=[]
    database=client["Youtube_data"]
    coll=database["CHANNEL_DETAIL"]
    for ch_data in coll.find({},{"_id":0,"channel_information":1}):
        ch_ids.append(ch_data["channel_information"]["Channel_id"])

    if channel_id in ch_ids:
        st.success("channel details of the given channel id is already exists")
    
    else:
        insert=CHANNEL_DETAILS(channel_id)
        st.success(insert)

if st.button("Migrate to sql"):
    Tables=tables()
    st.success(Tables)

show_tables=st.radio("SELECT THE TABLE FOR VIEW",("CHANNELS","PLAYLISTS","VIDEOS","COMMENTS"))

if show_tables=="CHANNELS":
    show_channels_table()

elif show_tables=="PLAYLISTS":
    show_playlists_table() 

elif show_tables=="VIDEOS":
    show_videos_table() 

elif show_tables=="COMMENTS":
    show_comments_table() 


mydb=psycopg2.connect(host="localhost",
                    user="postgres",
                    password="sujitha",
                    database="youtubedatas",
                    port="5433")
cursor=mydb.cursor()

question=st.selectbox("Select your questions",
                      ("1.All the videos and the channel name",
                       "2.Channels with most number of videos",
                       "3.10 most viewed videos",
                       "4.Comments in each videos",
                       "5.Videos with highest likes",
                       "6.Likes of all videos",
                       "7.Views of each channel",
                       "8.Videos published in the year of 2022",
                       "9.Average duration of all videos in each channel",
                       "10.Videos with highest number of comments"
                       ))

if question=="1.All the videos and the channel name":
    query1='''select title as videos,channel_name as channelname from videos'''
    cursor.execute(query1)
    mydb.commit()
    t1=cursor.fetchall()
    df1=pd.DataFrame(t1,columns=["Video title","Channel name"])
    st.write(df1)

elif question=="2.Channels with most number of videos":
    query2='''select channel_name as channelname,total_videos as no_videos from channel'''
    cursor.execute(query2)
    mydb.commit()
    t2=cursor.fetchall()
    df2=pd.DataFrame(t2,columns=["Channel name","No of videos"])
    st.write(df2)

elif question=="3.10 most viewed videos":
    query3='''select views as views,channel_name as channelname,title as videotitle from videos 
            where views is not null order by views desc limit 10'''
    cursor.execute(query3)
    mydb.commit()
    t3=cursor.fetchall()
    df3=pd.DataFrame(t3,columns=["Views","Channel name","Videotitle"])
    st.write(df3)

elif question=="4.Comments in each videos":
    query4='''select comments as no_comments,title as videotitle from videos where comments is not null'''
    cursor.execute(query4)
    mydb.commit()
    t4=cursor.fetchall()
    df4=pd.DataFrame(t4,columns=["No of comments","Videotitle"])
    st.write(df4)

elif question=="5.Videos with highest likes":
    query5='''select title as videotitle,channel_name as channelname,likes as likecount from videos 
                where likes is not null order by likes desc'''
    cursor.execute(query5)
    mydb.commit()
    t5=cursor.fetchall()
    df5=pd.DataFrame(t5,columns=["Video title","Channel name","likecount"])
    st.write(df5)

elif question=="6.Likes of all videos":
    query6='''select likes as likecount,title as videotitle from videos'''
    cursor.execute(query6)
    mydb.commit()
    t6=cursor.fetchall()
    df6=pd.DataFrame(t6,columns=["likecount","Video title"])
    st.write(df6)

elif question=="7.Views of each channel":
    query7='''select channel_name as channelname,views as totalviews from channel'''
    cursor.execute(query7)
    mydb.commit()
    t7=cursor.fetchall()
    df7=pd.DataFrame(t7,columns=["channel name","Total views"])
    st.write(df7)

elif question=="8.Videos published in the year of 2022":
    query8='''select title as videotitle,published_date as videorelease,channel_name as channelname from videos 
                where extract(year from published_date)=2022'''
    cursor.execute(query8)
    mydb.commit()
    t8=cursor.fetchall()
    df8=pd.DataFrame(t8,columns=["Video title","Published date","channel name"])
    st.write(df8)

elif question=="9.Average duration of all videos in each channel":
    query9='''select channel_name as channelname,AVG(duration) as avgduration from videos group by channel_name'''
    cursor.execute(query9)
    mydb.commit()
    t9=cursor.fetchall()
    df9=pd.DataFrame(t9,columns=["Channelname","Averageduration"])


    T9=[]
    for index,row in df9.iterrows():
        channel_title=row["Channelname"] 
        avg_duration=row["Averageduration"]
        avg_duration_str=str(avg_duration)
        T9.append(dict(channeltitle=channel_title,Average_duration=avg_duration_str))
    df=pd.DataFrame(T9)
    st.write(df)

elif question=="10.Videos with highest number of comments":
    query10='''select channel_name as channelname, title as videotitle,comments as comments from videos
            where comments is not null order by comments desc'''
    cursor.execute(query10)
    mydb.commit()
    t10=cursor.fetchall()
    d10=pd.DataFrame(t10,columns=["Channel name","Video title","No of comments"])
    st.write(d10)


    





