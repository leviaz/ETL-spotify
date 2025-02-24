import sqlalchemy
import pandas as  pd 
from sqlalchemy.orm import sessionmaker
import requests
import json
from datetime import datetime
import datetime
import sqlite3

DATABASE_LOCATION = "sqlite:///my_played_tracks.sqlite"
USER_ID="Leviaz"
TOKEN = "BQD1LhDVYkit5F-fh5hqhA7kNcOtdRDXlfHGynsTwH1c1h5nGFaMthuGfQQOL3mjjkluQmDzoNoRELfRAoozikaWTOxNOxLbteCeiCH0g0K0oJ4qlXD8E5lrTShf0ti8hRJ-OE-nSJc9tQ3MR1KDyfCS7G8FZy8gi7r9hSiFVwnSpGBH7dcqnwazjZ9WENDp2QCxRS9eHHclPK5Tj_bD8kf8Bvf1XdNDoCbUY2OeS8tvnJx7eWkGDfKcP7wPRkZb1SXq6mgAo2frnuuIiXrRacKQ3lOLygA4n8gOFUjZArNTjzsFqUFOnmoXlF8sYdS27NYfMLjHGLvBzH_-ofsU9M71FPD8ExPKvAM1LBDvcAfowSIzQivDcNaArgM"

def check_dataset(df: pd.DataFrame) -> bool:
    #Verify if dataframe is empty
    if df.empty:
        print("Songs already downloaded, Finishing execution ")
        return False
    #Verify if has more of one song at one timestamp
    if pd.Series(df['played_at']).is_unique:
        pass
    else:
        raise Exception("Primary Key Check is violated")
    #Verify if dataframe is null
    if df.isnull().values.any():
        raise Exception ("Null valued founds")
    
    yesterday = datetime.datetime.now() - datetime.timedelta(days=20)
    yesterday = yesterday.replace(hour=0,minute=0,microsecond=0)
    
    #timestamps =df["timestamp"].to_list()
    #for timestamp in timestamps:
    #    if datetime.datetime.strptime(timestamp,"%Y-%m-%d") > yesterday:
    #        raise Exception("The dataset is out of range of the days")


if __name__=="__main__":
    headers ={
        "Accept":"application/json",
        "Content-Type": "application/json",
        "Authorization" : "Bearer {token}".format(token=TOKEN)
    }

    today = datetime.datetime.now()
    yesterday= today - datetime.timedelta(days=1)
    yesterdayTimeStamp = int(yesterday.timestamp())*1000
    todayTimeStamp = int(today.timestamp())*1000
    
    r = requests.get("https://api.spotify.com/v1/me/player/recently-played?before={time}".format(time=todayTimeStamp),headers=headers)
    
    data=r.json()
    song_names =[]
    artist_names=[]
    played_at_list=[]
    timestamps = []
    
    for song in data["items"]:
        song_names.append(song["track"]["name"])
        artist_names.append(song["track"]["album"]["artists"][0]["name"])
        played_at_list.append(song["played_at"])
        timestamps.append(song["played_at"][0:10])
    
    #Pandas dataframes
    song_dict ={
        "song_name":song_names,
        "artist_name":artist_names,
        "played_at":played_at_list,
        "timestamp":timestamps
    }
    
    song_df = pd.DataFrame(song_dict, columns=["song_name","artist_name","played_at","timestamp"])
    
    if check_dataset(song_df):
        print("Validated data proceed to load stage")
        
        
    #load
    engine = sqlalchemy.create_engine(DATABASE_LOCATION)
    conn = sqlite3.connect("my_played_tracks.sqlite")
    cursor = conn.cursor()
    sql_query="""CREATE TABLE IF NOT EXISTS my_played_tracks(
            song_name VARCHAR(1000),
            artist_name VARVHAR(200),
            played_at VARCHAR(220),
            timestamp VARCHAR(220),
            CONSTRAINT primary_key_constraint PRIMARY KEY (played_at)
    )"""
    
    cursor.execute(sql_query)
    
    # use pandas to overwrite the table with the dataFrame and if data exists just append it
    try:
        song_df.to_sql("my_played_tracks",engine,index=False,if_exists="append")
    except:
        print("Data already loaded")
        
    print(song_df)
    
    conn.close()