import sqlalchemy
import pandas as  pd 
from sqlalchemy.orm import sessionmaker
import requests
import json
from datetime import datetime
import datetime
import sqlite3

DATABASE_LOCATION = "sqlite://my_played_tracks.sqlite"
USER_ID="Leviaz"
TOKEN = "BQA9kZZVvO_alnNgtpGsHqmVibgZ-ei5D7IWYEAGGTR4pDmKCQ2cRXe3de_6nD8dh2j0ETQG9TyDx7F0rQGMm8h9XucPZgKnFxdXEA5sr8cpiPXmDmJ_A9tMZmvTomhJnoEV2n58v2EuovQ3-IHSVmtQv-b92GVwC1kZ4QcKZih1CbEWIXD8p-uKRUToxAP7enr-lMT0vaa8dJ5PVEV0wn2Q34GZV4L1tMettPmg9Zq6kOQ9EuQOj-pI1dK9Oap84IF7Tss7OJ6_ZLXPfi99Oi9BIf_pcdtBsywY-E7RxG-9XNckJJnL-k6dJ3nB6KQurVvX_dvc1_bMS3dVXYjqxKOPoanIEN11XVaqEoJP7iwboMrIRUlH_eXZvXE"

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
    print(song_df)