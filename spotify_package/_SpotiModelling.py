import pandas as pd
import json
import numpy as np

from spotify_package.database._Mongo import *
from spotify_package._Spotify import *
from spotify_package._Downloader import *
import time
from collections import ChainMap
from datetime import timedelta, datetime, date

from spotify_package._keys import get_credentials

from numpy import mean, median
 




class SpotiModelling():



    def __init__(self, mongo, dic_countries):
        c1, c2 = get_credentials()
        self.spotipy = Spotify(c1, c2)
        self.song_db = mongo #DATABASE OF UNIQUE SONGS
        self.countries = dic_countries #Dictionary of code - country



    def get_week_data(self, df_songs, list_countries):
        df_week = df_songs.dropna()#dataframe has only one week
        data = [self.get_week_country_data(df_week, country)
                for country in list_countries]

        return dict(ChainMap(*data))


    #for each country in that week
    def get_week_country_data(self, df_week, country):

        df = df_week[df_week['country'] == country]
        json_df = df.to_json(orient="records")
        parsed = json.loads(json_df)

        #SINGLE SONG 
        #songs = [self.create_song(song) for song in parsed]
        #index_spotify = self.create_index(songs)

        #MULTI SONG
        songs, indexs_spotify = self.add_unique_song_all(parsed)
        mean_index = mean(indexs_spotify)
        median_index = median(indexs_spotify)
 
        #country_name = self.code_to_country(country.lower())
        country_name = country.lower()
        model = {
            "songs": songs,
            "mean_spotify_index": mean_index,
            "median_spotify_index": median_index
        }

        return {country_name: model}
    
    
    def split_by_hundred(self, l):

        if len(l) < 100:# < 100 TOP CHARTS
            return [l]
        else: #200 TOP CHARTS
            return [l[:100], l[100:]]
        

    def add_unique_song_all(self, df):
        #get data from db if exists, otherwise None

        query = [self.song_db.find_unique_song('songs', song['URL'].split('/')[-1]) for song in df if song['URL'] is not None] #rimuovere record vuoti (header dei dataset quando concat)

        none_songs = []
        for i, song in enumerate(query):#creates array of song_ids that are not in db
            if song is None:
                none_songs.append(df[i]['URL'].split('/')[-1])

        splitted = self.split_by_hundred(none_songs) #split array in arrays of max len 100

        q = []
        for l in splitted:#get tracks of groups of Ids then flat output
            q = q + self.spotipy.get_tracks_feature(l)


        indexs = []
        songs = []
        to_store = []
        for i, song in enumerate(query):#adds features to new songs
            song_index = None
            if song is None:
                features = q.pop(0)#removes first
                if features is not None: # features of that song dont exist
                    to_store.append([df[i], features])
                    #self.song_db.store_song(df[i], features)
                    song_index = features['valence'] + features['danceability'] + features['energy']
            else:
                song_index = song['features']['valence'] + song['features']['danceability'] + song['features']['energy']
            
            model = {
                "id": df[i]['URL'].split('/')[-1],
                "streams": df[i]['Streams'],
                "position": df[i]['Position'],
                "index": song_index
            }
            songs.append(model)
            if song_index is not None:
                indexs.append(song_index)
        if len(to_store) > 0:
            self.song_db.store_songs(to_store) #stores all new songs w/ features
        #returns list of enhanced songs and total index of single country
        return songs, indexs
                
    ##############DISMISSED###################
    
    # def add_unique_song(self, song_id, song):
    #     #check if song_id exist in collection songs

    #     query = self.song_db.find_unique_song('songs', song_id)
    #     if (query is None):  # else add song
    #         features = self.spotipy.get_track_feature(song_id)
    #         if features is None: # not save in db
    #             index = None
    #         else:
    #             index = features['valence'] + features['danceability'] + features['energy']
    #             self.song_db.store_song(song, features)
    #         return index
    #     else:
    #         index = query['features']['valence'] + query['features']['danceability'] + query['features']['energy']
    #         return index

    # def create_song(self, song):
    #     song_id = song['URL'].split('/')[-1]
    #     index = self.add_unique_song(song_id, song)
    #     model = {
    #         "id": song_id,
    #         "streams": song['Streams'],
    #         "position": song['Position'],
    #         "index": index
    #     }
    #     return model

    # def create_index(self, songs):
    #     index = 0
    #     n = 0
    #     for song in songs:
    #         if song['index'] is not None:
    #             index = index + song['index']
    #             n = n + 1

    #     index = index/n
    #     return index


######################################


    def get_country_codes(self):
        return list(self.countries)

    def code_to_country(self, code):
        return self.countries[code.lower()]


    def from_country_to_code(self, country):
        return [label for label in self.countries if self.countries[label] == country][0]


    def model_week(self, df, week):
        list_week_froms = df.week_from.unique()  # gets unique weeks (one)
        list_countries = df.country.unique()  # gets unique countries
        week_data = self.get_week_data(df, list_countries)
        return week_data


    def get_week(self, df_json):
        #week = df['week_from'].iloc[0]

        df = pd.read_json(df_json, orient='records')
        list_week_froms = df.week_from.unique()
        week = list_week_froms[0]
        monday = datetime.strftime((datetime.strptime(week,'%Y-%m-%d') + timedelta(days=3)), '%Y-%m-%d')

        model = {
            "week": monday,
            "spotify": self.model_week(df, week)
        }

        return model
