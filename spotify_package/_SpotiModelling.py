import pandas as pd
import json

from spotify_package.database._Mongo import *
from spotify_package._Spotify import *
from spotify_package._Downloader import *
import time
from collections import ChainMap
from datetime import timedelta, datetime, date

from ../keys.spotify_api import *





class SpotiModelling():



    def __init__(self):

        self.spotipy = Spotify(get_credentials())
        self.song_db = Mongo('progettoDB') #DATABASE OF UNIQUE SONGS
        self.countries = {
        "au": "australia",
        "ar": "argentina",
        "at": "austria",
        "be": "belgium",
        "bg": "bulgaria",
        "br": "brazil",
        "ca": "canada",
        #"cl": "chile",
        "co": "colombia",
        "ch": "switzerland",
        "cz": "czech_republic",
        "dk": "denmark",
        "de": "germany",
        "es": "spain",
        #"ec": "ecuador",
        "ee": "estonia",
        "fi": "finland",
        "fr": "france",
        "gr": "greece",
        "gb": "united_kingdom",
        "hu": "hungary",
        "hk": "hong_kong",
        #"id": "indonesia",
        "in": "india",
        "is": "iceland",
        "ie": "ireland",
        "il": "israel",
        "it": "italy",
        "lt": "lithuania",
        #"lu": "luxembourg",
        "lv": "latvia",
        "jp": "japan",
        "mx": "mexico",
        "my": "malaysia",
        "nl": "netherlands",
        "no": "norway",
        "nz": "new_zealand",
        "pl": "poland",
        "pt": "portugal",
        "ph": "philippines",
        # "py": "paraguay",
        "ro": "romania",
        "ru": "russia",
        "sg": "singapore",
        "se": "sweden",
        #"sk": "slovakia",
        "th": "thailand",
        "tr": "turkey",
        "tw": "taiwan",
        "ua": "ukraine",
        "uy": "uruguay",
        "us": "united_states",
        "vn": "vietnam",
        "za": "south_africa"
    }



    def get_week_data(self, df_songs, list_countries):
        #df_week = df_songs[df_songs['week_from'] == week]
        df_week = df_songs #dataframe has only one week
        data = [self.get_week_country_data(df_week, country)
                for country in list_countries]

        return dict(ChainMap(*data))


    #for each country in that week
    def get_week_country_data(self, df_week, country):

        df = df_week[df_week['country'] == country]
        json_df = df.to_json(orient="records")
        parsed = json.loads(json_df)

        songs = [self.create_song(song) for song in parsed]

        # print(json.dumps(parsed,indent=4))
        # time.sleep(10)
        index_spotify = self.create_index(songs)

        country_name = self.code_to_country(country.lower())
        model = {
            "songs": songs,
            "spotify_index": index_spotify,
        }

        return {country_name: model}


    def create_song(self, song):
        song_id = song['URL'].split('/')[-1]
        index = self.add_unique_song(song_id, song)
        model = {
            "id": song_id,
            "streams": song['Streams'],
            "position": song['Position'],
            "index": index
        }



        return model


    def add_unique_song(self, song_id, song):
        #check if song_id exist in collection songs

        query = self.song_db.find_unique_song('songs', song_id)
        if (query is None):  # else add song
            features = self.spotipy.get_track_feature(song_id)
            if features is None:
                # features = {
                #     'danceability': 0,
                #     'energy': 0,
                #     'loudness': 0,
                #     'mode': 0,
                #     'speechiness': 0,
                #     'acousticness': 0,
                #     'instrumentalness': 0,
                #     'liveness': 0,
                #     'valence': 0,
                #     'tempo': 0
                # }
                index = None
            else:
                index = features['valence'] + features['danceability'] + features['energy']
                self.song_db.store_song(song, features)
            return index
        else:
            index = query['features']['valence'] + query['features']['danceability'] + query['features']['energy']
            return index


    def create_index(self, songs):
        index = 0
        n = 0
        for song in songs:
            if song['index'] is not None:
                index = index + song['index']
                n = n + 1

        index = index/n
        return index





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
        model = {
            "week": week,
            "spotify": self.model_week(df, week)
        }

        return model
