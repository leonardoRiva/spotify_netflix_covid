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

        week = df.week_from.unique()[0] # YYYY-MM-DD

        json_df = df.to_json(orient="records")
        parsed = json.loads(json_df)

        

        #SINGLE SONG 
        #songs = [self.create_song(song) for song in parsed]
        #index_spotify = self.create_index(songs)

        #MULTI SONG
        songs, indexs_all, indexs_no_recent, valences, energies, danceabilities = self.add_unique_song_all(parsed, week)
        if len(indexs_all) > 0:
            mean_index_all = np.mean(indexs_all)/3
            median_index_all = np.median(indexs_all)/3
            weight_avg_mean_index_all = self.weighted_average_mean(indexs_all)    

            
        else:
            mean_index_all = None
            median_index_all = None
            weight_avg_mean_index_all = None

        if len(indexs_no_recent) > 0:
            mean_index_no_recent = np.mean(indexs_no_recent)/3
            median_index_no_recent = np.median(indexs_no_recent)/3
            weight_avg_mean_index_no_recent = self.weighted_average_mean(indexs_no_recent)    
        else:
            mean_index_no_recent = None
            median_index_no_recent = None
            weight_avg_mean_index_no_recent = None

        
        if len(valences) > 0:
            mean_valences = np.mean(valences)
            mean_energies = np.mean(energies)
            mean_danceabilities = np.mean(danceabilities)
        else:
            mean_valences = None
            mean_energies = None
            mean_danceabilities = None
 
        min_index_all = np.min(indexs_all)/3
        max_index_all = np.max(indexs_all)/3
        min_indexs_no_recent = np.min(indexs_no_recent)/3
        max_indexs_no_recent = np.max(indexs_no_recent)/3
        #country_name = self.code_to_country(country.lower())
        country_name = country.lower()
        model = {
            "songs": songs,
            "mean_index": mean_index_all,
            "median_index": median_index_all,
            "mean_index_no_recent": mean_index_no_recent,
            "median_index_no_recent": median_index_no_recent,
            "min_all": min_index_all,
            "max_all": max_index_all,
            "min_no_recent": min_indexs_no_recent,
            "max_no_recent": max_indexs_no_recent,
            "weighted_mean_index_no_recent": weight_avg_mean_index_no_recent,
            "weighted_mean_index_all": weight_avg_mean_index_all,
            "mean_valences": mean_valences,
            "mean_energies": mean_energies,
            "mean_danceabilities": mean_danceabilities
        }

        return {country_name: model}
    
    
    def weighted_average_mean(self, values):

        weights = list(range(1, len(values) + 1))[::-1]
        weighted_avg = np.average(values, weights=weights)
        return weighted_avg



    def split_by_number(self, n, l):

        a = []

        for i in range(0, math.ceil(len(l)/n)):
            a.append(l[n*i:n + n*i])
        
        return a


    def add_unique_song_all(self, df, week):
        #get data from db if exists, otherwise None

        query = [self.song_db.find_unique_song(song['URL'].split('/')[-1]) for song in df if song['URL'] is not None] #rimuovere record vuoti (header dei dataset quando concat)

        none_songs = []
        for i, song in enumerate(query):#creates array of song_ids that are not in db
            if song is None:
                none_songs.append(df[i]['URL'].split('/')[-1])

        splitted = self.split_by_number(50, none_songs) #split array in arrays of max len 50 (50 x tracks, 100 x features)

        q = []
        for l in splitted:#get tracks of groups of Ids then flat output
            #q = q + self.spotipy.get_tracks_feature(l)
            q = q + self.spotipy.get_features_date(l)
        
        indexs_all = []
        indexs_no_recent = []

        valences = []
        danceabilities = []
        energies = []

        songs = []
        to_store = []
        for i, song in enumerate(query):#adds features to new songs
            valence = None
            energy = None
            danceability = None

            song_index_all = song_index_no_recent = None
            if song is None:
                features = q.pop(0)#removes first
                if features is not None: # features of that song dont exist
                    to_store.append([df[i], features])
                    #self.song_db.store_song(df[i], features)
                    
                    song_index_all = features['valence'] + features['danceability'] + features['energy']
                    if self.is_song_old(features['release_date'], week): #if song is old
                        song_index_no_recent = song_index_all
                        valence = features['valence']
                        energy = features['energy']
                        danceability = features['danceability']

            else:
                song_index_all = song['features']['valence'] + song['features']['danceability'] + song['features']['energy']
                if self.is_song_old(song['release_date'], week): #if song is old
                    song_index_no_recent = song_index_all
                    valence = song['features']['valence']
                    energy = song['features']['energy']
                    danceability = song['features']['danceability']
            model = {
                "id": df[i]['URL'].split('/')[-1],
                "streams": df[i]['Streams'],
                "position": df[i]['Position'],
                "index": song_index_all,
            }
            songs.append(model)
            if song_index_all is not None:
                indexs_all.append(song_index_all)
            if song_index_no_recent is not None:
                indexs_no_recent.append(song_index_no_recent)

            if valence is not None:
                valences.append(valence)
            if energy is not None:
                energies.append(energy)
            if danceability is not None:
                danceabilities.append(danceability)
        if len(to_store) > 0:
            self.song_db.store_songs(to_store) #stores all new songs w/ features
        #returns list of enhanced songs and total index of single country
        return songs, indexs_all, indexs_no_recent, valences, energies, danceabilities
                

    def is_song_old(self, rel_date, week):
        d1 = datetime.strptime(rel_date, '%Y-%m-%d')
        d2 = datetime.strptime(week, '%Y-%m-%d')

        date_diff = d2-d1
        days_diff = date_diff.days

        if days_diff >= 90: #after 90days a song is declared as Old
            return True
        else:
            return False

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
 
        df = pd.read_json(df_json, orient='records')
        list_week_froms = df.week_from.unique()
        week = list_week_froms[0]
        monday = datetime.strftime((datetime.strptime(week,'%Y-%m-%d') + timedelta(days=3)), '%Y-%m-%d')

        model = {
            "week": monday,
            "spotify": self.model_week(df, week)
        }

        return model
