from variables import *
from IndexNormalizer import *
import pandas as pd
import pymongo
import numpy as np
import math
from datetime import datetime, timedelta
from scipy.stats import linregress


# classe per unire gli indici in una nuova collection

# # quando inizializzata, se la collection non esiste / è vuota,
# # la crea a partire da tutti i documenti presenti nelle 3 collection (settimane in comune)

# # quando notificata da una delle 3 controparti (su una settimana),
# # controlla se anche le altre 2 hanno quella settimana; se sì, carica un documento mergiando i dati (se non già presente)


class Merger:

    def __init__(self):
        self.df = pd.DataFrame()
        client = pymongo.MongoClient("mongodb://localhost:27017/")
        self.db = client[db_name()]
        self.col = self.db[merged_collection_name()]
        self.to_check = ['spotify', 'mobility', 'netflix']
        self.collection_names = collection_names()
        self.init_collection()



    def init_collection(self):
        # se non esiste ancora una collection con i dati mergiati, la crea
        n_merged_documents = self.col.count_documents({})
        n_documents = 1
        for c in self.collection_names:
            n_documents *= self.db[c].count_documents({})
        if n_merged_documents == 0 and n_documents > 0:
            print('[Merged] initializing collection')
            all_weeks = self.get_all_weeks()
            merged = self.merge_data(all_weeks)
            for m in merged:
                self.col.insert_one(m)
            print('[Merged] collection populated')



    def get_all_weeks(self):
        # restituisce una lista con tutte le settimane delle tre collection
        weeks = set()
        for col_name in self.collection_names:
            col = self.db[col_name]
            mydoc = col.find({}, {'week': 1, '_id': 0})
            for d in mydoc:
                weeks.add(d['week'])
        weeks_list = list(weeks)
        weeks_list.sort()
        return weeks_list



    def merge_data(self, weeks):
        # restituisce df completo, con gli indici per settimana e per nazione
        final = []
        collections = []
        for c in self.collection_names:
            collections.append(self.db[c])

        for week in weeks:
            week_doc ={}
            for country in spotify_get_countries_code():
                country_doc = {}
                for i,side in enumerate(self.to_check):

                    try:
                        result = collections[i].find_one({'week': week}, {side: 1, '_id': 0}) # query
                        country_doc[side] = {}

                        keys = set(result[side][country].keys())
                        keys.discard('songs')
                        keys.discard('movies')
                        
                        for k in keys:
                            country_doc[side][k] = result[side][country][k]

                    except:
                        country_doc[side] = None

                week_doc[country] = country_doc
            final.append({'week': week, 'indexes': week_doc})
        return final



    def notify(self, side, doc):
        # se questa settimana è già nella collection merge, la elimina
        result = self.col.find_one({'week': doc['week']})
        if result is not None:
            self.col.delete_one({'week': doc['week']})

        # carica il documento mergiato
        merged = self.merge_data([doc['week']])[0]
        self.col.insert_one(merged)
        
        print('[Merged] week added to the merged collection')



#-------------------


    def mongo_to_csv(self, filename, columns=[]):
        # salva i dati della collection merged_data in un csv, 
        # prendendo solo gli indici passati come parametro (tutti se [])
        self.mongo_to_df()
        df = self.df.copy()
        if columns != []:
            columns = ['country', 'week'] + columns
            to_drop = [x for x in list(df.columns) if x not in columns]
            df = df.drop(to_drop, axis=1)
        df.to_csv(filename, index=False, sep=';')



    def mongo_to_df(self):
        # salva i dati della collection merged_data in un dataframe

        # prende i nomi di tutti gli indici in ogni collection
        sides_columns = [] 
        for i,side in enumerate(self.to_check):
            result = self.db[self.collection_names[i]].find_one({})
            indexes_names = set(result[side]['it'].keys())
            indexes_names.discard('songs')
            indexes_names.discard('movies')
            sides_columns.append(list(indexes_names))

        columns = ['country', 'week'] + sides_columns[0] + sides_columns[1] + sides_columns[2]

        if self.df.empty:
            self.df = pd.DataFrame(columns=columns)
            result = self.col.find({})

            for x in result:
                week = x['week']
                indexes = x['indexes']
                tmp = []

                countries = set(indexes.keys())
                countries.discard('ua')
                countries.discard('ru')

                for country in countries:
                    row = []

                    for i,side in enumerate(indexes[country]):
                        ind = indexes[country][side]

                        if ind is not None:
                            values = [ind[col] for col in sides_columns[i]]
                        else:
                            values = [None for col in sides_columns[i]]

                        row += values

                    tmp.append([country, week] + row)

                self.df = self.df.append(pd.DataFrame(tmp, columns=columns))

            self.sort_df()
            self.smooth_df()


    def sort_df(self):
        self.df = self.df.sort_values(by=['week','country'])
        self.df = self.df.reset_index(drop=True)


    def smooth_df(self):
        df = pd.DataFrame(columns=self.df.columns)

        for country in set(self.df['country']):
            tmp = self.df[self.df['country']==country]
            
            smoothed_country = pd.DataFrame()
            smoothed_country['week'] = list(tmp['week']) #[1:-1])
            smoothed_country['country'] = [country]*len(list(smoothed_country['week']))

            columns = set(tmp.columns)
            columns.discard('week')
            columns.discard('country')

            for col in columns:
                values = list(tmp[col])
                smoothed_country[col] = self.moving_average(values, 3)

            df = df.append(smoothed_country)

        self.df = df.copy()
    

    def moving_average(self, x, w):
        # w = window size, DISPARI!
        x2 = [i for i in x if i is not None]
        sm = [None]*int((w-1)/2) + list(np.convolve(x2, np.ones(w), 'valid') / w) + [None]*int((w-1)/2)

        b = [True if i is not None else False for i in x]
        first = b.index(True)
        last = len(b)-b[::-1].index(True)

        x[first:last] = sm
        return x

# -----------------

        
    def correlation_csv(self, filename, column1, column2):
        # salva in un csv la correlazione tra i due indici passati come parametro
        self.mongo_to_df()
        tmp_df = pd.DataFrame()
        tmp_df['country'] = self.df['country']
        tmp_df[column1] = self.df[column1]
        tmp_df[column2] = self.df[column2]
        tmp_df = tmp_df.dropna()
        tmp_df = self.get_correlations(tmp_df, column1, column2)
        tmp_df.to_csv(filename, index=False, sep=';')


    def get_correlations(self, df, column1, column2):
        df_corr = pd.DataFrame(columns=['country', 'correlation'])
        for c in set(df['country']):
            tmp = df[df['country']==c]
            tmp = tmp.dropna()
            corr = linregress(list(tmp[column1]), list(tmp[column2]))
            df_corr.loc[len(df_corr)] = [c, corr.rvalue]
        df_corr = df_corr.sort_values(by=['correlation'])
        df_corr = df_corr.reset_index(drop=True)
        return df_corr


# --------------


    def every_songs_csv(self, filename):
        df = pd.DataFrame(columns=['country', 'week', 'title', 'artist', 'position', 'song_positivity'])

        songs_info = self.get_songs_dict()

        col = self.db[spotify_collection_name()]
        result = col.find({}, {'week': 1, 'spotify': 1, '_id': 0})

        for doc in result:
            week = doc['week']
            chart = doc['spotify']

            countries = set(chart.keys())
            countries.discard('ua')
            countries.discard('ru')

            for country in countries:
                limit = datetime.strptime(week, '%Y-%m-%d') + timedelta(days=-90)

                tmp = [[country, week, songs_info[song['id']]['title'], songs_info[song['id']]['artist'], song['position'], song['index']] 
                            for song in chart[country]['songs']
                            if (song['id'] in songs_info and datetime.strptime(songs_info[song['id']]['release_date'], '%Y-%m-%d') < limit)]

                df = df.append(pd.DataFrame(tmp, columns=['country', 'week', 'title', 'artist', 'position', 'song_positivity']))

            print('[every song merger] done week: ' + week)

        df = df.reset_index(drop=True)
        df.to_csv(filename, index=False, sep=';')


    def get_songs_dict(self): # dizionario per le info delle canzoni
        col_songs = self.db['songs']
        result = col_songs.find({}, {'song_id': 1, 'release_date': 1, 'artist': 1, 'track_name': 1, '_id': 0})
        songs_info = {s['song_id']: {'release_date': s['release_date'], 'artist': s['artist'], 'title': s['track_name']} for s in result}
        return songs_info


# ----------------


m = Merger()
m.mongo_to_csv('tuttoo.csv', ['mean_index_no_recent', 'mobility_index'])
m.correlation_csv('correlation_mean_index_no_recent.csv', 'mean_index_no_recent', 'mobility_index')
# m.every_songs_csv('singole_canzoni.csv')