from variables import *
from IndexNormalizer import *
import pandas as pd
import pymongo
import numpy as np
import math
from datetime import datetime, timedelta


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
                for i,col in enumerate(collections):
                    try:
                        result = (col.find({'week': week}, {self.to_check[i]: 1, '_id': 0}))[0] # query

                        if self.to_check[i] == 'spotify':
                            i1 = result['spotify'][country]['mean_valences']
                            i2 = result['spotify'][country]['mean_energies']
                            i3 = result['spotify'][country]['mean_danceabilities']
                            country_doc['spotify'] = {'mean_valences': i1, 'mean_energies': i2, 'mean_danceabilities': i3}

                        elif self.to_check[i] == 'mobility':
                            index = result['mobility'][country]['mobility_index']
                            country_doc['mobility'] = index

                        elif self.to_check[i] == 'netflix':
                            index = result['netflix'][country]['kw_mean']
                            index2 = result['netflix'][country]['plot_mean']
                            country_doc['netflix'] = {'kw_mean': index, 'plot_mean': index2}
                    except:
                        country_doc[self.to_check[i]] = None

                week_doc[country] = country_doc
            final.append({'week': week, 'indexes': week_doc})
        return final



    def notify(self, side, doc):
        # controlla se questa settimana non sia già presente nella collection
        result = self.col.find_one({'week': doc['week']})
        
        if result is not None: # se presente, inserisce il valore notificato (se è null)
            if result['indexes']['it'][side] is None:
                for country in spotify_get_countries_code():
                    result['indexes'][country][side] = doc[side][country][side+'_index']
                self.col.update_one({'week': doc['week']}, {'$set': {'indexes': result['indexes']}})
                print('[Merged] week updated in the merged collection')

        else: # altrimenti, crea un documento mergiando
            merged = self.merge_data([doc['week']])[0]
            self.col.insert_one(merged)
            print('[Merged] week added to the merged collection')



#-------------------


    def mongo_to_csv(self, filename):
        self.mongo_to_df()
        self.df.to_csv(filename, index=False, sep=';')


    def mongo_to_df(self):
        columns = ['country', 'week', 'mobility', 'netflix', 'spotify_valence','spotify_energy','spotify_danceability']
        if self.df.empty:
            self.df = pd.DataFrame(columns=columns)
            result = self.col.find({})
            for x in result:
                week = x['week']
                indexes = x['indexes']
                tmp = []

                for country in indexes:
                    if country not in ['ua', 'ru']:
                        mob_ind = indexes[country]['mobility']
                        net_ind = indexes[country]['netflix']
                        spo_ind = indexes[country]['spotify']

                        if net_ind is not None:
                            net_ind = net_ind['kw_mean']
                        # if spo_ind is not None:
                        #     spo_ind = spo_ind['mean']

                        tmp.append([country, week, mob_ind, net_ind, spo_ind['mean_valences'], spo_ind['mean_energies'], spo_ind['mean_danceabilities']])

                self.df = self.df.append(pd.DataFrame(tmp, columns=columns))
            self.sort_df()


    def sort_df(self):
        self.df = self.df.sort_values(by=['week','country'])
        self.df = self.df.reset_index(drop=True)


# -----------------

        
    def correlation_csv(self, filename):
        self.mongo_to_df()
        tmp_df = self.df.copy()
        del tmp_df['netflix']
        tmp_df = tmp_df.dropna()
        tmp_df = self.get_correlations(tmp_df)
        tmp_df.to_csv(filename, index=False, sep=';')


    def get_correlations(self, df):
        df_corr = pd.DataFrame(columns=['country', 'correlation'])
        for c in set(df['country']):
            tmp = df[df['country']==c]
            corr = np.corrcoef(tmp['mobility'], tmp['spotify'])[0][1]
            df_corr.loc[len(df_corr)] = [c, corr]
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

            countries = list(chart.keys())
            if 'ua' in countries:
                countries.remove('ua')
            if 'ru' in countries:
                countries.remove('ru')

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
m.mongo_to_csv('tutto.csv')
# m.correlation_csv('correlation_mobility_spotify.csv')
# m.every_songs_csv('singole_canzoni.csv')