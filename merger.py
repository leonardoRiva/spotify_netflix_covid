from variables import *
from IndexNormalizer import *
import pandas as pd
import pymongo
import numpy as np


# classe per unire gli indici in una nuova collection

# # quando inizializzata, se la collection non esiste / è vuota,
# # la crea a partire da tutti i documenti presenti nelle 3 collection (settimane in comune)

# # quando notificata da una delle 3 controparti (su una settimana),
# # controlla se anche le altre 2 hanno quella settimana; se sì, carica un documento mergiando i dati (se non già presente)


class Merger:

    def __init__(self):
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
                            index = result['spotify'][country]['mean_index_no_recent']
                            index2 = result['spotify'][country]['median_index_no_recent']
                            country_doc['spotify'] = {'mean': index, 'median': index2}

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
        df = self.mongo_to_df()
        df = self.sort_df(df)
        df = self.smooth(df)
        df = self.normalize_df(df)
        df = self.add_derivates(df)
        df.to_csv(filename, index=False, sep=';')



    def mongo_to_df(self):
        df = pd.DataFrame(columns=['country', 'week', 'mobility', 'netflix', 'spotify'])
        result = self.col.find({})
        for x in result:
            week = x['week']
            indexes = x['indexes']
            for country in indexes:
                if country not in ['ua', 'ru']:
                    mob_ind = indexes[country]['mobility']
                    net_ind = indexes[country]['netflix']
                    spot_ind = indexes[country]['spotify']

                    if net_ind is not None:
                        net_ind = net_ind['kw_mean']
                    if spot_ind is not None:
                        spot_ind = spot_ind['mean']

                    df.loc[len(df)] = [country, week, mob_ind, net_ind, spot_ind]
        return df



    def sort_df(self, df):
        df = df.sort_values(by=['week','country'])
        df = df.reset_index(drop=True)
        return df



    def smooth(self, df):
        new_df = pd.DataFrame(columns=df.columns)
        for c in set(df['country']):
            tmp = (df[df['country']==c]).copy()

            for t in self.to_check:
                y = list(tmp[t])
                none_indexes = [i for i in range(len(y)) if y[i] is None]
                y_without_none = [e for e in y if e is not None]
                x = list(np.arange(len(y_without_none)))
                y_smoothed = list(np.poly1d(np.polyfit(x,y_without_none,15))(x))
                for i in none_indexes:
                    y_smoothed.insert(i, None)
                tmp[t] = y_smoothed

            new_df = new_df.append(tmp)
        new_df = new_df.reset_index(drop=True)
        return new_df



    def normalize_df(self, df):
        for t in self.to_check:
            df[t] = (df[t] - df[t].min()) / (df[t].max() - df[t].min())
        return df



    def add_derivates(self, df):
        df2 = pd.DataFrame(columns=(df.columns))
        for c in set(df['country']):
            if c not in ['ua', 'ru']:
                tmp = df[df['country']==c].copy()

                for t in self.to_check:
                    tmp[t+'_derivative'] = [None] + list(np.diff(list(tmp[t])))

                tmp['netflix_scarti'] = abs(tmp['netflix_derivative'] - tmp['mobility_derivative'])
                tmp['spotify_scarti'] = abs(tmp['spotify_derivative'] - tmp['mobility_derivative'])

                df2 = pd.concat([df2, tmp])
        return df2



m = Merger()
m.mongo_to_csv('_final.csv')