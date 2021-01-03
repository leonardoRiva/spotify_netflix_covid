from variables import *
from indexNormalizer import *
import pandas as pd
import pymongo


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
        self.collection_names = collection_names() #['spoti_weeks', 'covid_weeks']
        self.init_collection()



    def init_collection(self):
        # se non esiste ancora una collection con i dati mergiati, la crea
        n_merged_documents = self.col.count_documents({})
        n_documents = 1
        for c in self.collection_names:
            n_documents *= self.db[c].count_documents({})
        if n_merged_documents == 0 and n_documents > 0:
            print('[Merged] initializing collection')
            common_weeks = self.get_common_weeks()
            merged = self.merge_data(common_weeks)
            for m in merged:
                self.col.insert_one(m)
            print('[Merged] collection populated')



    def get_common_weeks(self):
        # restituisce una lista con le settimane comuni a tutti e tre le collection
        weeks = {}
        for col_name in self.collection_names:
            col = self.db[col_name]
            mydoc = col.find({}, {'week': 1, '_id': 0}).sort('week',-1)
            weeks[col_name] = []
            for d in mydoc:
                weeks[col_name].append(d['week'])

        l = list(weeks.values())
        common_weeks = list(set.intersection(*map(set, l)))
        return common_weeks

    def mongo_to_csv(self):
        df = pd.DataFrame(columns=['country', 'week', 'spotify', 'mobility'])

        result = self.col.find({})
        for x in result:
            week = x['week']
            indexes = x['indexes']
            for country in indexes:
                spot_ind = indexes[country]['spotify']
                mob_ind = indexes[country]['mobility']
                #netflix
                df.loc[len(df)] = [country, week, spot_ind, mob_ind]

        # print(df)
        df.to_csv('data.csv', index=False, sep=';')



    def merge_data(self, common_weeks):
        # restituisce df completo, con gli indici per settimana e per nazione
        final = []

        collections = []
        for c in self.collection_names:
            collections.append(self.db[c])
        
        for week in common_weeks:
            week_doc ={}
            for country in spotify_get_countries_code():
                country_doc = {}
                for i,col in enumerate(collections):
                    result = (col.find({'week': week}, {self.to_check[i]: 1, '_id': 0}))[0] # query
                    try:
                        if self.to_check[i] == 'spotify':
                            index = result['spotify'][country]['median_index_no_recent']
                            index = normalizer(index, country) #normalize index in scale [0;1]
                        elif self.to_check[i] == 'mobility':
                            index = result['mobility'][country]['mobility_index']
                        elif self.to_check[i] == 'netflix':
                            index = result['netflix'][country]['netflix_index']
                    except:
                        index = None
                    country_doc[self.to_check[i]] = index
                week_doc[country] = country_doc
            final.append({'week': week, 'indexes': week_doc})
        return final



    def notify(self, side, week):

        #controlla se questa settimana non sia già presente nella collection
        result = self.col.find({'week': week})
        if len(list(result)):
            return

        # controlla se entrambe le altre due collection hanno questa settimana
        do = True
        to_check = self.to_check.copy()
        to_check.remove(side)
        col_names_dict = collection_names_dict()
        for n in to_check:
            col = self.db[col_names_dict[n]]
            result = col.find({'week': week})
            if not len(list(result)):
                do = False
        
        # se ce l'hanno, carica il documento mergiando
        if do:
            merged = self.merge_data([week])[0]
            self.col.insert_one(merged)
            print('[Merged] week added to merged collection')