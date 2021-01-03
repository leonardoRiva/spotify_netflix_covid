from spotify_package.database._Model import *
from pymongo import MongoClient

import sys, os
sys.path.append(os.path.abspath(os.path.join('..', 'spotify_netflix_covid')))
from variables import *

class Mongo():
    """
    Class ch s occup del interazion load/store with th db mongo
    """
   
    def __init__(self, db_name):
        self.client = MongoClient('mongodb://localhost:27017/')
        self.db = self.client[db_name]
        self.models = Model()
        self.tweets = []
        self.coll_name = 'songs'#'songs_2'

    def store_week(self, week):
        self.store(spotify_collection_name(), week)

    def store_song(self, song, features):
        doc = self.models.song_model(song, features)
        return self.store(self.coll_name, doc)
        
    def store_songs(self, songs):
        docs = [self.models.song_model(x[0], x[1]) for x in songs]
        return self.store_many(self.coll_name, docs)



    def store(self, collection, data):
        _id = self.db[collection].insert_one(data)
        return {**{'_id': _id}, **data}
    
    def store_many(self, collection, data):
        return self.db[collection].insert_many(data)

    def find_unique_song(self, song_id):
        return self.db[self.coll_name].find_one({"song_id": song_id})

    def find_query(self, collection, query={}):
        return self.db[collection].find(query)

    def kill_query(self, collection, query):
        return self.db[collection].delete_many(query)

