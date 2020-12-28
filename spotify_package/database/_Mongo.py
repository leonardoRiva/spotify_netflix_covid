from spotify_package.database._Model import *
from pymongo import MongoClient

class Mongo():
    """
    Class ch s occup del interazion load/store with th db mongo
    """
   
    def __init__(self, db_name):
        self.client = MongoClient('mongodb://localhost:27017/')
        self.db = self.client[db_name]
        self.models = Model()
        self.tweets = []

    def store_song_old(self, song):
        doc = self.models.song_model_old(song)
        return self.store('songs', doc)

    def store_week(self, week):
        self.store('spoti_weeks', week)

    def store_song(self, song, features):
        doc = self.models.song_model(song, features)
        return self.store('songs', doc)
        #TEST
        #self.store('songs_v2', doc)
        
        
    def store(self, collection, data):
        #return self.db[collection].insert(data)
        _id = self.db[collection].insert(data)
        return {**{'_id': _id}, **data}
        

    def find_unique_song(self, collection, song_id):
        return self.db[collection].find_one({"song_id": song_id})

    def find_query(self, collection, query):
        return self.db[collection].find(query)

    def kill_query(self, collection, query):
        return self.db[collection].delete_many(query)

