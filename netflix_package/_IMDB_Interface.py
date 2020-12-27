# IMDB avaliable movie keys:
#
# 'akas', 'animation department', 'art department', 'art directors', 'aspect ratio', 'assistant directors',
# 'box office', 'camera department', 'canonical title', 'cast', 'casting department', 'casting directors',
# 'certificates', 'cinematographers', 'color info', 'composers', 'costume departmen', 'costume designers',
# 'countries', 'country codes', 'cover url', 'director', 'directors', 'distributors', 'editorial department',
# 'editors', 'full-size cover url', 'genres', 'imdbID', 'kind', 'language codes', 'languages', 'location management',
# 'long imdb canonical title', 'long imdb title', 'make up department', 'miscellaneous', 'music department',
# 'original air date', 'original title', 'other companies', 'plot', 'plot outline', 'producers', 'production companies',
# 'production designers', 'production managers', 'rating', 'runtimes', 'script department', 'set decorators',
# 'smart canonical title', 'smart long imdb canonical title', 'sound department', 'sound mix', 'special effects',
# 'special effects companies', 'stunts', 'synopsis', 'title', 'top 250 rank', 'transportation department', 'visual effects',
# 'votes', 'writer', 'writers', 'year'

import pandas as pd
import numpy as np
import pymongo as pymdb
from imdb import IMDb
from textblob import TextBlob
from . _Movies_DB import Movies_DB

class IMDB_Interface():
    """
    Interfaccia di connessione a IMdb
    recupera info dei film
    """

    def __init__(self, movie_db):
        self.ia = IMDb()
        self.MDB = movie_db
        #client = pymdb.MongoClient('localhost:27017')
        #self.movie_db = MDB.get_db()
        #self.movie_db = client.movies_db
        # mdb = Movies_DB()
        # self.movie_db = mdb.get_db()

#------------------------------------------------------------------------------#

    def get_movie_key(self, title):
        if title != '':
            movies = self.ia.search_movie(title)
            if len(movies)>0:
                return movies[0].movieID
            else:
                return ''
        return ''

    def get_keywords(self, m_key):
        movie = self.ia.get_movie(m_key, info='keywords')
        if "keywords" in movie:
            return movie['keywords']
        else:
            return []

    def get_movies_infos(self, m_keys):
        out = {"_id":[], "title":[], "genres":[], "keywords":[], "plot outline":[]}
        for ik,m_key in enumerate(m_keys):
            if m_key != '':
                movie = None
                stored = False
                mov = self.MDB.find_movie(m_key)
                #mov = self.movie_db.movies.find({"_id": m_key})
                if mov.count() == 1:
                    print("found local")
                    stored = True
                    movie = mov[0]
                else:
                    print("search imdb")
                    movie = self.ia.get_movie(m_key)
                    movie["_id"] = m_key
                    movie["keywords"] = self.get_keywords(m_key)
                for k in out:
                    if k in movie:
                        if (type(movie[k]) is list):
                            movie[k] = ' '.join(movie[k])
                        if (type(movie[k]) is str):
                            movie[k].replace(";",",")
                    else:
                        movie[k] = ''
                [out[k].append(movie[k]) for k in out]
                if not stored:
                    self.MDB.store_movie(movie["_id"], movie["title"], movie["genres"], movie["keywords"], movie["plot outline"])
                    #self.movie_db.movies.insert_one({"_id":movie["_id"], "title":movie["title"], "genres":movie["genres"].split(), "keywords":movie["keywords"].split(), "plot outline":movie["plot outline"]})
            else:
                [out[k].append('') for k in out]
        return out

#------------------------------------------------------------------------------#

    def unique_titles(self, df):
        titles = np.unique(df[df["title"].notnull()]["title"].tolist())
        return titles

    def preprocess_df(self, df):
        titles = df["title"].tolist()
        print("get movie keys from imdb...")
        ids = [self.get_movie_key(t) for t in titles]
        data_from_imdb = self.get_movies_infos(ids)
        for k in data_from_imdb:
            df[k] = data_from_imdb[k]
        return df

#------------------------------------------------------------------------------#

    # def get_movies_field(self, m_keys, kfield):
    #     out = {kfield: []}
    #     for m_key in m_keys:
    #         if m_key != '':
    #             mov = self.movie_db.movies.find({"_id": m_key, kfield:{"$exists": True}})
    #             if mov.count()==0:
    #                 movie = self.ia.get_movie(m_key)
    #                 out[kfield].append(movie[kfield])
    #                 #upsert
    #             else:
    #                 movie = mov[0]
    #                 out[kfield].append(movie[kfield])
    #         else:
    #             out[kfield].append('')
    #     return out
    #
    # def add_movie_field(self, df, kfield):
    #     ids = df['ids'].tolist()
    #     field_data = self.get_movies_field(ids,kfield)
    #     df[kfiel]=field_data[kfield]
    #     return df
