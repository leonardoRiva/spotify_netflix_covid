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
import math
import threading
from threading import Thread
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

#------------------------------------------------------------------------------#

    def process_title(self, df_idxs, titles):
        out = []
        for i,t in enumerate(titles):
            id = self.get_movie_key(t)
            minfo = self.get_movies_infos([id])
            out.append({'pos':df_idxs[i], 'info':minfo})
        return out

    def get_movie_key(self, title):
        if title != '':
            movies = []

            try:
                movies = self.ia.search_movie(title)
            except Exception as e:
                pass

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
                if mov.count() == 1:
                    # print("found local")
                    stored = True
                    movie = mov[0]
                else:
                    # print("search imdb")
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
                    try:
                        self.MDB.store_movie(movie["_id"], movie["title"], movie["genres"], movie["keywords"], movie["plot outline"])
                    except Exception as e:
                        pass
            else:
                [out[k].append('') for k in out]
        return out

#------------------------------------------------------------------------------#

    def unique_titles(self, df):
        titles = np.unique(df[df["title"].notnull()]["title"].tolist())
        return titles

    def preprocess_df(self, df):
        titles = df["title"].tolist()
        df[['_id','genres','keywords','plot outline']] = ''

        n_thread = 1
        q_result = []
        q_thread = []
        step = math.floor(len(titles)/n_thread)
        t_bloks = [titles[i:i + step] for i in range(0, len(titles), step)]
        idxs_bloks = [range(0,10)[i:i + step] for i in range(0, 10, step)]

        for ib,tb in enumerate(t_bloks):
            idxs = idxs_bloks[ib]
            q_thread.append(Thread(target=lambda idxs,tb: q_result.extend(self.process_title(idxs, tb)), args=(idxs,tb,)))

        [t.start() for t in q_thread]
        [t.join() for t in q_thread]

        for r in q_result:
            idx = r['pos']
            info = r['info']
            for k in info:
                df.loc[idx,k] = info[k][0]
        return df
