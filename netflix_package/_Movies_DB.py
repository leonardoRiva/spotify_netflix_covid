import pymongo as pymdb
from . import genre_sentiment as GS
from . import _functions as FS

class Movies_DB():

    def __init__(self, GLV):
        client = pymdb.MongoClient('localhost:27017')
        self.db = client[GLV.db_name()]
        self.country_dict =  GLV.netflix_countries_codes()

    def find_movie(self, movie_id):
        movie = self.db.movies.find({"_id": movie_id})
        return movie

    def store_movie(self, _id, title, genres, keywords, plot_outline):
        self.db.movies.insert_one(self.movie_doc(_id, title, genres, keywords, plot_outline))
        return True

    def store_week_doc(self, week, country_dfs):
        country_codes = [self.country_dict[country_df["country"][0]] for country_df in country_dfs]
        country_movies_docs = [[self.movie_subdoc(country_df.iloc[r]) for r in range(len(country_df))] for country_df in country_dfs]
        week_doc = {
            'week': week,
            'netflix': {c_code: self.country_subdoc(c_code, country_movie_docs)[c_code] for c_code,country_movie_docs in zip(country_codes,country_movies_docs)}
        }
        self.db.netflix_chart.insert(week_doc)
        return week_doc

    def movie_doc(self, _id, title, genres, keywords, plot_outline):
        movie_doc = {
            "_id": _id,
            "title": title,
            "genres": genres.split(),
            "keywords": keywords.split(),
            "plot outline": plot_outline,
            "kw_sent": FS.kw_sent_mean(keywords),
            "plot_sent": FS.plot_sent_mean(plot_outline),
            "genre_sent": sum([GS.zgsent[g] for g in genres.split()])/len(genres.split()) if len(genres.split())>0 else None
        }
        return movie_doc

    def week_doc(self, week, country_code, movie_docs):
        week_doc = {
            "week": week,
            "netflix": self.country_subdoc(country_code, movie_docs)
        }
        return week_doc

    def country_subdoc(self, country_code, movie_docs):
        indexes = self.country_indexes(movie_docs)
        country_subdoc = {
            country_code: {
                "movies" : movie_docs,
                "meanp_gsent_noout": indexes['meanp_gsent_noout'],
                "gsent_cohesion": indexes['gsent_cohesion'],
                "genre_popularity": indexes['genre_popularity']
            }
        }
        return country_subdoc

    def movie_subdoc(self, df_row):
        genres = (df_row['genres']).split()
        subdoc = {
            "id":df_row["_id"],
            "title": df_row["title"],
            "position": int(df_row["position"]),
            "genres": genres,
            "gsent": sum([GS.zgsent[g] for g in genres])/len(genres) if len(genres)>0 else None
        }
        return subdoc

    def sentiment(self, movie_docs, sent_field):
        somma = 0
        for md in movie_docs:
            somma = somma + md[sent_field]
        return somma/(len(movie_docs)) if len(movie_docs)>0 else 0

    def country_indexes(self, movie_docs):
        gchart = {}
        cchart = {}
        for p,m in enumerate(movie_docs):
            for g in m['genres']:
                score = ((12-(p+1))/3) if (p+1)<10 else 1
                gchart[g] = gchart[g]+score if g in gchart else score
                cchart[g] = cchart[g]+1 if g in cchart else 1

        somma = 0
        count = 0
        for g in gchart:
            if g in GS.zgsent:
                somma = (somma + (GS.zgsent[g] * gchart[g]))
                count = count + gchart[g]
        meanp_noout = somma / count if count>0 else 0

        cchart = dict(sorted(cchart.items(), key=lambda item: item[1]))
        cc = []
        for i,k in enumerate(reversed(cchart)):
            if i < 10:
                cc.append(k)
        distTK=[]
        for ic,g in enumerate(cc):
            if ic < 5:
                if g in GS.zgsent:
                    distTK.append(GS.zgsent[g])
        stk=0
        for ik,tk in enumerate(distTK):
            if ik>0:
                stk = stk + abs(distTK[ik-1]-distTK[ik])
        mstk=stk/(len(distTK)-1) if len(distTK)>1 else None

        indexes = {
            'meanp_gsent_noout': meanp_noout,
            'gsent_cohesion': mstk,
            'genre_popularity': gchart
        }
        return indexes
