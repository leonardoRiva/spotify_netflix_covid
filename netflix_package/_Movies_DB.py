import pymongo as pymdb

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

    # def store_week(self, week_df):
    #     week = week_df["week_from"][0]
    #     country_code = self.country_dict[week_df["country"][0]]
    #     movie_docs = [self.movie_subdoc(week_df.iloc[r]) for r in range(len(week_df))]
    #     week_doc = self.db.netflix_chart.find({"week":week}).limit(1)
    #     if week_doc.count() == 1:
    #         if self.db.netflix_chart.find({"week":week, "netflix."+country_code : {"$exists": True}}).count() == 0:
    #             self.db.netflix_chart.update({"week":week}, {"$set": {"netflix."+country_code : self.country_subdoc(country_code, movie_docs)[country_code]}})
    #     else:
    #         week_doc = self.week_doc(week, country_code, movie_docs)
    #         self.db.netflix_chart.insert(week_doc)

    def movie_doc(self, _id, title, genres, keywords, plot_outline):
        movie_doc = {
            "_id": _id,
            "title": title,
            "genres": genres.split(),
            "keywords": keywords.split(),
            "plot outline": plot_outline
        }
        return movie_doc

    def week_doc(self, week, country_code, movie_docs):
        week_doc = {
            "week": week,
            "netflix": self.country_subdoc(country_code, movie_docs)
        }
        return week_doc

    def country_subdoc(self, country_code, movie_docs):
        country_subdoc = {
            country_code: {
                "movies" : movie_docs,
                "kw_sum" : self.sentiment(movie_docs, "kw_sent_sum"),
                "kw_mean" : self.sentiment(movie_docs, "kw_sent_mean"),
                "plot_sum" : self.sentiment(movie_docs, "plot_sent_sum"),
                "plot_mean" : self.sentiment(movie_docs, "plot_sent_mean")
            }
        }
        return country_subdoc

    def movie_subdoc(self, df_row):
        subdoc = {
            "id":df_row["_id"],
            "title": df_row["title"],
            "position": int(df_row["position"]),
            "kw_sent_sum": float(df_row["kw_sent_sum"]),
            "kw_sent_mean": float(df_row["kw_sent_mean"]),
            "plot_sent_sum": float(df_row["plot_sent_sum"]),
            "plot_sent_mean": float(df_row["plot_sent_mean"])
        }
        return subdoc

    def sentiment(self, movie_docs, sent_field):
        somma = 0
        for md in movie_docs:
            somma = somma + md[sent_field]
        return somma/(len(movie_docs)) if len(movie_docs)>0 else 0
