import pymongo as pymdb


class Movies_DB():

    def __init__(self):
        client = pymdb.MongoClient('localhost:27017')
        self.db = client.movies_db

    def get_db(self):
        return self.db

    def store_week(self, week_df):
        week = week_df["week_from"][0]
        country = week_df["country"][0]
        movie_docs = [self.movie_subdoc(week_df.iloc[r]) for r in range(len(week_df))]
        week_doc = self.db.big_movies.find({"week":week}).limit(1)
        if week_doc.count() == 1:
            if db.big_movies.find({"netflix.austria": {"$exists": true}}).count() > 0:
                self.db.big_movies.update({"week":week}, {"$set": {"netflix" : {country : self.country_subdoc(country, movie_docs)}}})
        else:
            week_doc = self.week_doc(week, country, movie_docs)
            self.db.big_movies.insert(week_doc)

    def week_doc(self, week, country, movie_docs):
        week_doc = {
            "week": week,
            "netflix": {
                country: self.country_subdoc(country, movie_docs)
            }
        }
        return week_doc

    def country_subdoc(self, country, movie_docs):
        country_subdoc = {
            "movies" : movie_docs,
            "netflix_index" : self.sentiment_mean(movie_docs)
        }
        return country_subdoc

    def movie_subdoc(self, df_row):
        subdoc = {
            "id":df_row["_id"],
            "position": int(df_row["position"]),
            "keywords_sentiment": float(df_row["kw_sent"])
        }
        return subdoc

    def sentiment_mean(self, movie_docs):
        somma = 0
        for md in movie_docs:
            somma = somma + md["keywords_sentiment"]
        return somma/(len(movie_docs)) if len(movie_docs)>0 else 0
