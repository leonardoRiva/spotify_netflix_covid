import numpy as np
import pandas as pd
from . import _functions as FS
from . _FlixPatrol import FlixPatrol
from . _IMDB_Interface import IMDB_Interface

class Netflix_Side():
    """
    Tutta la gestione dei soli dati relativi ai film
    """

    def __init__(self):
        self.FP = FlixPatrol(FS.get_countries())
        self.IMDB = IMDB_Interface()

#------------------------------------------------------------------------------#

    def get_week_doc(self, week):
        week_num = FS.get_week_number(week)
        df_week =  self.get_weeks_df([week_num], add_weeks_date=True, sentiment="keywords")
        #df_week.to_csv(week+"_movie.csv", sep=";", index=False)
        return self.build_week_doc(df_week)

    def get_weeks_df(self, weeks, add_weeks_date=True, sentiment=""):
        df = self.FP.get_weeks_chart(weeks)
        df = self.IMDB.preprocess_df(df)
        if add_weeks_date:
            df = self.add_weeks_date(df)
        if sentiment=="keywords":
            df = self.add_kw_sent(df)
        elif sentiment=="plot":
            df = self.add_plot_sent(df)
        return df

    def enrich_df(self, df, add_weeks_date=True, sentiment="keywords"):
        df = self.IMDB.preprocess_df(df)
        if add_weeks_date:
            df = self.add_weeks_date(df)
        if sentiment=="keywords":
            df = self.add_kw_sent(df)
        elif sentiment=="plot":
            df = self.add_plot_sent(df)
        return df

#------------------------------------------------------------------------------#

    def add_weeks_date(self, df):
        if "week" in df.columns:
            df["week_from"] = df["week"].astype(str).apply(lambda fd: FS.set_week_days(fd, fromto="from"))
            df["week_to"] = df["week"].astype(str).apply(lambda fd: FS.set_week_days(fd, fromto="to"))
        return df

    def add_kw_sent(self, df, mode="sum"):
        if "keywords" in df.columns:
            if mode=="sum":
                df["kw_sent"] = df["keywords"].astype(str).apply(lambda kws: FS.kw_sent_sum(kws))
            elif mode=="mean":
                df["kw_sent"] = df["keywords"].astype(str).apply(lambda kws: FS.kw_sent_mean(kws))
        return df

    def add_plot_sent(df, mode="sum"):
        if "plot outline" in df.columns:
            if mode=="sum":
                df["plot_sent"] = df["plot outline"].astype(str).apply(lambda plot: plot_sent_sum(plot))
            elif mode=="mean":
                df["plot_sent"] = df["plot outline"].astype(str).apply(lambda plot: plot_sent_mean(plot))
        return df

#------------------------------------------------------------------------------#

    def build_week_doc(self, df, sentiment_type="keywords"):
        netflix_week_doc = {}
        for c in FS.get_countries():
            df_country = df[df['country']==c]
            netflix_week_doc[FS.rename_country(c)] = self.build_week_indexes_subdoc(df_country)
        return netflix_week_doc

    def build_week_indexes_subdoc(self, df_country):
        movie_keys = (df_country[df_country["_id"].notnull()]["_id"]).tolist()
        sentiments = (df_country[df_country["kw_sent"].notnull()]["kw_sent"]).tolist() if "kw_sent" in df_country.columns else "no sentiment"
        movies_docs = []
        for i,k in enumerate(movie_keys):
            movies_docs.append({
                "id":movie_keys[i],
                "position": i,
                "keywords_sentiment":sentiments[i]
            })
        return movies_docs
