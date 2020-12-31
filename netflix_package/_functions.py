import pandas as pd
import datetime
from datetime import timedelta
from textblob import TextBlob

"""
Funzioni di supporto varie:
- from week number to date
- sentiment analysis: keywords, plot
"""

COMMON_COUNTRIES = ['Argentina', 'Australia', 'Austria', 'Belgium', 'Brazil', 'Bulgaria', 'Canada', 'Colombia', 'Czech Republic', 'Denmark', 'Estonia' 'Finland', 'France', 'Germany', 'Greece', 'Hong Kong', 'Hungary', 'Iceland', 'India', 'Ireland', 'Israel', 'Italy', 'Japan', 'Latvia', 'Lithuania', 'Malaysia', 'Mexico', 'Netherlands', 'New Zealand', 'Norway', 'Philippines', 'Poland', 'Portugal', 'Romania', 'Russia', 'Singapore', 'Slovakia', 'South Africa', 'Spain', 'Sweden', 'Switzerland', 'Taiwan', 'Thailand', 'Turkey', 'Ukraine', 'United Kingdom', 'United States', 'Uruguay', 'Vietnam']

def kw_sent_sum(keywords):
    keywords = keywords.split(" ")
    keywords = [TextBlob(kw.replace("-", " ")) for kw in keywords]
    sum = 0
    for kw in keywords:
        sum = sum + kw.sentiment.polarity
    return sum

def kw_sent_mean(keywords):
    return kw_sent_sum(keywords) / len(keywords.split(" "))

def plot_sent_sum(plot):
    if type(plot) is str:
        plot = plot.replace("\n", "")
        plot = plot.replace(".", ".\n")
        tb = TextBlob(plot)
        sum = 0
        for s in tb.sentences:
            sum = s.sentiment.polarity
        return sum
    else:
        return None

def plot_sent_mean(plot):
    if type(plot) is str:
        p = plot.replace("\n", "")
        p = plot.replace(".", ".\n")
        tb = TextBlob(p)
        return plot_sent_sum(plot) / len(tb.sentences)
    else:
        return None

def set_week_days(week, fromto=""):
    d = "2020-W"+str(int(float(week)))+'-1'
    date = datetime.datetime.strptime(d, "%Y-W%W-%w")
    if fromto == "from":
        from_day = date + timedelta(days=-7)
        return str(from_day.date())
    elif fromto == "to":
        to_day = date + timedelta(days=-1)
        return str(to_day.date())
    else:
        return None

def get_week_number(str_date):
    if type(str_date) is str:
        str_date = str_date.split('-')
        return datetime.date(int(str_date[0]), int(str_date[1]), int(str_date[2])).isocalendar()[1]

def get_countries():
    flix_patrol_countries = [(c.replace(' ', '-')).lower() for c in COMMON_COUNTRIES]
    return flix_patrol_countries

def rename_country(country):
    return (country.replace('-','_')).lower()
