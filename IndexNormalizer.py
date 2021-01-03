
from spotify_package.database._Mongo import *
# import sys, os
# #sys.path.append(os.path.abspath(os.path.join('..', 'spotify_netflix_covid')))
from variables import *
import pandas as pd
from collections import ChainMap

mongo = Mongo('old_weeksDB')

# (value - min)/(max - min)

data = mongo.find_query('spoti_weeks')
countries1 = get_country_codes()
countries = [
    'nl', 'my', 'mx', 'jp', 'lv', 'lt', 'gb', 'gr', 'fr', 'fi',
    'ee', 'es', 'ro', 'ph', 'pt', 'pl', 'nz', 'no', 'de', 'dk',
    'cz', 'ch', 'co', 'ca', 'za', 'vn', 'us', 'uy', 'it', 'il',
    'ie', 'hk', 'hu', 'br', 'bg', 'be', 'at', 'ar', 'au', 'tw',
    'tr', 'th', 'se', 'sg']

set_dif = set(countries1) - set(countries)
list_dif = list(set_dif)

print(list_dif)
# NOT INCLUDED IN OLD SPOTIFY
#['in', 'ua', 'ru']

# tot_max = []
# tot_min = []

# for doc in data:
#     mins = []
#     maxs = []
#     week = doc['week']
#     for country in doc['spotify']:
#         dic_max = {country: doc['spotify'][country]['max_no_recent']}
#         dic_min = {country: doc['spotify'][country]['min_no_recent']}
#         mins.append(dic_min)
#         maxs.append(dic_max)

#     mmx = dict(ChainMap(*maxs))
#     mmn = dict(ChainMap(*mins))
#     tot_max.append(mmx)
#     tot_min.append(mmn)

# df_max = pd.DataFrame(tot_max, columns=countries)
# df_min = pd.DataFrame(tot_min, columns=countries)

# df_max.to_csv('maxs.csv')
# df_min.to_csv('mins.csv')

df_max = pd.read_csv('maxs.csv')
df_min = pd.read_csv('mins.csv')
max_means = [{country: df_max[country].mean()} for country in countries]
min_means = [{country: df_min[country].mean()} for country in countries]

print(max_means)
# [
#     {"nl": 2.64748},
#     {"my": 2.42824},
#     {"mx": 2.5877600000000003},
#     {"jp": 2.54812},
#     {"lv": 2.5390800000000002},
#     {"lt": 2.56308},
#     {"gb": 2.5532000000000004},
#     {"gr": 2.42888},
#     {"fr": 2.55764},
#     {"fi": 2.766},
#     {"ee": 2.42664},
#     {"es": 2.6368},
#     {"ro": 2.42756},
#     {"ph": 2.52388},
#     {"pt": 2.48664},
#     {"pl": 2.51896},
#     {"nz": 2.52988},
#     {"no": 2.4532},
#     {"de": 2.60312},
#     {"dk": 2.49832},
#     {"cz": 2.544},
#     {"ch": 2.5462},
#     {"co": 2.6423200000000002},
#     {"ca": 2.42836},
#     {"za": 2.4586},
#     {"vn": 2.5345600000000004},
#     {"us": 2.38064},
#     {"uy": 2.6368},
#     {"it": 2.52484},
#     {"il": 2.582},
#     {"ie": 2.5586400000000005},
#     {"hk": 2.40836},
#     {"hu": 2.61032},
#     {"br": 2.6348400000000005},
#     {"bg": 2.49932},
#     {"be": 2.5206},
#     {"at": 2.6234800000000003},
#     {"ar": 2.6368},
#     {"au": 2.5420000000000003},
#     {"tw": 2.41152},
#     {"tr": 2.4174},
#     {"th": 2.5352799999999998},
#     {"se": 2.42672},
#     {"sg": 2.5174800000000004},
# ]

print(min_means)
# [
#     {"nl": 0.7559199999999998},
#     {"my": 0.7632439999999998},
#     {"mx": 0.95052},
#     {"jp": 0.89848},
#     {"lv": 0.7356879999999998},
#     {"lt": 0.738676},
#     {"gb": 0.6183479999999999},
#     {"gr": 0.7443},
#     {"fr": 1.020324},
#     {"fi": 0.7475599999999999},
#     {"ee": 0.730916},
#     {"es": 1.0025199999999999},
#     {"ro": 0.7502599999999998},
#     {"ph": 0.5848559999999999},
#     {"pt": 0.7530199999999998},
#     {"pl": 0.7564679999999998},
#     {"nz": 0.75188},
#     {"no": 0.57604},
#     {"de": 0.93672},
#     {"dk": 0.7245199999999998},
#     {"cz": 0.72834},
#     {"ch": 0.75188},
#     {"co": 0.7785599999999999},
#     {"ca": 0.7248719999999998},
#     {"za": 0.75728},
#     {"vn": 0.58844},
#     {"us": 0.7334159999999998},
#     {"uy": 1.05948},
#     {"it": 1.0652679999999999},
#     {"il": 0.9082000000000001},
#     {"ie": 0.6215999999999999},
#     {"hk": 0.6522},
#     {"hu": 0.739068},
#     {"br": 0.98384},
#     {"bg": 0.75484},
#     {"be": 0.7442839999999998},
#     {"at": 0.7420439999999999},
#     {"ar": 0.9508},
#     {"au": 0.7499999999999999},
#     {"tw": 0.6522},
#     {"tr": 0.744088},
#     {"th": 0.91356},
#     {"se": 0.7207439999999998},
#     {"sg": 0.7183559999999999},
# ]
