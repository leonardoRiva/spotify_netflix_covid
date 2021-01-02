from pymongo import MongoClient
import variables as GLV

#------------------------------------------------------------------------------#

# USAGE:
# in altro file: import db_queries as DBQ
# DBQ.send_query([list of country codes], [list of weeks], [list of topics], [da vedere])

#------------------------------------------------------------------------------#

_client = MongoClient('mongodb://localhost:27017/')
_db = _client[GLV.db_name()]

_all_countries = GLV.get_country_codes()
_all_weeks = GLV.get_common_weeks_list()
_all_topics = ['covid', 'netflix_chart', 'spoti_weeks'] # colleaction names
_topics_fields = {
    'covid': 'mobility',
    'netflix_chart': 'netflix',
    'spoti_weeks':'spotify'
}

#------------------------------------------------------------------------------#

def send_query(countries=_all_countries, weeks=_all_weeks, topics=_all_topics, constrains='mah'):
    res = {}
    for t in topics:
        query = _build_query(countries, weeks, t, constrains)
        r = eval(query)
        res[_topics_fields[t]] = r
    return res

def _build_query(countries, weeks, topic, constrains):
    q = '_db.'+topic+'.find('
    q += _q_weeks(weeks)
    q += ','
    q += _q_countries(_topics_fields[topic], countries)
    q += ')'
    return q

def _q_weeks(weeks):
    qw = '{"week": {"$in":weeks}}'
    return qw

def _q_countries(topic, countries):
    qc = '{"_id":0,'
    for ic,c in enumerate(countries):
        qc += '"' + topic + '.' + c + '":1'
        qc += ',' if ic < len(countries)-1 else '}'
    return qc

#------------------------------------------------------------------------------#

# def build_df(topic_docs, weeks, countries):
#     df = pd.DataFrame(columns=['country','weeks'])
#     for td in topic_docs:
#         df[[td]] = ''
#
#     for c in countries:
#         for w in weeks:
#             entries.append([c,w,])


#------------------------------------------------------------------------------#
