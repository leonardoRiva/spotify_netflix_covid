from datetime import datetime, timedelta
import pymongo


def get_weeks(collection_name):
   # trova ultima settimana salvata sul db, per restituire un array con tutte i lunedì fino ad oggi
   try:
      client = pymongo.MongoClient("mongodb://localhost:27017/")
      db = client[db_name()]
      collection = db[collection_name]
      mydoc = collection.find({}, {'week': 1, '_id': 0}).sort('week',-1).limit(1)
      last = mydoc[0]['week']
      client.close()
   except:
      last = '2020-08-03' # una settimana prima della 1°, perché poi sommo 7 giorni

   # se non è lunedì, trova il primo successivo
   i = datetime.strptime(last, '%Y-%m-%d') + timedelta(days=7)
   while i.weekday() != 0:
      i += timedelta(days=1)

   # calcola tutti i lunedì
   weeks = []
   today = datetime.today()
   while i < today:
      weeks.append(datetime.strftime(i, '%Y-%m-%d'))
      i += timedelta(days=7)
   return weeks


def db_name():
   return 'testDB' 

def spotify_collection_name():
   return 'spoti_weeks'

def covid_collection_name(): #cambiare nome
   return 'covid_weeks'

def netflix_collection_name(): #cambiare nome
   return 'netflix_chart'

def merged_collection_name():
   return 'merged_data'

def collection_names():
   return [spotify_collection_name(), covid_collection_name(), netflix_collection_name()]

def collection_names_dict():
   n = {}
   n['spotify'] = spotify_collection_name()
   n['mobility'] = covid_collection_name()
   n['netflix'] = netflix_collection_name()
   return n



# dizionario globale countries
COMMON_COUNTRIES = {
   "au":{
      "spotify":"australia",
      "netflix":"australia",
      "covid":"australia"
   },
   "ar":{
      "spotify":"argentina",
      "netflix":"argentina",
      "covid":"argentina"
   },
   "at":{
      "spotify":"austria",
      "netflix":"austria",
      "covid":"austria"
   },
   "be":{
      "spotify":"belgium",
      "netflix":"belgium",
      "covid":"belgium"
   },
   "bg":{
      "spotify":"bulgaria",
      "netflix":"bulgaria",
      "covid":"bulgaria"
   },
   "br":{
      "spotify":"brazil",
      "netflix":"brazil",
      "covid":"brazil"
   },
   "ca":{
      "spotify":"canada",
      "netflix":"canada",
      "covid":"canada"
   },
   "co":{
      "spotify":"colombia",
      "netflix":"colombia",
      "covid":"colombia"
   },
   "ch":{
      "spotify":"switzerland",
      "netflix":"switzerland",
      "covid":"switzerland"
   },
   "cz":{
      "spotify":"czech_republic",
      "netflix":"czech-republic",
      "covid":"czechia"
   },
   "dk":{
      "spotify":"denmark",
      "netflix":"denmark",
      "covid":"denmark"
   },
   "de":{
      "spotify":"germany",
      "netflix":"germany",
      "covid":"germany"
   },
   "es":{
      "spotify":"spain",
      "netflix":"spain",
      "covid":"spain"
   },
   "ee":{
      "spotify":"estonia",
      "netflix":"estonia",
      "covid":"estonia"
   },
   "fi":{
      "spotify":"finland",
      "netflix":"finland",
      "covid":"finland"
   },
   "fr":{
      "spotify":"france",
      "netflix":"france",
      "covid":"france"
   },
   "gr":{
      "spotify":"greece",
      "netflix":"greece",
      "covid":"greece"
   },
   "gb":{
      "spotify":"united_kingdom",
      "netflix":"united-kingdom",
      "covid":"united-kingdom"
   },
   "hu":{
      "spotify":"hungary",
      "netflix":"hungary",
      "covid":"hungary"
   },
   "hk":{
      "spotify":"hong_kong",
      "netflix":"hong-kong",
      "covid":"hong-kong"
   },
   "in":{
      "spotify":"india",
      "netflix":"india",
      "covid":"india"
   },
   "ie":{
      "spotify":"ireland",
      "netflix":"ireland",
      "covid":"ireland"
   },
   "il":{
      "spotify":"israel",
      "netflix":"israel",
      "covid":"israel"
   },
   "it":{
      "spotify":"italy",
      "netflix":"italy",
      "covid":"italy"
   },
   "lt":{
      "spotify":"lithuania",
      "netflix":"lithuania",
      "covid":"lithuania"
   },
   "lv":{
      "spotify":"latvia",
      "netflix":"latvia",
      "covid":"latvia"
   },
   "jp":{
      "spotify":"japan",
      "netflix":"japan",
      "covid":"japan"
   },
   "mx":{
      "spotify":"mexico",
      "netflix":"mexico",
      "covid":"mexico"
   },
   "my":{
      "spotify":"malaysia",
      "netflix":"malaysia",
      "covid":"malaysia"
   },
   "nl":{
      "spotify":"netherlands",
      "netflix":"netherlands",
      "covid":"netherlands"
   },
   "no":{
      "spotify":"norway",
      "netflix":"norway",
      "covid":"norway"
   },
   "nz":{
      "spotify":"new_zealand",
      "netflix":"new-zealand",
      "covid":"new-zealand"
   },
   "pl":{
      "spotify":"poland",
      "netflix":"poland",
      "covid":"poland"
   },
   "pt":{
      "spotify":"portugal",
      "netflix":"portugal",
      "covid":"portugal"
   },
   "ph":{
      "spotify":"philippines",
      "netflix":"philippines",
      "covid":"philippines"
   },
   "ro":{
      "spotify":"romania",
      "netflix":"romania",
      "covid":"romania"
   },
   "ru":{
      "spotify":"russia",
      "netflix":"russia",
      "covid":"russia"
   },
   "sg":{
      "spotify":"singapore",
      "netflix":"singapore",
      "covid":"singapore"
   },
   "se":{
      "spotify":"sweden",
      "netflix":"sweden",
      "covid":"sweden"
   },
   "th":{
      "spotify":"thailand",
      "netflix":"thailand",
      "covid":"thailand"
   },
   "tr":{
      "spotify":"turkey",
      "netflix":"turkey",
      "covid":"turkey"
   },
   "tw":{
      "spotify":"taiwan",
      "netflix":"taiwan",
      "covid":"taiwan"
   },
   "ua":{
      "spotify":"ukraine",
      "netflix":"ukraine",
      "covid":"ukraine"
   },
   "uy":{
      "spotify":"uruguay",
      "netflix":"uruguay",
      "covid":"uruguay"
   },
   "us":{
      "spotify":"united_states",
      "netflix":"united-states",
      "covid":"united-states"
   },
   "vn":{
      "spotify":"vietnam",
      "netflix":"vietnam",
      "covid":"vietnam"
   },
   "za":{
      "spotify":"south_africa",
      "netflix":"south-africa",
      "covid":"south-africa"
   }
}

def netflix_countries_codes():
   dict = {}
   for code in COMMON_COUNTRIES:
      dict[COMMON_COUNTRIES[code]['netflix']] = code
   return dict

# ritorna la stessa cosa di get_country_to_code_dict
def covid_countries_codes():
   dict = {}
   for code in COMMON_COUNTRIES:
      dict[COMMON_COUNTRIES[code]['covid']] = code
   return dict

# ritorna la stessa cosa di get_code_to_country_dict()
def spotify_codes_countries():
   dict = {}
   for code in COMMON_COUNTRIES:
      dict[code] = COMMON_COUNTRIES[code]['spotify']
   return dict

# return list of codes
def spotify_get_countries_code():
   arr = []
   for code in COMMON_COUNTRIES:
      arr.append(code)
   return arr



def get_common_weeks_list():
   return ['2020-08-10',
            '2020-08-17',
            '2020-08-24',
            '2020-08-31',
            '2020-09-07',
            '2020-09-14',
            '2020-09-21',
            '2020-09-28',
            '2020-10-05',
            '2020-10-12',
            '2020-10-19',
            '2020-10-26',
            '2020-11-02',
            '2020-11-09',
            '2020-11-16',
            '2020-11-23',
            '2020-11-30',
            '2020-12-07',
            '2020-12-14',
            '2020-12-21']
