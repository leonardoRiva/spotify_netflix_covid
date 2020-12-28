from datetime import datetime, timedelta


def get_weeks(collection):
    # trova ultima settimana salvata sul db, per aggiungere solo le successive
    try:
        mydoc = collection.find({}, {'week': 1, '_id': 0}).sort('week',-1).limit(1)
        last = mydoc[0]['week']
    except:
        last = '2020-08-03' # una settimana prima della 1°, perché poi sommo 7 giorni
    weeks = []
    i = datetime.strptime(last, '%Y-%m-%d') + timedelta(days=7)
    while i < datetime.today():
        weeks.append(datetime.strftime(i, '%Y-%m-%d'))
        i = i + timedelta(days=7)
    return weeks


def db_name():
    return 'progettoDB'

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


# questi li toglierei..

def get_country_to_code_dict():
    return {'argentina': 'ar', 'australia': 'au', 'austria': 'at', 'belgium': 'be',
        'bulgaria': 'bg', 'brazil': 'br', 'canada': 'ca', 'switzerland': 'ch',
        'colombia': 'co', 'czechia': 'cz', 'germany': 'de', 'denmark': 'dk',
        'spain': 'es', 'estonia': 'ee', 'finland': 'fi', 'france': 'fr',
        'united-kingdom': 'gb', 'greece': 'gr', 'hong-kong': 'hk', 'hungary': 'hu',
        'india': 'in', 'ireland': 'ie', 'israel': 'il', 'italy': 'it', 'japan': 'jp',
        'lithuania': 'lt', 'latvia': 'lv', 'mexico': 'mx', 'malaysia': 'my',
        'netherlands': 'nl', 'norway': 'no', 'new-zealand': 'nz', 'philippines': 'ph',
        'poland': 'pl', 'portugal': 'pt', 'romania': 'ro', 'russia': 'ru',
        'singapore': 'sg', 'sweden': 'se', 'thailand': 'th', 'turkey': 'tr', 'taiwan': 'tw',
        'ukraine': 'ua', 'uruguay': 'uy', 'united-states': 'us', 'vietnam': 'vn',
        'south-africa': 'za'}

def get_code_to_country_dict():
    return {"au": "australia", "ar": "argentina", "at": "austria",
        "be": "belgium", "bg": "bulgaria", "br": "brazil", "ca": "canada",
        "co": "colombia", "ch": "switzerland", "cz": "czech_republic",
        "dk": "denmark", "de": "germany", "es": "spain", "ee": "estonia",
        "fi": "finland", "fr": "france", "gr": "greece", "gb": "united_kingdom",
        "hu": "hungary", "hk": "hong_kong", "in": "india",
        "ie": "ireland", "il": "israel", "it": "italy", "lt": "lithuania",
        "lv": "latvia", "jp": "japan", "mx": "mexico", "my": "malaysia",
        "nl": "netherlands", "no": "norway", "nz": "new_zealand",
        "pl": "poland", "pt": "portugal", "ph": "philippines",
        "ro": "romania", "ru": "russia", "sg": "singapore",
        "se": "sweden", "th": "thailand", "tr": "turkey", "tw": "taiwan",
        "ua": "ukraine", "uy": "uruguay", "us": "united_states",
        "vn": "vietnam", "za": "south_africa"}



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
