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
        "hu": "hungary", "hk": "hong_kong", "in": "india", "is": "iceland",
        "ie": "ireland", "il": "israel", "it": "italy", "lt": "lithuania",
        "lv": "latvia", "jp": "japan", "mx": "mexico", "my": "malaysia",
        "nl": "netherlands", "no": "norway", "nz": "new_zealand",
        "pl": "poland", "pt": "portugal", "ph": "philippines",
        "ro": "romania", "ru": "russia", "sg": "singapore",
        "se": "sweden", "th": "thailand", "tr": "turkey", "tw": "taiwan",
        "ua": "ukraine", "uy": "uruguay", "us": "united_states",
        "vn": "vietnam", "za": "south_africa"}