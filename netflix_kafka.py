from kafka import KafkaProducer
from kafka import KafkaConsumer
from netflix_package._FlixPatrol import FlixPatrol
from netflix_package._Netflix_Side import Netflix_Side
from netflix_package._Movies_DB import Movies_DB
import pandas as pd
import json
import time
import variables as GLV
from threading import Thread
import concurrent.futures

#------------------------------------------------------------------------------#

countries_old = {
   "argentina":{
      "netflix":"argentina"
   },
   "australia":{
      "netflix":"australia"
   },
   "austria":{
      "netflix":"austria"
   },
   "belgium":{
      "netflix":"belgium"
   },
   "brazil":{
      "netflix":"brazil"
   },
   "bulgaria":{
      "netflix":"bulgaria"
   },
   "canada":{
      "netflix":"canada"
   },
   "colombia":{
      "netflix":"colombia"
   },
   "czech_republic":{
      "netflix":"czech-republic"
   },
   "denmark":{
      "netflix":"denmark"
   },
   "france":{
      "netflix":"france"
   },
   "germany":{
      "netflix":"germany"
   },
   "greece":{
      "netflix":"greece"
   },
   "hong_kong":{
      "netflix":"hong-kong"
   },
   "hungary":{
      "netflix":"hungary"
   },
   "iceland":{
      "netflix":"iceland"
   },
   "india":{
      "netflix":"india"
   },
   "ireland":{
      "netflix":"ireland"
   },
   "israel":{
      "netflix":"israel"
   },
   "italy":{
      "netflix":"italy"
   },
   "japan":{
      "netflix":"japan"
   },
   "latvia":{
      "netflix":"latvia"
   },
   "lithuania":{
      "netflix":"lithuania"
   },
   "malaysia":{
      "netflix":"malaysia"
   },
   "mexico":{
      "netflix":"mexico"
   },
   "netherlands":{
      "netflix":"netherlands"
   },
   "new_zealand":{
      "netflix":"new-zealand"
   },
   "norway":{
      "netflix":"norway"
   },
   "philippines":{
      "netflix":"philippines"
   },
   "poland":{
      "netflix":"poland"
   },
   "portugal":{
      "netflix":"portugal"
   },
   "romania":{
      "netflix":"romania"
   },
   "russia":{
      "netflix":"russia"
   },
   "singapore":{
      "netflix":"singapore"
   },
   "south_africa":{
      "netflix":"south-africa"
   },
   "spain":{
      "netflix":"spain"
   },
   "sweden":{
      "netflix":"sweden"
   },
   "switzerland":{
      "netflix":"switzerland"
   },
   "taiwan":{
      "netflix":"taiwan"
   },
   "thailand":{
      "netflix":"thailand"
   },
   "turkey":{
      "netflix":"turkey"
   },
   "ukraine":{
      "netflix":"ukraine"
   },
   "united_kingdom":{
      "netflix":"united-kingdom"
   },
   "united_states":{
      "netflix":"united-states"
   },
   "uruguay":{
      "netflix":"uruguay"
   },
   "vietnam":{
      "netflix":"vietnam"
   }
}
countries = GLV.netflix_countries_codes()
weeks = GLV.get_common_weeks_list()

#------------------------------------------------------------------------------#

def get_netflix_producer():
    FP_Scraper = FlixPatrol(c for c in [countries])
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8'))

    for w in weeks[3:6]:
        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
             res_future = list(map(lambda c: executor.submit(FP_Scraper.get_weeks_chart, w, c), countries))
             for rf in concurrent.futures.as_completed(res_future):
                 sdf = rf.result()
                 producer.send(topic='netflix', value=sdf.to_json()) # invia i dati alla coda
                 print("send netflix scraped week: " + w + ", country: " + sdf["country"][0])
                 time.sleep(0.1) # senza lo sleep non va, senza motivo

    # for ic,c in enumerate(countries):
    #     for w in weeks[0:3]:
    #         scraped_data = FP_Scraper.get_weeks_chart(w, c)
    #         producer.send(topic='netflix', value=scraped_data.to_json()) # invia i dati alla coda
    #         print("send netflix scraped week: " + w + ", country: " + c)
    #         time.sleep(0.1) # senza lo sleep non va, senza motivo

#------------------------------------------------------------------------------#

def get_netflix_consumer():
    MDB = Movies_DB(GLV)
    NF_Side = Netflix_Side(MDB)

    consumer = KafkaConsumer(
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='latest',
        consumer_timeout_ms=10000, #da togliere o aumentare, chiude connessione dopo x ms che non riceve messaggi
        value_deserializer=lambda v: json.loads(v.decode('utf-8')))

    consumer.subscribe(['netflix'])

    for msg in consumer:
        df = pd.read_json(msg.value)
        df_full = NF_Side.enrich_df(df)
        MDB.store_week(df_full)
        print("\n\n" + "netflix consumed: \n" + str(df_full) + "\n\n")

#------------------------------------------------------------------------------#

if __name__ == "__main__":
    tp = Thread(target=get_netflix_producer)
    tc = Thread(target=get_netflix_consumer)

    tc.start()
    tp.start()

    tc.join()
