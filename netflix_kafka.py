from kafka import KafkaProducer
from kafka import KafkaConsumer
from netflix_package._FlixPatrol import FlixPatrol
from netflix_package._Netflix_Side import Netflix_Side
from netflix_package._Movies_DB import Movies_DB
import pandas as pd
import json
import time
import variables as GLV
import math
import time
from threading import Thread

#------------------------------------------------------------------------------#

countries = GLV.netflix_countries_codes()
weeks = GLV.get_weeks('netflix_chart')

#------------------------------------------------------------------------------#

def get_netflix_producer():
    FP_Scraper = FlixPatrol(c for c in [countries])
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8'))

    for w in weeks:
        n_thread = 4
        q_result = []
        q_thread = []
        cname = [k for k in countries]
        step = math.floor(len(cname)/n_thread)
        c_bloks = [cname[i:i + step] for i in range(0, len(cname), step)]

        for cb in c_bloks:
            q_thread.append(Thread(target=lambda w,cb : q_result.extend(FP_Scraper.get_week_chart(w,cb)), args=(w,cb,)))

        [t.start() for t in q_thread]
        [t.join() for t in q_thread]

        print('[NETFLIX] SEND ALL week ' +  str(w))
        producer.send(topic='netflix', value={'week':w, 'countries': q_result})
        time.sleep(0.1)

#------------------------------------------------------------------------------#

def get_netflix_consumer(merger=''):
    MDB = Movies_DB(GLV)
    NF_Side = Netflix_Side(MDB)

    consumer = KafkaConsumer(
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='latest',
        consumer_timeout_ms=9999999999, #da togliere o aumentare, chiude connessione dopo x ms che non riceve messaggi
        value_deserializer=lambda v: json.loads(v.decode('utf-8')))

    consumer.subscribe(['netflix'])

    for msg in consumer:
        tstart = time.time()

        week = msg.value['week']
        country_dfs = msg.value['countries']

        n_thread = 8
        q_result = []
        q_thread = []
        step = math.floor(len(country_dfs)/n_thread)
        c_bloks = [country_dfs[i:i + step] for i in range(0, len(country_dfs), step)]

        for cb in c_bloks:
            dfs = [pd.read_json(c) for c in cb]
            q_thread.append(Thread(target=lambda dfs: q_result.extend(NF_Side.enrich_dfs(dfs)), args=(dfs,)))

        [t.start() for t in q_thread]
        [t.join() for t in q_thread]

        week_doc = MDB.store_week_doc(week, q_result)

        print("\n" + "[NETFLIX] CONSUMED ALL week " + str(week) + " in " + str(time.time()-tstart) + "\n")
        merger.notify('netflix', week_doc["week"]) # TODO TEST

#------------------------------------------------------------------------------#

if __name__ == "__main__":
    tp = Thread(target=get_netflix_producer)
    tc = Thread(target=get_netflix_consumer)

    tc.start()
    tp.start()

    tc.join()
