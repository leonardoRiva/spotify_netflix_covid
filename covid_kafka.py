from covid_package._mobility import Covid_Side
from datetime import datetime, timedelta
from variables import get_weeks
from kafka import KafkaProducer
from kafka import KafkaConsumer
from variables import *
import pymongo
import json
import time



def get_covid_producer():
    # connessione a kafka come producer
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8'))

    # classe per il download dei dati
    mob = Covid_Side()
    weeks = get_weeks(covid_collection_name())
    print('[Covid] data downloaded')

    # invio dei documenti
    for w in weeks:
        downloaded_data = mob.get_week_doc(w)
        if downloaded_data != '':
            producer.send(topic='covid', value=downloaded_data) # invia i dati alla coda
        else:
            print('[Covid] week not present in the dataset')
        time.sleep(0.2) # senza lo sleep non va, senza motivo



def get_covid_consumer(merger):
    # connessione a kafka come consumer
    consumer = KafkaConsumer(
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='latest',
        consumer_timeout_ms=8000, #da togliere o aumentare, chiude connessione dopo x ms che non riceve messaggi
        value_deserializer=lambda v: json.loads(v.decode('utf-8')))

    # connessione al topic kafka
    consumer.subscribe(['covid'])

    # connessione a mongo
    client = pymongo.MongoClient("mongodb://localhost:27017/")
    collection = client[db_name()][covid_collection_name()]  

    # in ascolto di nuovi messaggi sulla coda
    for msg in consumer:
        doc = msg.value
        collection.insert_one(doc)
        print("[Covid] document inserted correctly! " + doc['week'])
        merger.notify('mobility', doc['week'])