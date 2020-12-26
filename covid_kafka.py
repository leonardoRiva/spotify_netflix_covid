from covid_package.mobility_v6 import Covid_Side
from kafka import KafkaProducer
from kafka import KafkaConsumer
import pymongo
import json
import time



def get_covid_producer():
    # connessione a kafka come producer
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8'))

    # classe per il download dei dati (+ pezzottone con le settimane perché non ho voglia di calcolarle)
    mob = Covid_Side()
    weeks = ['2020-03-23', '2020-11-16', '2020-07-13', '2020-10-26', '2020-06-01', '2020-07-27', 
            '2020-03-30', '2020-10-19', '2020-06-15', '2020-10-05', '2020-11-30', '2020-09-07', '2020-03-02', 
            '2020-05-04', '2020-05-11', '2020-08-03', '2020-02-24', '2020-03-16', '2020-09-21', '2020-08-24', 
            '2020-02-17', '2020-11-23', '2020-12-14', '2020-07-20', '2020-06-22', '2020-04-20', '2020-08-31', 
            '2020-04-13', '2020-09-28', '2020-03-09', '2020-07-06', '2020-05-25', '2020-04-27', '2020-08-10', 
            '2020-11-09', '2020-09-14', '2020-05-18', '2020-04-06', '2020-06-29', '2020-08-17', '2020-12-07', 
            '2020-11-02', '2020-06-08', '2020-10-12']
    weeks = ['2020-03-23', '2020-11-16']

    # invio dei documenti
    for w in weeks:
        downloaded_data = mob.get_week_doc_complete(w)
        producer.send(topic='covid', value=downloaded_data) # invia i dati alla coda
        time.sleep(0.2) # senza lo sleep non va, senza motivo




def get_covid_consumer():
    # connessione a mongo
    client = pymongo.MongoClient("mongodb://localhost:27017/")
    db = client["test"]
    collection = db["covid"]  

    # connessione a kafka come consumer
    consumer = KafkaConsumer(
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='latest',
        consumer_timeout_ms=8000, #da togliere o aumentare, chiude connessione dopo x ms che non riceve messaggi
        value_deserializer=lambda v: json.loads(v.decode('utf-8')))

    # connessione al topic kafka
    consumer.subscribe(['covid'])

    # in ascolto di nuovi messaggi sulla coda
    for msg in consumer:
        collection.insert_one(msg.value)
        print("Document inserted correctly!")