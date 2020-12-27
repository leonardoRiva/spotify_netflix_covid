from kafka import KafkaProducer
from kafka import KafkaConsumer
import json
import time
from spotify_package._SpotiModelling import *
from spotify_package._Downloader import *
from variables import get_weeks

#INIT 
mongo = Mongo('progettoDB')

def get_spotify_producer():
  producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'))

  downloader = Downloader()
  weeks = get_weeks()

  for week in weeks:
    #send to kafka queue
    df = downloader.get_data(week)
    producer.send(topic="spotify", value=df)# invia i dati alla coda
    #producer.send(topic='spotify', value=downloader.get_data(week))
    time.sleep(0.2) # senza lo sleep non va, senza motivo
  #[send_queue(producer, downloader.get_data(week)) for week in weeks]


def send_queue(producer, df):
  #send to kafka queue
  producer.send(topic='spotify', value=df) # invia i dati alla coda
  time.sleep(0.2) # senza lo sleep non va, senza motivo



def get_spotify_consumer():

  spoty_side = SpotiModelling()
  consumer = KafkaConsumer(
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='latest',
    consumer_timeout_ms=1000000, #da togliere o aumentare, chiude connessione dopo x ms che non riceve messaggi
    #esecuzione da terminare manualmente altrimenti attendere sopracitati ms al termine
    value_deserializer=lambda v: json.loads(v.decode('utf-8')))

  consumer.subscribe(['spotify'])

  for msg in consumer:
    print("Kafka consumed")
    #print(msg.value)
    model = spoty_side.get_week(msg.value)# query a spotify
    mongo.store_week(model)# upload su mongo
    print("Document inserted correctly!")
    