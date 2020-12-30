from kafka import KafkaProducer
from kafka import KafkaConsumer
import json
import time
from spotify_package.database._Mongo import *
from spotify_package._SpotiModelling import *
from spotify_package._Downloader import *
from variables import *

#INIT 
mongo = Mongo(db_name())

#CREARE TOPIC 'SPOTIFY' CON CONFIG MAX.MESSAGE.BYTES SETTATO A PIU' DI 2 MB
#ALTRIMENTI PRODUCER NON TRASMETTE E CONSUMER NON RICEVE

def get_spotify_producer():
  producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    max_request_size=5242880, #5 MB
    value_serializer=lambda v: json.dumps(v).encode('utf-8'))

  countries = spotify_get_countries_code()
  downloader = Downloader(countries)
  weeks = get_weeks('spoti_weeks') #name collection weeks

  [send_queue(producer, downloader.get_data(week)) for week in weeks]


def send_queue(producer, df):
  #send to kafka queue
  producer.send(topic='spotify', value=df) # invia i dati alla coda
  time.sleep(0.2) # senza lo sleep non va, senza motivo

# def save_to_file(dic):

#     # print(dic)
#     with open('filePROVA.txt', 'w') as file:
#         file.write(json.dumps(dic))

def get_spotify_consumer():

  dic_countries = spotify_codes_countries()
  spoty_side = SpotiModelling(mongo, dic_countries)
  consumer = KafkaConsumer(
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='latest',
    max_partition_fetch_bytes= 5242880, #5 MB
    consumer_timeout_ms=300000, #da togliere o aumentare, chiude connessione dopo x ms che non riceve messaggi
    #esecuzione da terminare manualmente altrimenti attendere sopracitati ms al termine
    value_deserializer=lambda v: json.loads(v.decode('utf-8')))

  consumer.subscribe(['spotify'])

  for msg in consumer:
    print("[Spotify] kafka consumed")
    if len(msg.value) > 2:
      model = spoty_side.get_week(msg.value)# query a spotify
      mongo.store_week(model)# upload su mongo
      print("[Spotify] document inserted correctly!")
    else:
      print('[Spotify] week not present on spotify charts')
    