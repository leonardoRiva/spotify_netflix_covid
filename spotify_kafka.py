from kafka import KafkaProducer
from kafka import KafkaConsumer
import json
import time



def get_spotify_producer():
  producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'))


  countries = ['it', 'es'] # mettere tutti gli stati
  weeks = ['2020-08-10', '2020-08-17'] # mettere tutte le settimane

  for c in countries:
    for w in weeks:
      downloaded_data = ('canzoni_' + c + '_' + w) # scaricare qui il csv
      producer.send(topic='spotify', value=downloaded_data) # invia i dati alla coda
      time.sleep(0.2) # senza lo sleep non va, senza motivo




def get_spotify_consumer():
  consumer = KafkaConsumer(
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='latest',
    consumer_timeout_ms=1000, #da togliere o aumentare, chiude connessione dopo x ms che non riceve messaggi
    value_deserializer=lambda v: json.loads(v.decode('utf-8')))

  consumer.subscribe(['spotify'])

  for msg in consumer:
    print(msg.value)
    # query a spotify
    # upload su mongo
