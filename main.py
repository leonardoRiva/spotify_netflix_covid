from threading import Thread
from netflix_kafka import *
from spotify_kafka import *
from covid_kafka import *
import json
import time


consumers = []
producers = []

consumers.append(Thread(target=get_netflix_consumer))
producers.append(Thread(target=get_netflix_producer))

consumers.append(Thread(target=get_spotify_consumer))
producers.append(Thread(target=get_spotify_producer))

consumers.append(Thread(target=get_covid_consumer))
producers.append(Thread(target=get_covid_producer))


for c in consumers:
    c.start()

time.sleep(0.2) # da mettere perché se no si perde il primo messaggio (booooooh)

for p in producers:
    p.start()

# main attende che i thread concludano
for c in consumers:
    c.join()
