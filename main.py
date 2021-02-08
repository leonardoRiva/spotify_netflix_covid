from threading import Thread
from netflix_kafka import *
from spotify_kafka import *
from covid_kafka import *
from merger import Merger
import json
import time



consumers = []
producers = []

merger = Merger()


consumers.append(Thread(target=get_netflix_consumer, args=(merger,)))
producers.append(Thread(target=get_netflix_producer))

consumers.append(Thread(target=get_spotify_consumer, args=(merger,)))
producers.append(Thread(target=get_spotify_producer))

consumers.append(Thread(target=get_covid_consumer, args=(merger,)))
producers.append(Thread(target=get_covid_producer))

for c in consumers:
    c.start()

time.sleep(0.2) # da mettere perché se no si perde il primo messaggio

for p in producers:
    p.start()

# main attende che i thread concludano
for c in consumers:
    c.join()
