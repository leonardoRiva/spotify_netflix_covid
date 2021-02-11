# spotify_netflix_covid
Spotify's songs &amp; Netflix's movies positivity over Covid incidence

### Features
The software uses Apache Kafka to download and elaborate data from Spotify CSV charts, Netflix FlixPatrol.com charts (scraped) and Apple-Google mobility. These data is updated every week for 45 countries.
These data is then integrated with the help of IMDb API and Spotify API. 
Finally, everything is stored in a MongoDB database.

### Installation
Before setting up the project, these softwares need to be installed:
  - Apache Kafka (https://kafka.apache.org/downloads)
  - Apache ZooKeeper (https://zookeeper.apache.org/releases.html#download)
  - MongoDB (https://www.mongodb.com/try)

Start the aforementioned software processes, and then create 3 Kafka topics ('netflix', 'spotify', 'covid'), with the following command:
```
bin/kafka-topics.sh --zookeeper localhost:2181 --create --topic topic_name --partitions 1 --replication-factor 1 --config max.message.bytes=5242880
```
After that, install these python libraries through ```pip install```:
  - numpy
  - pymongo
  - kafka-python
  - pandas
  - spotipy
  - IMDbPY
  - textblob 
 
Textblob need also the package corpora: ```python -m textblob.download_corpora```

### Usage
To start the project, just type this simple command:
```
python main.py
```
It will start 6 threads:
  - 3 kafka producers, that will download the data from the sources
  - 3 kafka consumers, that will elaborate indexes and upload to MongoDB
