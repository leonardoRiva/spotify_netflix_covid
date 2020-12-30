# spotify_netflix_covid
Spotify's songs &amp; Netflix's movies positivity over Covid incidence

### Features
The software uses Apache Kafka to download and elaborate data from Spotify charts, Netflix charts and Apple-Google mobility. These data is updated every week for 47 countries.
These data is then integrated with the help of Imdb and Spotify API. 
Finally, everything is stored in a MongoDB database.

### Installation
Apache Kafka and MongoDB needs to be installed and running on the computer.
Create 3 Kafka topics ('netflix', 'spotify', 'covid'), with the following command:
```
bin/kafka-topics.sh --zookeeper localhost:2181 --create --topic topic_name --partitions 1 --replication-factor 1 --config max.message.bytes=5242880
```
Then install these python libraries:
  - pymongo
  - kafka
  - pandas
  - spotipy
  - imdbpy
  - textblob

### Usage
Just run main.py
It will start 6 threads:
  - 3 kafka producers, that will download the data from the sources
  - 3 kafka consumers, that will elaborate indexes and upload to MongoDB