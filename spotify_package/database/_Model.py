class Model():
    """
    Classe per gestire/controllare/costruire gli schemi json dei documenti da memorizzare
    """
    def __init__(self):
        pass


    def song_model(self, song, features): #usato nella versione programmatica
        song_id = song['URL'].split('/')[-1]
        song_doc = {
            "song_id": song_id,
            "artist": song['Artist'],
            "track_name": song['Track_Name'],
            "features":  {
                "danceability": features['danceability'],
                "acousticness": features['acousticness'],
                "energy": features['energy'],
                "instrumentalness": features['instrumentalness'],
                "liveness": features['liveness'],
                "loudness": features['loudness'],
                "mode": features['mode'],
                "speechiness": features['speechiness'],
                "tempo": features['tempo'],
                "valence": features['valence']
                }
            }
        return song_doc


    
