import spotipy
from spotipy.oauth2 import SpotifyClientCredentials


class Spotify():

    def __init__(self, client_id, client_secret):
        self.SPOTIPY_CLIENT_ID = client_id
        self.SPOTIPY_CLIENT_SECRET = client_secret
        self.client_credentials_manager = SpotifyClientCredentials(client_id= self.SPOTIPY_CLIENT_ID, 
                                                            client_secret=self.SPOTIPY_CLIENT_SECRET)
        self.sp = spotipy.Spotify(client_credentials_manager = self.client_credentials_manager)


    def get_track_feature(self, _id):
        return self.sp.audio_features(_id)[0]