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

    def get_tracks_feature(self, ids):
        return self.sp.audio_features(ids)


    def get_features_date(self, ids):
        features = self.get_tracks_feature(ids)
        dates = self.get_albums_date(ids)

        return [self.merg(a,b) for a, b in zip(features, dates)]


    def merg(self, a,b):
        if a is not None:
            a['release_date'] = b
        return a

    def get_albums_date(self, ids):

        tracks = self.sp.tracks(ids, market=None)['tracks']
        dates = [self.get_date(t['album']['release_date'], t['album']['release_date_precision']) for t in tracks]
        return dates

    def get_track(self, _id):
        return self.sp.track(_id, market=None)

    def get_search(self, _id):
        return self.sp.search(_id)
    

    def get_date(self, rel_date, precision):

        if precision == 'year':
            if rel_date == '0000':
                rel_date = '1800'
            return rel_date + '-01-01' #default to 1-1
        elif precision == 'month':
            return rel_date + '-01' #default to 1
        else:
            return rel_date #.split('-')[0]