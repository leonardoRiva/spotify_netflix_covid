import pandas as pd
import requests

import time
from datetime import timedelta, datetime, date




class Downloader():



    def __init__(self):
        self.country_codes= [
                        # 'AR', 'AU', 'AT',  
                        # 'BE', 'BR', 'BG',  
                        # 'CA', 'CO', 'CZ', 'CH',
                        # 'DK', 'DE',
                        # 'ES', 'EE',
                        # 'FI', 'FR',  
                        # 'GR', 'GB',
                        # 'HK', 'HU', 
                        # 'IS', 'IE', 'IT', 'IL', 'IN',
                        # 'JP', 
                        # 'LV', 'LT',  
                        # 'MY', 'MX', 
                        # 'NL', 'NZ', 'NO', 
                        # 'PH', 'PL', 'PT',
                        # 'RO', 'RU',
                        # 'SG', 'SE',  
                        # 'TW', 'TR', 'TH',
                        # 'US', 'UY', 'UA',
                        'VN',
                        'ZA'
                        ]



    def build_url(self, nation, week):
        return "https://spotifycharts.com/regional/" + nation + "/weekly/" + week + "/download"

    def get_week_format(self, day):
        d = datetime.strptime(day, '%Y-%m-%d')
        diffMonday = d.weekday()
        d = d + timedelta(days=-diffMonday)
        s = d + timedelta(days=-3)
        prev_day = str(s.date())
        d = datetime.strptime(prev_day, '%Y-%m-%d')
        d = d + timedelta(days=7)
        next_day = str(d.date())
        date_week = prev_day + '--' + next_day
        return date_week

    def download(self, nation, week):
        url = self.build_url(nation, week)
        req = requests.get(url)
        if (req.status_code == 200):
            url_content = req.content
            return url_content
        else:
            return False



    def mos_single(self, n, w):
        dd = self.download(n.lower(), w)
        if not dd:
            return []
        csv_file = open('temp.csv', 'wb')
        csv_file.write(dd)
        csv_file.close()
        df = pd.read_csv('temp.csv', quotechar='"', names=["Position","Track_Name",
                                                            "Artist","Streams","URL"],
                        index_col=False)
        df = df.drop([0,1], axis=0) #remove first two rows
        # for index, song in enumerate(df['URL']):
        #   song_id = song.split('/')[-1]
        #   df = add_features(df, index, song_id)
        df['week_from'] = w.split("--")[0]
        df['week_to'] = w.split("--")[1]
        df['country'] = n
        
        return df

    def mos(self, df, week):
        for code in self.country_codes:
            df = df.append(self.mos_single(code, week))
            print('done '+ code)
        return df


    


    #main of class
    def get_data(self, day): #data of week X

        week = self.get_week_format(day)
        df = self.mos(pd.DataFrame(columns=["Position","Track_Name","Artist",
                                            "Streams","URL"]), week)
        df_json = df.to_json(orient='records')
        #print(df_json)
        return df_json
    

    