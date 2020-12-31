import pandas as pd
import requests
import math
import time
from datetime import timedelta, datetime, date
from threading import Thread
import queue
import numpy as np
import os

class Downloader():

    def __init__(self, countries):
        self.country_codes = countries # list of country codes


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
        filename = './temps/temp'+str(n)+str(w)+'.csv'
        os.makedirs(os.path.dirname(filename), exist_ok=True)
        csv_file = open(filename, 'wb') # creates temp file
        csv_file.write(dd)
        csv_file.close()
        df = pd.read_csv('temp.csv', quotechar='"', names=["Position","Track_Name",
                                                            "Artist","Streams","URL"],
                        index_col=False)
        os.remove(filename) # delete temp file
  
        df = df.drop([0,1], axis=0) #remove first two rows

        df['week_from'] = w.split("--")[0]
        df['week_to'] = w.split("--")[1]
        df['country'] = n
        return df


    def mos(self, df, week):
        q = queue.Queue()
        threads_list = []
        n_threads = 8
        splitted_codes = [list(x) for x in np.array_split(self.country_codes, n_threads)]
        for l_codes in splitted_codes:
            threads_list.append(Thread(target=lambda q, arg1, arg2: q.put(self.mos_group(arg1, arg2)), args=(q, l_codes, week)))

        for t in threads_list:
            t.start()
        for t in threads_list:
            t.join()

        l = [] # queue to list
        while not q.empty():
            x = q.get()
            l.append(x)
        l = [item for sublist in l for item in sublist] # flattening the list

        try: # concat the dfs
            df = pd.concat(l)
            df = df.reset_index()
            del df['index']
            return df
        except:
            return pd.DataFrame() # empty dataframe if week not in charts


    def mos_group(self, codes, week):
        dfs = [self.mos_single(code,week) for code in codes]
        print('[Spotify] downloaded: ' + week + ' -> ' + str(codes))
        return dfs


    #main of class
    def get_data(self, day): #data of week X
        week = self.get_week_format(day)
        df = self.mos(pd.DataFrame(columns=["Position","Track_Name","Artist",
                                            "Streams","URL"]), week)
        df_json = df.to_json(orient='records')
        return df_json
