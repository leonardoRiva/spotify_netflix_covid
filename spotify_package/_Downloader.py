import pandas as pd
import requests
import math
import time
from datetime import timedelta, datetime, date
from threading import Thread
import queue


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
        csv_file = open('temp.csv', 'wb')
        csv_file.write(dd)
        csv_file.close()
        df = pd.read_csv('temp.csv', quotechar='"', names=["Position","Track_Name",
                                                            "Artist","Streams","URL"],
                        index_col=False)

        # print(n)
        # print(df.iloc[0])
        # print(df.iloc[1])
        df = df.drop([0,1], axis=0) #remove first two rows

        # for index, song in enumerate(df['URL']):
        #   song_id = song.split('/')[-1]
        #   df = add_features(df, index, song_id)
        df['week_from'] = w.split("--")[0]
        df['week_to'] = w.split("--")[1]
        df['country'] = n

        return df

    def mos(self, df, week):
        # for code in self.country_codes:
        #     df = df.append(self.mos_single(code, week))
        #     print('done '+ code)
        # return df
        q = queue.Queue()
        threads_list = []
        n_threads = 4  
        for l_codes in self.split_array(self.country_codes, n_threads):
            threads_list.append(Thread(target=lambda q, arg1, arg2: q.put(self.mos_group(arg1, arg2)), args=(q, l_codes, week)))

        # for code in self.country_codes:
        #     threads_list.append(Thread(target=lambda q, arg1, arg2: q.put(self.mos_single(arg1, arg2)), args=(q, code, week)))

        for t in threads_list:
            t.start()
        for t in threads_list:
            t.join()


        for i in q.get():
            if df is None:
                df = i
            else:
                df = pd.concat([df, i])
        while not q.empty():
            for i in q.get():
                df = pd.concat([df, i])

        df = df.reset_index()
        del df['index']
        return df

    def mos_group(self, codes, week):
        dfs = [self.mos_single(code,week) for code in codes]
        print(week)
        print('done' + str(codes))
        return dfs

    def split_array(self, arr, ts):

        n = math.ceil(len(arr)/ts)
        a = []
        for i in range(0,ts):
            a.append(arr[i*n:n+i*n])
        
        return a




    #main of class
    def get_data(self, day): #data of week X

        week = self.get_week_format(day)
        df = self.mos(pd.DataFrame(columns=["Position","Track_Name","Artist",
                                            "Streams","URL"]), week)
        df_json = df.to_json(orient='records')
        #print(df_json)
        return df_json
