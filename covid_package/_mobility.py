import pandas as pd
import sys, os
import math
from variables import *

class Covid_Side:

    def __init__(self):
        self.df = self.get_df_data()


    def get_df_data(self):
        # downloads the csv, cleans it and calculates the index; returns a pandas dataframe
        downloaded_df = self.download_csv()
        cleaned_df = self.clean_csv(downloaded_df)
        df = self.calculate_index(cleaned_df)
        df = self.get_normalized_mobility(df)
        return df


    def download_csv(self):
        # downloads the dataset
        url = 'https://raw.githubusercontent.com/ActiveConclusion/COVID19_mobility/master/summary_reports/summary_report_countries.csv'
        return pd.read_csv(url)


    def clean_csv(self, df):
        # deletes useless columns, renames the remaining ones, 
        # interpolates internal missing values, deletes rows with NaN
        del df['transit']
        del df['grocery and pharmacy']
        del df['residential']
        del df['workplaces']
        del df['transit stations']
        df = df.rename({'retail and recreation': 'recreation'}, axis='columns').reset_index(drop=True)
        df = self.interpolate_nan(df)
        df = df.dropna()
        return df

    
    def interpolate_nan(self, df):
        # returns a dataframe with missing values interpolated
        new_df = pd.DataFrame(columns=df.columns)
        for c in set(df['country']):
            tmp = (df[df['country']==c]).copy()
            for col in ['parks', 'recreation', 'driving', 'walking']:
                tmp[col] = self.interpolate_nan_single(list(tmp[col]))
            new_df = new_df.append(tmp)
        new_df = new_df.reset_index(drop=True)
        return new_df


    def interpolate_nan_single(self, a):
        # interpolates the missing values by "connecting" previous and next value 
        start = 0
        for i in range(0,len(a)):
            if not math.isnan(a[i]):
                break
            start += 1

        first = True
        for i in range(start,len(a)):
            if first and math.isnan(a[i]):
                i1 = i-1
                first = False

            if not first and not math.isnan(a[i]):
                i2 = i
                first = True

                step = (a[i2] - a[i1])/(i2-i1)
                k = i1+1
                v = a[i1]
                while k < i2:
                    v += step
                    a[k] = v
                    k += 1
        return a


    
    def calculate_index(self, df):
        # calculates the mobility index, with a mean of the values for each country and each week
        df = self.calculate_rolling_mean(df) #rolling mean
        df = df[pd.to_datetime(df['date']).dt.day_name().isin(['Monday'])] #selecting mondays
        df['mobility'] = df.mean(axis=1) #mean between the 4 indexes
        for col in ['recreation', 'parks', 'driving', 'walking']:
            del df[col]
        return df


    def calculate_rolling_mean(self, df):
        # calculates the rolling mean of the mobility, for each country, with a 7-days window
        new_df = pd.DataFrame(columns=df.columns)
        for c in set(df['country']):
            tmp = df[df['country']==c]
            tmp = self.calculate_rolling_mean_single(tmp)
            new_df = new_df.append(tmp)
        new_df = new_df.reset_index(drop=True)
        return new_df


    def calculate_rolling_mean_single(self, df):
        new_df = pd.DataFrame()
        new_df['country'] = list(df['country'][6:])
        new_df['date'] = list(df['date'][:-6])
        for col in ['recreation', 'parks', 'driving', 'walking']:
            new_df[col] = list(df[col].rolling(window=7).mean()[6:])
        return new_df


    def get_normalized_mobility(self, df):
        # normalizes the mobility between -1 and 1
        values = list(df['mobility'])
        n = []
        for v in values:
            n.append(self.get_mobility_level_single(v))
        df['mobility_norm'] = n
        return df


    def get_mobility_level_single(self, m):
        # from [...,-76] to [84,...]
        # intervals length = 8
        v = -1
        limit = -76
        while m >= limit and limit < 84:
            v += 0.1
            limit += 8
        return round(v, 2)+0



    def get_week_doc(self, week):
        # returns the final json to be uploaded to mongodb
        if week in set(self.df['date']):
            tmp = self.df[self.df['date']==week]
            doc = {}
            countries = list(tmp['country'])
            mobilities = list(tmp['mobility'])
            mobilities_norm = list(tmp['mobility_norm'])
            country_dict = covid_countries_codes()
            for i in range(len(countries)):
                c = countries[i].lower().replace(' ', '-')
                if c in country_dict:
                    doc[country_dict[c]] = {'mobility_abs_value': mobilities[i], 'mobility_index': mobilities_norm[i]}
            final = {'week': week, 'mobility': doc}
            return final
        else:
            return ''