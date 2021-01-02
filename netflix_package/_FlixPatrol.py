import requests
import pandas as pd
from . import _functions as FS

class FlixPatrol():
    """
    Scarpettare flixpatrol.com
    fornisce in output un dataframe city;week;position;title
    """

    def __init__(self, all_countries):
        self.all_countries = all_countries
        self.base_url = "https://flixpatrol.com/top10/netflix/" + "<COUNTRY>" + "/2020-" + "<WEEK>"

#------------------------------------------------------------------------------#

    def get_countries(self):
        html = requests.get("https://flixpatrol.com/top10/netflix/italy/history").text
        lines = html.split("\n")
        lines = [l.lstrip() for l in lines]
        lines = lines[113:272]
        countries=[]
        for l in lines:
            if l != '':
                countries.append(l[l.rfind('">')+2 : l.index("</a>")])
        return countries

#------------------------------------------------------------------------------#

    def build_url(self, country, week):
        url = self.base_url.replace("<COUNTRY>", country)
        week = "00"+str(week) if week<10 else "0"+str(week)
        url = url.replace("<WEEK>", week)
        return url

    def html_lines(self, url):
        html = requests.get(url).text
        lines = html.split("\n")
        lines = [l.lstrip() for l in lines]
        if '<div class="alert alert-darker">No data for selected period or region</div>' in lines:
            return -1
        else:
            return lines

    def get_titles(self, lines):
        titles = []
        for i in range(1,11):
            try:
                it = lines.index('<div class="col-lg-6 col-6 pt-1 pb-1 threedots">')+1
                if str(i) in lines[it-2]:
                    tl = lines[it]
                    title = tl[tl.index('title="')+7 : ]
                    title = title[: title.index('">')]
                    title = title.replace('&#39;','')
                    titles.append(title)
                else:
                    titles.append("")
                lines = lines[it+1 : ]
            except:
                titles.append('')
        return titles

#------------------------------------------------------------------------------#

    def get_week_chart(self, week, countries=[]):
        if type(week) is str:
            week = FS.get_week_number(week)
        if type(countries) is str:
            countries = [countries]
        elif len(countries)==0:
            countries=self.all_countries

        urls = [self.build_url(country, week) for country in countries]
        htmls_lines = [self.html_lines(url) for url in urls]

        if type(htmls_lines) is int and htmls_lines==-1:
            return -1

        week_titles = []
        for html_lines in htmls_lines:
            #print(html_lines[4])
            week_titles.append(self.get_titles(html_lines))
        entries = []
        for i,wt in enumerate(week_titles):
            [entries.append([countries[i], week, p+1, t]) for p,t in enumerate(wt)]
        df = pd.DataFrame(entries, columns=['country','week','position', 'title'])
        return df

    def get_weeks_chart(self, weeks, countries=[]):
        df = pd.DataFrame(columns=['country','week','position', 'title'])


        if type(weeks) is str:
            weeks=[weeks]
        if type(weeks) is int:
            weeks = [weeks]
        if type(countries) is str:
            countries = [countries]
        elif len(countries)==0:
            countries = self.all_countries

        for w in weeks:
            single_week = self.get_week_chart(w, countries)
            if type(single_week) is int and single_week == -1:
                return -1;
            else:
                # print("week n. " + str(w) + " downloaded")
                df = df.append(single_week)
        return df

#------------------------------------------------------------------------------#
