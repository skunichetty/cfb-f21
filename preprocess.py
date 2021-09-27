"""
Scripts for fetching and generating datasets.

Note that unless you have an API key with collegefootballdata.com, you will not be able
to actually run many of these functions. Its free to get your own, but you will have to get one for yourself.
To do this, get an API key, create a file called config.py in your directory, and place the key in that file
under the attribute `cfbkey`
"""
import requests
import pandas as pd
from config import cfbkey
import json
import os

from typing import List

years = ["2013", "2014", "2015", "2016", "2017", "2018", "2019", "2020", "2021"]


def get_b1g(years: List[str]):
    url = "http://api.collegefootballdata.com/games?year={}&seasonType=regular&conference=B1G"
    for year in years:
        r = requests.get(
            url.format(year), headers={"Authorization": "Bearer " + cfbkey}
        )
        with open("./data/raw_data/%s.json" % year, "w") as fout:
            fout.write(r.text)
            fout.close()


def build_games_data(years: List[str]):
    df = pd.read_json("./data/raw_data/%s.json" % years[0])
    for year in years[1:]:
        temp = pd.read_json("./data/raw_data/%s.json" % year)
        df = df.append(temp)
    df.to_csv("./data/games.csv")


def fetch_game_stats(fname: str):
    df = pd.read_csv(fname)
    cache_count = ((len(df) - len(df) % 100) + (len(df) % 100 > 0) * 100) // 100
    overall_stats = None
    url = "http://api.collegefootballdata.com/games/teams?year={}&gameId={}"
    for i in range(cache_count):
        if os.access("./data/raw_data/temp/game_stats_%s.csv" % i, os.F_OK):
            print("Skipped Cached Data: %d" % (i))
            continue
        stats = None
        for index, row in df[100 * i : 100 * i + 100].iterrows():
            print("Cache: %d, Current Game: %d" % (i, index))
            r = requests.get(
                url.format(row["season"], row["id"]),
                headers={"Authorization": "Bearer " + cfbkey},
            )
            if r.text != "[]":
                file = json.loads(r.text)[0]
                if file["teams"][0]["homeAway"] == "home":
                    home = file["teams"][0]
                    away = file["teams"][1]
                else:
                    home = file["teams"][1]
                    away = file["teams"][0]
                for stat in home["stats"]:
                    file["home.%s" % stat["category"]] = [stat["stat"]]
                for stat in away["stats"]:
                    file["away.%s" % stat["category"]] = [stat["stat"]]
                file["teams"] = None
            else:
                break
            temp = pd.DataFrame(file)
            if stats is not None:
                stats = stats.append(temp)
            else:
                stats = temp
        if stats is not None:
            stats.to_csv("./data/raw_data/temp/game_stats_%s.csv" % i)
    for file in os.scandir("./data/raw_data/temp"):
        temp = pd.read_csv(file.path)
        if overall_stats is not None:
            overall_stats = overall_stats.append(temp)
        else:
            overall_stats = temp
    overall_stats.to_csv("./data/game_stats.csv")


if not os.access("./data/raw_data/2013.json", os.F_OK):
    print("Raw Data not found, retrieving data")
    get_b1g(years)
build_games_data(years)
fetch_game_stats("./data/games.csv")
