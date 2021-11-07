"""
Scripts for fetching and generating datasets. Run this script as "python3 preprocess.py" to generate the
datasets used for this project

Note that unless you have an API key with collegefootballdata.com, you will not be able
to actually run many of these functions. Its free to get your own, but you will have to get one for yourself.
To do this, get an API key, create a file called config.py in your directory, and place the key in that file
under the attribute `cfbkey`
"""
import requests
import pandas as pd
from pandas import io
import numpy as np
from config import cfbkey
import json
import os

from typing import List


def efficiencyFormatter(df):
    # Original Author -  Michael West (@Season5Ryze on github)
    # Edited by Sachchit Kunichetty to fit new data generation algorithm

    df_stats = df
    home_third_down = df_stats.loc[:, "home.thirdDownEff"].values
    away_third_down = df_stats.loc[:, "away.thirdDownEff"].values
    home_fourth_down = df_stats.loc[:, "home.fourthDownEff"].values
    away_fourth_down = df_stats.loc[:, "away.fourthDownEff"].values

    # Remove problematic index
    away_third_down = np.delete(away_third_down, [0])

    # Loop that parses given array and converts each value into a float of X/Y where the format of the string is "XX-YY"
    def convEff(down_array):
        for index, item in enumerate(down_array):
            if item is not np.nan:
                if item[0] == "0":
                    down_array[index] = 0
                elif len(item) == 3 and item[0] != "0":
                    down_array[index] = float(item[0]) / float(item[2])
                elif len(item) == 4:
                    down_array[index] = float(item[0]) / (
                        float(item[2]) * 10 + float(item[3])
                    )
                elif len(item) == 5:
                    down_array[index] = (
                        float(item[0]) * 10 + float(item[1])
                    ) / (float(item[3]) * 10 + float(item[4]))
            else:
                down_array[index] = np.nan
        return down_array

    # Run function on each numpy array
    home_fourth_down = convEff(home_fourth_down)
    home_third_down = convEff(home_third_down)
    away_fourth_down = convEff(away_fourth_down)
    away_third_down = convEff(away_third_down)

    # Add problematic value back in by hand
    away_third_down = np.insert(away_third_down, 0, "0.4")

    # Debug size issues: print("H 4 Sz: ", home_fourth_down.size, "\n", "H 3 Sz: ", home_third_down.size, "\n","A 4 Sz: ", away_fourth_down.size, "\n","A 3 Sz: ", home_third_down.size, "\n")

    # Add values to data frame column
    df_stats["home.thirdDownEff"] = home_third_down
    df_stats["home.fourthDownEff"] = home_fourth_down
    df_stats["away.thirdDownEff"] = away_third_down
    df_stats["away.fourthDownEff"] = away_fourth_down

    # Debug dataframe issues: print(df_stats["home.thirdDownEff"].head(5))
    return df_stats


def fetch_games(
    seasons: List[str],
    clear_cache: bool = False,
    cache_dir="./data/temp/games/",
    output_path="./data/games.csv",
):
    """
    Fetches all college football matchups for selected seasons.

    Args:
        seasons (List[str]): Seasons to generate matchup data for.
        clear_cache (bool, optional): If true, clears cached data and fetches data. Defaults to False.
        cache_dir (str, optional): Directory to cache fetched data to. Defaults to "./data/temp/games/".
        output_path (str, optional): Directory to store final csv to. Defaults to "./data/games.csv".
    """
    url = "http://api.collegefootballdata.com/games?year={}&seasonType=regular"
    cached_data_addr = cache_dir + "{}.csv"
    temp_data_addr = cache_dir + "{}.json"
    games = None
    for year in seasons:
        formatted_cached_data_addr = cached_data_addr.format(year)
        formatted_temp_data_addr = temp_data_addr.format(year)
        if os.access(formatted_cached_data_addr, os.F_OK) and not clear_cache:
            df = pd.read_csv(formatted_cached_data_addr)
            if games is None:
                games = df
            else:
                games = games.append(df)
                print(
                    "Skipped cached data: {}".format(formatted_cached_data_addr)
                )
            continue
        print("Fetching Season {}".format(year))
        r = requests.get(
            url.format(year), headers={"Authorization": "Bearer " + cfbkey}
        )
        with open(formatted_temp_data_addr, "w") as file:
            file.write(r.text)
        df = pd.read_json(formatted_temp_data_addr)
        os.remove(formatted_temp_data_addr)
        df.to_csv(formatted_cached_data_addr, index=False)
        if games is None:
            games = df
        else:
            games = games.append(df)
    games.to_csv(output_path, index=False)


def fetch_game_stats(
    seasons: List[str],
    clear_cache: bool = False,
    cache_dir="./data/temp/game_stats/",
    output_path="./data/game_stats.csv",
):
    """
    Fetches game-by-game statistics for matchups in selected seasons.

    Args:
        seasons (List[str]): Seasons to fetch game-by-game statistics for.
        clear_cache (bool, optional): If true, clears cached data and fetches new data. Defaults to False.
        cache_dir (str, optional): The directory to cache data to. Defaults to "./data/temp/game_stats/".
        output_path (str, optional): The output path to save the final csv to. Defaults to "./data/game_stats.csv".
    """
    overall_stats = None
    url = "http://api.collegefootballdata.com/games/teams?year={}&week={}"
    cached_file_addr = cache_dir + "{}.csv"
    for year in seasons:
        formatted_cached_file_addr = cached_file_addr.format(year)
        if os.access(formatted_cached_file_addr, os.F_OK) and not clear_cache:
            temp = pd.read_csv(formatted_cached_file_addr)
            if overall_stats is None:
                overall_stats = temp
            else:
                overall_stats = overall_stats.append(temp)
            print("Skipped cached data: {}".format(formatted_cached_file_addr))
            continue
        week = 1
        dictionary = {}
        count = 0
        r = requests.get(
            url.format(year, week),
            headers={"Authorization": "Bearer " + cfbkey},
        )
        while r.text != "[]":
            print("Fetching Season {}, Week {}".format(year, week))
            temp = json.loads(r.text)
            for file in temp:
                if file["teams"][0]["homeAway"] == "home":
                    home = file["teams"][0]
                    away = file["teams"][1]
                else:
                    home = file["teams"][1]
                    away = file["teams"][0]
                if "id" not in dictionary:
                    dictionary["id"] = []
                dictionary["id"].append(file["id"])
                for stat in home["stats"]:
                    key = "home.{}".format(stat["category"])
                    home[key] = stat["stat"]
                    if key not in dictionary:
                        dictionary[key] = [None] * count
                for stat in away["stats"]:
                    key = "away.{}".format(stat["category"])
                    away[key] = stat["stat"]
                    if key not in dictionary:
                        dictionary[key] = [None] * count
                for key in dictionary:
                    if key != "id":
                        if key[:4] == "home":
                            if key in home:
                                dictionary[key].append(home[key])
                            else:
                                dictionary[key].append(None)
                        else:
                            if key in away:
                                dictionary[key].append(away[key])
                            else:
                                dictionary[key].append(None)
                count += 1
            week += 1
            r = requests.get(
                url.format(year, week),
                headers={"Authorization": "Bearer " + cfbkey},
            )
        temp = pd.DataFrame(dictionary)
        temp.to_csv(formatted_cached_file_addr, index=False)
        if overall_stats is None:
            overall_stats = temp
        else:
            overall_stats = overall_stats.append(temp)
    overall_stats = efficiencyFormatter(overall_stats)
    overall_stats.to_csv(output_path, index=False)


def genTeamIds(years):
    team_ids = {}
    url = "http://api.collegefootballdata.com/teams?year={}"
    for year in years:
        r = requests.get(
            url.format(year), headers={"Authorization": "Bearer " + cfbkey}
        )
        for team in json.loads(r.text):
            if team["school"] not in team_ids:
                team_ids[team["school"]] = team["id"]
    s = json.dumps(team_ids)
    with open("./data/team_ids.json", "w") as file:
        file.write(s)


def getRecruitingRankings(years):
    url = "http://api.collegefootballdata.com/recruiting/teams?year={}"
    dictionary = {
        "year": [],
        "team": [],
        "team_id": [],
        "rank": [],
        "points": [],
    }
    with open("./data/team_ids.json") as fin:
        team_names = json.load(fin)
    for year in years:
        r = requests.get(
            url.format(year), headers={"Authorization": "Bearer " + cfbkey}
        )
        for rank in json.loads(r.text):
            dictionary["year"].append(rank["year"])
            dictionary["team"].append(rank["team"])
            dictionary["team_id"].append(team_names[rank["team"]])
            dictionary["rank"].append(rank["rank"])
            dictionary["points"].append(rank["points"])
    df = pd.DataFrame(dictionary)
    df.to_csv("./data/recruiting_ranks.csv", index=False)


def main():
    if not os.access("./data/temp/", os.F_OK):
        os.mkdir("./data/temp")
    if not os.access("./data/temp/games", os.F_OK):
        os.mkdir("./data/temp/games")
    if not os.access("./data/temp/game_stats", os.F_OK):
        os.mkdir("./data/temp/game_stats")
    years = [
        "2010",
        "2011",
        "2012",
        "2013",
        "2014",
        "2015",
        "2016",
        "2017",
        "2018",
        "2019",
        "2020",
        "2021",
    ]
    effective_years = years[3:]
    if not os.access("./data/team_ids.json", os.F_OK):
        genTeamIds(years)
    print("-----Generating games.csv-----")
    fetch_games(effective_years)
    print("-----Generating game_stats.csv-----")
    fetch_game_stats(effective_years)
    print("-----Generating recruiting_ranks.csv-----")
    if not os.access("./data/recruiting_ranks.csv", os.F_OK):
        getRecruitingRankings(years)


if __name__ == "__main__":
    main()
