#Convert down efficiency statistics to a percentage, Written by Michael West for MDST FA '21 CFB Project, 10/10/21
import pandas as pd
import numpy as np
def efficiencyFormatter(df):
    #Read data, drop rows that have NaN values for our data
    df_stats = df
    #Create NumPy arrays for each of our column's values
    home_third_down = df_stats.loc[:, "home.thirdDownEff"].values
    away_third_down = df_stats.loc[:, "away.thirdDownEff"].values
    home_fourth_down = df_stats.loc[:, "home.fourthDownEff"].values
    away_fourth_down = df_stats.loc[:, "away.thirdDownEff"].values
    #Debug third down issue: print(df_stats["away.thirdDownEff"])

    #Remove problematic index
    away_third_down = np.delete(away_third_down, [0])

    #Loop that parses given array and converts each value into a float of X/Y where the format of the string is "XX-YY"
    def convEff(down_array):
        for index, item in enumerate(down_array):
            #Debug error in specific ojects: print("Index: ", index, "\n", "Item: ", item)
            if(item[0] == '0'):
                down_array[index] = 0
            elif(len(item) == 3 and item[0] != '0'):
                down_array[index] = float(item[0]) / float(item[2])
            elif(len(item) == 4):
                down_array[index] = float(item[0]) / (float(item[2])*10 + float(item[3]))
            elif(len(item) == 5):
                down_array[index] = (float(item[0])*10 + float(item[1])) / (float(item[3])*10 + float(item[4]))
        return down_array

    #Run function on each numpy array
    home_fourth_down = convEff(home_fourth_down)
    home_third_down = convEff(home_third_down)
    away_fourth_down = convEff(away_fourth_down)
    away_third_down = convEff(away_third_down)

    #Add problematic value back in by hand
    away_third_down = np.insert(away_third_down, 0, "0.4")

    #Debug size issues: print("H 4 Sz: ", home_fourth_down.size, "\n", "H 3 Sz: ", home_third_down.size, "\n","A 4 Sz: ", away_fourth_down.size, "\n","A 3 Sz: ", home_third_down.size, "\n")

    #Add values to data frame column
    df_stats["home.thirdDownEff"] = home_third_down
    df_stats["home.fourthDownEff"] = home_fourth_down
    df_stats["away.thirdDownEff"] = away_third_down
    df_stats["away.fourthDownEff"] = away_fourth_down

    #Debug dataframe issues: print(df_stats["home.thirdDownEff"].head(5))
    return df_stats
