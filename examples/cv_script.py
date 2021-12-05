from sklearn.model_selection import RandomizedSearchCV, train_test_split
from sklearn.svm import SVC
import pandas as pd
from scipy.stats import uniform

"""
    Example CV script.
"""


data = pd.read_csv("./data/games.csv")
# ----do your data cleaning here----
data = data[data["home_points"].notna()]
data = data[data["away_points"].notna()]
data = data[data["attendance"].notna()]
data["label"] = data.apply(
    lambda row: int(row["home_points"] > row["away_points"]), axis=1
)
# ------------------------------
# Define Model parameters to test
params = {
    "kernel": ["rbf", "linear"],
    "C": uniform(loc=0.001, scale=3),
    "gamma": uniform(loc=1, scale=10),
}
# Create Randomized Search CV
svc = SVC()
cv = RandomizedSearchCV(
    svc, param_distributions=params, n_jobs=-1, verbose=True
)
print(data["attendance"])
X_train, X_test, y_train, y_test = train_test_split(
    data["attendance"].to_numpy().reshape(-1, 1), data["label"], test_size=0.2
)
cv.fit(X_train, y_train)
print(cv.get_params())
