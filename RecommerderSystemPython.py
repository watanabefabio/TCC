import time
import timeit
import pandas as pd
import numpy as np

class RecommerderSystemPython:

    def __init__(self, sourceFiles):
        self.sourceFiles = sourceFiles
        self.movies = None
        self.ratings = None
        self.users = None
        self.usersSimilarity = None

    def close(self):
        self.sourceFiles = None
        self.movies = None
        self.ratings = None
        self.users = None
        self.usersSimilarity = None

    def LoadMovies(self):
        path = self.sourceFiles + '/movies.csv'
        self.movies = pd.read_csv(path, ',', index_col=["movieId"])

    def LoadUsersAndRatings(self):
        path = self.sourceFiles + "/ratings.csv"
        self.ratings = pd.read_csv(path, ',', index_col=["userId", "movieId"], usecols=["userId", "movieId", "rating"])
        self.users = pd.DataFrame({"avgRatings": self.ratings.groupby("userId")["rating"].mean()}).reset_index().set_index("userId")

    def AppendUsersSimilarity(self, usersSimilarityAux):
        if (self.usersSimilarity is None):
            self.usersSimilarity = usersSimilarityAux
        else:
            self.usersSimilarity = pd.concat([self.usersSimilarity, usersSimilarityAux])

    def CalculateSimilarity(self):
        for index, row in self.users.iterrows():
            movies = self.ratings.query("userId == @index").reset_index().set_index("movieId")
            ratingsUsers_x_User = pd.merge(self.ratings.query("userId != @index").reset_index().set_index("movieId")\
            , movies, left_index=True, right_index=True, how="inner").reset_index().set_index(["userId_y", "userId_x"])
            ratingsUsers_x_User["rx_2"] = pd.eval("ratingsUsers_x_User.rating_x ** 2")
            ratingsUsers_x_User["ry_2"] = pd.eval("ratingsUsers_x_User.rating_y ** 2")
            ratingsUsers_x_User["rxy"] = pd.eval("ratingsUsers_x_User.rating_x * ratingsUsers_x_User.rating_y")
            usersSimilarityAux = ratingsUsers_x_User.groupby(["userId_y", "userId_x"])\
            .agg({"rx_2": {"srx_2": "sum"}, "ry_2": {"sry_2": "sum"}, "rxy": {"srxy": "sum"}})
            usersSimilarityAux.columns = usersSimilarityAux.columns.droplevel(0)
            usersSimilarityAux["similarity"] = usersSimilarityAux["srxy"] / np.sqrt(usersSimilarityAux["srx_2"] * usersSimilarityAux["sry_2"])
            self.AppendUsersSimilarity(usersSimilarityAux.loc[:,["similarity"]].reset_index()\
            .rename(index=str, columns={"userId_y": "userId1", "userId_x": "userId2"}).set_index(["userId1", "userId2"]))
    
    def GetRecommendations(self, userId, recommendationsNum):
        userAvgRatings = self.users.query("userId == @userId")["avgRatings"].unique()[0]
        mostSimilarUsers = self.usersSimilarity.query("userId1 == @userId").sort_values(by=["similarity"], ascending=False)
        moviesRatedByUser = self.ratings.query("userId == @userId").reset_index()["movieId"].unique()
        mostSimilarUsersMoviesAux = pd.merge(self.ratings[self.ratings.index.map(lambda x: x[1] not in moviesRatedByUser)].reset_index().set_index("userId"),\
        mostSimilarUsers.reset_index().set_index("userId2"), left_index=True, right_index=True, how="inner")\
        .reset_index().rename(index=str, columns={"index": "userId2"}).set_index("userId2")
        mostSimilarUsersMovies = pd.merge(mostSimilarUsersMoviesAux, self.users, left_index=True, right_index=True, how="inner").set_index("movieId")
        mostSimilarUsersMovies["calcAux"] = mostSimilarUsersMovies["similarity"] * (mostSimilarUsersMovies["rating"] - mostSimilarUsersMovies["avgRatings"])
        predictionsAux = mostSimilarUsersMovies.groupby("movieId")\
        .agg({"similarity": {"ssim": "sum"}, "calcAux": {"scalcAux": "sum"}})
        predictionsAux.columns = predictionsAux.columns.droplevel(0)
        predictionsAux["predictedRating"] = userAvgRatings + (1 / predictionsAux["ssim"]) * predictionsAux["scalcAux"]
        predictions = pd.merge(self.movies, predictionsAux, left_index=True, right_index=True, how="inner")\
        .sort_values(by=["predictedRating"], ascending=False).loc[:,["title", "predictedRating"]]
        print(predictions.head(recommendationsNum))

def GetRecommendationsForUserTest():
    rsPython.GetRecommendations(1, 10)

rsPython = RecommerderSystemPython("C:/Users/fwata/Google Drive/TCC/Desenvolvimento/Movielens/ml-latest-small/ml-latest-small")
print("LoadMovies: ", timeit.timeit(stmt=rsPython.LoadMovies, number=1))
print("LoadUsersAndRatings: ", timeit.timeit(stmt=rsPython.LoadUsersAndRatings, number=1))
print("CalculateSimilarity: ", timeit.timeit(stmt=rsPython.CalculateSimilarity, number=1))
print("GetRecommendations: ", timeit.timeit(stmt=GetRecommendationsForUserTest, number=1))
rsPython.close()
