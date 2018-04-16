import sys
import time
import neo4j.v1 as neo
import timeit
from enum import Enum
from neo4j.v1 import basic_auth
from neo4j.v1.result import StatementResult, Record
from neo4j.v1.api import GraphDatabase, Driver, Session, Transaction

class RecommenderSystemNeo4j:

    def __init__(self, serverURI, user, pwd):
        token = basic_auth(user, pwd)
        self.driver = GraphDatabase.driver(serverURI, auth=token)

    def close(self):
        self.driver.close()

    def DeleteAll(self):
        startTime = time.time()
        with self.driver.session() as session:
            session.run("MATCH ()-[r]-() DELETE r")
            session.run("MATCH (n) DELETE n")
        print("DeleteAll: ", time.time() - startTime)

    def HasRecords(self, results):
        try:
            for rec in results:
                return True
        except Exception:
            return False

    def CreateIndex(self, label, *args):
        startTime = time.time()
        txtDelimiter = "', '"
        txtProperties = "'" + txtDelimiter.join(args) + "'"
        delimiter = ", "
        properties = delimiter.join(args)
        with self.driver.session() as session:
            idxs = session.run("""
            CALL db.indexes() YIELD label, properties
            WHERE label = {label} AND ALL(x IN [{properties}] WHERE x IN properties)
            RETURN label, properties, COUNT(label) AS n
            """, label = label, properties = txtProperties)
            if self.HasRecords(idxs):
                session.run("DROP INDEX ON :{0}({1})".format(label, properties))
            session.run("CREATE INDEX ON :{0}({1})".format(label, properties))
        print("CreateIndex ", label, "{", properties, "}: ", time.time() - startTime)

    def LoadMovies(self):
        startTime = time.time()
        with self.driver.session() as session:
            session.run("""
            USING PERIODIC COMMIT 5000
            LOAD CSV WITH HEADERS FROM "file:///movies.csv" AS line
            CREATE (m:Movie{movieId:toInteger(line.movieId), title:line.title, genres:line.genres})
            """)
        print("LoadMovies: ", time.time() - startTime)

    def LoadUsersAndRatings(self):
        startTime = time.time()
        with self.driver.session() as session:
            session.run("""
            USING PERIODIC COMMIT 5000
            LOAD CSV WITH HEADERS FROM "file:///ratings.csv" AS line
            MATCH (m:Movie{movieId:toInteger(line.movieId)})
            MERGE (u:User{userId:toInteger(line.userId), userName:'User ' + toString(line.userId)})
            CREATE (u)-[:Rated{rating:toFloat(line.rating)}]->(m)
            """)
        with self.driver.session() as session:
            session.run("""
            MATCH (u:User)-[r:Rated]->(m:Movie)
            WITH u, AVG(r.rating) AS avgRatings
            SET u.avgRatings = avgRatings
            """)
        print("LoadUserAndRatings: ", time.time() - startTime)

    def CalculateSimilarity(self):
        startTime = time.time()
        with self.driver.session() as session:
            session.run("""
            MATCH (u1:User)-[r1:Rated]->(m:Movie)<-[r2:Rated]-(u2:User)
            WHERE u1 <> u2
            WITH u1, u2,
            SUM(r1.rating ^ 2) AS sX2,
            SUM(r2.rating ^ 2) AS sY2,
            SUM(r1.rating * r2.rating) AS sXY
            MERGE (u1)-[s:Similarity]-(u2)
            SET s.similarity = (sXY / sqrt(sX2 * sY2))
            """)
        print("CalculateSimilarity: ", time.time() - startTime)

    def GetRecommendations(self, userId, recommendationsNum):
        startTime = time.time()
        with self.driver.session() as session:
            for rec in session.run("""
                MATCH (u2:User)-[r:Rated]->(m:Movie), (u2)-[s:Similarity]-(u1:User {userId:{userId}})
                WHERE NOT((u1)-[:Rated]->(m))
                WITH m.movieId AS movieId, m.title AS title, s.similarity AS similarity, r.rating AS rating, u1.avgRatings AS avg1, u2.avgRatings AS avg2
                ORDER BY title, similarity DESC
                WITH movieId, title, avg1 + (1 / SUM(similarity)) * SUM(similarity * (rating - avg2)) AS predictedRating
                ORDER BY predictedRating DESC
                RETURN movieId, title, predictedRating
                LIMIT {recommendationsNum}
                """, userId = userId, recommendationsNum = recommendationsNum):
                print("movieId: ", rec["movieId"], " title: ", rec["title"], "predictedRating: ", rec["predictedRating"])
        print("GetRecommendations userId ", userId, ": ", time.time() - startTime)

srNeo4j = RecommenderSystemNeo4j("bolt://localhost:7687", "tcc", "tccneo4j")
srNeo4j.DeleteAll()
srNeo4j.CreateIndex("Movie", "movieId")
srNeo4j.CreateIndex("User", "userId")
srNeo4j.LoadMovies()
srNeo4j.LoadUsersAndRatings()
srNeo4j.CalculateSimilarity()
srNeo4j.GetRecommendations(1, 10)
srNeo4j.close()
