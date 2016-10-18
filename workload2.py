#!/usr/bin/python3

import sys
import itertools
from math import sqrt
from operator import add
from os.path import join, isfile, dirname
import re
from itertools import combinations
import numpy as np
from collections import defaultdict

from pyspark import SparkConf, SparkContext

global record_separator
record_separator = "::"


def parseRating(line):
    """
    Parses a rating record in MovieLens format userId,movieId,rating,timestamp .
    """
    fields = re.findall('("[^"]+"|[^,]+)', line.strip())
    return int(fields[3]) % 10, (int(fields[0]), int(fields[1]), float(fields[2]))

def parseMovie(line):
    """
    Parses a movie record in MovieLens format movieId,movieTitle .
    """
    fields = re.findall('("[^"]+"|[^,]+)', line.strip())
    return fields[0], fields[1]


def parseRating_personal(line):
    '''
    Parse a rating record in Movielens formart user_id,(movie_id,rating)
    '''
    fields = re.findall('("[^"]+"|[^,]+)', line.strip())
    return int(fields[3]) % 10, (fields[0], fields[1], float(fields[2]))


def loadRatings(ratingsFile):
    """jkl;
    Load ratings from file.
    """
    if not isfile(ratingsFile):
        print("File %s does not exist." % ratingsFile)
        sys.exit(1)
    f = open(ratingsFile)
    ratings = filter(lambda r: r[2] > 0, [parseRating(line)[1] for line in f])
    f.close()
    if not ratings:
        print("No ratings provided.")
        sys.exit(1)
    else:
        return ratings

def moviecombinations(record):
    for m1,m2 in combinations(record,2):
        return (m1[0],m2[0]),(m1[1],m2[1])

def cosinesim(record):
    i = 0.0
    j1 = 0.0
    j2 = 0.0
    count = 0
    movie_pair, ratings = record
    for rating1, rating2 in ratings:
        i = i+ (rating1 * rating2)
        j1 = j1+ ((rating1)**2)
        j2 = j2+ ((rating2)**2)
        count = count+1
    similarity = (i/(np.math.sqrt(j1)*np.math.sqrt(j2)))
    return(movie_pair, (similarity, count))

def moviekey(record):
    movie_pair, similarity_count = record
    movie1, movie2 = movie_pair
    return (movie1, (movie2, similarity_count))

def sorted_movies(record,n):
    movie_id, movie_similarity = record
    movie_similarity.sort(key=lambda x: x,reverse=True)
    return movie_id, movie_similarity[:n]

if __name__ == "__main__":
    #hash the lines below to run on pycharm
    if len(sys.argv) != 3:
        print("Usage: pyspark --master yarn-client movie_lens_ALS.py movieLensDataDir personalRatingsFile")
        sys.exit(1)

    # set up environment
    conf = SparkConf() \
      .setAppName("MovieLensALS") \
      .set("spark.executor.memory", "2g")
    sc = SparkContext(conf=conf)
    logger = sc._jvm.org.apache.log4j
    logger.LogManager.getLogger("org").setLevel(logger.Level.ERROR)
    logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)

    # load personal ratings
    #replace sys.argv[2] with local path to personal ratings
    myRatings = loadRatings(sys.argv[2])
    #myRatings = loadRatings("/Users/SKalthur/Documents/Cloud Computing/Assignment_folder/personalRatings.txt")

    myRatingsRDD = sc.parallelize(myRatings, 1)
    myRatingsRDD = myRatingsRDD.map(lambda x: (str(x[0]),str(x[1]),x[2]))

    #myRatingsRDD = myRatingsRDD.map(parseRating_personal).values()
    #myRatingsRDD = sc.textFile(join("/Users/SKalthur/Documents/Cloud Computing/Assignment_folder/","personalRatings.txt")).map(parseRating_personal).values()
    #myRatingsRDD = sc.textFile(sys.argv[2]).map(parseRating_personal).values()
    #myRatingsRDD = sc.textFile(join(sys.argv[2], "personalRatings.txt")).map(parseRating)

    #convert personalratings to dictionary
    myratings_dict = {}
    for (userid, movie, rating) in myRatingsRDD.collect():
        myratings_dict[str(movie)] = rating
    #print(myratings_dict)

    #convert personalratings to tuple format (userid, (movieid, rating))
    myRatingsRDD_userkey = myRatingsRDD.map(lambda x: (str(x[0]),(str(x[1]),x[2]))).groupByKey().mapValues(list)

    # load ratings and movie titles
    # hash the below code to run on pycharm
    movieLensHomeDir = sys.argv[1]

    # ratings is an RDD of (last digit of timestamp, (userId, movieId, rating))
    #replace movieLensHomeDir with the local dir - SG
    #ratings = sc.textFile(join("/Users/SKalthur/Documents/Cloud Computing/Assignment_folder/", "ratings.csv")).map(parseRating)
    ratings = sc.textFile(join(movieLensHomeDir, "ratings.csv")).map(parseRating)
    # movies is an RDD of (movieId, movieTitle)
    #replace movieLensHomeDir with the local dir - SG
    movies = dict(sc.textFile(join(movieLensHomeDir, "movies.csv")).map(parseMovie).collect())
    #movies = dict(sc.textFile(join("/Users/SKalthur/Documents/Cloud Computing/Assignment_folder/", "movies.csv")).map(parseMovie).collect())
    #print(movies)
    numRatings = ratings.count()

    #construct (userid1, [(movieid1, rating),(movieid2, rating)....]
    new_ratings = ratings.values().map(lambda r: (str(r[0]), (str(r[1]),r[2]))).groupByKey().mapValues(list)
    #print(new_ratings.take(5))
    #construct (movie1, movie2), (rating1, rating2)...
    movie_pairs = new_ratings.filter(lambda x:len(x[1])>1).map(lambda x: moviecombinations(x[1])).groupByKey().mapValues(list)

    #Find cosine similarity -> (movie1, movie2), (similarity, count of users who co-rated(i and j))
    movie_cosine = movie_pairs.map(lambda x: cosinesim(x))
    #print(movie_cosine.take(10))
    #construct group by movie1, (movie2, similarity, count)
    movie_key = movie_cosine.map(lambda r: moviekey(r)).groupByKey().mapValues(list)
    #print(movie_key.take(10))

    #Gives the cosine similarity,number of corated user between the 15 movies which is rated andall other movies in the dataset
    movie_sim_dict = {}
    for (movie,movie_sim_data) in movie_key.collect():
        movie_sim_dict[movie] = movie_sim_data
    #print(movie_sim_dict)

    #Assignment workload2 - Neighbourhood formation
    p1 = { key:value for key, value in movie_sim_dict.items() if key in myratings_dict }
    #print(p1)
    #Assignment workload2 - Mapper functionality for mapping other movies in the dataset to 10 most similar in the movies in the personal ratings

    #filters out the entire similarity list to show only movies present in the personal.text to the m2
    movies_n = movie_cosine.filter(lambda x: (x[0][1]) in myratings_dict).map(lambda r: moviekey(r)).groupByKey().mapValues(list)
    movies_n_sorted = movies_n.map(lambda p: sorted_movies(p,10)).collect()
    #print(movies_n_sorted)
    #dictionary creation for the above
    movie_sim_dict_n = {}
    for (movie,movie_sim_data) in movies_n_sorted:
        movie_sim_dict_n[movie] = movie_sim_data
    #print(movie_sim_dict_n)

    #recommendation generation

    sim_rating_sums = defaultdict(int)
    sim_sums = defaultdict(int)

    for (user,movie, rating) in myRatingsRDD.map(lambda x: (x[0], x[1], x[2])).collect():


            if movie_sim_dict.get(movie,None):    # get the movies similar from the dictionary
            #if movie_sim_dict_n.get(movie,None):
                for (movie_sim,(sim,count)) in movie_sim_dict.get(movie,None):
                #for (movie_sim,(sim,count)) in movie_sim_dict_n.get(movie,None):
                    if movie_sim != movie:
                        sim_rating_sums[movie_sim] += sim * rating
                        sim_sums[movie_sim] += sim


    predictions = [(total/sim_sums[movie],movie) for movie,total in sim_rating_sums.items()]
    predictions.sort(key = lambda x: x[0],reverse=True)
    predictions_topn = predictions[:50]

    print("Movies recommended for you:")
    for i in range(len(predictions_topn)):
        print(("%2d: %s" % (i + 1, movies[predictions_topn[i][1]])).encode('ascii', 'ignore'))

    # clean up
    sc.stop()