#   --master yarn-client \
#   --num-executors 5 \
#   output1a-b.py
from pyspark import SparkContext
import re

## This function convert entries of movies.csv into key,value pair of the following format
# movie_id -> genre
# since there may be multiple genre per movie, this function returns a list of key,value pair
def pair_movie_to_genre(record):
  try:
    fields = re.findall('("[^"]+"|[^,]+)', record.strip())
    #print(fields[2])
    genres = fields[2].strip().split("|")
    movie_id = fields[0]
    return [(movie_id, genre.strip()) for genre in genres]
  except:
    return []

# This function convert entries of ratings.csv into key,value pair of the following format
def extract_rating(record):
   try:
     user_id, movie_id, rating, timestamp = record.strip().split(",")
     return (movie_id, (user_id, (1, float(rating))))
   except:
     return ()

#Count the number of user
def count_user(record):
   try:
     user_id, movie_id, rating, timestamp = record.strip().split(",")
     return (user_id, 1)
   except:
     return ()
#Counts the rating for whole dataset
def rating_user(record):
   try:
     user_id, movie_id, rating, timestamp = record.strip().split(",")
     return (user_id, float(rating))
   except:
     return ()

#This function is used by reduceByKey function to sum the count of the same key
# This functions takes in two values - merged count from previous call of sum_rating_count, and the currently processed count
def sum_rating_count(reduced_count, current_count):
   return reduced_count+current_count

def map_to_pair1(record): # record = genre, (user_id,(countg, rating))
    genre, user_count_rate = record
    user_id, count_rate = user_count_rate
    countg, rating = count_rate
    return ((genre, user_id), countg)

def map_to_pair2(record): # record = (genre, user), countg
    genre_user, countg = record
    genre, user = genre_user
    return (user, (genre, countg))

def map_to_pair3(record): #userid, ((Genre, countg), ratingd))
    user, genre_countg_ratingd = record
    genre_countg, ratingd = genre_countg_ratingd
    genre, countg = genre_countg
    return (genre, (user,(countg, ratingd)))

# format user_id, (user_countd, ratingd)
def map_average_rating(record):
    user_id, countd_ratingd = record
    countd, ratingd = countd_ratingd
    rating_avg_d = round(ratingd /countd,1)
    return (user_id, rating_avg_d)

#format (genre,(userid,count))
def rearrangeusers(record):
    genre_userid, count = record
    genre, userid = genre_userid
    return (genre,(userid,count))

#combiner used for aggregate function
def merge_max_movie(top_movie_list, current_movie_count):
  top_list = sorted(top_movie_list+[current_movie_count], key=lambda rec:rec[-1], reverse=True)
  return top_list[:5]
#combiner used for aggregate function
def merge_combiners(top_movie_list_1, top_movie_list_2):
  top_list = sorted(top_movie_list_1+top_movie_list_2, key=lambda rec:rec[-1], reverse=True)
  return top_list[:5]

#combiner used for aggregate function
def merge_max_movie_u(top_movie_list, current_movie_count):

  top_list = sorted(top_movie_list+[current_movie_count], key=lambda rec:rec[-1], reverse=True)
  return top_list[:1]
#combiner used for aggregate function
def merge_combiners_u(top_movie_list_1, top_movie_list_2):
  top_list = sorted(top_movie_list_1+top_movie_list_2, key=lambda rec:rec[-1], reverse=True)
  return top_list[:1]

if __name__ == "__main__":
 sc = SparkContext(appName="Top 5 Users per Genre ")

#read ratings.csv to rdd
 ratings = sc.textFile("/share/movie/ratings.csv")

#read movie_data.csv to rdd
 movie_data = sc.textFile("/share/movie/movies.csv")

#call function to transform ratings -> movieid, (userid, (count,rating))
 user_ratings_count = ratings.map(extract_rating) #.reduceByKey(sum_rating_count)
#print(user_ratings_count.take(10))

#call function to tranform movie_data -> movieid, (title, genre) # genre is broken down by pipe
 movie_genre = movie_data.flatMap(pair_movie_to_genre)
#print(movie_genre.take(50))
#movie_genre.saveAsTextFile("combinedusergenre_02upd")

#call function to join movie_genre with user_ratings_count -> (genre, user), count
 join_users_genre = movie_genre.join(user_ratings_count).values().map(map_to_pair1).reduceByKey(sum_rating_count)
#print(join_users_genre.take(50))


#for question 1(d)
 user_count = ratings.map(count_user).reduceByKey(sum_rating_count)
#print(user_count.take(10))

 rating_count = ratings.map(rating_user).reduceByKey(sum_rating_count)
#print(rating_count.take(10))

#(user_id, #sum user count)join (user_id, #sum of rating) --> user_id, ratingd
 join_users_rating = user_count.join(rating_count).map(map_average_rating)
#print(join_users_rating.take(10))

# --> user, (genre, countg)
 map_genre_countg = join_users_genre.map(map_to_pair2)
#print(map_genre_countg.take(10))

# --> userid, ((Genre, countg), ratingd))
 join_genre_ratingd = map_genre_countg.join(join_users_rating)
#print(join_genre_ratingd.take(10))

# --> Genre, (User, (countg, rating))
 map_genre_countg = join_genre_ratingd.map(map_to_pair3)
#print(map_genre_countg.take(10))

# call function to output average rating for top 5 users per genre for entire dataset -> genre1, ((user1, (countg, ratingd))
 genre_top5_users_ratingd = map_genre_countg.aggregateByKey([], merge_max_movie, merge_combiners, 1)
 genre_top5_users_ratingd.saveAsTextFile("Average_2")

















