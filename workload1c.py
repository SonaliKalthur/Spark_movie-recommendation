#   --master yarn-client \
#   --num-executors 5 \
#   Workload1c.py

from pyspark import SparkContext
import re


# This function convert entries of movies.csv into key,value pair of the following format
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
# (movie_id, (user_id, (1, float(rating))))

def extract_rating(record):
   try:
     user_id, movie_id, rating, timestamp = record.strip().split(",")
     return (movie_id, (user_id, (1, float(rating))))
   except:
     return ()

# This function convert entries of ratings.csv into key,value pair of the following format
# user_id -> 1
def count_user(record):
   try:
     user_id, movie_id, rating, timestamp = record.strip().split(",")
     return (user_id, 1)
   except:
     return ()
     
# This function is used by reduceByKey function to sum the count of the same key
# This functions takes in two values - merged count from previous call of sum_rating_count, and the currently processed count
def sum_rating_count(reduced_count, current_count):
   return reduced_count+current_count

# This functions convert tuples of genre, ((user_id,(countg, rating))) into key,value pair of the following format
# ((genre, user_id), countg)
def map_to_pair1(record): # record = genre, (user_id,(countg, rating))
    genre, user_count_rate = record
    user_id, count_rate = user_count_rate
    countg, rating = count_rate
    return ((genre, user_id), countg)

# This functions convert tuples of genre, ((user_id,(countg, rating))) into key,value pair of the following format
# ((genre, user_id), rating)
def map_to_pair2(record): # record = genre, (user_id,(countg, rating))
    genre, user_count_rate = record
    user_id, count_rate = user_count_rate
    countg, rating = count_rate
    return ((genre, user_id), rating)


# This function takes the statistic of ratings per genre and calculate the average rating
def map_average_rating(record):
  genre_user_id, countg_rating = record
  genre, user_id = genre_user_id
  countg, rating = countg_rating
  rating_average = round(rating /countg,1)
  return (genre, (user_id,(countg, rating_average)))


# This functions convert tuples of (genre1,userid1), count into key,value pair of the following format
# (user, (genre,count))
def rearrange2(record): #record = (genre1,userid1), count
    genre_user, count = record
    genre, user = genre_user
    return (user, (genre,count))

# This functions convert tuples of (genre1,userid1), count into key,value pair of the following format
# (genre,(userid,count))
def rearrangeusers(record):
    genre_userid, count = record
    genre, userid = genre_userid
    return (genre,(userid,count))

# This function is used by the aggregateByKey function to get the top 5 users,count pair per genre is being merged in the same combiner
# This function takes in either the starting value []
# The (user_id, (count per genre,average rating per genre))
def merge_max_movie(top_movie_list, current_movie_count):
  top_list = sorted(top_movie_list+[current_movie_count], key=lambda rec:rec[-1], reverse=True)
  return top_list[:5]


# This function is used by the aggregateByKey function to get the top 5 users,count pair per genre is being merged in the same combiner
# This function takes in either the starting value []
# The (user_id, (count per genre,average rating per genre))
def merge_combiners(top_movie_list_1, top_movie_list_2):
  top_list = sorted(top_movie_list_1+top_movie_list_2, key=lambda rec:rec[-1], reverse=True)
  return top_list[:5]


if __name__ == "__main__":
 sc = SparkContext(appName="Top 5 Users per Genre ")

#read ratings.csv to rdd
 ratings = sc.textFile("/share/movie/ratings.csv")

#read movie_data.csv to rdd
 movie_data = sc.textFile("/share/movie/movies.csv")

#call function to transform ratings -> movieid, (userid, count)
 user_ratings_count = ratings.map(extract_rating) #.reduceByKey(sum_rating_count)


#call function to tranform movie_data -> movieid, (title, genre) # genre is broken down by pipe
 movie_genre = movie_data.flatMap(pair_movie_to_genre)

#movie_genre.saveAsTextFile("combinedusergenre_02upd")

#call function to join movie_genre with user_ratings_count -> (genre, user), count
 join_users_genre = movie_genre.join(user_ratings_count).values().map(map_to_pair1).reduceByKey(sum_rating_count)


 join_users_genre_1 = movie_genre.join(user_ratings_count).values().map(map_to_pair2).reduceByKey(sum_rating_count)


#((genre, user_id), countg)join((genre, user_id), rating)) ---> ((genre,user_id),(#sum of count,#sum of rating))
 join_rating_genre_user = join_users_genre.join(join_users_genre_1).map(map_average_rating)


# call function to output top 5 users per genre -> genre1, [(user1, count), (user2, count)...(user5, count)]
 genre_top5_users = join_rating_genre_user.aggregateByKey([], merge_max_movie, merge_combiners, 1)
 genre_top5_users.saveAsTextFile("Average_Rating_Genre3")






















