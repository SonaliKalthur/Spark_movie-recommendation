#   --master yarn-client \
#   --num-executors 5 \
#   output1a-b.py

from pyspark import SparkContext
import re
#
# ####### function to count users in the ratings.csv

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
# (movie_id, (user_id, 1))
def extract_rating(record):
   try:
     user_id, movie_id, rating, timestamp = record.strip().split(",")
     return (movie_id, (user_id, 1))
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

# This functions convert tuples of ((title, genre), count)into key,value pair of the following format
# (genre, user)--> counter
def map_to_pair(record): # record = (title, genre), count
   #title_genre, count = record
    genre, count = record
    user, counter = count
   #title, genre = title_genre
    return ((genre, user), counter)

# This functions convert tuples of ((genre1,userid1), count) into key,value pair of the following format
# (user, (genre,count))   
def rearrange2(record): #record = (genre1,userid1), count
    genre_user, count = record
    genre, user = genre_user
    return (user, (genre,count))
    
# This functions convert tuples of ((genre1,userid1), count) into key,value pair of the following format
# (user, (genre,count))
def rearrangeusers(record):
    genre_userid, count = record
    genre, userid = genre_userid
    return (genre,(userid,count))

def rearrange3(record): #record = user, ((genre, countg), countd)
    user, genre_countg_countd = record
    genre_countg, countd = genre_countg_countd
    genre, countg = genre_countg
    return (genre, (user,(countg,countd)))

#combiner used for aggregate function
def merge_max_movie(top_movie_list, current_movie_count):
  top_list = sorted(top_movie_list+[current_movie_count], key=lambda rec:rec[-1], reverse=True)
  return top_list[:5]

#used for aggregate function
def merge_combiners(top_movie_list_1, top_movie_list_2):
  top_list = sorted(top_movie_list_1+top_movie_list_2, key=lambda rec:rec[-1], reverse=True)
  return top_list[:5]

#used for aggregate function
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

#call function to transform ratings -> movieid, (userid, count)
 user_ratings_count = ratings.map(extract_rating) #.reduceByKey(sum_rating_count)
#print(user_ratings_count.take(10))

#call function to tranform movie_data -> movieid, (title, genre) # genre is broken down by pipe
 movie_genre = movie_data.flatMap(pair_movie_to_genre)
#print(movie_genre.take(10))
#movie_genre.saveAsTextFile("combinedusergenre_02upd")

#call function to join movie_genre with user_ratings_count -> (genre, user), count
 join_users_genre = movie_genre.join(user_ratings_count).values().map(map_to_pair).reduceByKey(sum_rating_count)
#print(join_users_genre.take(10))

# call function to rearrange (genre, user), count -> genre, (user, count)
 join_users_genre_reduce = join_users_genre.map(rearrangeusers)
#print(join_users_genre_reduce.take(10))

# call function to output top 5 users per genre -> genre1, [(user1, count), (user2, count)...(user5, count)]
 genre_top5_users = join_users_genre_reduce.aggregateByKey([], merge_max_movie, merge_combiners, 1)
#print(genre_top5_users.take(10))

#******* solution for workload1(b)**********
# call function to count users per dataset -> user, countperdataset
 user_count_value = ratings.map(count_user).reduceByKey(sum_rating_count)
#print(user_count_value.take(100))

# rearrange from (genre, user), count -> user, (genre, count)
 rearrange_user = join_users_genre.map(rearrange2)
#print(rearrange_user.take(10))

# join countofdataset with countofgenre -> user, (genre, countg), countd
 countd_countg_join = rearrange_user.join(user_count_value)
#print(countd_countg_join.take(5))

#  -> genre, (user, (countg, countd))
 user_rearrange3 = countd_countg_join.map(rearrange3)
#print(user_rearrange3.take(10))

#output -> user
 genre_top5_users = user_rearrange3.aggregateByKey([], merge_max_movie, merge_combiners, 1)
 genre_top5_users.saveAsTextFile("Top5MoviesPerGenre_Wholedataset_1")



















