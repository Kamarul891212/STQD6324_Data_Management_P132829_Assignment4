# STQD6324 Data Management

# Assignment 4

# MovieLens 100k Exploratory with Cassandra and Spark2

## Introduction

The analysis use the 'u.user' file from the MovieLens 100k Dataset (ml-100k.zip) with Python script to execute Cassandra Query Language (CQL) and Spark2 Structured Query Language (SQL) to get the following answer:

+ The average rating for each movie.
+ The top ten movies with the highest average ratings.
+ The users that rated at least 50 movies and their favourite movie genres.
+ All the users with age that is less than 20 years old.
+ All the users that have the occupation "scientist" and their age is between 30 and 40 years old.

The Python script include the following elements:

+ Python libraries used to execute Spark2 and Cassandra elements.
+ Functions to parse the u.user file into HDFS.
+ Functions to load, read and create Resilient Distributed Dataset (RDD) objects.
+ Functions to convert the RDD objects into DataFrame.
+ Functions to write the DataFrame into the Keyspace database created in Cassandra.
+ Functions to read the table back from Cassandra into a new DataFrame.

# Data Exploratory

## Dataset - MovieLens 100k

The MovieLens 100k dataset is a popular dataset used in the field of recommender systems and collaborative filtering research. It consists of 100,000 ratings from 943 users on 1,682 movies, collected by the GroupLens Research Project at the University of Minnesota. Each user has rated at least 20 movies. The dataset includes user demographic information such as age, gender and occupation, as well as movie information including titles, genres and release dates. It is widely used for benchmarking and developing recommendation algorithms due to its manageable size and well-structured data.

### u.data     

+ The full u data set, 100,000 ratings by 943 users on 1,682 items. Each user has rated at least 20 movies. Users and items are numbered consecutively from 1. The data is randomly ordered. This is a tab separated list of **user id | item id | rating | timestamp.** The time stamps are unix seconds since 1/1/1970 UTC.
              
### u.item

+ Information about the items (movies); this is a tab separated list of **movie id | movie title | release date | video release date | IMDb URL | unknown | Action | Adventure | Animation | Children's | Comedy | Crime | Documentary | Drama | Fantasy |    Film-Noir | Horror | Musical | Mystery | Romance | Sci-Fi | Thriller | War | Western |** The last 19 fields are the genres, a 1 indicates the movie is of that genre, a 0 indicates it is not; movies can be in several genres at once. The movie ids are the ones used in the u.data data set.
              
### u.user

+ Demographic information about the users; this is a tab separated list of **user id | age | gender | occupation | zip code**  The user ids are the ones used in the u.data data set.

## Code in Cassandra Query Language (CQL)

```
cqlsh

cqlsh> CREATE KEYSPACE IF NOT EXISTS movielens WITH replication = {'class': 'Sim                                                                                                                                                             pleStrategy', 'replication_factor': '1'};
cqlsh> CREATE TABLE IF NOT EXISTS movielens.movie_ratings (
   ...     movie_id int,
   ...     title text,
   ...     avg_rating float,
   ...     PRIMARY KEY (movie_id)
   ... );
cqlsh> CREATE TABLE IF NOT EXISTS movielens.top_ten_movies_avg_rating (
   ...     movie_id int,
   ...     title text,
   ...     avg_rating float,
   ...     rating_count int,
   ...     PRIMARY KEY (movie_id)
   ... );
cqlsh> CREATE TABLE IF NOT EXISTS movielens.top_ten_movies_rating_count (
   ...     movie_id int,
   ...     title text,
   ...     avg_rating float,
   ...     rating_count int,
   ...     PRIMARY KEY (movie_id)
   ... );
cqlsh> CREATE TABLE IF NOT EXISTS movielens.popular_movies (
   ...     movie_id int,
   ...     title text,
   ...     avg_rating float,
   ...     rating_count int,
   ...     PRIMARY KEY (movie_id)
   ... );
cqlsh> CREATE TABLE IF NOT EXISTS movielens.favourite_genres (
   ...     user_id int,
   ...     gender text,
   ...     genre text,
   ...     genre_count int,
   ...     PRIMARY KEY (user_id, genre)
   ... );
cqlsh> CREATE TABLE IF NOT EXISTS movielens.young_users (
   ...     user_id int,
   ...     age int,
   ...     gender text,
   ...     occupation text,
   ...     zip_code text,
   ...     PRIMARY KEY (user_id)
   ... );
cqlsh> CREATE TABLE IF NOT EXISTS movielens.scientist_users (
   ...     user_id int,
   ...     age int,
   ...     gender text,
   ...     occupation text,
   ...     zip_code text,
   ...     PRIMARY KEY (user_id)
   ... );
cqlsh> exit
```

## Writing Spark Output into Cassandra

```
vi Cassandra_Spark_Movielens.py
```

```
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# List of genre names based on the u.item file structure
genre_names = [
    "unknown", "Action", "Adventure", "Animation", "Children's", "Comedy",
    "Crime", "Documentary", "Drama", "Fantasy", "Film-Noir", "Horror",
    "Musical", "Mystery", "Romance", "Sci-Fi", "Thriller", "War", "Western"
]

def parse_rating(line):
    fields = line.split('\t')
    return Row(user_id=int(fields[0]), movie_id=int(fields[1]), rating=int(fields[2]), timestamp=int(fields[3]))

def parse_movie(line):
    fields = line.split('|')
    genres = [genre_names[i - 5] for i in range(5, 24) if fields[i] == '1']
    return Row(movie_id=int(fields[0]), title=fields[1], genres=genres)

def parse_user(line):
    fields = line.split('|')
    return Row(user_id=int(fields[0]), age=int(fields[1]), gender=fields[2], occupation=fields[3], zip_code=fields[4])
    
def write_to_cassandra(dataframe, table, keyspace):
    dataframe.write \
        .format("org.apache.spark.sql.cassandra") \
        .options(table=table, keyspace=keyspace) \
        .mode("append") \
        .save()

def read_from_cassandra(table, keyspace):
    return spark.read \
        .format("org.apache.spark.sql.cassandra") \
        .options(table=table, keyspace=keyspace) \
        .load()

if __name__ == "__main__":
    # Create a SparkSession
    spark = SparkSession.builder \
        .appName("MovieLensAnalysis") \
        .config("spark.cassandra.connection.host", "127.0.0.1") \
        .getOrCreate()

    # Load the ratings data
    ratings_lines = spark.sparkContext.textFile("hdfs:///user/maria_dev/kamarul/u.data")
    ratings = ratings_lines.map(parse_rating)
    ratingsDataset = spark.createDataFrame(ratings)

    # Load the movies data
    movies_lines = spark.sparkContext.textFile("hdfs:///user/maria_dev/kamarul/u.item")
    movies = movies_lines.map(parse_movie)
    moviesDataset = spark.createDataFrame(movies)

    # Load the users data
    users_lines = spark.sparkContext.textFile("hdfs:///user/maria_dev/kamarul/u.user")
    users = users_lines.map(parse_user)
    usersDataset = spark.createDataFrame(users)

    # Calculate the average rating for each movie
    averageRatings = ratingsDataset.groupBy("movie_id").agg(F.avg("rating").alias("avg_rating"))

    # Join with the movie titles
    movieRatings = averageRatings.join(moviesDataset, "movie_id").select("movie_id", "title", "avg_rating")

    # Display the average rating for each movie (top 10 without sorting)
    print("Average rating for each movie:")
    movieRatings.show(10, truncate=False)

    # Calculate the average rating and count of ratings for each movie
    movieStats = ratingsDataset.groupBy("movie_id").agg(
        F.avg("rating").alias("avg_rating"),
        F.count("rating").alias("rating_count")
    )

    # Join with the movie titles
    movieRatingsWithCount = movieStats.join(moviesDataset, "movie_id").select("movie_id", "title", "avg_rating", "rating_count")

    # Identify the top ten movies with the highest average rating 
    topTenMovies = movieRatingsWithCount.orderBy(F.desc("avg_rating")).limit(10).select("movie_id", "title", "avg_rating", "rating_count")

    # Display the top ten movies with the highest rating count
    print("Top ten movies with the highest rating average:")
    topTenMovies.show(truncate=False)
    
    # Identify the top ten movies with the highest rating count
    topTenMoviesRatingCount = movieRatingsWithCount.orderBy(F.desc("rating_count")).limit(10).select("movie_id", "title", "avg_rating", "rating_count")

    # Display the top ten movies with the highest rating count
    print("Top ten movies with the highest rating count:")
    topTenMoviesRatingCount.show(truncate=False)

    # Filter out movies with less than 10 ratings
    popularMovies = movieStats.filter("rating_count > 10")

    # Join with the movie titles
    movieRatingsWithCount1 = popularMovies.join(moviesDataset, "movie_id").select("movie_id", "title", "avg_rating", "rating_count")

    # Identify the movie with the highest average rating
    topMovie = movieRatingsWithCount1.orderBy(F.desc("avg_rating")).limit(10).select("movie_id", "title", "avg_rating", "rating_count")

    # Display the movie with the highest average rating
    print("Movie with the highest average rating (with more than 10 ratings count):")
    topMovie.show(truncate=False)

    # Find the users who have rated at least 50 movies
    usersWith50Ratings = ratingsDataset.groupBy("user_id").agg(F.count("rating").alias("num_ratings")).filter("num_ratings >= 50")

    # Explode genres array for easier aggregation
    explodedMoviesDataset = moviesDataset.withColumn("genre", F.explode(F.col("genres")))

    # Join datasets to get user, rating, and genre information
    usersGenres = ratingsDataset.join(explodedMoviesDataset, "movie_id").join(usersWith50Ratings, "user_id")

    # Aggregate to find the count of ratings per genre for each user
    usersGenresCount = usersGenres.groupBy("user_id", "genre").agg(F.count("genre").alias("genre_count"))

    # Window function to identify favourite genre(s) for each user
    windowSpec = Window.partitionBy("user_id").orderBy(F.desc("genre_count"))

    favouriteGenres = usersGenresCount.withColumn("rank", F.rank().over(windowSpec)).filter(F.col("rank") == 1).drop("rank")

    # Join with usersDataset to get gender
    favouriteGenresWithGender = favouriteGenres.join(usersDataset, "user_id").select("user_id", "gender", "genre", "genre_count")

    # Display the users who have rated at least 50 movies, their favourite movie genres, and gender (top 10)
    print("Top ten users who have rated at least 50 movies, their favourite movie genres, and gender:")
    favouriteGenresWithGender.show(10, truncate=False)

    # Find all the users with age less than 20 years old
    youngUsers = usersDataset.filter(usersDataset["age"] < 20)

    # Display the users with age less than 20 years old (top 10)
    print("Top ten users with age less than 20 years old:")
    youngUsers.show(10, truncate=False)

    # Find all the users who have the occupation "scientist" and their age is between 30 and 40 years old
    scientistUsers = usersDataset.filter((usersDataset["occupation"] == "scientist") & (usersDataset["age"] >= 30) & (usersDataset["age"] <= 40))

    # Display the users who have the occupation "scientist" and their age is between 30 and 40 years old (top 10)
    print("Top ten users who have the occupation 'scientist' and their age is between 30 and 40 years old:")
    scientistUsers.show(10, truncate=False)
    
    # Write the movieRatings DataFrame into Cassandra keyspace
    write_to_cassandra(movieRatings, "movie_ratings", "movielens")

    # Write the topTenMovies DataFrame into Cassandra keyspace
    write_to_cassandra(topTenMovies, "top_ten_movies_avg_rating", "movielens")

    # Write the topTenMoviesRatingCount DataFrame into Cassandra keyspace
    write_to_cassandra(topTenMoviesRatingCount, "top_ten_movies_rating_count", "movielens")

    # Write the movieRatingsWithCount1 DataFrame into Cassandra keyspace
    write_to_cassandra(movieRatingsWithCount1, "popular_movies", "movielens")

    # Write the favouriteGenresWithGender DataFrame into Cassandra keyspace
    write_to_cassandra(favouriteGenresWithGender, "favourite_genres", "movielens")

    # Write the youngUsers DataFrame into Cassandra keyspace
    write_to_cassandra(youngUsers, "young_users", "movielens")

    # Write the scientistUsers DataFrame into Cassandra keyspace
    write_to_cassandra(scientistUsers, "scientist_users", "movielens")

    # Read the tables back from Cassandra into new DataFrames
    movieRatingsFromCassandra = read_from_cassandra("movie_ratings", "movielens")
    topTenMoviesFromCassandra = read_from_cassandra("top_ten_movies_avg_rating", "movielens")
    topTenMoviesRatingCountFromCassandra = read_from_cassandra("top_ten_movies_rating_count", "movielens")
    popularMoviesFromCassandra = read_from_cassandra("popular_movies", "movielens")
    favouriteGenresFromCassandra = read_from_cassandra("favourite_genres", "movielens")
    youngUsersFromCassandra = read_from_cassandra("young_users", "movielens")
    scientistUsersFromCassandra = read_from_cassandra("scientist_users", "movielens")

    # Display the DataFrames read back from Cassandra
    print("Movie ratings from Cassandra:")
    movieRatingsFromCassandra.show(10, truncate=False)

    print("Top ten movies by average rating from Cassandra:")
    topTenMoviesFromCassandra.show(10, truncate=False)

    print("Top ten movies by rating count from Cassandra:")
    topTenMoviesRatingCountFromCassandra.show(10, truncate=False)

    print("Popular movies from Cassandra:")
    popularMoviesFromCassandra.show(10, truncate=False)

    print("Favourite genres from Cassandra:")
    favouriteGenresFromCassandra.show(10, truncate=False)

    print("Young users from Cassandra:")
    youngUsersFromCassandra.show(10, truncate=False)

    print("Scientist users from Cassandra:")
    scientistUsersFromCassandra.show(10, truncate=False)

    # Stop the SparkSession
    spark.stop()
```

## Output

### Question 1: Average rating for each movie

![Alt text](https://github.com/Kamarul891212/STQD6324_Data_Management_P132829_Assignment4/blob/master/images/image01.jpg)

### Question 2: Top ten movies with the highest average ratings

![Alt text](https://github.com/Kamarul891212/STQD6324_Data_Management_P132829_Assignment4/blob/master/images/image02.jpg)

### Question 3: Users rated at least 50 movies and their favourite movie genres

![Alt text](https://github.com/Kamarul891212/STQD6324_Data_Management_P132829_Assignment4/blob/master/images/image03.jpg)

### Question 4: Users with age that is less than 20 years old

![Alt text](https://github.com/Kamarul891212/STQD6324_Data_Management_P132829_Assignment4/blob/master/images/image04.jpg)

### Question 5: Users with occupation "scientist" and age between 30 and 40 years old

![Alt text](https://github.com/Kamarul891212/STQD6324_Data_Management_P132829_Assignment4/blob/master/images/image05.jpg)