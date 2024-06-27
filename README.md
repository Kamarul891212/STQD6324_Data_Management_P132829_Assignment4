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
cqlsh> USE movielens;
cqlsh:movielens> CREATE TABLE IF NOT EXISTS names (
             ... movie_id int,
             ... title text,
             ... release_date text,
             ... video_release_date text,
             ... url text,
             ... unknown int,
             ... action int,
             ... adventure int,
             ... animation int,
             ... children int,
             ... comedy int,
             ... crime int,
             ... documentary int,
             ... drama int,
             ... fantasy int,
             ... film_noir int,
             ... horror int,
             ... musical int,
             ... mystery int,
             ... romance int,
             ... sci_fi int,
             ... thriller int,
             ... war int,
             ... western int,
             ... PRIMARY KEY (movie_id)
             ... );
cqlsh:movielens> CREATE TABLE IF NOT EXISTS ratings (
             ... user_id int,
             ... movie_id int,
             ... rating int,
             ... time int,
             ... PRIMARY KEY (user_id, movie_id)
             ... );
cqlsh:movielens> CREATE TABLE IF NOT EXISTS users (
             ... user_id int,
             ... age int,
             ... gender text,
             ... occupation text,
             ... zip text,
             ... PRIMARY KEY (user_id)
             ... );
cqlsh:movielens> exit
```

## Writing Spark Output into Cassandra

```
vi Cassandra_Spark_Movielens.py
```

```
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql import functions

def parseInput1(line):
    fields = line.split('|')
    return Row(user_id = int(fields[0]), age = int(fields[1]), gender = fields[2], occupation = fields[3], zip = fields[4])

def parseInput2(line):
    fields = line.split("\t")
    return Row(user_id = int(fields[0]), movie_id = int(fields[1]), rating = int(fields[2]), time = int(fields[3]))

def parseInput3(line):
    fields = line.split("|")
    return Row(movie_id = int(fields[0]), title = fields[1], release_date = fields[2], video_release_date = fields[3], url = fields[4],
    unknown = int(fields[5]), action = int(fields[6]), adventure = int(fields[7]), animation = int(fields[8]), children =int(fields[9]),
    comedy = int(fields[10]), crime = int(fields[11]), documentary = int(fields[12]), drama = int(fields[13]), fantasy = int(fields[14]),
    film_noir = int(fields[15]), horror = int(fields[16]), musical = int(fields[17]), mystery = int(fields[18]), romance = int(fields[19]),
    sci_fi = int(fields[20]), thriller = int(fields[21]), war = int(fields[22]), western = int(fields[23]))

if __name__ == "__main__":
    #Create a SparkSession
    spark = SparkSession.builder.appName("Assignment4").config("spark.cassandra.connection.host", "127.0.0.1").getOrCreate()

    #Get the raw data
    u_user = spark.sparkContext.textFile("hdfs:///user/maria_dev/kamarul/u.user")
    u_data = spark.sparkContext.textFile("hdfs:///user/maria_dev/kamarul/u.data")
    u_item = spark.sparkContext.textFile("hdfs:///user/maria_dev/kamarul/u.item")

    #Convert it to a RDD of Row objects
    users = u_user.map(parseInput1)
    ratings = u_data.map(parseInput2)
    names = u_item.map(parseInput3)

    #Convert into a DataFrame
    users_df = spark.createDataFrame(users)
    ratings_df = spark.createDataFrame(ratings)
    names_df = spark.createDataFrame(names)

    #Write into Cassandra
    users_df.write\
        .format("org.apache.spark.sql.cassandra")\
        .mode('append')\
        .options(table="users", keyspace="movielens")\
        .save()

    ratings_df.write\
        .format("org.apache.spark.sql.cassandra")\
        .mode('append')\
        .options(table="ratings", keyspace="movielens")\
        .save()

    names_df.write\
        .format("org.apache.spark.sql.cassandra")\
        .mode('append')\
        .options(table="names", keyspace="movielens")\
        .save()

    #Read it back from Cassandra into a new DataFrame
    readUsers = spark.read\
    .format("org.apache.spark.sql.cassandra")\
    .options(table="users", keyspace="movielens")\
    .load()

    #Read it back from Cassandra into a new DataFrame
    readRatings = spark.read\
    .format("org.apache.spark.sql.cassandra")\
    .options(table="ratings", keyspace="movielens")\
    .load()

    #Read it back from Cassandra into a new DataFrame
    readNames = spark.read\
    .format("org.apache.spark.sql.cassandra")\
    .options(table="names", keyspace="movielens")\
    .load()

    readUsers.createOrReplaceTempView("users")
    readRatings.createOrReplaceTempView("ratings")
    readNames.createOrReplaceTempView("names")

    # The average rating for each movie
    avg_rating = spark.sql("""SELECT n.title, AVG(rating) AS avgRating  FROM ratings r
                   JOIN names n ON r.movie_id = n.movie_id
                   GROUP BY n.title""")

    print("Average Rating for each Movie")
    avg_rating.show(10)

    # The top ten movies with the highest average ratings.
    top10_highest_avgrating = spark.sql("""SELECT n.title, AVG(rating) AS avgRating, COUNT(*) as rated_count
                   FROM ratings r
                   JOIN names n on r.movie_id = n.movie_id
                   GROUP BY n.title
                   HAVING rated_count > 10
                   ORDER BY avgRating DESC""")

    print("Top 10 Movie with Highest Average Rating with More than 10 being Rated")
    top10_highest_avgrating.show(10)

    # Find the users who have rated at least 50 movies and identify their favourite movie genres

    # Extract user who have rated more than 50 movies
    users_50 = spark.sql("""SELECT user_id, COUNT(movie_id) AS rated_count
                FROM ratings
                GROUP BY user_id
                HAVING COUNT(movie_id) >= 50
                ORDER BY user_id ASC""")

    print("Table Users have Rated atleast 50 Movies")
    users_50.show(10)

    # Create temporary table
    users_50.createOrReplaceTempView("users_50")

    # Identify ratings given by user for each genres
    user_genre_ratings = spark.sql("""SELECT
        r.user_id,
        CASE
            WHEN n.action = 1 THEN 'Action'
            WHEN n.adventure = 1 THEN 'Adventure'
            WHEN n.animation = 1 THEN 'Animation'
            WHEN n.children = 1 THEN 'Children'
            WHEN n.comedy = 1 THEN 'Comedy'
            WHEN n.crime = 1 THEN 'Crime'
            WHEN n.documentary = 1 THEN 'Documentary'
            WHEN n.drama = 1 THEN 'Drama'
            WHEN n.fantasy = 1 THEN 'Fantasy'
            WHEN n.film_noir = 1 THEN 'Film-Noir'
            WHEN n.horror = 1 THEN 'Horror'
            WHEN n.musical = 1 THEN 'Musical'
            WHEN n.mystery = 1 THEN 'Mystery'
            WHEN n.romance = 1 THEN 'Romance'
            WHEN n.sci_fi = 1 THEN 'Sci-Fi'
            WHEN n.thriller = 1 THEN 'Thriller'
            WHEN n.war = 1 THEN 'War'
            WHEN n.western = 1 THEN 'Western'
            ELSE 'Unknown'
        END AS genre,
        SUM(r.rating) AS total_rating
    FROM ratings r
    JOIN names n ON r.movie_id = n.movie_id
    JOIN users_50 u ON r.user_id = u.user_id
    GROUP BY r.user_id, genre
    ORDER BY user_id ASC""")

    #Create temporary table
    user_genre_ratings.createOrReplaceTempView("user_genre_ratings")

    # Favourite genre based on the highest amount of ratings given by user
    fav_genre = spark.sql("""
    SELECT user_id, genre, total_rating
    FROM (
        SELECT user_id, genre, total_rating,
               ROW_NUMBER() OVER(PARTITION BY user_id ORDER BY total_rating DESC) AS row_num
        FROM user_genre_ratings
    ) AS ranked_genres
    WHERE row_num = 1
    ORDER BY user_id
    """)
    print("User's Favourite Genre")
    fav_genre.show(10)

    # All users that is less than 20 years old
    less_20 = spark.sql("SELECT * FROM users WHERE age < 20")
    print("Users less than 20 years old")
    less_20.show(10)

    # All the users who have the occupation scientist and their age is between 30 and 40 years old
    scientist = spark.sql("SELECT * FROM users WHERE occupation = 'scientist' AND age BETWEEN 30 AND 40")
    print("Scientist and Age between 30 and 40")
    scientist.show(10)

    # Display all tables
    
    print("Average Rating for each Movie")
    avg_rating.show(10)
    print("Top 10 Movie with Highest Average Rating with More than 10 being Rated")
    top10_highest_avgrating.show(10)
    print("Table Users have Rated atleast 50 Movies")
    users_50.show(10)
    print("User's Favourite Genre")
    fav_genre.show(10)
    print("Users less than 20 years old")
    less_20.show(10)
    print("Scientist and Age between 30 and 40")
    scientist.show(10)

    #Stop spark session
    spark.stop()
```

## Output

### Question 1: Average rating for each movie

**Average Rating for each Movie**

```
+--------------------+------------------+
|               title|         avgRating|
+--------------------+------------------+
|       Psycho (1960)| 4.100418410041841|
|   Annie Hall (1977)| 3.911111111111111|
|Snow White and th...|3.7093023255813953|
|Heavenly Creature...|3.6714285714285713|
|         Cosi (1996)|               4.0|
|Night of the Livi...|          3.421875|
|When We Were King...| 4.045454545454546|
| Three Wishes (1995)|3.2222222222222223|
|    Fair Game (1995)|2.1818181818181817|
| If Lucy Fell (1996)|2.7586206896551726|
+--------------------+------------------+
only showing top 10 rows
```

### Question 2: Top ten movies with the highest average ratings

**Top 10 Movie with Highest Average Rating with More than 10 being Rated**

```
+--------------------+------------------+-----------+
|               title|         avgRating|rated_count|
+--------------------+------------------+-----------+
|Close Shave, A (1...| 4.491071428571429|        112|
|Schindler's List ...| 4.466442953020135|        298|
|Wrong Trousers, T...| 4.466101694915254|        118|
|   Casablanca (1942)|  4.45679012345679|        243|
|Wallace & Gromit:...| 4.447761194029851|         67|
|Shawshank Redempt...| 4.445229681978798|        283|
|  Rear Window (1954)|4.3875598086124405|        209|
|Usual Suspects, T...| 4.385767790262173|        267|
|    Star Wars (1977)|4.3584905660377355|        583|
| 12 Angry Men (1957)|             4.344|        125|
+--------------------+------------------+-----------+
only showing top 10 rows
```

### Question 3: Users rated at least 50 movies and their favourite movie genres

**Users have Rated atleast 50 Movies**

```
+-------+-----------+
|user_id|rated_count|
+-------+-----------+
|      1|        272|
|      2|         62|
|      3|         54|
|      5|        175|
|      6|        211|
|      7|        403|
|      8|         59|
|     10|        184|
|     11|        181|
|     12|         51|
+-------+-----------+
only showing top 10 rows
```

**User's Favourite Genre**

```
+-------+------+------------+
|user_id| genre|total_rating|
+-------+------+------------+
|      1| Drama|         297|
|      2| Drama|         100|
|      3|Action|          39|
|      5|Action|         176|
|      6| Drama|         292|
|      7| Drama|         442|
|      8|Action|         159|
|     10| Drama|         259|
|     11|Comedy|         223|
|     12| Drama|          74|
+-------+------+------------+
only showing top 10 rows
```

### Question 4: Users with age that is less than 20 years old

**Users less than 20 years old**

```
+-------+---+------+----------+-----+
|user_id|age|gender|occupation|  zip|
+-------+---+------+----------+-----+
|    609| 13|     F|   student|55106|
|    621| 17|     M|   student|60402|
|    887| 14|     F|   student|27249|
|    270| 18|     F|   student|63119|
|    761| 17|     M|   student|97302|
|    258| 19|     F|   student|77801|
|    367| 17|     M|   student|37411|
|    206| 14|     F|   student|53115|
|    462| 19|     F|   student|02918|
|    550| 16|     F|   student|95453|
+-------+---+------+----------+-----+
only showing top 10 rows
```

### Question 5: Users with occupation "scientist" and age between 30 and 40 years old

**Scientist and Age between 30 and 40**

```
+-------+---+------+----------+-----+
|user_id|age|gender|occupation|  zip|
+-------+---+------+----------+-----+
|     71| 39|     M| scientist|98034|
|    107| 39|     M| scientist|60466|
|    918| 40|     M| scientist|70116|
|    337| 37|     M| scientist|10522|
|    554| 32|     M| scientist|62901|
|     40| 38|     M| scientist|27514|
|    874| 36|     M| scientist|37076|
|    643| 39|     M| scientist|55122|
|    543| 33|     M| scientist|95123|
|    183| 33|     M| scientist|27708|
+-------+---+------+----------+-----+
only showing top 10 rows
```





