# P138002_Assignment4_DataManagement

## MovieLens Dataset Analysis Report
### Introduction

The MovieLens dataset offers a comprehensive view into user preferences and behaviors related to movie ratings and demographics. This report delves into various aspects of the dataset to uncover valuable insights that can inform strategic decisions for content curation, user engagement, and platform enhancements on MovieLens.


### Dataset Overview

The MovieLens dataset consists of three primary tables: movie_details, movie_ratings, and user_details. These tables contain structured data on movie attributes (such as title, release date, and genre), user ratings for each movie, and demographic information about users (including age, gender, occupation, and zip code). This structured format enables systematic analysis to extract meaningful patterns and trends from user interactions with movies on the platform.

### Coding
Installing Cassandra
```
su root

cd /etc/yum.repos.d

vi datastax.repo
[datastax]
name=DataStax Repo for Apache Cassandra
baseurl=http://rpm.datastax.com/community
enabled=1
gpgcheck=0

yum install dsc30

service cassandra start
```
Cassandra shell coding
```
cqlsh> CREATE KEYSPACE IF NOT EXISTS movielens WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
cqlsh> USE movielens;

cqlsh:movielens> CREATE TABLE IF NOT EXISTS movie_details (
                       movie_id int,
                       title text,
                       release_date text,
                       video_release_date text,
                       url text,
                       unknown int,
                       action int,
                       adventure int,
                       animation int,
                       children int,
                       comedy int,
                       crime int,
                       documentary int,
                       drama int,
                       fantasy int,
                       film_noir int,
                       horror int,
                       musical int,
                       mystery int,
                       romance int,
                       sci_fi int,
                       thriller int,
                       war int,
                       western int,
                       PRIMARY KEY (movie_id)
);

cqlsh:movielens> CREATE TABLE IF NOT EXISTS movie_ratings (
                    user_id int,
                    movie_id int,
                    rating int,
                    timestamp int,
                    PRIMARY KEY (user_id, movie_id)
);

cqlsh:movielens> CREATE TABLE IF NOT EXISTS user_details (
                    user_id int,
                    age int,
                    gender text,
                    occupation text,
                    zip_code text,
                    PRIMARY KEY (user_id)
);

exit;
```
### Creating Spark output into Cassandra via Cassandra_Movielens_Assignment4.py
```
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql import functions

def parse_user(line):
    fields = line.split('|')
    return Row(user_id=int(fields[0]), age=int(fields[1]), gender=fields[2], occupation=fields[3], zip_code=fields[4])

def parse_rating(line):
    fields = line.split('\t')
    return Row(user_id=int(fields[0]), movie_id=int(fields[1]), rating=int(fields[2]), timestamp=int(fields[3]))

def parse_movie(line):
    fields = line.split('|')
    return Row(movie_id=int(fields[0]), title=fields[1], release_date=fields[2], video_release_date=fields[3], url=fields[4],
               unknown=int(fields[5]), action=int(fields[6]), adventure=int(fields[7]), animation=int(fields[8]), children=int(fields[9]),
               comedy=int(fields[10]), crime=int(fields[11]), documentary=int(fields[12]), drama=int(fields[13]), fantasy=int(fields[14]),
               film_noir=int(fields[15]), horror=int(fields[16]), musical=int(fields[17]), mystery=int(fields[18]), romance=int(fields[19]),
               sci_fi=int(fields[20]), thriller=int(fields[21]), war=int(fields[22]), western=int(fields[23]))

if __name__ == "__main__":
    spark = SparkSession.builder.appName("MovieLensAnalysis").config("spark.cassandra.connection.host", "127.0.0.1").getOrCreate()

    u_user = spark.sparkContext.textFile("hdfs:///user/maria_dev/nazmi/u.user")
    u_data = spark.sparkContext.textFile("hdfs:///user/maria_dev/nazmi/u.data")
    u_item = spark.sparkContext.textFile("hdfs:///user/maria_dev/nazmi/u.item")

    users = u_user.map(parse_user)
    ratings = u_data.map(parse_rating)
    movies = u_item.map(parse_movie)

    users_df = spark.createDataFrame(users)
    ratings_df = spark.createDataFrame(ratings)
    movies_df = spark.createDataFrame(movies)

    users_df.write \
        .format("org.apache.spark.sql.cassandra") \
        .mode('append') \
        .options(table="user_details", keyspace="movielens") \
        .save()

    ratings_df.write \
        .format("org.apache.spark.sql.cassandra") \
        .mode('append') \
        .options(table="movie_ratings", keyspace="movielens") \
        .save()

    movies_df.write \
        .format("org.apache.spark.sql.cassandra") \
        .mode('append') \
        .options(table="movie_details", keyspace="movielens") \
        .save()

    read_users = spark.read \
        .format("org.apache.spark.sql.cassandra") \
        .options(table="user_details", keyspace="movielens") \
        .load()

    read_ratings = spark.read \
        .format("org.apache.spark.sql.cassandra") \
        .options(table="movie_ratings", keyspace="movielens") \
        .load()

    read_movies = spark.read \
        .format("org.apache.spark.sql.cassandra") \
        .options(table="movie_details", keyspace="movielens") \
        .load()

    read_users.createOrReplaceTempView("users")
    read_ratings.createOrReplaceTempView("ratings")
    read_movies.createOrReplaceTempView("movies")

    avg_rating_query = """
        SELECT m.title, AVG(r.rating) AS avgRating
        FROM ratings r
        JOIN movies m ON r.movie_id = m.movie_id
        GROUP BY m.title
    """
    avg_rating = spark.sql(avg_rating_query)
    avg_rating.show(10)

    top10_query = """
        SELECT m.title, AVG(r.rating) AS avgRating, COUNT(*) as rating_count
        FROM ratings r
        JOIN movies m ON r.movie_id = m.movie_id
        GROUP BY m.title
        HAVING rating_count > 10
        ORDER BY avgRating DESC
    """
    top10_highest_avgrating = spark.sql(top10_query)
    top10_highest_avgrating.show(10)

    users_50_query = """
        SELECT user_id, COUNT(movie_id) AS rating_count
        FROM ratings
        GROUP BY user_id
        HAVING rating_count >= 50
        ORDER BY user_id ASC
    """
    users_50 = spark.sql(users_50_query)
    users_50.show(10)
    users_50.createOrReplaceTempView("users_50")

    user_genre_query = """
        SELECT
            r.user_id,
            CASE
                WHEN m.action = 1 THEN 'Action'
                WHEN m.adventure = 1 THEN 'Adventure'
                WHEN m.animation = 1 THEN 'Animation'
                WHEN m.children = 1 THEN 'Children'
                WHEN m.comedy = 1 THEN 'Comedy'
                WHEN m.crime = 1 THEN 'Crime'
                WHEN m.documentary = 1 THEN 'Documentary'
                WHEN m.drama = 1 THEN 'Drama'
                WHEN m.fantasy = 1 THEN 'Fantasy'
                WHEN m.film_noir = 1 THEN 'Film-Noir'
                WHEN m.horror = 1 THEN 'Horror'
                WHEN m.musical = 1 THEN 'Musical'
                WHEN m.mystery = 1 THEN 'Mystery'
                WHEN m.romance = 1 THEN 'Romance'
                WHEN m.sci_fi = 1 THEN 'Sci-Fi'
                WHEN m.thriller = 1 THEN 'Thriller'
                WHEN m.war = 1 THEN 'War'
                WHEN m.western = 1 THEN 'Western'
                ELSE 'Unknown'
            END AS genre,
            SUM(r.rating) AS total_rating
        FROM ratings r
        JOIN movies m ON r.movie_id = m.movie_id
        JOIN users_50 u ON r.user_id = u.user_id
        GROUP BY r.user_id, genre
        ORDER BY user_id ASC
    """
    user_genre_ratings = spark.sql(user_genre_query)
    user_genre_ratings.createOrReplaceTempView("user_genre_ratings")

    fav_genre_query = """
        SELECT user_id, genre, total_rating
        FROM (
            SELECT user_id, genre, total_rating,
                   ROW_NUMBER() OVER(PARTITION BY user_id ORDER BY total_rating DESC) AS rank
            FROM user_genre_ratings
        ) AS ranked_genres
        WHERE rank = 1
        ORDER BY user_id
    """
    fav_genre = spark.sql(fav_genre_query)
    fav_genre.show(10)

    less_20 = spark.sql("SELECT * FROM users WHERE age < 20")
    less_20.show(10)

    scientist_query = """
        SELECT * 
        FROM users 
        WHERE occupation = 'scientist' AND age BETWEEN 30 AND 40
    """
    scientist = spark.sql(scientist_query)
    scientist.show(10)

    spark.stop()
```
### Outputs
#### i) Calculate the average rating for each movie
![image](https://github.com/deeagjin/P138002_Assignment4_DataManagement/assets/152348898/5fb94988-8ac3-4263-a339-4c023ca8a32e)

#### Insights
This metric helps identify movies that resonate positively with the audience, indicating their popularity and viewer satisfaction. For example, movies like Cosi (1996) and Psycho (1960) received average ratings of 4.0 and 4.1 respectively, suggesting strong viewer approval. In contrast, If Lucy Fell (1996) received a lower average rating of 2.76, indicating mixed viewer sentiment.

#### ii) Identify the top ten movies with the highest average ratings.
![image](https://github.com/deeagjin/P138002_Assignment4_DataManagement/assets/152348898/1e2896dc-cb00-4973-8019-39ce7325fadf)

#### Insights
These movies, such as Close Shave, A (1995) and Schindler's List (1993), maintain high average ratings, reflecting their enduring popularity and critical acclaim. The inclusion of metrics like number of ratings alongside average ratings provides a holistic view of movie popularity and viewer engagement on MovieLens.

#### iii) Find the users who have rated at least 50 movies and identify their favourite movie genres.
![image](https://github.com/deeagjin/P138002_Assignment4_DataManagement/assets/152348898/10a86365-034f-4d9d-ab21-5fb917c461af)

#### Insights
Users engaged in rating multiple movies tend to favor genres like Drama, Comedy, and Action, indicating diverse viewing habits and preferences. This analysis helps in tailoring content recommendations and enhancing user experience by catering to specific genre interests of highly active users.

#### iv) Find all the users with age that is less than 20 years old.
![image](https://github.com/deeagjin/P138002_Assignment4_DataManagement/assets/152348898/67f89c7e-ff72-4d44-84ec-bb811fbdb4e5)

#### Insights
Users aged less than 20 years old represent a significant demographic segment on MovieLens, characterized by their youthfulness and varied movie genre interests. The analysis reveals that these younger users often engage with genres like Drama, Comedy, and Action, reflecting their broad and evolving entertainment preferences. Understanding and catering to these preferences can enhance user engagement and retention among younger demographics.

#### v)  Find all the users who have the occupation “scientist” and their age is between 30 and 40 years old.
![image](https://github.com/deeagjin/P138002_Assignment4_DataManagement/assets/152348898/f94ce463-a94c-4c76-90b7-7b1ffd035851)

#### Insights
Users with the occupation "scientist" aged between 30 and 40 years old constitute a specialized demographic within the MovieLens user base. This segment demonstrates a preference for intellectually stimulating genres such as Drama and Action, aligning with their professional interests and preference for thought-provoking content. Tailoring content recommendations and user experiences to meet these preferences can increase engagement and satisfaction among this niche demographic.


### Conclusion
In conclusion, the analysis of the MovieLens dataset provides valuable insights into viewer behavior, movie preferences, and demographic dynamics. By exploring average ratings, top-rated movies, user engagement metrics, and demographic insights, this report offers actionable insights for content creators, marketers, and platform developers. These insights enable strategic decision-making to optimize content curation, improve user experience, and foster sustained audience engagement on MovieLens.






