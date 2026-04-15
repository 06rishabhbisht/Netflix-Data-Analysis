#3.1 Production Trends
'''Group by release_year and type to calculate the total count of movies and TV shows released each year.'''

from pyspark.sql.functions import col
df = spark.read.csv("hdfs:///user/root/final_project/cleaned_data", header=True, inferSchema=True)
df_filtered = df.filter((col("release_year") >= 2007) & (col("release_year") <= 2023))
df_production_trends = df_filtered.groupBy("release_year", "type").count().orderBy("release_year")
df_production_trends.write.mode("overwrite").saveAsTable("production_trends")

#Regional Distribution
'''Aggregate the data based on release_year, type (movie or TV show), country, and the exploded genres.'''

from pyspark.sql import functions as F
df = spark.read.csv("hdfs:///user/root/final_project/cleaned_data", header=True, inferSchema=True)
df_filtered = df.filter((F.col("release_year") >= 2007) & (F.col("release_year") <= 2023))
df_exploded = df_filtered.withColumn("genre", F.explode(F.split(F.col("listed_in"), "\|")))
df_country_exploded = df_exploded.withColumn("country", F.explode(F.split(F.col("country"), ","))) \
.withColumn("country", F.trim(F.col("country")))
df_aggregated = df_country_exploded.groupBy("release_year", "type", "country", "genre") \
.agg(
F.count("*").alias("count")) \
.orderBy("release_year")
df_aggregated.write.mode("overwrite").saveAsTable("regional_distribution")

#Viewer Preferences
'''Aggregate movie and TV show durations at the release_year level. calculate MIN, MAX, and AVG to get the full distribution on durations.'''

from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType
df = spark.read.csv("hdfs:///user/root/final_project/cleaned_data", header=True, inferSchema=True)
df_filtered = df.filter((df['release_year'] >= 2007) & (df['release_year'] <= 2023))
df_movies = df_filtered.filter(df_filtered['type'] == 'movie')
df_tv_shows = df_filtered.filter(df_filtered['type'] == 'tv show')
movie_duration_by_year = df_movies.groupBy('release_year').agg(
F.min('duration_minutes').alias('min_movie_duration'),
F.max('duration_minutes').alias('max_movie_duration'),
F.avg('duration_minutes').alias('avg_movie_duration')
)
tv_show_duration_by_year = df_tv_shows.groupBy('release_year').agg(
F.min('duration_seasons').alias('min_tv_show_duration'),
F.max('duration_seasons').alias('max_tv_show_duration'),
F.avg('duration_seasons').alias('avg_tv_show_duration')
)
movie_duration_by_year.write.mode("overwrite").saveAsTable("movie_duration_by_year")
tv_show_duration_by_year.write.mode("overwrite").saveAsTable("tv_show_duration_by_year")

#Genre Popularity
'''Explore which genres are most popular and how they relate to viewer ratings and types of content.'''

from pyspark.sql import functions as F
df = spark.read.csv("hdfs:///user/root/final_project/cleaned_data", header=True, inferSchema=True)
df_genres = df.withColumn(
"genre", F.explode(F.split(F.col("listed_in"), "\|")))
df_genres = df_genres.withColumn("genre", F.trim(F.lower(F.col("genre"))))
genre_popularity = df_genres.groupBy("genre", "type", "release_year").count()
genre_popularity = genre_popularity.orderBy(F.desc("count"), F.asc("release_year"))
genre_popularity.write \
.mode("overwrite") \
.saveAsTable("genre_popularity")

#Content Popularity based on Rating and Country
'''Explore which genres are most popular and how they relate to viewer ratings and types of content.'''

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
df_cleaned = spark.read.csv("hdfs:///user/root/final_project/cleaned_data", header=True, inferSchema=True)
df_filtered = df_cleaned.filter((df_cleaned.release_year >= 2007) & (df_cleaned.release_year <= 2023))
df_exploded_country = df_filtered.withColumn("country", F.explode(F.split(F.col("country"), ",")))
df_exploded_country = df_exploded_country.withColumn("country", F.trim(F.col("country")))
rating_country_count_df = df_exploded_country.groupBy("rating", "country").count().orderBy("count", ascending=False)
rating_country_count_df.show(truncate=False)
rating_country_count_df.write \
.mode("overwrite") \
.saveAsTable("rating_country_popularity")