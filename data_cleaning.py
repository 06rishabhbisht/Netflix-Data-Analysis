#Data Loading into HDFS
'''mkdir /root/final_project
hadoop fs -mkdir /user/root/final_project
hadoop fs -put netflix_titles.csv /user/root/final_project
hadoop fs -ls /user/root/final_project'''


# Load the CSV file from HDFS
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType

df = spark.read.csv("hdfs:///user/root/final_project/netflix_titles.csv", header=True, inferSchema=True)
df.printSchema()
#df.show(5, truncate=False)

#Check for missing values in the dataset.
from pyspark.sql.functions import col, sum
null_counts = df.select([(sum(col(c).isNull().cast("int")).alias(c)) for c in df.columns])
null_counts.show()

#Unique Value Counts
for column in df.columns:
distinct_count = df.select(column).distinct().count()
print("Column: {}, Distinct Count: {}".format(column, distinct_count))

#Explore the distribution of categorical columns
df.groupBy("type").count().show()
df.groupBy("rating").count().orderBy("count", ascending=False).show()

#Remove Unnecessary Columns
df = df.drop("director", "description", "show_id", "date_added")

#Handle Missing Values
df = df.fillna({"cast": "Unknown", "country": "Unknown"})
df = df.na.drop(subset=["rating", "duration"])

#Data Type Correction
#The duration column is transformed into numeric fields (duration_minutes for movies and duration_seasons for TV shows)
from pyspark.sql.functions import col, regexp_replace, when
from pyspark.sql.types import IntegerType
df = df.withColumn(
"duration_minutes",
F.when(
df["type"] == "Movie",
F.regexp_extract(df["duration"], r"(\d+)", 0).cast(IntegerType()) ).otherwise(None)
)
df = df.withColumn(
"duration_seasons",
F.when(
df["type"] == "TV Show",
F.regexp_extract(df["duration"], r"(\d+)\s*(seasons?)?", 1).cast(IntegerType())
).otherwise(None)
)
df = df.withColumn("cast", F.regexp_replace("cast", ",", "|"))
df = df.withColumn("listed_in", F.regexp_replace("listed_in", ",", "|"))

#Remove Duplicates
total_rows = df.count()
distinct_rows = df.distinct().count()
duplicate_rows = total_rows - distinct_rows
print "Total Rows: {}".format(total_rows)
print "Distinct Rows: {}".format(distinct_rows)
print "Duplicate Rows: {}".format(duplicate_rows)
df = df.dropDuplicates()

#Clean up string values by trimming and converting to lowercase.
string_columns = [col_name for col_name, dtype in df.dtypes if dtype == "string"]
for col_name in string_columns:
df = df.withColumn(col_name, F.trim(F.lower(F.col(col_name))))

#Remove special characters in title column
df = df.withColumn("title", F.regexp_replace(F.col("title"), "[^a-zA-Z0-9 ]", ""))
df.select("title").show(truncate=False)

#Save Cleaned Data to HDFS
output_path = "hdfs:///user/root/final_project/cleaned_data"
df.coalesce(1).write.csv(output_path, header=True, mode="overwrite")

#Check Cleaned Data file
'''hadoop fs -ls /user/root/final_project/cleaned_data'''