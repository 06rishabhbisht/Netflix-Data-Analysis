---Exploratory Analysis with Hive

---1.Production Trends
--Annual Distribution of Movie vs TV Show Releases

SELECT release_year,
MAX(CASE WHEN type = 'movie' THEN count ELSE 0 END) AS total_movies,
MAX(CASE WHEN type = 'tv show' THEN count ELSE 0 END) AS total_tv_shows
FROM production_trends
GROUP BY release_year
ORDER BY release_year;

---Growth Rate of TV Shows and Movies Over Time

WITH yearly_growth AS (
SELECT release_year,
MAX(CASE WHEN type = 'movie' THEN count ELSE 0 END) AS total_movies,
MAX(CASE WHEN type = 'tv show' THEN count ELSE 0 END) AS total_tv_shows
FROM production_trends
GROUP BY release_year
)
SELECT a.release_year,
a.total_movies,
a.total_tv_shows,
ROUND((a.total_movies - COALESCE(b.total_movies, 0)) / COALESCE(b.total_movies, 1) * 100, 2) AS movie_growth_rate,
ROUND((a.total_tv_shows - COALESCE(b.total_tv_shows, 0)) / COALESCE(b.total_tv_shows, 1) * 100, 2) AS tv_show_growth_rate
FROM yearly_growth a
LEFT JOIN yearly_growth b
ON a.release_year = b.release_year + 1
WHERE a.release_year > 2007
ORDER BY a.release_year;

---Total Productions by Year (Movies + TV Shows)
SELECT release_year,
MAX(CASE WHEN type = 'movie' THEN count ELSE 0 END) + MAX(CASE WHEN type = 'tv show' THEN count ELSE 0 END) AS total_productions
FROM production_trends
GROUP BY release_year
ORDER BY release_year;

---Percentage of TV Shows vs Movies Over Time

SELECT release_year,
(MAX(CASE WHEN type = 'movie' THEN count ELSE 0 END) / (MAX(CASE WHEN type = 'movie' THEN count ELSE 0 END) + MAX(CASE WHEN type = 'tv show' THEN count ELSE 0 END))) * 100 AS movie_percentage,
(MAX(CASE WHEN type = 'tv show' THEN count ELSE 0 END) / (MAX(CASE WHEN type = 'movie' THEN count ELSE 0 END) + MAX(CASE WHEN type = 'tv show' THEN count ELSE 0 END))) * 100 AS tv_show_percentage
FROM production_trends
GROUP BY release_year
ORDER BY release_year;

---2.Regional Distribution
---Highest Distribution of Genres by Country

SELECT country, genre, SUM(count) AS total_count
FROM regional_distribution
GROUP BY country, genre
ORDER BY total_count DESC;

---Genre Distribution of Movie Show Across Countries

SELECT country, genre, SUM(count) AS movie_count
FROM regional_distribution
WHERE country != '' AND country IS NOT NULL AND type = 'movie'
GROUP BY country, genre
ORDER BY movie_count DESC;

---Genre Distribution of TV Show Across Countries

SELECT country, genre, SUM(count) AS tv_show_count
FROM regional_distribution
WHERE country != '' AND country IS NOT NULL AND type = 'tv show'
GROUP BY country, genre
ORDER BY tv_show_count DESC;

---Yearly distribution of genres by country and type (2021)

SELECT release_year, country, genre, type,
SUM(count) AS total_count
FROM regional_distribution
WHERE release_year = 2021 AND country != 'unknown'
GROUP BY release_year, country, genre, type
ORDER BY release_year, total_count DESC;

---3.Viewer Preferences
---Movie Duration Analysis (MIN, MAX, AVG) per Year

SELECT release_year, min_movie_duration, max_movie_duration,
ROUND(avg_movie_duration, 2) AS avg_movie_duration
FROM movie_duration_by_year
ORDER BY release_year;

---TV Show Duration Analysis (MIN, MAX, AVG) per Year

SELECT release_year, min_tv_show_duration,max_tv_show_duration,
ROUND(avg_tv_show_duration, 2) AS avg_tv_show_duration
FROM tv_show_duration_by_year
ORDER BY release_year;

---4.Genre Popularity
---Analyze which genres are the most popular

SELECT genre, SUM(count) AS genre_popularity
FROM genre_popularity
GROUP BY genre
ORDER BY genre_popularity DESC
LIMIT 10;

---Compare the number of new versus older releases within each genre

SELECT genre,
CASE
WHEN release_year BETWEEN 2017 AND 2021 THEN 'new' WHEN release_year BETWEEN 2012 AND 2016 THEN 'old' ELSE 'other'
END AS release_category,
SUM(count) AS release_count
FROM genre_popularity
WHERE release_year BETWEEN 2012 AND 2021
GROUP BY genre,
CASE
WHEN release_year BETWEEN 2017 AND 2021 THEN 'new'
WHEN release_year BETWEEN 2012 AND 2016 THEN 'old'
ELSE 'other'
END
ORDER BY genre, release_category;

---Top 10 genres of movies

SELECT genre, type, SUM(count) AS type_count
FROM genre_popularity
WHERE type = 'movie'
GROUP BY genre, type
ORDER BY type_count DESC, genre LIMIT 10;

---Top 10 genres of tv shows

SELECT genre, type, SUM(count) AS type_count
FROM genre_popularity
WHERE type = 'tv show'
GROUP BY genre, type
ORDER BY type_count DESC, genre
LIMIT 10;

---5.Content Popularity based on Rating and Country
---Content popularity by combinations of rating and country

SELECT rating, country, count
FROM rating_country_popularity
where country !='unknown'
ORDER BY count DESC;

---Content popularity by Rating

SELECT rating, SUM(count) AS total_content
FROM rating_country_popularity
GROUP BY rating
ORDER BY total_content DESC;

---Content Distribution by Country

SELECT country, SUM(count) AS total_content
FROM rating_country_popularity
where country != 'unknown'
GROUP BY country
ORDER BY total_content DESC
LIMIT 10;