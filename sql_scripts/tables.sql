-- main cleaned table
drop table if exists movies_cleaned
create table if not exists movies_cleaned(
id INT,
movie_title char(50),
movie_facebook_likes char(50),
color char(50),
director_name char(50),
prolific_director char(50),
director_facebook_likes char(50),
actor_1_name char(50),
actor_2_name char(50),
actor_3_name char(50),
prolific_actor_1 char(50),
prolific_actor_2 char(50),
prolific_actor_3 char(50),
actor_1_facebook_likes char(50),
actor_2_facebook_likes char(50),
actor_3_facebook_likes char(50),
budget char(50),
gross char(50),
genres char(50),
num_critic_for_reviews char(50),
num_voted_users char(50),
facenumber_in_poster char(50),
plot_keywords char(50),
movie_imdb_link char(50),
num_user_for_reviews char(50),
language char(50),
country char(50),
content_rating char(50),
title_year char(50),
imdb_score char(50),
oscar_nom_movie char(50),
oscar_nom_actor char(50),
duration char(50),
aspect_ratio char(50),
Action char(50),
Adventure char(50),
Animation char(50),
Biography char(50),
Comedy char(50),
Crime char(50),
Documentary char(50),
Drama char(50),
Family char(50),
Fantasy char(50),
"Film-Noir" char(50),
History char(50),
Horror char(50),
Music char(50),
Musical char(50),
Mystery char(50),
News  char(50),
Romance char(50),
"Sci-Fi" char(50),
Sport char(50),
Thriller char(50),
War char(50),
Western char(50),
imdbID char(50),
Released_Date char(50),
Type char(50),
PRIMARY KEY(id, movie_title))



-- audit table
drop table if exists audit_movies_cleaned
create table if not exists audit_movies_cleaned(
id INTEGER PRIMARY KEY AUTOINCREMENT,
job_starttime DATE,
job_endtime DATE,
records_extracted_raw int,
records_ingested_cleaned int,
latest_movie_date DATE)