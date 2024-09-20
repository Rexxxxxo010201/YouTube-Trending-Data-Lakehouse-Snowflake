use database assignment_1;


-- Q1
-- Retrieve the top 3 most viewed Gaming videos for each country on 2024-04-01, ordered by country and rank


with top_3_videos as (
    select
        country,
        title,
        channeltitle,
        view_count,
        row_number() over (partition by country order by view_count desc) as rk 
        -- Assign a unique rank to each video within each country based on view_count, with the highest view_count ranked first
    from table_youtube_final
    where
        trending_date = '2024-04-01' -- Only for the date 2024-04-01
        and category_title = 'Gaming'
)
select *
from top_3_videos
where
    rk between 1 and 3 -- Filters for top 3
order by country, rk;


-- Q2
-- Count the number of distinct videos with titles containing "BTS" (case insensitive) for each country, and order by count in descending order


select
    country,
    count(distinct video_id) as ct
from table_youtube_final
where
    title ilike '%bts%' -- Case insensitive substring
group by country
order by ct desc;



-- Q3
-- Retrieve the most viewed video for each country, year, and month in 2024, with likes_ratio (percentage of likes to view_count) truncated to 22 decimals. Results are ordered by year_month and country.




with ranking_views as(
select
    country, 
    date_trunc('month', trending_date)::DATE AS year_month, 
    -- Truncate every date into year_month and cast it as a date which will automatically give YYYY-MM-01
    title,
    channeltitle, 
    category_title,
    view_count,
    round((likes/view_count)*100,2) as likes_ratio,
    row_number() over(partition by country, year_month order by view_count desc) as rk
    -- Assign a rank to each video within each country and month based on view_count, with the highest view_count ranked first
from table_youtube_final
where (trending_date >= '2024-01-01') -- Only for the year 2024. Max trending date till April 2024
)
select 
    country, 
    year_month,
    title,
    channeltitle, 
    category_title,
    view_count,
    likes_ratio
from ranking_views
where rk=1 -- Filter for the top video
order by year_month, country;





-- Q4
-- For each country, identify the category_title with the most distinct videos and calculate its percentage (2 decimals) of the total distinct videos in that country. Only include data from 2022. Order results by category_title and country.



with view_category as(
    with count_category as(
    select 
        country, category_title,
        count(distinct video_id) as total_category_video,
        row_number() over (partition by country order by total_category_video desc) as rk
        -- Assign a rank to each category within each country based on the total number of videos in that category, with the highest count ranked first.
    from table_youtube_final
    where trending_date >= '2022-01-01' -- Filter for only the data beyond 2022
    group by country, category_title
    order by total_category_video desc)
    select
        *
    from 
    count_category
    where rk = 1 -- Filter for the top category video
    ),
view_country as(
    select
        country,
        count (distinct video_id) as total_country_video
    from table_youtube_final
    where trending_date >= '2022-01-01' -- Filter for only the data beyond 2022
    group by country
)
select 
    view_category.country,
    view_category.category_title,
    view_category.total_category_video,
    view_country.total_country_video,
    round((view_category.total_category_video / view_country.total_country_video) * 100,2) as percentage -- percentage of total_category_video out of total_country_video
from view_category
inner join 
    view_country using(country) -- To combine the 2 counts using the common column- Country
order by category_title, country;



-- Q5
-- Identify the channeltitle that has produced the most distinct videos and return the count of these videos.


select
    channeltitle,
    count(distinct video_id) as total_channel_video
from table_youtube_final
group by channeltitle
order by total_channel_video desc
limit 1; -- Displays only the top channeltitle
