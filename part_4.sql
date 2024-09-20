use database assignment_1;

-- Get the start and end date for the entire data

select
    min(trending_date) as start_date,
    max(trending_date) as end_date
from
    table_youtube_final;


-- start_date is 2020-08-12
-- end_date is 2024-04-15
-- So, the last 2 quarters of interest are Q1 2024 and Q4 2023

-- Now, we will consider only the latest (max) view count of each distinct video in each quarter to get the growth rate of categories


-- Subquery to calculate the maximum view count for each video within each quarter
with max_view_counts as (
    select
        category_title,
        video_id,
        case
            when trending_date between '2023-10-01' and '2023-12-31' then 'Q4_2023'
            when trending_date between '2024-01-01' and '2024-03-31' then 'Q1_2024'
        end as quarter,
        max(view_count) as max_view_count
    from
        table_youtube_final
    where
        trending_date between '2023-10-01' and '2024-03-31' and category_title not in('Music','Entertainment')
    group by
        category_title,
        video_id,
        quarter
),
-- Subquery to calculate the average view count and the count of distinct videos for each category and quarter
quarterly_avg_view_count as (
    select
        category_title,
        quarter,
        avg(max_view_count) as avg_view_count,
        count(distinct video_id) as distinct_video_count
    from
        max_view_counts
    group by
        category_title,
        quarter
),
-- Subquery to filter categories with at least 100 distinct videos in both quarters and calculate the growth rate
filtered_categories as (
    select
        q4.category_title,
        q4.avg_view_count as q4_avg_view_count,
        q1.avg_view_count as q1_avg_view_count,
        ((q1.avg_view_count - q4.avg_view_count) / q4.avg_view_count) * 100 as growth_rate
    from
        quarterly_avg_view_count q4
    join
        quarterly_avg_view_count q1 on q4.category_title = q1.category_title
    where
        q4.quarter = 'Q4_2023'
        and q1.quarter = 'Q1_2024'
        and q4.distinct_video_count >= 100
        and q1.distinct_video_count >= 100
)
-- Final select to retrieve the category title, average view counts for both quarters, and growth rate, ordered by growth rate in descending order
select
    category_title,
    q4_avg_view_count,
    q1_avg_view_count,
    growth_rate
from
    filtered_categories
order by
    growth_rate desc;

    


-- Check the view for growth rate
select * from growth_rate;




-- To check whether the category is consistently growing across all countries, we will query to get the growth rate of "Film & Animation" for each country in descending order

with category_growth as (
    select
        country,
        avg(q4_avg_view_count) as avg_q4_2023_view_count,
        avg(q1_avg_view_count) as avg_q1_2024_view_count,
        avg(growth_rate) as avg_growth_rate
    from
        growth_rate
    where
        category_title = 'Film & Animation'
    group by
        country
)

select
    country,
    avg_q4_2023_view_count,
    avg_q1_2024_view_count,
    avg_growth_rate
from
    category_growth
order by
    avg_growth_rate desc;


-- Film & Animation shows a high growth rate for Japan and India
