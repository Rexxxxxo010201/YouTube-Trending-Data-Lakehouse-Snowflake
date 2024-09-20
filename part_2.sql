use database assignment_1;

-- Q1
-- Find category_title duplicates in 'table_youtube_category' ignoring categoryid

select 
    distinct(category_title) -- Retrieve unique category_title
from 
    table_youtube_category
group by 
    country,
    category_title
having
    count(*) > 1; -- Include only those category titles that appear more than once within the same country


-- Q2
-- Identify category_title in 'table_youtube_category' that appears in only one country.

select
    category_title
from
    table_youtube_category
group by
    category_title
having
    count(distinct country) = 1; -- Apply condition after aggregation 
    -- Filter results to include only categories that appear in exactly one country
    



-- Q3
-- Retrieve categoryid of the missing category_title in 'table_youtube_final'

select
    distinct categoryid -- Retrieve the unique category ID
from
    table_youtube_final
where
    category_title is null; -- Only include rows where the category_title is missing
    

-- Q4
-- Update 'table_youtube_final' to replace NULL values in category_title with the result from the previous query

update
    table_youtube_final
set
    category_title = ( -- Column to be updated
        select
            category_title
        from
            table_youtube_category -- To get the updated category_title stored in table_youtube_category
        where 
            categoryid = (
            -- Get the result of the previous query
                select
                    distinct categoryid
                from
                    table_youtube_final
                where
                    category_title is null
            ) 
    )
where
-- To ensure that the update is applied only to the records where category_title is missing
    category_title is null; 


-- Q5
-- Find the title of videos in 'table_youtube_final' that don’t have a channeltitle

select 
    title 
from 
    table_youtube_final 
where 
    channeltitle is null;

-- Q6
-- Delete from 'table_youtube_final' any record with video_id = '#NAME?'

delete from
    table_youtube_final
where
    video_id = '#NAME?';

-- Q7
-- Create 'table_youtube_duplicates' to store bad duplicates using the row_number() function, to be deleted in the next query


create or replace table table_youtube_duplicates as
with rank_bad_duplicates as (
    select
        *,
        row_number() over (
            partition by video_id, country, trending_date
            order by view_count desc
        ) as row_num
        -- Assign a unique rank to each video within each country and trending date, ordered by view_count in descending order.
    from
        table_youtube_final
)
select
    id, 
    video_id,
    title,
    publishedat,
    channelid,
    channeltitle,
    categoryid,
    category_title,
    trending_date,
    view_count,
    likes,
    dislikes,
    comment_count,
    country
from
    rank_bad_duplicates
where
    row_num > 1; --To keep records except the one with the highest number of view_count (bad duplicates)



-- Q8
-- Delete duplicates from 'table_youtube_final' using 'table_youtube_duplicates'

delete from table_youtube_final yf
using table_youtube_duplicates yd
where yf.id = yd.id; -- Delete where the unique identifier 'id' (UUID) in 'table_youtube_final' matches the 'id' in 'table_youtube_duplicates'

-- Q9
-- Count the number of rows in 'table_youtube_final'

select count(*) as total_rows
from table_youtube_final;

-- Verified 2,597,494 rows
