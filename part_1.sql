-- Q3a
-- Create a database named 'assignment_1' 


create database if not exists assignment_1;

use database assignment_1;

-- Q3b
-- Create a stage 'stage_assignment' pointing to Azure Storage.

create or replace stage stage_assignment
url='azure://utsbdearjun.blob.core.windows.net/bde-assignment-1'
credentials=(azure_sas_token='?sv=2022-11-02&ss=b&srt=co&sp=rwdlaciytfx&se=2024-12-30T17:06:38Z&st=2024-08-18T10:06:38Z&spr=https&sig=ogC33rlPVZLOjdHcT7zYJe4%2BglT0jz8PmIJopgS%2Fw7M%3D');

-- Q4
-- Ingest the data as external tables on Snowflake.


create or replace external table ex_table_youtube_trending
(
    video_id varchar as (value:c1::varchar),
    title varchar as (value:c2::varchar),
    publishedat timestamp_ntz as (value:c3::timestamp_ntz),
    channelid varchar as (value:c4::varchar),
    channeltitle varchar as (value:c5::varchar),
    categoryid int as (value:c6::int),
    trending_date date as (value:c7::date),
    view_count int as (value:c8::int),
    likes int as (value:c9::int),
    dislikes int as (value:c10::int),
    comment_count int as (value:c11::int)
)
with location = @stage_assignment/youtube_trending
file_format = (type = 'csv' 
                field_delimiter = ',' 
                skip_header = 1 
                null_if = ('\\n', 'null', 'nul', '') 
                field_optionally_enclosed_by = '"')
pattern = '.*youtube_trending_data\.csv';

create or replace external table ex_table_youtube_category
with location = @stage_assignment/youtube-category
file_format = (type = 'json')
pattern = '.*_category_id\\.json';


-- Q5
-- Transfer the data from external tables to the respective internal tables


create or replace table table_youtube_trending as
select 
    video_id,
    title,
    publishedat,
    channelid,
    channeltitle,
    categoryid,
    trending_date,
    view_count,
    likes,
    dislikes,
    comment_count,
    substring(metadata$filename, 18, 2)::varchar(2) as country  -- Eg. to extract 'IN' from 'youtube_trending/IN_youtube_trending_data.csv'
    -- Index starts from 1 so 'IN' is at position 18 with length 2
from 
    ex_table_youtube_trending;
    

create or replace table table_youtube_category as
select
    substring(metadata$filename, 18, 2)::varchar(2) as country,  -- Extract country code
    item.value:id::int as categoryid,
    item.value:snippet:title::varchar as category_title
from
    ex_table_youtube_category,
    lateral flatten(input => value:items) as item;

-- Q6
-- Create 'table_youtube_final' with a left join of 'table_youtube_trending' and 'table_youtube_category' on country and categoryid, adding 'ID' with 'UUID_STRING()'.


create or replace table table_youtube_final as
select
    uuid_string() as id,  -- unique identifier
    t.video_id,
    t.title,
    t.publishedat,
    t.channelid,
    t.channeltitle,
    t.categoryid,
    c.category_title,
    t.trending_date,
    t.view_count,
    t.likes,
    t.dislikes,
    t.comment_count,
    t.country
from
    table_youtube_trending t
left join
    table_youtube_category c
on
    t.country = c.country
    and t.categoryid = c.categoryid; 


-- Verifying the row count
select count(*) as row_count
from table_youtube_final;

-- Verified 2,667,041 rows
