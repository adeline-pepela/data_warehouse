# Sparkify Song Play Analysis ETL Pipeline

This ETL pipeline is designed to create a star schema optimized for song play analysis for the Sparkify startup. The schema includes fact and dimension tables.

## Purpose

The purpose of this database is to help Sparkify analyze user song plays and answer various analytical questions. It will allow Sparkify to perform song play analysis, such as finding the most played song, identifying the highest usage time of day by hour for songs, and more.

## Database Schema Design

The schema includes the following tables:

- `songplays`: Fact table containing records associated with song plays.
- `users`: Dimension table for user information.
- `songs`: Dimension table for song information.
- `artists`: Dimension table for artist information.
- `time`: Dimension table for timestamps of records in songplays.

## ETL Pipeline

The ETL pipeline consists of two main processes:

1. Loading data from S3 into staging tables in Redshift.
2. Transforming and inserting data from staging tables into the analytics tables.

The SQL queries for these processes are defined in `sql_queries.py`.

## Example Queries

Here are some example queries and results for song play analysis:

### Most Played Song

```sql
SELECT s.title, COUNT(sp.songplay_id) as play_count
FROM songplays sp
JOIN songs s ON sp.song_id = s.song_id
GROUP BY s.title
ORDER BY play_count DESC
LIMIT 1;
