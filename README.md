# Sparify Music Site Data Wareshouse

## Content
- Summary
- Execution Instructions
- Database Diagram
- Sample Analytics Queries
- References
    
## Summary

The purpose of this data warehouse is to provide a way for Sparkify analytics team to understand the songs and artists that their users are listening to.  Also, the database was design to answers regarding viability of the site and the type of customers using to their site. For instance, are users willing to pay in order play music on their Web site. Thus, the database was designed to enable the analytics team to quantify the songs being played for different dimensions such as time, artist and user’s demographics.  With the current database design, they will be able to answer questions like the ones below:

- What is the popularity of each song? 
- What is the popularity of each artist?
- What is the popularity of each song per weekday?
- What songs are paying users listening to?
- What is the percentage of users paying to listening music per month?
- What percent of paid users are male/female per month?

Hence, the database was design not just to answer their current questions, but their anticipated future questions.

## Execution Instructions

To create the tables, insert and select statements, please run the following command at the terminal prompt:
- python create_tables.py

FYI, the NOT NULL constraint was not set on the songplays.song_id and songplays.artists_id, because most the songs and artist in the log files are not in the song files.

To load the tables, please run the command below:
- python etl.py

## Database Diagram
The database consist of the five tables are depicted in the diagram below. A star schema was used to facilate the running if the analytical queries. The fact table songplays and dimension tables can be join using the foriegn keys: time.timestamp, users.user_id, artists.artist_id and songs.song_id. Please see examples of the above in the "Sample Analytic Queries" section below.

![Database Design](file:///music_site_db_design.PNG)

## Sample Analytic Queries

Below are the queries for each of the questions mentioned above. You can query the database by using the **analytic_qry.ipynb** Notebook.

What is the popularity of each song?

    SELECT
         s.title
        ,COUNT(*) cnt
    FROM songplays sp
    JOIN songs s ON s.song_id = sp.song_id 
    JOIN time tm ON tm.start_time = sp.start_time
    GROUP BY 1
    ORDER BY 1
    ;
    
What is the popularity of each artist?

    SELECT
        a.name
        , COUNT(*) cnt
    FROM songplays sp
    JOIN artists a ON a.artist_id = sp.artist_id
    GROUP BY 1
    ORDER BY 1
    ;


What is the popularity of each song per day?

    SELECT
        tm.weekday
        , CASE
            WHEN tm.weekday = 0 THEN 'Sunday'
            WHEN tm.weekday = 1 THEN 'Monday'
            WHEN tm.weekday = 2 THEN 'Tuesday'
            WHEN tm.weekday = 3 THEN 'Wednesday'
            WHEN tm.weekday = 4 THEN 'Thursday'
            WHEN tm.weekday = 5 THEN 'Friday'
            WHEN tm.weekday = 6 THEN 'Saturday'
         END day_nm
        , s.title
        , COUNT(*) cnt
    FROM songplays sp
    JOIN songs s ON s.song_id = sp.song_id 
    JOIN time tm ON tm.start_time = sp.start_time
    GROUP BY 1, 2, 3
    ORDER BY 1
    ;


What songs are paying users listening to?

    SELECT s.title, COUNT(*) cnt
    FROM songplays sp
    JOIN songs s ON s.song_id = sp.song_id
    WHERE sp.level = 'paid'
    GROUP BY 1
    ORDER BY 2
    ;


What is the percentage of users who are paying to listening music per month?

    SELECT
         usr.year_mnth
        , pay_usr.cnt
        , usr.cnt
        ,(pay_usr.cnt::DEC(10,2) / usr.cnt::DEC(10,2))::DEC(5,1) * 100
    FROM
    (
        SELECT TO_CHAR(start_time, 'YYYY-MM') year_mnth, COUNT(DISTINCT user_id) cnt
        FROM songplays sp
        GROUP BY 1
    ) usr
    LEFT JOIN
    ( 
        SELECT TO_CHAR(start_time, 'YYYY-MM') year_mnth, COUNT(DISTINCT sp.user_id) cnt
        FROM songplays sp
        WHERE level = 'paid'
        GROUP BY 1
    ) pay_usr
    ON pay_usr.year_mnth = usr.year_mnth
    ORDER BY 1
    ;


What percent of paid users are male/female per month?

    SELECT
         usr.year_mnth
        , paid_usr_cnt
        , paid_male_cnt
        ,(pay_usr.paid_male_cnt::DEC(10,2) /
            usr.paid_usr_cnt::DEC(10,2))::DEC(5,1) * 100 AS "paid_male_%"
    FROM
    (
        SELECT TO_CHAR(start_time, 'YYYY-MM') year_mnth
    , COUNT(DISTINCT user_id) paid_usr_cnt
        FROM songplays sp
        WHERE level = 'paid'
        GROUP BY 1
    ) usr
    LEFT JOIN
    ( 
        SELECT TO_CHAR(start_time, 'YYYY-MM') year_mnth
         , COUNT(DISTINCT sp.user_id) paid_male_cnt
        FROM songplays sp
        JOIN users u ON u.user_id = sp.user_id
        WHERE u.gender = 'M'
        AND sp.level = 'paid'
        GROUP BY 1
    ) pay_usr ON pay_usr.year_mnth = usr.year_mnth
    ORDER BY 1
    ;
    
## References

PostgreSQL/Psycopg2 upsert syntax to update columns: 

https://dba.stackexchange.com/questions/167591/postgresql-psycopg2-upsert-syntax-to-update-columns

PostgreSQL Primary Key:

http://www.postgresqltutorial.com/postgresql-primary-key/

PostgreSQL Upsert Using INSERT ON CONFLICT statement:

http://www.postgresqltutorial.com/postgresql-upsert/

PostgreSQL Tutorial:

https://www.tutorialspoint.com/postgresql/index.htm

Python - How to convert JSON File to Dataframe:

https://stackoverflow.com/questions/41168558/python-how-to-convert-json-file-to-dataframe

How to normalize json correctly by Python Pandas:

https://stackoverflow.com/questions/46091362/how-to-normalize-json-correctly-by-python-pandas

Reading and Writing JSON to a File in Python:

https://stackabuse.com/reading-and-writing-json-to-a-file-in-python/

JSON to pandas DataFrame:

https://stackoverflow.com/questions/21104592/json-to-pandas-dataframe

Understanding PostgreSQL Timestamp Data Types:

http://www.postgresqltutorial.com/postgresql-timestamp/

Psycopg 2.8.4.dev0 documentation:

http://initd.org/psycopg/docs/usage.html#query-parameters
