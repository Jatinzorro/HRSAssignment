## **Part 1** <br>
Amazon Athena is one of the tools in our AWS Data Analytics ecosystem that allow us to execute
SQL queries on our S3 Data Lake. Amazon Athena is an interactive serverless query engine, which
runs Presto underneath. Presto provides a range of powerful analytic SQL functions that come very
handy when performing analysis on datasets. You can find more information on Amazon Athena
here and Presto functions here.
Given the following sample table reservations that records how hotel rooms bookings change over
time:<br>
```
room_number | reservation_status | checkin_date | checkout_date | room_type
1           | OPEN              | ...          | ...           | ...
2           | OPEN              | ...          | ...           | ...
1           | CLOSED            | ...          | ...           | ...
1           | OPEN              | ...          | ...           | ...
2           | CLOSED            | ...          | ...           | ...
3           | CLOSED            | ...          | ...           | ...
...         | ...               | ...          | ...           | ...
...         | ...               | ...          | ...           | ...
999         |                   | ...          | ...           | ...
```
Write an Amazon Athena (or Presto) SQL query that creates a table from the reservations table,
that shows only the most recent state of each room that has not been reserved yet. Store the new
table in PARQUET data format and partition by checkin_date . 
Below is the SQL query for this. <br>
**---SQL QUERY SOLUTION---** <br>
```
CREATE EXTERNAL TABLE room_recent_status as
(
    with latest_reservation_status as (
        SELECT ROW_NUMBER () OVER ( 
            PARTITION BY room_number
            ORDER BY checkout_date 
        ) row_number,room_number,reservation_status,checkin_date,checkout_date
        FROM reservations
        where reservation_status = 'OPEN'
    ) select room_number,reservation_status,checkin_date,checkout_date
    from latest_reservation_status 
    where row_number = 1
)
PARTITIONED BY (checkin_date STRING)
STORED AS PARQUET
LOCATION 's3://bucket_name//andPAth';
```
### PART(1.a)
Assuming that analysts and data consultants often filter on the reservation_status column.<br>
To reduce the amount of data scanned by Amazon Athena and at the same time improve query speed, what other techniques can you put
in place to fulfill this.<br>
#### Solution:
- Query Condition: Filter by reservation_status should be first in the where condition
- As we are already using parquet, which is a columnar format. So it will help in fast retrieval of data
- Only include required columns in your query.
- Sometimes we just want to see a portion of data to get the base idea, So using limit in queries can be helpful
- Compression can be used to reduce the number of files as each file will contain more data, gZip or snappy goes well with parquet format
- we can even save data in buckets based on 'reservation_status' column, this will make queries faster.


## **Part 2:**
Assume we have a table called bookings with the following columns:<br>
date (DATE),
hotel_name (VARCHAR),
room_type (VARCHAR),
booking_amount (DECIMAL)<br>
The bookings table stores daily reservations information for various hotels and their room types.
Your task is to write a Presto SQL query that returns the following information for each month and
category combination: <br>
- month (VARCHAR) - the month in which the sales occurred, in the format 'YYYY-MM'<br>
- room_type (VARCHAR) - the category of the product sold<br>
- total_bookings (DECIMAL) - the total bookings amount for the given category in the given
month<br>
- average_bookings (DECIMAL) - the average sales amount for the given category in the given
month<br>
- rank (INTEGER) - the rank of the category based on total sales amount in the given month.<br>
- Categories with the same total sales amount should have the same rank, and the next rank
should be skipped.
The query should return the results sorted in ascending order by month , and then by rank .
You should use grouping sets and window functions in your query.
<br>**SQL Query Solution** <br>
```
SELECT
  date_format(date, 'YYYY-MM') AS month,
  room_type,
  sum(booking_amount) AS total_bookings,
  avg(booking_amount) AS average_bookings,
  dense_rank() OVER (PARTITION BY date_format(date, 'YYYY-MM') ORDER BY sum(booking_amount) DESC) AS rank
FROM
  bookings 
GROUP BY
  GROUPING SETS (
    (date_format(date, 'YYYY-MM'), room_type),
    (date_format(date, 'YYYY-MM'))
  )
ORDER BY
  month ASC, rank ASC;
```
