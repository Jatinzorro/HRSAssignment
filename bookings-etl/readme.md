# Prerequisites To Run this code
- Java(1.8+), spark, jupyterlab
- in src directory code is written for the problem statement defined 
- Run 'booking_job.ipynb' file one by one, which used hotel_function.py file as methods
- in data_destination and data_source folders data is kept for reading and writing purpose
- tests directory contains test cases and also the test data that we will use for testing purpose

# Problem Statement
- Data is stored in src/data_sources/hotels.csv
- Write an Apache Spark application in Py thon that reads bookings data and extract every
booking that the Tour Operators as Market Segment designations.
- Add two extra fields to each of the extracted bookings with the name arrival_date and
departure_date . The arrival_date field would be a combination of the arrival_date_year ,
arrival_date_month and arrival_date_day_of_month while the departure_date can be derived
from adding arrival_date field to the stays_in_weekend_nights and stays_in_week_nights .
- Finally, an extra field with_family_breakfast . The with_family_breakfast field will have Yes if
the sum of children and babies is greater than zero otherwise No .
- The resulting dataset should be saved as parquet file.

