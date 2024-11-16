from pyspark.sql import SparkSession
from pyspark.sql.types import *

spark = SparkSession.builder\
    .config("spark.jars", "./postgresql-42.7.4 20.13.35.jar")\
    .appName("Project_1")\
    .master("local[*]")\
    .enableHiveSupport()\
    .getOrCreate()

class Postgres:
    driver = "org.postgresql.Driver"
    url = "jdbc:postgresql://localhost:5432/postgres"
    user = "workspace"
    password = "7352"

class Airports:
    structType = StructType([
        StructField('IATA_CODE', StringType()),
        StructField('Airport', StringType()),
        StructField('City', StringType()),
        StructField('Latitude', FloatType()),
        StructField('MLongitude', FloatType())
    ])
    path = './dags/data/airports.csv'
    name = 'airports'

class Airlines:
    structType = StructType([
        StructField('IATA_CODE', StringType()),
        StructField('Airline', StringType())
    ])
    path = './dags/data/airlines.csv'
    name = 'airlines'

class Flights_pak:
    structType = StructType([
        StructField('DATE', DateType()),
        StructField('DAY_OF_WEEK', StringType()),
        StructField('AIRLINE', StringType()),
        StructField('FLIGHT_NUMBER', IntegerType()),
        StructField('TAIL_NUMBER', StringType()),
        StructField('ORIGIN_AIRPORT', StringType()),
        StructField('DESTINATION_AIRPORT', StringType()),
        StructField('DEPARTURE_DELAY', FloatType()),
        StructField('DISTANCE', FloatType()),
        StructField('ARRIVAL_DELAY', FloatType()),
        StructField('DIVERTED', IntegerType()),
        StructField('CANCELLED', IntegerType()),
        StructField('CANCELLATION_REASON', StringType()),
        StructField('AIR_SYSTEM_DELAY', IntegerType()),
        StructField('SECURITY_DELAY', IntegerType()),
        StructField('AIRLINE_DELAY', IntegerType()),
        StructField('LATE_AIRCRAFT_DELAY', IntegerType()),
        StructField('WEATHER_DELAY', IntegerType()),
        StructField('DEPARTURE_HOUR', FloatType()),
        StructField('ARRIVAL_HOUR', FloatType())
    ])
    path = './dags/data/flights_pak.csv'
    name = 'flights'

def read_csv(spark: SparkSession, name: str, schema: StructType, path: str):
    return spark.read\
        .options(delimiter=',')\
        .options(header=True)\
        .schema(schema)\
        .csv(path)\
        .createOrReplaceTempView(name)

read_csv(spark, Airports.name, Airports.structType, Airports.path)
read_csv(spark, Airlines.name, Airlines.structType, Airlines.path)
read_csv(spark, Flights_pak.name, Flights_pak.structType, Flights_pak.path)

top_5_avg_delay = spark.sql("""
                                SELECT airline, ROUND((AVG(arrival_delay) + AVG(departure_delay)) / 2, 3) as avg_delay_hours
                                FROM flights
                                GROUP BY airline
                                ORDER BY avg_delay_hours DESC
                                LIMIT 5
                            """).show()
top_5_flight_cancellation = spark.sql("""
                                        WITH CTE AS (
                                        SELECT origin_airport, ROUND((SUM(cancelled) / COUNT(*)) * 100, 2) AS cancellation_percent
                                        FROM flights
                                        GROUP BY origin_airport
                                        )
                                        SELECT airport, cancellation_percent
                                        FROM CTE INNER JOIN airports
                                        ON CTE.origin_airport = airports.IATA_code
                                        ORDER BY cancellation_percent DESC
                                        LIMIT 5
                                    """)
rank_day_time_delays = spark.sql("""
                                    SELECT CASE
                                                WHEN departure_hour BETWEEN 0 AND 5 THEN 'Night'
                                                WHEN departure_hour BETWEEN 6 AND 11 THEN 'Morning'
                                                WHEN departure_hour BETWEEN 12 AND 17 THEN 'Day'
                                                WHEN departure_hour BETWEEN 18 AND 23 THEN 'Evening'
                                            END AS day_time,
                                            RANK() OVER (ORDER BY SUM(departure_delay) DESC) AS delay_rank
                                    FROM flights
                                    GROUP BY day_time
                                """)

def write_in_postgres(dataframe, name):
    return dataframe.write\
        .format("jdbc")\
        .option("driver", Postgres.driver)\
        .option("url", Postgres.url)\
        .option("user", Postgres.user)\
        .option("password", Postgres.password)\
        .option("dbtable", name)\
        .mode("overwrite")\
        .save()

write_in_postgres(rank_day_time_delays, 'rank_day_time_delays')
write_in_postgres(top_5_flight_cancellation, 'top_5_flight_cancellation')
write_in_postgres(top_5_avg_delay, 'top_5_avg_delay')

