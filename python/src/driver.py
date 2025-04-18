from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark import SparkConf
from datetime import datetime

# Configuring Spark settings
conf = SparkConf()
conf.set("spark.dynamicAllocation.enabled", "true")  # Allow Spark to dynamically adjust the number of executors
conf.set("spark.dynamicAllocation.minExecutors", "2")  # Minimum number of executors
conf.set("spark.dynamicAllocation.maxExecutors", "5")  # Maximum number of executors
conf.set("spark.dynamicAllocation.initialExecutors", "2")  # Number of executors at start
conf.set("spark.executor.memory", "4g")  # Memory allocated to each executor
conf.set("spark.driver.memory", "4g")  # Memory allocated to the driver
conf.set("spark.yarn.executor.memoryOverhead", "1024")  # Extra memory for each executor
conf.set("spark.yarn.driver.memoryOverhead", "1024")  # Extra memory for the driver

# Creating a Spark session
spark = SparkSession.builder \
    .appName("CapStone_Project") \
    .config(conf=conf) \
    .getOrCreate()

# Setting Spark log level to show only errors
spark.sparkContext.setLogLevel('ERROR')
sc = spark.sparkContext

# Adding Python files to the Spark context so they can be used in the code
sc.addPyFile('/home/hadoop/python/src/db/dao.py')
sc.addPyFile('/home/hadoop/python/src/db/geo_map.py')
sc.addFile('/home/hadoop/python/src/rules/rules.py')

# Importing modules that handle database operations, geographic data, and rules for determining fraud
import geo_map
import dao
import rules

# Reading streaming data from a Kafka topic
kafka_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "18.211.252.152:9092") \
    .option("subscribe", "transactions-topic-verified") \
    .load()

# Defining the schema (structure) of the data we're expecting
schema = StructType([
    StructField("card_id", StringType(), True),
    StructField("member_id", StringType(), True),
    StructField("amount", StringType(), True),
    StructField("postcode", StringType(), True),
    StructField("pos_id", StringType(), True),
    StructField("transaction_dt", StringType(), True)
])

# Cleaning up the JSON data from Kafka
kafka_df = kafka_df.withColumn("cleaned_value", regexp_replace(col("value").cast("string"), r'\\\"', '"'))
kafka_df = kafka_df.withColumn("cleaned_value", regexp_extract(col("cleaned_value"), r'\{.*\}', 0))

# Parsing the JSON data into a DataFrame using the schema
json_df = kafka_df.withColumn("json_data", from_json(col("cleaned_value"), schema))
transact_data_raw = json_df.select("json_data.*")

# Converting columns from string to appropriate types like long (integer) and timestamp
transact_data_raw = transact_data_raw \
    .withColumn("card_id", col("card_id").cast("string")) \
    .withColumn("member_id", col("member_id").cast("long")) \
    .withColumn("amount", col("amount").cast("long")) \
    .withColumn("postcode", col("postcode").cast("long")) \
    .withColumn("pos_id", col("pos_id").cast("long")) \
    .withColumn("transaction_dt", to_timestamp(col("transaction_dt"), "dd-MM-yyyy HH:mm:ss"))

# Function to fetch the credit score from HBase database based on card ID
def fetch_score(card_id):
    card_id_str = str(card_id) + '.0'  # Adding .0 to card_id to match the key format in HBase
    hdao = dao.HBaseDao.get_instance()  # Getting an instance of the HBase DAO (Data Access Object)
    data_fetch = hdao.get_data(card_id_str, 'look_up_table')  # Fetching data from HBase
    score = data_fetch.get(b'info:score')  # Extracting the score field
    if score is None:
        return '0'  # Return 0 if no score is found
    return score.decode('utf-8') if isinstance(score, bytes) else score  # Convert bytes to string if needed

# Function to fetch the postcode from HBase database based on card ID
def postcode_data(card_id):
    card_id_str = str(card_id) + '.0'  # Adding .0 to card_id to match the key format in HBase
    hdao = dao.HBaseDao.get_instance()
    data_fetch = hdao.get_data(card_id_str, 'look_up_table')
    postcode = data_fetch.get(b'info:postcode')
    if postcode is None:
        return ''  # Return an empty string if no postcode is found
    return postcode.decode('utf-8') if isinstance(postcode, bytes) else postcode

# Function to fetch the UCL (Upper Control Limit) from HBase database based on card ID
def ucl_data(card_id):
    card_id_str = str(card_id) + '.0'  # Adding .0 to card_id to match the key format in HBase
    hdao = dao.HBaseDao.get_instance()
    data_fetch = hdao.get_data(card_id_str, 'look_up_table')
    ucl = data_fetch.get(b'info:UCL')
    if ucl is None:
        return '0'  # Return 0 if no UCL is found
    return ucl.decode('utf-8') if isinstance(ucl, bytes) else ucl

# Function to fetch the last transaction date from HBase database based on card ID
def lTransD_data(card_id):
    card_id_str = str(card_id) + '.0'  # Adding .0 to card_id to match the key format in HBase
    hdao = dao.HBaseDao.get_instance()
    data_fetch = hdao.get_data(card_id_str, 'look_up_table')
    transaction_dt = data_fetch.get(b'info:transaction_dt')
    if transaction_dt is None:
        return ''  # Return an empty string if no transaction date is found
    return transaction_dt.decode('utf-8') if isinstance(transaction_dt, bytes) else transaction_dt

# Function to calculate the distance between the last known postcode and the current postcode
def distance_calc(last_postcode, postcode):
    gmap = geo_map.GEO_Map.get_instance()  # Getting an instance of the GEO_Map class

    # Retrieve latitude and longitude for both postcodes
    last_lat = gmap.get_lat(last_postcode)
    last_lon = gmap.get_long(last_postcode)
    lat = gmap.get_lat(postcode)
    lon = gmap.get_long(postcode)

    # Check if any of the coordinates are None (missing)
    if None in [last_lat, last_lon, lat, lon]:
        print(f"Missing coordinates: last_lat={last_lat}, last_lon={last_lon}, lat={lat}, lon={lon}")
        return float('nan')  # Return NaN if any coordinate is missing

    # Calculate and return the distance between the two postcodes
    return gmap.distance(last_lat, last_lon, lat, lon)

# Function to calculate the speed of a transaction based on distance and time difference
def speed_cal(dist, time):
    if time is None or time <= 0:  # Check if time is missing or non-positive
        return -1.0  # Return -1.0 to indicate an error in calculation
    return dist / time  # Calculate and return the speed

# Function to determine the status (FRAUD or GENUINE) of a transaction
def status(amount, UCL, score, speed, card_id, transaction_dt, postcode, member_id, pos_id):
    base_status = rules.fraud_status(amount, UCL, score, speed)  # Call fraud_status from rules.py to get the status
    card_id_str = str(card_id) + '.0'  # Correct the format of the card_id for HBase key

    hdao = dao.HBaseDao.get_instance()  # Get an instance of HBase DAO
    look_up = hdao.get_data(key=card_id_str, table='look_up_table')  # Get lookup data from HBase

    if not look_up:  # If lookup data is empty, use default values
        look_up = {
            b'info:UCL': b'0',
            b'info:transaction_dt': b'',
            b'info:postcode': b''
        }

    if base_status == "fraud":
        status = "FRAUD"  # Set status to FRAUD if the transaction is suspicious
    elif base_status == "genuine":
        status = "GENUINE"  # Set status to GENUINE if the transaction is legitimate
        # Update the lookup table in HBase with the latest transaction data
        look_up[b'info:transaction_dt'] = str(transaction_dt).encode('utf-8')
        look_up[b'info:postcode'] = str(postcode).encode('utf-8')
        hdao.write_data(card_id_str, look_up, 'look_up_table')

    else:
        return "error"  # Return error for unexpected values

    # Create a row for the card_transactions table with transaction details
    row = {
                b'info:card_id': str(card_id).encode('utf-8'),
        b'info:member_id': str(member_id).encode('utf-8'),
        b'info:amount': str(amount).encode('utf-8'),
        b'info:postcode': str(postcode).encode('utf-8'),
        b'info:pos_id': str(pos_id).encode('utf-8'),
        b'info:transaction_dt': str(transaction_dt).encode('utf-8'),
        b'info:status': str(status).encode('utf-8')
    }

    # Generate a unique key for each row in the card_transactions table
    key = f'{card_id}.{member_id}.{transaction_dt.strftime("%Y-%m-%d %H:%M:%S")}.{datetime.now().strftime("%Y%m%d%H%M%S")}'
    hdao.write_data(key.encode('utf-8'), row, 'card_transactions')  # Write the transaction data to HBase

    return status  # Return the transaction status

# Create UDFs (User-Defined Functions) for use in Spark transformations
fraud_status_udf = udf(status, StringType())  # UDF for determining fraud status
speed_udf = udf(speed_cal, DoubleType())  # UDF for calculating speed
distance_udf = udf(distance_calc, DoubleType())  # UDF for calculating distance

# UDFs for retrieving data from HBase
lTransD_data_udf = udf(lTransD_data, StringType())  # UDF for getting last transaction date
ucl_data_udf = udf(ucl_data, StringType())  # UDF for getting UCL value
postcode_data_udf = udf(postcode_data, StringType())  # UDF for getting postcode
fetch_score_udf = udf(fetch_score, StringType())  # UDF for getting credit score

# Adding columns to the DataFrame with data retrieved from HBase and calculated values
df_with_score = transact_data_raw \
    .withColumn("score", fetch_score_udf(col("card_id"))) \
    .withColumn("last_postcode", postcode_data_udf(col("card_id"))) \
    .withColumn("UCL", ucl_data_udf(col("card_id"))) \
    .withColumn("last_transaction_date", date_format(to_timestamp(lTransD_data_udf(col("card_id")), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"), "yyyy-MM-dd HH:mm:ss")) \
    .withColumn("transaction_dt", to_timestamp(col("transaction_dt"), "dd-MM-yyyy HH:mm:ss")) \
    .withColumn("last_postcode", col("last_postcode").cast("integer")) \
    .withColumn("postcode", col("postcode").cast("integer")) \
    .withColumn("distance", distance_udf(col("last_postcode"), col("postcode"))) \
    .withColumn("time_diff_hours", (unix_timestamp(col("transaction_dt")) - unix_timestamp(col("last_transaction_date"))) / 3600) \
    .withColumn("time_diff_hours_abs", abs(col("time_diff_hours"))) \
    .withColumn("speed", speed_udf(col("distance"), col("time_diff_hours_abs"))) \
    .withColumn("status", fraud_status_udf(col("amount"), col("UCL"), col("score"), col("speed"), col("card_id"), col("transaction_dt"), col("postcode"), col("member_id"), col("pos_id")))

# Select the columns to output in the final DataFrame
final_df = df_with_score.select("card_id", "member_id", "amount", "postcode", "pos_id", "transaction_dt", "status")

# Write the processed data to the console for debugging or monitoring
query1 = final_df \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

# Wait for the streaming query to finish (this will keep the program running)
query1.awaitTermination()


