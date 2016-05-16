from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from datetime import *

sc = SparkContext(appName="DriverBuddy")
sqlContext = SQLContext(sc)


# Load the large csv file into an rdd
taxiData = sc.textFile("/tmp/data/xaf")
schemaString = taxiData.first()

# Start by having all fields of type string
fields = [StructField(field_name, StringType(), True) for field_name in schemaString.split(',')]

# Update the datatypes of all columns

# pickup_datetime
fields[1].dataType = TimestampType()
# dropoff_datetime
fields[2].dataType = TimestampType()
# passenger_count
fields[3].dataType = IntegerType()
# trip_distance
fields[4].dataType = FloatType()
# pickup_longitude
fields[5].dataType = FloatType()
# pickup_latitude
fields[6].dataType = FloatType()
# rate_code
fields[7].dataType = IntegerType()
# dropoff_longitude
fields[9].dataType = FloatType()
# dropoff_latitude
fields[10].dataType = FloatType()
# fare_amount
fields[12].dataType = FloatType()
# surcharge
fields[13].dataType = FloatType()
# mta_tax
fields[14].dataType = FloatType()
# tip_amount
fields[15].dataType = FloatType()
# tolls_amount
fields[16].dataType = FloatType()
# total_amount
fields[17].dataType = FloatType()

# create the dataframe schema
schema = StructType(fields)

# remove the header from the data
taxiHeader = taxiData.filter(lambda line: "vendor_id" in line)
taxiDataNoHeader = taxiData.subtract(taxiHeader)

# parse the data
# round pickup longitude and latitude to 4 decimal points, so we can group the close locations
taxiTemp = taxiDataNoHeader.map(lambda k: k.split(",")).map(lambda p: (p[0], datetime.strptime(p[1], "%Y-%m-%d %H:%M:%S"), datetime.strptime(p[2], "%Y-%m-%d %H:%M:%S"),
  int(p[3]), float(p[4]), float('%.3f'%(float(p[5]))), float('%.3f'%(float(p[6]))), int(p[7]), p[8],
  float(p[9]), float(p[10]), p[11], float(p[12]), float(p[13]), float(p[14]), float(p[15]), float(p[16]), float(p[17]) ))

taxiDataFrame = sqlContext.createDataFrame(taxiTemp, schema)

# sort by trip distance and filter by today's month and day
taxiDataFrame = taxiDataFrame.orderBy(taxiDataFrame.trip_distance.desc())

# generate 24 datasets for every hour of the day
taxiDataFrame.registerTempTable("trips")
now = datetime.now()

for hour in range(24):
  # trips = sqlContext.sql("SELECT pickup_datetime, pickup_longitude, pickup_latitude, trip_distance FROM trips where month(pickup_datetime) = " + str(now.month)
  #                        + " and day(pickup_datetime) = " + str(now.day)
  #                        + " and hour(pickup_datetime) = " + str(hour))
  trips = sqlContext.sql("SELECT pickup_datetime, pickup_longitude, pickup_latitude, trip_distance FROM trips where month(pickup_datetime) = " + str(4)
                         + " and day(pickup_datetime) = " + str(25)
                         + " and hour(pickup_datetime) = " + str(hour))
  if trips.count() > 0:
    trips.write.save("/tmp/data/trips" + str(hour), format="json")
