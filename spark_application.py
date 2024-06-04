from pyspark.sql import SparkSession 
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# class Pillow:
#     def __init__(self,_id, snoringRange, respirationRate, bodyTemperature, limbMovement, bloodOxygen, rem, hoursSleeping, heartRate, stresState):
#         self._id = _id
#         self.snoringRange = snoringRange
#         self.respirationRate = respirationRate
#         self.bodyTemperature = bodyTemperature
#         self.limbMovement = limbMovement
#         self.bloodOxygen = bloodOxygen
#         self.rem = rem
#         self.hoursSleeping = hoursSleeping
#         self.heartRate = heartRate
#         self.stresState = stresState


schema = StructType([
    StructField("_id", StringType(), True),
    StructField("snoringRange", StringType(), True),
    StructField("respirationRate", StringType(), True),
    StructField("bodyTemperature", StringType(), True),
    StructField("limbMovement", StringType(), True),
    StructField("bloodOxygen", StringType(), True),
    StructField("rem", StringType(), True),
    StructField("hoursSleeping", StringType(), True),
    StructField("heartRate", StringType(), True),
    StructField("stresState", StringType(), True)
])


spark = SparkSession.builder.master("local[2]").appName("SensorDataStreaming").getOrCreate()

socketDF = spark.readStream.format("socket").option("host", "localhost").option("port", 9999).load()

parsedDF = socketDF.select(from_json(col("value"), schema).alias("data")).select("data.*")

castedDF = parsedDF.select(
    col("_id"),
    col("snoringRange").cast(DoubleType()).alias("snoringRange"),
    col("respirationRate").cast(DoubleType()).alias("respirationRate"),
    col("bodyTemperature").cast(DoubleType()).alias("bodyTemperature"),
    col("limbMovement").cast(DoubleType()).alias("limbMovement"),
    col("bloodOxygen").cast(DoubleType()).alias("bloodOxygen"),
    col("rem").cast(DoubleType()).alias("rem"),
    col("hoursSleeping").cast(DoubleType()).alias("hoursSleeping"),
    col("heartRate").cast(DoubleType()).alias("heartRate"),
    col("stresState").cast(DoubleType()).alias("stresState")
)

# debug_query = parsedDF.writeStream.outputMode("append").format("console").option("truncate", False).start()

filteredDF = castedDF.where("heartRate < 75 and respirationRate > 20")

# filteredParsedDF = filteredDF.filter(col("_id").isNotNull())
filteredDF = filteredDF.coalesce(1)


csv_query = filteredDF.writeStream.outputMode("append").format("csv").option("path", "pillowOutput").option("checkpointLocation", "checkpoint").option("header", True).start()


console_query = filteredDF.writeStream.outputMode("append").format("console").option("truncate", False).start()

csv_query.awaitTermination()
console_query.awaitTermination()
# debug_query.awaitTermination()
