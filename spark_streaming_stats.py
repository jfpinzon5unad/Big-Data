from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, min, max
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType

# Crear la sesión de Spark sin depender de Hadoop
spark = SparkSession.builder \
    .appName("Stream Processing") \
    .master("local[1]") \
    .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
    .config("spark.hadoop.fs.defaultFS", "file://") \
    .config("spark.hadoop.yarn.resourcemanager.hostname", "") \
    .config("spark.hadoop.fs.hdfs.impl", "") \
    .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem") \
    .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse") \
    .config("spark.hadoop.native.lib", "") \
    .getOrCreate()

# Leer datos desde Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "test-topic") \
    .load()

# Convertir el campo 'value' a cadena JSON
df_str = df.selectExpr("CAST(value AS STRING) as json_str")

# Definir el esquema del JSON
schema = StructType([
    StructField("sensor_id", IntegerType()),
    StructField("value", DoubleType()),
    StructField("timestamp", DoubleType())  # timestamp como número UNIX
])

# Parsear el JSON
df_json = df_str.selectExpr(f"from_json(json_str, '{schema.simpleString()}') as data")
df_parsed = df_json.select("data.*")

# Calcular estadísticas sobre el campo "value"
df_stats = df_parsed.select(
    avg(col("value")).alias("average"),
    min(col("value")).alias("min"),
    max(col("value")).alias("max")
)

# Definir la ubicación del checkpoint
checkpoint_dir = "C:/spark_checkpoint"

# Mostrar los resultados por consola
query_stats = df_stats.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", False) \
    .option("checkpointLocation", checkpoint_dir) \
    .start()

# Mantener el stream activo
query_stats.awaitTermination()
