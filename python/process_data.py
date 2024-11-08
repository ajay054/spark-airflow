import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def main(input_path, output_path):
    spark = SparkSession.builder.appName("DataProcessingJob").getOrCreate()
    df = spark.read.csv(input_path, header=True, inferSchema=True)
    processed_df = df.filter(col("age") > 21)
    processed_df.write.mode("overwrite").csv(output_path, header=True)
    spark.stop()

if __name__ == "__main__":
    input_path = sys.argv[1] if len(sys.argv) > 1 else "/home/ajayconnect/data/input.csv"
    output_path = sys.argv[2] if len(sys.argv) > 2 else "/home/ajayconnect/data/output"
    main(input_path, output_path)
