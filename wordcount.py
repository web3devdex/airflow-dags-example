from pyspark.sql import SparkSession
import sys

if __name__ == "__main__":
    input_path = sys.argv[1] if len(sys.argv) > 1 else "/opt/airflow/dags/repo/input.txt"
    output_path = sys.argv[2] if len(sys.argv) > 2 else "/opt/airflow/dags/repo/output.txt"

    spark = SparkSession.builder.appName("WordCount").getOrCreate()

    # Đọc file text
    text_file = spark.read.text(input_path).rdd

    # Tách từ và đếm
    counts = (text_file
              .flatMap(lambda line: line[0].split(" "))
              .map(lambda word: (word, 1))
              .reduceByKey(lambda a, b: a + b))

    # Lưu kết quả
    counts.saveAsTextFile(output_path)
    spark.stop()
