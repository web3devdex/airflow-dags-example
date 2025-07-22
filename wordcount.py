from pyspark.sql import SparkSession
import sys
import logging

# Cấu hình logging
logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s",
    level=logging.INFO
)
logger = logging.getLogger(__name__)

if __name__ == "__main__":
    input_path = "/opt/airflow/dags/repo/input.txt"
    output_path = "/opt/airflow/dags/repo/result.txt"

    spark = SparkSession.builder.appName("WordCount").getOrCreate()

    # Đọc file text
    text_file = spark.read.text(input_path).rdd

    # Tách từ và đếm
    counts = (text_file
              .flatMap(lambda line: line[0].split(" "))
              .map(lambda word: (word, 1))
              .reduceByKey(lambda a, b: a + b))
    print("printing count")
    for word, cnt in counts:
        print(f"{word}: {cnt}")
        logger.info(f"{word}: {cnt}")

    # Lưu kết quả
    counts.saveAsTextFile(output_path)
    spark.stop()
