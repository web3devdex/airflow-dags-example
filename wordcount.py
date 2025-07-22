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
    input_path = sys.argv[1] if len(sys.argv) > 1 else "/opt/airflow/dags/repo/input.txt"
    output_path = sys.argv[2] if len(sys.argv) > 2 else "/opt/airflow/dags/repo/output"

    spark = SparkSession.builder.appName("WordCount").getOrCreate()

    # Đọc file text
    text_file = spark.read.text(input_path).rdd

    # Tách từ và đếm
    counts = (text_file
              .flatMap(lambda line: line[0].split(" "))
              .map(lambda word: (word, 1))
              .reduceByKey(lambda a, b: a + b))
    print("printing count")
    for word, cnt in counts.take(10):
        logger.info(f"{word}: {cnt}")

    # Lưu kết quả
    counts.saveAsTextFile(output_path)
    spark.stop()
