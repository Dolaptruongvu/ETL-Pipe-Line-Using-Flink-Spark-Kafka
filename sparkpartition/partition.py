from pyspark.sql import SparkSession

def main():
    # Initialize SparkSession with AWS credentials and S3A committer information
    spark = SparkSession.builder \
        .appName("SplitCSVFile") \
        .master("local[*]") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.2,com.amazonaws:aws-java-sdk-bundle:1.11.1026") \
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .config("spark.hadoop.mapreduce.outputcommitter.factory.scheme.s3a", "org.apache.hadoop.fs.s3a.commit.S3ACommitterFactory") \
        .config("spark.hadoop.fs.s3a.committer.name", "magic") \
        .getOrCreate()

    # Path to the CSV file on MinIO (S3)
    input_path = "s3a://mybucket/large_file_100MB.csv"
    output_path = "s3a://mybucket/partition1/"

    # Read the CSV file
    df = spark.read.option("header", "true").csv(input_path)

    # Calculate the size of the header
    header_size = len(df.schema.json().encode("utf-8"))

    # Set the maximum size for each file
    max_file_size_mb = 0.5
    max_file_size_bytes = max_file_size_mb * 1024 * 1024

    # Calculate the size of each row
    row_sizes = df.rdd.map(lambda row: len(",".join([str(x) for x in row]).encode("utf-8"))).collect()

    # Calculate the total bytes of all rows
    total_size_of_rows = sum(row_sizes)

    # Calculate the maximum number of rows per file
    avg_row_size = total_size_of_rows / len(row_sizes)
    rows_per_file = int((max_file_size_bytes - header_size) / avg_row_size)

    # Calculate the number of partitions
    num_rows = df.count()
    num_partitions = int((num_rows / rows_per_file) + (1 if num_rows % rows_per_file > 0 else 0))

    print(f"Header size: {header_size} bytes")
    print(f"Total size of rows: {total_size_of_rows} bytes")
    print(f"Average row size: {avg_row_size} bytes")
    print(f"Rows per file: {rows_per_file}")
    print(f"Number of rows: {num_rows}")
    print(f"Number of partitions: {num_partitions}")

    # Split the file into partitions and write to S3 (MinIO)
    df.repartition(num_partitions) \
        .write.option("header", "true") \
        .mode("overwrite") \
        .csv(output_path)

    # Stop SparkSession
    spark.stop()

if __name__ == "__main__":
    main()
