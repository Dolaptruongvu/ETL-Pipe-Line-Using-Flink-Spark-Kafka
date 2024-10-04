
# ETL Pipeline Using Flink, Spark, and Kafka

## Introduction

This project demonstrates how to create an ETL pipeline using Flink, Spark, and Kafka. The pipeline will read large datasets, partition them, and process them through the pipeline to Kafka.

## Prerequisites

**Dataset**: You need a large dataset to work with. For this project, there is a `generatedata.py` script located in the `data` folder that can generate sample data.


## Step 1: Set Up SBT Project

To proceed with processing the partitioned data, you need to create a new SBT project. Follow the instructions in the following tutorial to set up your SBT project:  
[Getting Started with Scala and SBT](https://docs.scala-lang.org/getting-started/sbt-track/getting-started-with-scala-and-sbt-on-the-command-line.html).

## Step 2: Download Required Plugins

Download the necessary plugins for your SBT project. In the `plugins` folder, there is a file that contains links to download the plugins. Make sure to place the downloaded plugins in the `plugins` folder.

## Step 3: Configure and Build SBT Project

1. After creating your SBT project, copy the provided `build.sbt` file into the root of your project directory, and the `Main.scala` file into the src/main/scala directory.
2. Run the following command to build the project and generate the JAR file:

    ```bash
    sbt assembly
    ```

## Step 4: Running Docker Compose

Ensure that your Docker environment is up and running by executing the following command:

```bash
docker-compose up --build
```

## Step 5: Generate and Upload Dataset

1. Use the `generatedata.py` script located in the `data` folder to generate your large dataset.
2. Upload the generated dataset to a bucket in MinIO.

## Step 6: Partition Data Using Spark

The `sparkpartition` folder contains the `partition.py` script. This script will partition your large dataset in MinIO and store the resulting partitioned files into a designated folder in the MinIO bucket.

## Step 7: Submit the JAR to Flink

Upload the generated JAR file to Flink and submit the job to run the ETL process.

## Step 8: Check Results in Kafka

Once the job is running, you can check Kafka UI to see the results of the ETL process.

## Notes

- This pipeline is designed to process large datasets efficiently by splitting and processing them in chunks.
