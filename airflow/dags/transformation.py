from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, udf
from pyspark.sql.types import (StructType, StructField, StringType, LongType, DateType)
import re
from nltk.corpus import stopwords
import boto3
import os



def transform_youtube_data():
    print("airflow here...")
    # Initialize Spark session
    spark = SparkSession.builder.appName("YT-pipeline").getOrCreate()
    
    # Define the schema
    schema = StructType([
        StructField("ChannelTitle", StringType(), nullable=False),
        StructField("Title", StringType(), nullable=False),
        StructField("Published_date", StringType(), nullable=False),  # Read as String initially
        StructField("Views", LongType(), nullable=False),
        StructField("Likes", LongType(), nullable=False),
        StructField("Comments", LongType(), nullable=False)
    ])
    
    # Read the CSV file with the defined schema
    df = spark.read.csv("/opt/airflow/data/Youtube_Data_Raw.csv", schema=schema, header=True)

    # Drop rows with null values
    df = df.na.drop()

    # Remove duplicate rows
    df = df.dropDuplicates()

    # Set up stopwords
    STOPWORDS = set(stopwords.words('english'))

    # Define the text cleaning function
    def cleaning_data(text):
        cleaned_text = re.sub('[^a-zA-Z]', ' ', text)  # Remove non-alphabetic characters
        cleaned_text = re.sub(' +', ' ', cleaned_text)  # Replace multiple spaces with a single space
        cleaned_text = cleaned_text.lower()  # Convert to lowercase
        cleaned_text = cleaned_text.split()  # Split into words
        cleaned_text = [word for word in cleaned_text if word not in STOPWORDS]  # Remove stopwords
        cleaned_text = ' '.join(cleaned_text)  # Join words back into a string
        return cleaned_text.strip()  # Remove any trailing spaces

    # Register the cleaning function as a UDF
    clean_text_udf = udf(cleaning_data, StringType())
    
    # Clean the data and cast columns to the correct types
    df_cleaned = df.withColumn('ChannelTitle', clean_text_udf(col('ChannelTitle'))) \
                   .withColumn('Title', clean_text_udf(col('Title'))) \
                   .withColumn('Published_date', to_date(col('Published_date'))) \
                   .withColumn('Views', col('Views').cast(LongType())) \
                   .withColumn('Likes', col('Likes').cast(LongType())) \
                   .withColumn('Comments', col('Comments').cast(LongType()))
    
    # Write the cleaned DataFrame to a CSV file
    output_path = '/opt/airflow/data/Youtube_Data_transformation/'
    df_cleaned.write.csv(output_path, header=True, mode='overwrite')

    return output_path

transform_youtube_data()