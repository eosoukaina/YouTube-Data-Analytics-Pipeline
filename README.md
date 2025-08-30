# YouTube Data ETL Pipeline with PySpark and AWS

## Overview 🚀
This project builds an ETL (Extract, Transform, Load) pipeline that retrieves data from the **YouTube Data API**, transforms it using **PySpark**, and loads the data into **AWS S3**. The pipeline is orchestrated using **Apache Airflow** for automation, and the final data is visualized in **AWS QuickSight** to analyze metrics from three great channels in the IT and data engineering/science domains: **freeCodeCamp**, **Alex The Analyst**, and **3Blue1Brown**.

The scraped data includes key metrics such as:
- **ChannelTitle**: Name of the YouTube channel.
- **Title**: Title of the video.
- **Published_date**: The date the video was published.
- **Views**: The number of views the video has.
- **Likes**: The number of likes for the video.
- **Comments**: The total number of comments for the video.

## Tools Used 🛠️

This project utilized the following technologies and tools:

- **YouTube Data API**: Leveraged to extract data from YouTube channels and videos.
- **Python**: Used for data extraction, transformation, and integration into the ETL pipeline.
- **PySpark**: Employed for large-scale data processing and transformation tasks.
- **AWS S3**: Used for data storage and management in a scalable and cost-effective manner.
- **Apache Airflow**: Utilized to orchestrate the ETL pipeline and manage workflow scheduling.
- **Docker**: Facilitated the creation of containerized environments for consistent deployment.
- **AWS QuickSight**: Implemented for data visualization and dashboard creation to analyze insights.

## Data Pipeline
![ec](https://github.com/user-attachments/assets/b2fbcaee-10f5-4702-ad44-4a4957c7aa25)

## Key Highlights 🌟

- **Data Extraction**: Scraped YouTube data using the YouTube Data API, retrieving critical video and channel metrics from **freeCodeCamp**, **Alex The Analyst**, and **3Blue1Brown**.

- **Data Transformation**: Leveraged **PySpark** for cleaning and transforming the data, ensuring that it is ready for loading into AWS S3. This involved converting data types, handling missing values, and extracting time-based features to track growth over the years.

- **Data Loading**: Transformed data was loaded into **AWS S3** for long-term storage and to support the visualizations created in AWS QuickSight.

- **Orchestration**: The entire ETL pipeline was orchestrated using **Apache Airflow**, ensuring automation and scheduling of the data pipeline for continuous updates.

- **Visualization**: **AWS QuickSight** was used for interactive visualizations, providing insightful dashboards that offer a deeper understanding of the performance of the channels over time.


## Prerequisites 📋

Before setting up and running the project, ensure that you have the following credentials and tools ready:

### 1. Obtain a YouTube Data API Key

1. Go to the [Google Developers Console](https://console.developers.google.com/).
2. Create a new project.
3. In the API Library, search for **YouTube Data API v3** and enable it for your project.
4. Navigate to the **Credentials** section, generate new credentials, and select **API Key**.
5. Copy the generated API Key for use in the project.

### 2. Acquire AWS Access Key ID and Secret Access Key

1. Log in to your [AWS Management Console](https://aws.amazon.com/console/).
2. Navigate to **IAM (Identity and Access Management)**.
3. Create a new user with programmatic access.
4. Attach a policy that grants permissions for Amazon S3 (e.g., **AmazonS3FullAccess**).
5. Generate an Access Key ID and Secret Access Key for this user.
6. Save these keys securely, as you will need them to access AWS services from within the project.

### 3. Required Tools

- **Docker**: Download and install Docker from the [official website](https://www.docker.com/get-started).
- **Docker Compose**: Docker Compose should be installed as part of the Docker setup.

Make sure all the required tools and credentials are prepared before proceeding with the setup.


## Setup Instructions 📝

Follow these steps to set up and run the project on your machine:

### Step 1: Clone the repository

```bash
git clone https://github.com/YourUsername/YouTube-ETL-Pipeline.git
cd YouTube-ETL-Pipeline
```


### Step 2: Create a .env File for Credentials

Create a .env file in the root directory of the project and add the following environment variables:

```plaintext
AWS_ACCESS_KEY_ID=Your AWS Access Key ID
AWS_SECRET_ACCESS_KEY=Your AWS Secret Access Key
YOUTUBE_API_KEY=Your YouTube Data API Key
```

### Step 3: Build the Docker Containers

```bash
docker compose build
```

### Step 4: Start the Docker Containers

```bash
docker compose up
```

### Step 5: Execute the ETL Pipeline

```bash
docker exec -it youtube_data_pipeline-airflow_pipeline-1 python /opt/airflow/dags/youtube_etl_dag.py
```

### Step 6: Access the Airflow Web Interface 🌐

You can now access the Airflow web interface to manage your ETL process DAGs (Directed Acyclic Graphs).

- **URL**: [http://localhost:8080](http://localhost:8080)

#### Initial Login

If you are accessing the Airflow web interface for the first time, you will need to log in with the following credentials:

- **Username**: `admin`
- **Password**: The password can be found in the `standalone_admin_password.txt` file located in the Airflow folder created after running the `docker-compose up` command.


Here is our DAG, which successfully executed the ETL pipeline as intended : 
![dag](https://github.com/user-attachments/assets/a8094cd5-100f-4cdc-9ed9-93a51eb02aff)


### Step 7: Visualization with AWS QuickSight
Once the data is loaded into AWS S3, set up AWS QuickSight for data visualization:

Open AWS QuickSight.
Connect to the S3 bucket.
Create visualizations to explore the YouTube data and derive insights.

![vis](https://github.com/user-attachments/assets/6629521a-83dc-48fc-a086-b7edb2c3de69)



## Conclusion 🎉

In this project, we successfully developed a robust ETL pipeline that extracts data from the YouTube Data API, transforms it using PySpark, and loads it into AWS S3 for further analysis. By orchestrating the entire process with Apache Airflow, we ensured efficient workflow management and seamless execution. The integration with AWS QuickSight allows for insightful data visualization, enabling users to gain valuable insights into YouTube channel performance. This project not only demonstrates the effectiveness of using modern data engineering tools but also showcases the potential for scalable and efficient data processing solutions in the realm of data analytics.


## Contact

For questions or suggestions, feel free to reach out:

📩 elhadifi.soukaina@gmail.com
