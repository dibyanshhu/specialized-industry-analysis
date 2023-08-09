# Interview Assignment Solution - Data Engineering
# specialized Industry cahnge Analysis

This project aims to analyze the fluctuation of specialized industries in terms of daily revenue over the last 24 hours, comparing it with the average revenue of the last 30 days. The analysis is based on historical and recent order data and customer information.

## Table of Contents

- [Overview](#overview)
- [Docker-compose.yml](#update)
- [Data](#data)
- [Usage](#usage)
- [Results](#results)
- [Dependencies](#dependencies)
- [License](#license)

## Overview

The project involves using PySpark to process historical and recent order data, customer information, and specialized industries. The main steps of the project include:

1. Loading historical and recent order data.
2. Union of historical and recent order data.
3. Exploding order lines to separate rows.
4. Joining order data with customer data.
5. Calculating daily revenue for the last 24 hours.
6. Calculating average revenue for the last 30 days.
7. Calculating industry fluctuation.
8. Selecting top fluctuating industries.

## Docker-compose-yml

- In the 'docker-composer.yml' made change for setup the hive metastore up and running for that it needed to be have the specivif version of docker image file for trinodb, mariadb, minio. 

## Data

The project requires the following data:

- Historical order data
- Recent order data
- Customer data

The order data includes order IDs, customer IDs, order lines (product ID, volume, price), amount, and timestamps. The customer data includes customer IDs, company names, and specialized industries.

## Usage

1. Clone the repository:

   ```bash
   git clone 
   cd specialized-industry-change-analysis

  - Install the required dependencies:
  pip install pyspark

  - Run the spark submit script:
  spark-submit \
  --master local[*] \
  --packages org.apache.hadoop:hadoop-aws:2.7.3 \
  spark-jobs-python/solution.py

  The script will output the top 3 fluctuating industries based on the analysis.

## Results
The script will display the top 3 specialized industries that have experienced the most significant fluctuation in terms of daily revenue over the last 24 hours, compared to the average revenue of the last 30 days.

## License

This project is licensed under the [MIT License](LICENSE).




