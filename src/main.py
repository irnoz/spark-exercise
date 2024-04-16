from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, sum as _sum, max as _max

def create_spark_session():
    """Create and return a Spark session."""
    return SparkSession.builder \
        .appName("Transaction Analysis") \
        .getOrCreate()

def read_data(spark, file_path):
    """Read data from a CSV file into a DataFrame."""
    return spark.read.csv(file_path, header=True, inferSchema=True)

def preprocess_data(df):
    """Convert timestamp to date and return the updated DataFrame."""
    return df.withColumn("date", to_date("timestamp"))

def calculate_max_amount_per_day(df):
    """Calculate the maximum transaction amount per day."""
    return df.groupBy("date").agg(_max("amount").alias("max_amount"))

def calculate_sum_amount_per_atm(df):
    """Calculate the sum of transaction amounts per ATM."""
    return df.groupBy("atm_id").agg(_sum("amount").alias("sum_amount"))

def save_results(df, output_path):
    """Save the DataFrame results to the given path."""
    df.write.csv(output_path, mode="overwrite")

def run(input_path, output_path_max, output_path_sum):
    """Main function to run the analysis."""
    # Initialize Spark session
    spark = create_spark_session()

    # Read and preprocess the data
    df = read_data(spark, input_path)
    df = preprocess_data(df)

    # Perform calculations
    max_amount_per_day = calculate_max_amount_per_day(df)
    sum_amount_per_atm = calculate_sum_amount_per_atm(df)

    # Show and save results
    max_amount_per_day.show()
    sum_amount_per_atm.show()
    save_results(max_amount_per_day, output_path_max)
    save_results(sum_amount_per_atm, output_path_sum)

    # Stop Spark session
    spark.stop()

def main():
    # input_path = "data/in/transactions.txt"
    # output_path_max = "data/out/transactions/max_amount_per_day"
    # output_path_sum = "data/out/transactions/sum_amount_per_atm"
    # run(input_path, output_path_max, output_path_sum)

    input_path = "data/in/transactions-log.txt"
    output_path_max = "data/out/transactions-log/max_amount_per_day"
    output_path_sum = "data/out/transactions-log/sum_amount_per_atm"
    run(input_path, output_path_max, output_path_sum)

if __name__ == "__main__":
    main()
