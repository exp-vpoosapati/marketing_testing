#!/usr/bin/env python

"""
High level class which initializes a spark context & parses the input adobe file data

Input: Marketing data file from Adobe
Output: Parsed file which generates output data containing the following:
1. Search engine, Keyword & the corresponding revenue from each output.

"""
import os, sys
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.dirname(CURRENT_DIR))
from datetime import date
from pyspark.sql import SparkSession
from PythonUdfRegister import PythonUdfRegister
from MarketingDataProcessor import MarketingDataProcessor
from PythonUdfs import PythonUdfs

if __name__ == "__main__":
    """
        Main script to launch each individual elements
    """
    spark = SparkSession\
        .builder\
        .appName("SampleMarketingUseCase")\
        .getOrCreate()

    # input file path from the source.
    file_name_path = sys.argv[1]
    print (file_name_path)

    # register all the UDFs
    register_udf = PythonUdfRegister(spark)
    register_udf.register_udfs()

    # initialize reading source file into Spark dataframe
    marketing_data = MarketingDataProcessor(spark, file_name_path)
    marketingDf = marketing_data.read_spark_file()

    # Transform the spark dataframe to a Spark sql temporary table for easier processing
    test_view = marketing_data.convert_df_to_view(marketingDf, 'marketing_data_tbl')

    # Run the parsing of individual elements & output the data into a final file.
    marketing_data.generate_parse_search_engine('marketing_data_tbl')

    # Stop the final run.
    spark.stop()
