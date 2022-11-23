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

class MarketingDataProcessor:
    def __init__(self, sparkSession, input_file_path):
        self.input_file_path = input_file_path
        self.sparkSession = sparkSession

    def gen_file_name(self):
        return self.input_file_path

    def read_spark_file(self):
        df1 = self.sparkSession.read.option("delimiter", "\t").option("header","true").csv(self.input_file_path)
        return df1

    def convert_df_to_view(self, df, view_name):
        df.createOrReplaceTempView(view_name)
        return view_name

    def generate_parse_search_engine(self, view_name):

        ''' Applying all UDFs to extract referral search engine,
                keyword & extact revenue if it is a transaction
        '''
        sql_run = """
                    SELECT  ip,
                            hit_time_gmt,
                            referrer,
                            product_list,
                            split(product_list,';') as product_array,
                            trim(split(referrer,'.//')[1]) as referral_cleaned,
                            gen_searchengine_udf(referrer) as final_referral,
                            --gen_revenue(product_list) as total_revenue,
                            generate_keyword_udf(referrer) as keyword,
                            CASE WHEN is_purchase_event(event_list) = 1 THEN  gen_revenue(product_list)
                                ELSE 0 END as total_revenue
                    FROM {view}
                    ORDER BY 1 ASC, 2 ASC
                    """.format(view=view_name)
        sqlDF = self.sparkSession.sql(sql_run)
        sqlDF.show(50,truncate=True)

        ''' Apply attribution logic to find last non NULL search engine,
                keyword
        '''
        sqlDF.createOrReplaceTempView('updated_hit_df')
        sql_run = """
                    SELECT ip,
                            hit_time_gmt,
                            referrer,
                            final_referral,
                            total_revenue,
                            last_value(final_referral,true) over (partition by ip
                                    ORDER BY hit_time_gmt
                                    ROWS between unbounded preceding and current row
                                    ) as last_final_referral,
                            last_value(keyword,true) over (partition by ip
                                    ORDER BY hit_time_gmt
                                    ROWS between unbounded preceding and current row
                                    ) as last_keyword
                    FROM {view}
                    """.format(view='updated_hit_df')
        sqlDF2 = self.sparkSession.sql(sql_run)
        sqlDF2.show(50,truncate=True)
        sqlDF2.createOrReplaceTempView('updated_hit_df_with_keyword')

        ''' Group by the total revenue by search engine & keyword
        '''
        sql_run = """
                    SELECT
                            last_final_referral as `Search Engine Domain`,
                            last_keyword as `Search Keyword`,
                            SUM(total_revenue) as Revenue
                    FROM {view}
                    GROUP BY 1, 2
                    ORDER BY SUM(total_revenue) DESC
                    """.format(view='updated_hit_df_with_keyword')
        sqlDF3 = self.sparkSession.sql(sql_run)
        sqlDF3.show(50,truncate=True)
        sqlDF3.createOrReplaceTempView('final_df')

        ''' Saving back the final results into output directory
                currently hardcoded the output path as we only take 1 argument for source.
                In reality, we can update this to point to local S3 for test &
                    client S3 for production.
        '''
        file_output_path = 's3://finaladobeoutput/OUTPUT'
        sqlDF3.coalesce(1).write.mode('overwrite').option("header", "true").csv(file_output_path)
        return
