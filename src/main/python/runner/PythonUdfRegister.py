import os, sys
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.dirname(CURRENT_DIR))
from datetime import date
import os

from pyspark.sql import SparkSession

class PythonUdfRegister:
    def __init__(self, sparkSession):
        self.sparkSession = sparkSession

    def register_udfs(self):

        '''
         Generates the search engine from the URL datastructure and extracts
         relevant search engine.
         Currently handles keywords by a hardcoded list, but can be specifically expanded
         to other engines with simple addition to the list.
        '''
        def convert_to_se(referral_string):
            search_engine_list = ['google','bing','yahoo']
            if len(referral_string) == 0:
                return None

            sub_elem = referral_string.split('://')
            if len(sub_elem) > 1:
                referral_cleaned = sub_elem[1]
                elements_split = referral_cleaned.split('.')
                for element in elements_split:
                    if element.lower() in search_engine_list:
                        return (element.lower())
            return None

        self.sparkSession.udf.register("gen_searchengine_udf", convert_to_se)

        ''' UDF to extract revenue from the original string based value.
        '''

        def gen_revenue(product_list):
            if product_list is None:
                return 0
            sub_elem = product_list.split(';')
            if len(sub_elem) >= 4:
                if (sub_elem[3].strip() != ''):
                    return int(sub_elem[3].strip())
                else:
                    return 0
            return 0
        self.sparkSession.udf.register("gen_revenue", gen_revenue)

        '''
            UDF for Keyword Generation and this is unique to
                each search engine based on the pattern of how it is logged.
        '''

        def generate_keyword_udf(referral_string):
            search_engine_list = ['google','bing','yahoo']
            keyword_dict = {
                'google':'q=',
                'bing':'q=',
                'yahoo':'p=',
                'organic':'k='
            }

            if len(referral_string) == 0:
                return None

            sub_elem = referral_string.split('://')
            if len(sub_elem) > 1:
                referral_cleaned = sub_elem[1]
                elements_split = referral_cleaned.split('.')
                keyword_split = referral_cleaned.split('?')
                if len(keyword_split) > 1:
                    sub_keyword_list = keyword_split[1].split('&')
                else:
                    sub_keyword_list = []
                for element in elements_split:
                    if element.lower() in search_engine_list:
                        for key_elem in sub_keyword_list:
                            if key_elem.startswith(keyword_dict[element.lower()]):
                                return key_elem[2:].lower().replace('+',' ')

                for key_elem in sub_keyword_list:
                    if key_elem.startswith('k='):
                        return key_elem[2:].lower().replace('+',' ')

            return None

        self.sparkSession.udf.register("generate_keyword_udf", generate_keyword_udf)
