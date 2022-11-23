#!/usr/bin/env python
'''
This contains core UDFs leveraged for Marketing Data parsing
'''
import os, sys
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.dirname(CURRENT_DIR))


class PythonUdfs:
    def __init__(self):
        pass

    def is_purchase_event(self, event_list):
        if event_list is None:
            return 0
        if (event_list == ''):
            return 0
        event_list_split = event_list.split(',')
        for elem in event_list_split:
            if elem == '1':
                return 1
        return 0


    def convert_to_se(self, referral_string):
        search_engine_list = ['google','bing','yahoo']
        if len(referral_string) == 0:
            return None

        sub_elem = referral_string.split('://')
        if len(sub_elem) > 1:
            referral_cleaned = sub_elem[1]
            elements_split = referral_cleaned.split('.')
            print (elements_split)
            for element in elements_split:
                if element.lower() in search_engine_list:
                    return (element.lower()+'.com')
        return None

    # UDF for revenue
    def gen_revenue(self, product_list):
        print (product_list)
        if product_list is None:
            return 0
        sub_elem = product_list.split(';')
        print (sub_elem)
        if len(sub_elem) >= 4:
            if (sub_elem[3].strip() != ''):
                return int(sub_elem[3].strip())
            else:
                return 0
        return 0

    # UDF for Keyword Generation
    def generate_keyword_udf(self, referral_string):
        if referral_string is None:
            return None

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
            print (elements_split)
            for element in elements_split:
                print (element)
                if element.lower() in search_engine_list:
                    for key_elem in sub_keyword_list:
                        if key_elem.startswith(keyword_dict[element.lower()]):
                            return key_elem[2:].lower().replace('+',' ')

            for key_elem in sub_keyword_list:
                if key_elem.startswith('k='):
                    return key_elem[2:].lower().replace('+',' ')
        return None
