import unittest
import os, sys
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.dirname(CURRENT_DIR))

from runner.PythonUdfs import PythonUdfs

class Testing(unittest.TestCase):
    def test_generate_parse_search_engine(self):
        referral_string = 'http://www.google.com/search?hl=en&client=firefox-a&rls=org.mozilla%3Aen-US%3Aofficial&hs=ZzP&q=Ipod&aq=f&oq=&aqi='
        expected_value = 'google'
        udf = PythonUdfs()
        self.assertEqual(expected_value, udf.convert_to_se(referral_string))

        referral_string = 'http://search.yahoo.com/search?p=cd+player&toggle=1&cop=mss&ei=UTF-8&fr=yfp-t-701'
        expected_value = 'yahoo'
        udf = PythonUdfs()
        self.assertEqual(expected_value, udf.convert_to_se(referral_string))

        referral_string = 'http://www.bing.com/search?q=Zune&go=&form=QBLH&qs=n'
        expected_value = 'bing'
        udf = PythonUdfs()
        self.assertEqual(expected_value, udf.convert_to_se(referral_string))

        referral_string = 'http://www.esshopzilla.com/product/?pid=as23233'
        expected_value = None
        udf = PythonUdfs()
        self.assertEqual(expected_value, udf.convert_to_se(referral_string))

        referral_string = '    '
        expected_value = None
        udf = PythonUdfs()
        self.assertEqual(expected_value, udf.convert_to_se(referral_string))

    def test_generate_revenue(self):
        product_list = "Electronics;Zune-32GB;1;250;"
        expected_value = 250
        udf = PythonUdfs()
        self.assertEqual(expected_value, udf.gen_revenue(product_list))


        product_list = "Electronics;Ipod - Nano - 8GB;1;190; "
        expected_value = 190
        udf = PythonUdfs()
        self.assertEqual(expected_value, udf.gen_revenue(product_list))

        product_list = "Electronics;Ipod - Touch - 32GB;1;290;"
        expected_value = 290
        udf = PythonUdfs()
        self.assertEqual(expected_value, udf.gen_revenue(product_list))

        product_list = " "
        expected_value = 0
        udf = PythonUdfs()
        self.assertEqual(expected_value, udf.gen_revenue(product_list))

        product_list = None
        expected_value = 0
        udf = PythonUdfs()
        self.assertEqual(expected_value, udf.gen_revenue(product_list))

    def test_generate_keyword_udf(self):
        referral_string = 'http://www.google.com/search?hl=en&client=firefox-a&rls=org.mozilla%3Aen-US%3Aofficial&hs=ZzP&q=Ipod&aq=f&oq=&aqi='
        expected_value = 'ipod'
        udf = PythonUdfs()
        self.assertEqual(expected_value, udf.generate_keyword_udf(referral_string))

        referral_string = 'http://www.bing.com/search?q=Zune&go=&form=QBLH&qs=n'
        expected_value = 'zune'
        udf = PythonUdfs()
        self.assertEqual(expected_value, udf.generate_keyword_udf(referral_string))

        referral_string = 'http://search.yahoo.com/search?p=cd+player&toggle=1&cop=mss&ei=UTF-8&fr=yfp-t-701'
        expected_value = 'cd player'
        udf = PythonUdfs()
        self.assertEqual(expected_value, udf.generate_keyword_udf(referral_string))


if __name__ == '__main__':
    unittest.main()
