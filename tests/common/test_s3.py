import os
import unittest
from io import StringIO, BytesIO

import boto3
import pandas as pd
from moto import mock_s3

from xetra.common.custom_exceptions import WrongFormatException
from xetra.common.s3 import S3BucketConnector

class TestS3BucketConnectorMethods(unittest.TestCase):

    def setUp(self):
        self.mock_s3 = mock_s3()
        self.mock_s3.start()

        self.s3_access_key = 'AWS_ACCESS_KEY_ID'
        self.s3_secret_key = 'AWS_SECRET_ACCESS_KEY'
        self.s3_endpoint_url = 'https://s3.eu-central-1.amazonaws.com'
        self.s3_bucket_name = 'test-bucket'

        os.environ[self.s3_access_key] = 'KEY1'
        os.environ[self.s3_secret_key] = 'KEY2'

        self.s3 = boto3.resource(service_name = 's3', endpoint_url=self.s3_endpoint_url)
        self.s3.create_bucket(Bucket=self.s3_bucket_name,
                              CreateBucketConfiguration={'LocationConstraint':'eu-central-1'
                              })

        self.s3_bucket = self.s3.Bucket(self.s3_bucket_name)
        self.s3_bucket_conn = S3BucketConnector(self.s3_access_key,
                                                self.s3_secret_key,
                                                self.s3_endpoint_url,
                                                self.s3_bucket_name)


    def tearDown(self):
        self.mock_s3.stop()

    def test_list_files_in_prefix_ok(self):

        prefix_exp = 'prefix/'
        key1_exp = f'{prefix_exp}test1.csv'
        key2_exp = f'{prefix_exp}test2.csv'
        csv_content = """col1,col2
        valA,valB"""
        self.s3_bucket.put_object(Body=csv_content, Key=key1_exp)
        self.s3_bucket.put_object(Body=csv_content, Key=key2_exp)

        list_result = self.s3_bucket_conn.list_files_in_prefix(prefix_exp)

        self.assertEqual(len(list_result), 2)
        self.assertIn(key1_exp, list_result)
        self.assertIn(key2_exp, list_result)
        self.s3_bucket.delete_objects(Delete={
                                        'Objects': [
                                            {
                                                'Key': key1_exp
                                            },
                                            {
                                                'Key': key2_exp
                                            }

                                    ]
                                })

    def test_list_files_in_prefix_wrong_prefix(self):
        prefix_exp = 'no-prefix/'
        list_result = self.s3_bucket_conn.list_files_in_prefix(prefix_exp)
        self.assertTrue(not list_result)

    def test_read_csv_to_df_ok(self):
        key_exp = 'test.csv'
        col1_exp = 'col1'
        col2_exp = 'col2'
        val1_exp = 'val1'
        val2_exp = 'val2'
        log_exp = f'Reading file {self.s3_endpoint_url}/{self.s3_bucket_name}/{key_exp}'
        csv_content = f'{col1_exp},{col2_exp}\n{val1_exp},{val2_exp}'
        self.s3_bucket.put_object(Body=csv_content, Key = key_exp)

        with self.assertLogs() as logm:
            df_result = self.s3_bucket_conn.read_csv_to_df(key_exp)
            self.assertIn(log_exp,logm.output[0])

        self.assertEqual(df_result.shape[0], 1)
        self.assertEqual(df_result.shape[1], 2)
        self.assertEqual(val1_exp, df_result[col1_exp][0])
        self.assertEqual(val2_exp, df_result[col2_exp][0])

        self.s3_bucket.delete_objects(
            Delete={
                'Objects':[
                    {
                        'Key': key_exp
                    }
                ]
            }
        )

    def test_write_df_to_s3_empty(self):

        """
        test
        """
        return_exp = None
        log_exp = 'The dataframe is empty! No file will be written!'
        df_empty = pd.DataFrame()
        key = 'key.csv'
        file_format = 'csv'

        with self.assertLogs() as logm:
            result = self.s3_bucket_conn.write_df_to_s3(df_empty, key, file_format)
            self.assertIn(log_exp, logm.output[0])
        self.assertEqual(return_exp, result)

    def test_write_df_to_s3_csv(self):

        """
        test
        """
        return_exp = True
        df_exp = pd.DataFrame([['A','B'],['C','D']], columns=['col1','col2'])
        key_exp = 'test.csv'
        log_exp = f'Writing file to {self.s3_endpoint_url}/{self.s3_bucket_name}/{key_exp}'

        file_format = 'csv'

        with self.assertLogs() as logm:
            result = self.s3_bucket_conn.write_df_to_s3(df_exp, key_exp, file_format)
            self.assertIn(log_exp, logm.output[0])
        data = self.s3_bucket.Object(key=key_exp).get().get('Body').read().decode('utf-8')
        out_buffer = StringIO(data)
        df_result = pd.read_csv(out_buffer)
        self.assertEqual(return_exp, result)
        self.assertTrue(df_exp.equals(df_result))
        self.s3_bucket.delete_objects(
            Delete={
                'Objects':[
                    {
                        'Key': key_exp
                    }
                ]
            }
        )
    def test_write_df_to_s3_parquet(self):

        """
        test
        """
        return_exp = True
        df_exp = pd.DataFrame([['A','B'],['C','D']], columns=['col1','col2'])
        key_exp = 'test.parquet'
        log_exp = f'Writing file to {self.s3_endpoint_url}/{self.s3_bucket_name}/{key_exp}'

        file_format = 'parquet'

        with self.assertLogs() as logm:
            result = self.s3_bucket_conn.write_df_to_s3(df_exp, key_exp, file_format)
            self.assertIn(log_exp, logm.output[0])
        data = self.s3_bucket.Object(key=key_exp).get().get('Body').read()
        out_buffer = BytesIO(data)
        df_result = pd.read_parquet(out_buffer)
        self.assertEqual(return_exp, result)
        self.assertTrue(df_exp.equals(df_result))
        self.s3_bucket.delete_objects(
            Delete={
                'Objects':[
                    {
                        'Key': key_exp
                    }
                ]
            }
        )
    def test_write_df_to_s3_wrong_format(self):

        """
        test
        """
        df_exp = pd.DataFrame([['A','B'],['C','D']], columns=['col1','col2'])
        key_exp = 'test.parquet'
        format_exp = 'wrong_format'
        log_exp = f'The file format {format_exp} is not supported to be written to s3!'
        exception_exp = WrongFormatException

        with self.assertLogs() as logm:
            with self.assertRaises(exception_exp):
                self.s3_bucket_conn.write_df_to_s3(df_exp, key_exp, format_exp)
            self.assertIn(log_exp, logm.output[0])


if __name__ == "__main__":
    unittest.main()

