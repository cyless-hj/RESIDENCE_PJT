import json
import requests
from infra.util import std_day
import boto3


class SafeDeliveryExtractor:

    URL = 'http://openapi.seoul.go.kr:8088/79504a72666a686a3931414b6b7059/json/safeOpenBox'
    START_IDX = '1'
    END_INDEX = '1000'
    BUCKET_NAME = 'residencebucket'
    FILE_DIR = 'raw_data/SAFE_DELIVERY/SAFE_DELIVERY_' + std_day() + '_'

    @classmethod
    def extract_data(cls):

        s3 = boto3.client('s3')

        try:
            data = cls._load_api(cls.START_IDX, cls.END_INDEX)
            data_dict = json.loads(data)
            cls._upload_to_s3(s3, data, 1)
            n = cls._generate_page_len(data_dict)

        except Exception as e:
            raise e

        for i in range(2, n + 1):
            try:
                start, end = cls._set_page_idx(i)
                data = cls._load_api(start, end)
                cls._upload_to_s3(s3, data, i)

            except Exception as e:
                raise e

    @classmethod
    def _set_page_idx(cls, i):
        start = str(1000 * (i - 1) + 1)
        end = str(1000 * i)
        return start,end

    @classmethod
    def _generate_page_len(cls, data_dict):
        data_len = data_dict['safeOpenBox']['list_total_count']
        n = data_len // 1000 + 1
        return n

    @classmethod
    def _upload_to_s3(cls, s3, data, i):
        return s3.put_object(Body=data, Bucket=cls.BUCKET_NAME, Key=cls.FILE_DIR + str(i) + '.json')

    @classmethod
    def _load_api(cls, start, end):
        res = cls.URL + '/' + start + '/' + end + '/'
        data = requests.get(res).text
        return data
