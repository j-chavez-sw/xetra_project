import collections
from datetime import datetime

import pandas as pd

from xetra.common.constants import MetaProcessFormat
from xetra.common.custom_exceptions import WrongMetaFileException
from xetra.common.s3 import S3BucketConnector


class MetaProcess():

    @staticmethod
    def update_meta_file(extract_date_list: list, meta_key: str, s3_bucket_meta: S3BucketConnector):
        """updating meta file"""
        df_new = pd.DataFrame(columns=[MetaProcessFormat.META_SOURCE_DATE_COL.value,
                                       MetaProcessFormat.META_PROCESS_COL.value])
        df_new[MetaProcessFormat.META_SOURCE_DATE_COL.value] = extract_date_list
        df_new[MetaProcessFormat.META_PROCESS_COL.value] = datetime.today().strftime(
            MetaProcessFormat.META_PROCESS_DATE_FORMAT.value)
        try:
            df_old = s3_bucket_meta.read_csv_to_df(meta_key)
            if collections.Counter(df_old.columns) != collections.Counter(df_new.columns):
                raise WrongMetaFileException
            df_all = pd.concat([df_old,df_new])

        except s3_bucket_meta.session.client('s3').exceptions.NoSuckKey:
            df_all = df_new
        s3_bucket_meta.write_df_to_s3(df_all, meta_key, MetaProcessFormat.META_FILE_FORMAT.value)
        return True

    @staticmethod
    def return_date_list():
        pass
