import os
from os import listdir
from os.path import isfile, join
from airflow.models.baseoperator import BaseOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


class S3FileTransferOperator(BaseOperator):
    # Use this prefix as default value to get correct s3 bucket key
    PREFIX_S3_BUCKET_KEY = "2020"

    def __init__(self,
                 aws_conn_id="",
                 operation="",
                 local_file_path="",
                 output="",
                 s3_bucket="",
                 s3_key="",
                 *args,
                 **kwargs):
        super(S3FileTransferOperator, self).__init__(*args, **kwargs)
        # operation should be UPLOAD or DOWNLOAD
        self.operation = operation
        self.aws_conn_id = aws_conn_id
        self.local_file_path = local_file_path
        self.output_path = output
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key

    def execute(self, context):
        s3 = S3Hook(aws_conn_id=self.aws_conn_id)
        self.log.info(f"Operation: {self.operation}")
        if self.operation != "DOWNLOAD" and self.operation != "UPLOAD":
            raise TypeError("operation should be 'UPLOAD' or 'DOWNLOAD'")
        if self.operation == "DOWNLOAD":
            keys = s3.list_keys(bucket_name=self.s3_bucket, prefix=S3FileTransferOperator.PREFIX_S3_BUCKET_KEY)
            for key in keys:
                try:
                    s3.download_file(bucket_name=self.s3_bucket, key=key, local_path=self.output_path)
                except ValueError:
                    self.log.info(ValueError)
                self.log.info(f"downloading from s3://{self.s3_bucket}/{key}")
            self.log.info(f"Download finished - file store in {self.output_path}")
        if self.operation == "UPLOAD":
            files = [f for f in listdir(self.local_file_path) if isfile(join(self.local_file_path, f))]
            self.s3_key = self.s3_key + "/"
            for file in files:
                self.log.info(f"file path: {file}")
                try:
                    s3.load_file(filename=join(self.local_file_path, file), bucket_name=self.s3_bucket,
                                 key=os.path.join(self.s3_key, file))
                except ValueError:
                    self.log.info(ValueError)
                self.log.info(f"uploading to s3://{self.s3_bucket}/{self.s3_key}")
            self.log.info("Upload finished")
