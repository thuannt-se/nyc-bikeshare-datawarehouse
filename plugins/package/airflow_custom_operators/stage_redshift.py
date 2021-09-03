from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models.baseoperator import BaseOperator

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    copy_sql = """copy {} 
                  from {} 
                  region {} credentials 'aws_iam_role={}'
                  {};
               """
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 redshift_conn_id="",
                 table="",
                 region="'us-west-2'",
                 s3_bucket_id="",
                 s3_key="",
                 iam_role="",
                 format_as="FORMAT AS PARQUET",
                 compupdate_statupdate_off=True,
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.region = region
        self.s3_bucket_id = s3_bucket_id
        self.s3_key = s3_key
        self.iam_role = iam_role
        self.format_as = format_as
        self.compupdate_statupdate_off = compupdate_statupdate_off

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info("Clearing data from Redshift table before staging")
        redshift.run("DELETE FROM {}".format(self.table))

        self.log.info("Copying data from S3 to Redshift")
        s3_path = "'s3://{}/{}'".format(self.s3_bucket_id, self.s3_key)
        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            self.table,
            s3_path,
            self.region,
            self.iam_role,
            self.format_as
        )
        if(self.compupdate_statupdate_off):
            formatted_sql = formatted_sql + " COMPUPDATE OFF STATUPDATE OFF"
        
        redshift.run(formatted_sql)
        self.log.info('StageToRedshiftOperator done')




