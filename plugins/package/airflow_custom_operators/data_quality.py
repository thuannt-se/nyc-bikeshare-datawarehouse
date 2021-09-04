from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models.baseoperator import BaseOperator

class DataQualityOperator(BaseOperator):
    select_count = "Select count(*) from {}"
    select_count_not_null = "Select count(*) from {} where {} is null"
    ui_color = '#89DA59'

    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 redshift_conn_id = "",
                 table = "",
                 table_id = "",
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.table_id = table_id

    def checkDataNotEmpty(self, hook):
        records = hook.get_records(DataQualityOperator.select_count.format(self.table))
        if len(records) < 1 or len(records[0]) < 1:
            raise ValueError(f"Data quality check failed. {self.table} returned no results")
        num_records = records[0][0]
        if num_records < 1:
            raise ValueError(f"Data quality check failed. {self.table} contained 0 rows")
        self.log.info(f"Data quality on table {self.table} check passed with {records[0][0]} records")
        
    def checkDataNotNull(self, hook):
        records = hook.get_records(DataQualityOperator.select_count_not_null.format(self.table, self.table_id))
        if len(records) > 1 or len(records[0]) > 1:
            raise ValueError(f"Data quality check failed. {self.table} returned record with id null")
        num_records = records[0][0]
        if num_records > 1:
            raise ValueError(f"Data quality check failed. {self.table} contained more than 1 rows with id null")
        self.log.info(f"Data quality on table {self.table} check passed with {records[0][0]} records")

    
    def execute(self, context):
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.checkDataNotEmpty(redshift_hook)
        self.checkDataNotNull(redshift_hook)