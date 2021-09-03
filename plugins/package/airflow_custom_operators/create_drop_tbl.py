from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import BaseOperator

class CreateDropTableRedshiftOperator(BaseOperator):
    
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 aws_conn_id = "",
                 redshift_conn_id = "",
                 drop_sql = "",
                 create_sql = "",
                 *args, **kwargs):
        
        super(CreateDropTableRedshiftOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        self.redshift_conn_id = redshift_conn_id
        self.drop_sql = drop_sql
        self.create_sql = create_sql

        
    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info("Drop table from Redshift before running")
        redshift.run(self.drop_sql)

        self.log.info("Tables were dropped")
        self.log.info("Create tables in RedShift")

        redshift.run(self.create_sql)
        self.log.info('CreateDropTableRedshiftOperator done')
