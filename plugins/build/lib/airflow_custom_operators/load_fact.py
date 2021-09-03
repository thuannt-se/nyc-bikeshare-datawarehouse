from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models.baseoperator import BaseOperator


class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    def __init__(self,
                 # Define your operators params (with defaults) here
                 redshift_conn_id="",
                 sql_insert = "",
                 table = "",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.sql_insert = sql_insert
        self.table = table

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info("Clearing data from Redshift table before insert")
        redshift.run("DELETE FROM {}".format(self.table))

        self.log.info("Insert data from staging table to Redshift table")
        redshift.run(self.sql_insert)
        self.log.info('LoadFactOperator done')
