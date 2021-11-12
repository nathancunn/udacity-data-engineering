"""
This data quality operator ensures that the data loaded contain
no NULL values and the membership status only contain two distinct
values.
"""
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        redshift = PostgresHook(self.redshift_conn_id)
        no_errors = True
        
        self.log.info("Checking for NULL values")
        tables = ["users", "songs", "artists"]
        cols = ["userid", "title", "name"]
        for table, col in zip(tables, cols):
            value = redshift.get_records(f"SELECT COUNT(*) FROM {table} WHERE {col} IS NULL")[0]
            if value[0] != 0:
                no_errors = False
                self.log.info(f"Found {value} NULL values in {table}:{col}; expected 0")
                
        value = redshift.get_records("SELECT COUNT(DISTINCT level) FROM songplays")[0]
        if value[0] != 2:
            no_errors = False
            self.log.info(f"Found {value} distinct membership levels in songplays table, expected 2")
        
        if not no_errors:
            self.log.info("Tests failed")
            raise ValueError("Tests failed")
        else:
            self.log.info("No errors found")
            
        
        