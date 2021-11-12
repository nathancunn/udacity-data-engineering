"""
This processes the dimension tables, truncating if necessary.
"""
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'
    
    insert_sql = """
    INSERT INTO {}
    {};
    """
    truncate_sql = """
    TRUNCATE {};
    """
    
    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 truncate = False,
                 sql = "",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.truncate = truncate
        self.sql = sql
        # Map params here
        # Example:
        # self.conn_id = conn_id

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        if self.truncate:
            self.log.info(f"Truncating table {self.table}")
            formatted_sql = LoadDimensionOperator.truncate_sql.format(self.table)
            redshift.run(formatted_sql)
        self.log.info(f"Loading table {self.table}")            
        formatted_sql = LoadDimensionOperator.insert_sql.format(
            self.table,
            self.sql
        )
        redshift.run(formatted_sql)
        self.log.info(f"Finished loading {self.table}")
        
