from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'
    
    insert_sql_template = """
    INSERT INTO {}
    {}
    """
    
    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 sql, 
                 aws_credentials_id = 'aws_credentials',
                 conn_id = 'redshift',
                 table = 'songplays',
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        self.conn_id = conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.sql = sql

    def execute(self, context):
        
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id = self.conn_id)
                
        self.log.info(f"Inserting data into {self.table} fact table")
#         rendered_key = self.s3_key.format(**context)
        
        formatted_sql = self.insert_sql_template.format(
            self.table,
            self.sql
        )
        redshift.run(formatted_sql)
        