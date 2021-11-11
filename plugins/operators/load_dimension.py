from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'
    
    insert_sql_template = """
    INSERT INTO {}
    {}
    """

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 table,
                 sql, 
                 aws_credentials_id = 'aws_credentials',
                 conn_id = 'redshift',
                 append_data = False,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        self.table = table
        self.conn_id = conn_id
        self.aws_credentials_id = aws_credentials_id
        self.sql = sql
        self.append_data = append_data
        
    def execute(self, context):
        
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id = self.conn_id)
                
        self.log.info(f"Inserting data into {self.table} dimension table")
#         rendered_key = self.s3_key.format(**context)
        
        formatted_sql = self.insert_sql_template.format(
            self.table,
            self.sql
        )
        
        
        if self.append_data:
            redshift.run(formatted_sql)

        else:
            sql_statement = 'DELETE FROM %s' % self.table
            redshift.run(sql_statement)
            redshift.run(formatted_sql)