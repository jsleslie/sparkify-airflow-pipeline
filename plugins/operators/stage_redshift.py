from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    
    stage_sql_template = """
    COPY {}
    FROM '{}'
    ACCESS_KEY_ID '{}'
    SECRET_ACCESS_KEY '{}'
    REGION '{}'
    JSON '{}'
    """

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 redshift_conn_id="redshift",
                 aws_credentials_id = "aws_credentials",
                 table = "",
                 s3_path="s3a://udacity-dend",
                 s3_bucket="",
                 s3_key="A/A/A/",
                 region = 'us-west-2',
                 jsonpath= 'auto',
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.s3_path = s3_path
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.region = region
        self.jsonpath = jsonpath
        
    def execute(self, context):
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id = self.redshift_conn_id)
        
        
        self.log.info('Copying data from S5 to Redshift')
#         rendered_key = self.s3_key.format(**context)
        s3_loc = "{}/{}/{}".format(self.s3_path,self.s3_bucket, self.s3_key)
        formatted_sql = self.stage_sql_template.format(
            self.table,
            s3_loc,
            credentials.access_key,
            credentials.secret_key,
            self.region,
            self.jsonpath
        )
        redshift.run(formatted_sql)







