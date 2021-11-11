from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'
    
    
    @apply_defaults
    def __init__(self,
                 dq_checks,
                 conn_id = 'redshift',
                 aws_credentials_id = 'aws_credentials',
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        
        self.conn_id = conn_id
        self.aws_credentials_id = aws_credentials_id
        self.dq_checks = dq_checks

    def execute(self, context):
        
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift_hook = PostgresHook("redshift")
        
        for check in self.dq_checks:
            sql = check.get('check_sql')
            exp_result = check.get('expected_result')

            records = redshift_hook.get_records(sql)[0]
            self.log.info(f"Records value is {records[0]}")
            # compare with the expected results
      
            if records[0] == exp_result:
                self.log.info(f"Data quality check {self.dq_checks.index(check)} on table {sql.split(' ')[3]} passed")
                
            else:
                raise ValueError(f"Data quality check {self.dq_checks.index(check)} failed on table {sql.split(' ')[3]}")
            