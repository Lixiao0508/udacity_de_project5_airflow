from airflow.contrib.hooks.aws_hook import AwsHook
# from airflow.providers.amazon.aws.hooks.s3 import S3Hook ##No module named 'airflow.providers??
# from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator  ##??
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator): 
    ui_color = '#358140'
    
    #parameter: s3 key is templatable = it can be replaced
    #It tells airflow that it needs to render out that key before it gets passed into the operator
    template_fields = ("s3_key",) 
    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        IGNOREHEADER {}
        FORMAT AS JSON '{}'
    """
#     When key names matches with column names, you don't need a json path file to parse the data and you can use the auto mode for dealing with json. If key names in json files are different from the columns names of the table to be loaded, or the keys to be parsed are of different depth, a json path file is required to be able to parse the data correctly.

#DELIMITER '{}'
    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # redshift_conn_id=your-connection-name
                 
                 #default parameters
                 aws_credentials_id="",
                 redshift_conn_id="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 copy_json_option="auto",
                 ignore_headers=1,
                 *args, **kwargs):
        

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs) 
        # Map params here
        # Example:
        # self.conn_id = conn_id
        
        self.aws_credentials_id = aws_credentials_id
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.copy_json_option = copy_json_option
        self.ignore_headers = ignore_headers
            
        
    def execute(self, context):
        #self.log.info('StageToRedshiftOperator not implemented yet')
        
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info("Clearing data from destination Redshift table")
        redshift.run("DELETE FROM {}".format(self.table))

        self.log.info("Copying data from S3 to Redshift")
        
        #use context variable from s3 key parameter received in the init function to render s3 key
        rendered_key = self.s3_key.format(**context) #take the key and pass in the context to render it
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
        
        #pass a couple of parameters from arguments into copy_sql
        formatted_sql = StageToRedshiftOperator.copy_sql.format( 
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.ignore_headers,
            self.copy_json_option
        )
        redshift.run(formatted_sql)
