from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 redshift_conn_id="",
                 table="",
                 sql_template="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table=table
        self.sql_template=sql_template
        
        
    def execute(self, context):
        self.log.info('LoadFactOperator not implemented yet')
        redshift_hook=PostgresHook(postgres_conn_id=redshift_conn_id)
        formatted_sql=f"""
        insert into {self.table} {sql_template}
        """
        redshift_hook.run(formatted_sql)
        self.log.info('LoadFactOperator is loaded...')
        
