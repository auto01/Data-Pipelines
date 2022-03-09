from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 table_list=[],
                 redshift_conn_id="",
                 sql_template="",
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.table_list=table_list
        self.redshift_conn_id=redshift_conn_id
        self.sql_template=sql_template
        

    def execute(self, context):
        self.log.info('DataQualityOperator not implemented yet')
        redshift_hook=PostgresHook(postgres_conn_id=redshift_conn_id)
        for q in self.table_list:
            sql_formatted=self.sql_template.format(q)
            res=redshift_hook.get_first(sql_formatted)[0]
            if res == 0:
                raise ValueError(f"failed query:{sql_formatted},found zero rows..!!")
            
        