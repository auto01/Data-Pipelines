from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 sql_template="",
                 truncate_table="",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table=table,
        self.sql_template=sql_template,
        self.truncate_table=truncate_table

    def execute(self, context):
        self.log.info('LoadDimensionOperator not implemented yet')
        redshift_hook=PostgresHook(postgres_conn_id=redshift_conn_id)
        if self.truncate_table:
            self.log.info("Deleting rows in the {} table".format(self.table))
            query=f"delete from {self.table}"
            redshift_hook.run(query)
            
        formatted_sql=f"""
        insert into {self.table} {sql_template}
        """
        redshift_hook.run(formatted_sql)
        self.log.info('LoadDimensionOperator is loaded...')        
