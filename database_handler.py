import pymysql
import logging



class mysql_client(object):

    def __init__(self, if_reconnect=True, *args, **kwargs):
        # check if user want to reconnect db after each query
        self.if_reconnect = if_reconnect
        self.args, self.kwarg = args, kwargs
        # if do not want to reconnect, establish connection from start
        if not if_reconnect:
            self.establish_connection()



    def establish_connection(self):
        try:
            if not self.args and not self.kwarg:
                self.db=pymysql.connect(read_default_file='mysql_auth.cnf', read_default_group='quant_localhost')
            else:
                self.db = pymysql.connect(*self.args, **self.kwarg)
            self.cursor = self.db.cursor()
        except:
            raise


    def select_qeury(self, query):
        try:
            if self.if_reconnect:
                self.establish_connection()
            self.cursor.execute(query)
            if self.if_reconnect:
                self.db.close()
            return self.cursor.fetchall()
        except:
            logging.critical('execution error: \n{0}'.format(query))
            raise


    def commit_query(self, query):
        try:
            if self.if_reconnect:
                self.establish_connection()
            result = self.cursor.execute(query)
            self.db.commit()
            if self.if_reconnect:
                self.db.close()
            return result
        except:
            logging.critical('execution error: \n{0}'.format(query))
            raise


    def get_table_info(self, schema, table):
        return self.select_qeury('describe `{0}`.`{1}`;'.format(schema, table))


    def show_schemas(self):
        return self.select_qeury('show schemas;')


    def show_tables(self, schema):
        self.commit_query('use {0};'.format(schema))
        return self.select_qeury('show tables;')


    def list_to_update_query(self, schema, table, values, type='insert'):
        if type not in ["insert", "replace"]:
            logging.critical('updating query type should be either insert or update!')
            raise TypeError
        try:
            update_query = "{type} into `{schema}`.`{table}` VALUES (".format(type=type, schema=schema, table=table)
            for i in values:
                if isinstance(i, str):
                    update_query += "'{0}',".format(pymysql.escape_string(i))
                elif isinstance(i, float):
                    update_query += "{0},".format(i)
                elif isinstance(i, int):
                    update_query += "{0},".format(i)
                elif not i:
                    update_query += "NULL, "
                else:
                    update_query += "'{0}',".format(i)
            update_query = update_query[:-1] + ');'
            return update_query
        except:
            raise
