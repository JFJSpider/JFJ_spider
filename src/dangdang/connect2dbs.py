import psycopg2
from psycopg2 import sql

class DatabaseManager:
    # 默认数据库连接配置
    DB_CONFIG = {
        'host': 'localhost',           # 数据库主机
        'port': 5432,                  # PostgreSQL 默认端口
        'user': 'postgres',       # 数据库用户名
        'password': '123456',   # 数据库密码
        'dbname': 'reslib' # 数据库名称
    }

    def __init__(self, host=None, port=None, user=None, password=None, dbname=None):
        """
        初始化数据库连接配置。
        可以在创建类时修改连接配置，否则使用默认配置。
        """
        self.host = host or self.DB_CONFIG['host']
        self.port = port or self.DB_CONFIG['port']
        self.user = user or self.DB_CONFIG['user']
        self.password = password or self.DB_CONFIG['password']
        self.dbname = dbname or self.DB_CONFIG['dbname']
        self.connection = None
        self.cursor = None

    def connect(self):
        """连接到数据库"""
        try:
            self.connection = psycopg2.connect(
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.password,
                dbname=self.dbname
            )
            self.cursor = self.connection.cursor()
            # print("连接成功！")

        except Exception as e:
            # print(f"连接失败: {e}")
            raise

    def close(self):
        """关闭数据库连接"""
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
        # print("连接已关闭。")

    def execute_query(self, query, params=None):
        """执行查询（SELECT）"""
        try:
            self.cursor.execute(query, params)
            result = self.cursor.fetchall()
            return result
        except Exception as e:
            # print(f"查询执行失败: {e}")
            raise

    def execute_update(self, query, params=None):
        """执行更新（INSERT, UPDATE, DELETE）"""
        try:
            self.cursor.execute(query, params)
            self.connection.commit()  # 提交事务
            # print("更新成功！")
        except Exception as e:
            # print(f"更新执行失败: {e}")
            self.connection.rollback()  # 回滚事务
            raise

    def insert(self, table, columns, values):
        """插入数据"""
        column_sentence = '('
        for column in columns:
            column_sentence += column + ','
        column_sentence = column_sentence.rstrip(",") + ')'
        values_sentence = '('
        for value in values:
            values_sentence +='%s,'
        values_sentence = values_sentence.rstrip(",") + ')'
        INSERT_query = f"INSERT INTO {table} {column_sentence} VALUES {values_sentence}"
        self.execute_update(INSERT_query, values)
        

    def update(self, table, set_columns, set_values, where_condition, where_values):
        """更新数据"""
        column_sentence = '('
        for column in set_columns:
            column_sentence += column + ','
        column_sentence = column_sentence.rstrip(",") + ')'
        values_sentence = '('
        for value in set_values:
            values_sentence += '%s,'
        values_sentence = values_sentence.rstrip(",") + ')'
        UPDATE_query = f"UPDATE {table} SET {column_sentence} = {values_sentence} WHERE {where_condition}='{where_values}'"
        self.execute_update(UPDATE_query, set_values)


    def delete(self, table, where_condition, where_values):
        """删除数据"""
        query = sql.SQL("DELETE FROM {table} WHERE {condition}").format(
            table=sql.Identifier(table),
            condition=sql.Identifier(where_condition)
        )
        self.execute_update(query, where_values)

