import pymysql
import pandas as pd
import json
import datetime
class MysqlClient(object):
    def __init__(self, conn):
        self._dbName = conn['db']
        self.__db = pymysql.connect(host=conn['host'],
                             port=conn['port'],
                             db=conn['db'],
                             user=conn['userName'],
                             password=conn['password'],
                             charset='utf8',
                             use_unicode=True
                             )
        self.cursor = self.__db.cursor(cursor=pymysql.cursors.DictCursor)

    # 获取所有的table 表格
    def getAllTableName(self, name):
        sql = "show tables"
        result = self.execute(sql)
        #print(result)
        table_key = 'Tables_in_' + self._dbName
        tableList = [item[table_key] for item in result if item[table_key].startswith(name)]
        # for table in tableList:
        #     print(table)
        return tableList

    # 获取table中条目数
    def getAllRowCount(self, tableName):
        sql = "select count(*) from %s" % tableName
        result = self.execute(sql)
        return result[0]["count(*)"]

    def getDataFromTable(self, tableName, offset, limit):
        sql = "select * from %s limit %d offset %d" % (tableName, limit, offset)
        df = pd.read_sql(sql, self.__db)
        # 类型转换
        bitList = []
        for k, v in df.iloc[0].items():
            if isinstance(v, bytes):
                bitList.append(k)
        # print(bitList)
        for col in bitList:
            df[col] = df[col].apply(lambda x: ord(x))  # 转换为int
        # print(df.dtypes)
        df.drop('id', axis=1, inplace=True)
        count = len(df)
        # print(df.iloc[0])

        record = json.loads(df.T.to_json()).values()

        for item in record:
            item['gps_time'] = datetime.datetime.utcfromtimestamp(item['gps_time']/1000)  if item['gps_time'] is not None else 0

        return count, record


    def execute(self,sql):
        try:
            # 执行sql 语句
            self.cursor.execute(sql)
            # 获取所有记录列表
            results = self.cursor.fetchall()
            return results
        except:
            print("Error: unable to fetch data")

    def __del__(self):
        if self.__db:
            self.__db.close()



