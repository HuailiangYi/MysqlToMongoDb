from dbClient.mysqlClient import MysqlClient
from dbClient.mongoClient import MongoClient
from record.record import Record

# mysql 连接信息
mysqlConn = {
    'host': '139.159.198.104',
    'port': 3306,
    'db': 'gps_static_init',
    'userName': 'root',
    'password': 'Cesiumai-2018@#'
}

mongoConn = {
    'host': '192.168.10.41',
    'port': 27017,
    'db': 'gpshisdata',
    'authDb': 'admin',
    'userName': 'admin',
    'password': 'openstack'
}

# 每次处理的数据量
everyCount = 10000


if __name__ == '__main__':
    sqlDb = MysqlClient(mysqlConn)
    mongoDb = MongoClient(mongoConn)

    # 获取tableName
    tableNameList = sqlDb.getAllTableName('tb_vehicle_status')
    for tableName in tableNameList:
        print("开始迁移表：%s" % tableName)
        total = sqlDb.getAllRowCount(tableName)
        print("%s数据总量：%d" % (tableName, total))

        # 计数
        startIndex = 0
        sum = 0

        while startIndex < total:
            limit = everyCount
            if startIndex + limit > total:
                limit = total - startIndex
            # 获取数据
            count, data = sqlDb.getDataFromTable(tableName, startIndex, limit)

            # 插入数据到mongodb
            mongoDb.insertMany(tableName, data)
            sum += count
            print("表%s插入数据量sum=%d" % (tableName, sum))
            startIndex += limit

        if sum != total:
            print("获取数据总量不正确")
        else:
            print("%s插入全部数据" % tableName)

