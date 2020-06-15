from __future__ import print_function
from dbClient.mysqlClient import MysqlClient
from dbClient.mongoClient import MongoClient
from record.record import Record


# mongodb 连接信息
# mongoConnPro = {
#     'host': '121.37.247.7',
#     'port': 27017,
#     'db': 'gpshisdata',
#     'authDb': 'gpshisdata',
#     'userName': 'gpshisdata2',
#     'password': 'gpshisdata2'
# }

# local
# mongoConnPro = {
#     'host': '192.168.10.41',
#     'port': 27017,
#     'db': 'gpshisdata',
#     'authDb': 'admin',
#     'userName': 'admin',
#     'password': 'openstack'
# }

mongoConn = {
    'host': '139.159.197.228',
    'port': 27017,
    'db': 'media_collection',
    'authDb': 'media_collection',
    'userName': 'cesiumei',
    'password': '123456'
}

# production
mongoConnPro = {
    'host': '10.0.1.186',
    'port': 8635,
    'db': 'media_collection',
    'authDb': 'media_collection',
    'userName': 'mecol',
    'password': 'Openstack#2020'
}

# 每次处理的数据量
everyCount = 10000

# 存储处理数据历史记录的文件地址
recordPath = 'migrate.db'

tableName = 'media_collection'

if __name__ == '__main__':
    mongDbPro = MongoClient(mongoConnPro)
    mongoDb = MongoClient(mongoConn)
    record = Record(recordPath)

    print("开始迁移表：%s" % tableName)
    total = mongoDb.getCount(tableName)
    print("%s数据总量：%d" % (tableName, total))

    # 计数
    startIndex = 0
    sum = 0

    # 后期记录表中的记录
    if record.findKeyExist('migrate'):
        # 存在该记录, 取出处理的数据量
        handleNum = record.getHandledNum('migrate')
        if handleNum != total:
            startIndex = handleNum
            sum = handleNum

    print("%s startIndex= %d, sum = %d" % (tableName, startIndex, sum))
    # 读取数据
    while startIndex < total:
        limit = everyCount
        if startIndex + limit > total:
            limit = total - startIndex
        # 获取数据
        count, data = mongoDb.getDataFromDocument(tableName, startIndex, limit)
        #print("dataLen=%d" % count)

        # 插入数据到mongodb
        mongDbPro.insertMany(tableName, data)
        sum += count
        print("表%s插入数据量sum=%d" % (tableName, sum))
        # 更新处理数据入库
        record.updateHandledNum('migrate', sum)
        startIndex += limit

    if sum != total:
        print("获取数据总量不正确")
    else:
        print("%s插入全部数据" % tableName)

