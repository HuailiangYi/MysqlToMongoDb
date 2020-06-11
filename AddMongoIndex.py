from dbClient.mongoClient import MongoClient

mongoConn = {
    'host': '10.0.1.186',
    'port':8635 ,
    'db': 'gpshisdata',
    'authDb': 'gpshisdata',
    'userName': 'gphis',
    'password': 'Openstack#2020'
}



# 每次处理的数据量
everyCount = 10000


if __name__ == '__main__':
    mongoDb = MongoClient(mongoConn)

    for name in  mongoDb.getDocument("tb_vehicle_status_his"):
        print("Create index: {}".format(name))
        mongoDb.addIndex(name)



