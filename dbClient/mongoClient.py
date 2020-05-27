import pymongo

class MongoClient(object):
    def __init__(self, conn):
        self.__client = pymongo.MongoClient(conn['host'], conn['port'])
        db = self.__client[conn['authDb']]
        db.authenticate(conn['userName'], conn['password'])
        self.myDb = self.__client[conn['db']]

    def connect(self, dbName, dbCol):
        self.myDb = self.__client[dbName]
        self.myCol = self.myDb[dbCol]

    def insertMany(self, collection, data):
        myCollection = self.myDb[collection]
        myCollection.insert_many(data, ordered=False)

    # 获取所有数据的时间间隔
    def findMinMaxTime(self):
        return self.myCol.aggregate([
            {"$group":
                {
                    "_id": None,
                    "maxValue": {
                        "$max": "$f_time"
                    },
                    "minValue": {
                      "$min": "$f_time"
                    }
                }
            }
        ])


    # 获取指定时间端内的数据
    def findListByDuration(self, minTime, maxTime):
        print("min:%d, max:%d" % (minTime, maxTime))
        # return self.myCol.find(({
        #     "f_time": {
        #         "$gte": minTime,
        #         "$lte": maxTime
        #     }
        # }))
        print(self.myCol)
        return self.myCol.aggregate([
            {"$match":
                {
                    'f_time': {
                        "$gte": minTime,
                        "$lte": maxTime
                    }
                }
            }
        ])

    # 删除列表中的所有数据
    def removeList(self, dataList):
        result = self.myCol.delete_many({"_id": {"$in": dataList}})
        return result


