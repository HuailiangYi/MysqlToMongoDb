from __future__ import print_function
import pymongo
import datetime

class MongoClient(object):
    def __init__(self, conn):
        self.__client = pymongo.MongoClient(conn['host'], conn['port'])
        db = self.__client[conn['authDb']]
        db.authenticate(conn['userName'], conn['password'])
        self.myDb = self.__client[conn['db']]

    def updateTime(self, collection):
        myCollection = self.myDb[collection]
        for item in myCollection.find():
            try:
                update_time = item['gps_time'] + datetime.timedelta(hours=-8)
                myCollection.update_one({"_id": item["_id"]}, {"$set": {"gps_time":update_time }})
            except:
                continue

    def updateTimeByCondition(self, collection, condition, gps_time):
        myCollection = self.myDb[collection]
        try:
            update_time = gps_time + datetime.timedelta(hours=-8)
            print(update_time)
            myCollection.update_one(condition, {"$set": {"gps_time":update_time }})
        except:
            pass



    def showIndex(self, collection):
        myCollection = self.myDb[collection]
        print(myCollection.index_information())

    def addIndex(self, collection):
        myCollection = self.myDb[collection]
        myCollection.create_index('vehicleid', background=True)
        myCollection.create_index([('vehicleid', 1), ('gps_time', 1)], background=True)


    def getDocument(self, name_prefix):
        collection_names = self.myDb.list_collection_names(session=None)
        return  [ name for name in collection_names if name.startswith(name_prefix)]


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


