import shelve

class Record(object):
    def __init__(self, dbPath):
        self.__s = shelve.open(dbPath)

    def findKeyExist(self, key):
        for k in self.__s.keys():
            if k == key:
                return True
        return False

    def updateHandledNum(self, key, count):
        self.__s[key] = count

    def getHandledNum(self, key):
        return self.__s[key]

    def __del__(self):
        self.__s.close()