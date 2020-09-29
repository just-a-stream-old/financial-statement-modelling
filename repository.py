from pymongo import MongoClient


class MongoRepository:

    # Todo: Add return type hints to methods - forgot!

    def __init__(self, database: str, host: str = "localhost", port: str = 27017, username: str = None,
                 password: str = None):
        self.database = database
        self.host = host
        self.port = port
        self.username = username
        self.password = password

    def query(self, collection: str, query: dict):
        return self._return_collection(collection).find(query)

    def find_one(self, collection: str, query=None):
        return self._return_collection(collection).find_one({} if query is None else query)

    def find_all(self, collection: str):
        return self._return_collection(collection).find()

    def _return_collection(self, collection: str):
        return self._connect_to_mongodb()[collection]

    def _connect_to_mongodb(self):
        if self.username and self.password:
            uri = "mongodb://%s:%s@%s:%s/%s" % (self.username, self.password, self.host, self.port, self.database)
            connection = MongoClient(uri)
        else:
            connection = MongoClient(self.host, self.port)

        return connection[self.database]