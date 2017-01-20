from pymongo import (
    MongoClient,
    errors
)


class MongoDB(object):
    """
    Handle mongoDB connection
    """
    mongo_client = None
    mongodb = None

    def __init__(self, uri, database):
        """
        Establish connection to mongoDB database

        :param uri: mongoDB URI
        :param database: database name
        """
        try:
            self.mongo_client = MongoClient(uri)
            self.mongodb = self.mongo_client[database]
            # print("Connected successfully to: {}".format(database))
        except errors.ConnectionFailure as e:
            print("Could not connect to database: {}".format(e))

def get_new():
    return MongoDB('localhost', 'urls')
