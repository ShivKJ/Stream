from psycopg2 import connect
from psycopg2.extras import DictConnection


class DB:
    def __init__(self, *, db, username, password, host='localhost', port=5432):
        self.dbname = db
        self.username = username
        self.password = password
        self.host = host,
        self.port = port

    @property
    def dict_conn(self):
        return connect(**self.__dict__, connection_factory=DictConnection)

    @property
    def conn(self):
        return connect(**self.__dict__)
