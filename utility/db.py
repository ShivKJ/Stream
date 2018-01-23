from psycopg2 import connect
from psycopg2.extensions import connection
from psycopg2.extras import DictConnection


class DB:
    def __init__(self, *, dbname, user, password, host='localhost', port=5432):
        self.dbname = dbname
        self.user = user
        self.password = password
        self.host = host
        self.port = port

    @property
    def dict_conn(self) -> connection:
        return connect(**self.__dict__, connection_factory=DictConnection)

    @property
    def conn(self) -> connection:
        return connect(**self.__dict__)
