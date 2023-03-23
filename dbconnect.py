import pymysql

class Connection :
    def __init__(self, host, user, password, db, charset='utf8', port=3306, dict=False):
        self.host = host
        self.user = user
        self.password = password
        self.db = db
        self.charset = charset
        self.port = port
        self.dict = dict

    def connect(self):
        self._conn = pymysql.connect(host=self.host, user=self.user, password=self.password, db=self.db, charset=self.charset, port=self.port)

        if self.dict: self._cursor = self._conn.cursor(pymysql.cursors.DictCursor)
        else:         self._cursor = self._conn.cursor()

    def __enter__(self):
        return self
 
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
 
    @property
    def connection(self):
        return self._conn
 
    @property
    def cursor(self):
        return self._cursor

 
    def commit(self):
        self.connection.commit()
 
    def close(self, commit=True):
        if commit:
            self.commit()
        self.connection.close()
 
    def execute(self, sql, params=None):
        self.cursor.execute(sql, params or ())
 
    def fetchall(self):
        return self.cursor.fetchall()
 
    def fetchone(self):
        return self.cursor.fetchone()
 
    def query(self, sql, params=None):
        self.cursor.execute(sql, params or ())
        return self.fetchall()
 
    def rows(self):
        return self.cursor.rowcount

