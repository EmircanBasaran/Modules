import psycopg2
from sshtunnel import SSHTunnelForwarder
import datetime
import pyodbc


class Module:
    @classmethod
    def connect(self):
        try:
            # DB CONNECTION
            dn = 'dbname'
            du = 'dbuser'
            dp = 'dbpwd'
            dh = 'localhost'
            dbp = '5432'
            connect_timeout = 900
            cs = "dbname=%s user=%s password=%s host=%s port=%s connect_timeout=%s" % (
            dn, du, dp, dh, dbp, connect_timeout)
            conn = psycopg2.connect(cs)
            cursor = conn.cursor()
            self.cursor = cursor
            self.conn = conn
            print('Connection is ON')
            Module.log(0, 0, "'Connection'", 1, 'Success')
            self.conn.commit()
            return cursor, conn
        except Exception as ERR:
            Module.log(0, 0, "'Connection'", 0, ERR)
            self.conn.commit()

    @classmethod
    def connectEnd(self, COMPANYID, MSYSTEMID):
        try:
            Module.log(COMPANYID, MSYSTEMID, "'connectEnd'", 1, 'Success')
            self.conn.commit()
            self.cursor.close()
            self.conn.close()
            try:
                self.cursorMS.close()
                self.connMS.close()
            except Exception as Err:
                pass
            try:
                self.cursorFire.close()
                self.connFire.close()
            except Exception as Err:
                pass
            print('Committed & Connection is OFF')

        except Exception as ERR:
            Module.log(COMPANYID, MSYSTEMID, "'connectEnd'", 0, ERR)

    @classmethod
    def log(self, COMPANYID, MSYSTEMID, FUNCTIONNAME, ISSUCCESS, ERR):
        if ISSUCCESS == 0:
            self.conn.rollback()
        ERR = str(ERR)
        ERR = ERR.replace("'", '')
        ERR = "'" + ERR + "'"
        JOBDATE = "'" + str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")) + "'"
        SQL = 'INSERT INTO RAW.LOGS(JOBDATE,COMPANYID,MSYSTEMID,FUNCTIONNAME,ISSUCCESS,ERR) VALUES ({},{},{},{},{},{})'.format(
            JOBDATE, COMPANYID, MSYSTEMID, FUNCTIONNAME, ISSUCCESS, ERR)
        self.cursor.execute(SQL)
        self.conn.commit()
        if ISSUCCESS == 0:
            self.cursor.close()
            self.conn.close()
            try:
                self.cursorMS.close()
                self.connMS.close()
            except Exception as Err:
                pass
            try:
                self.cursorFire.close()
                self.connFire.close()
            except Exception as Err:
                pass

    @classmethod
    def connectMS(self, COMPANYID, MSYSTEMID, IP, PORT, DBNAME, UID, PWD):
        #connstring = 'DRIVER={SQL Server};SERVER='+str(IP)+',3433;DATABASE=dbname;UID=dbuser;PWD=dbpwd'
        connstring = 'DRIVER={SQL Server};SERVER=' + str(IP) + ',' + str(PORT) + ';DATABASE=' + str(DBNAME) + ';UID=' + str(UID) + ';PWD=' + str(PWD)
        try:
            print(connstring)
            connMS = pyodbc.connect(connstring)
            cursorMS = connMS.cursor()
            self.connMS = connMS
            self.cursorMS = cursorMS
            print('SQL Connection Success')
            Module.log(COMPANYID, MSYSTEMID, "'MSSQLConnection'", 1, 'Success')
        except Exception as ERR:
            print(ERR)
            print('SQL Connection UNSUCCESSFUL')
            Module.log(COMPANYID, MSYSTEMID, "'MSSQLConnection'", 0, ERR)

        return connMS

    @classmethod
    def connectFire(self, COMPANYID, MSYSTEMID, IP):
        try:
            connstring = 'DRIVER={Firebird/InterBase(r) driver};DBNAME='+str(IP)+':C:\\{}.DB;UID=dbuser;PWD=dbpwd'.format(str(MSYSTEMID))
            connFire = pyodbc.connect(connstring)
            cursorFire = connFire.cursor()
            self.connFire = connFire
            self.cursorFire = cursorFire
            print('Firebird Connection Success')
            Module.log(COMPANYID, MSYSTEMID, "'FirebirdConnection'", 1, 'Success')
        except Exception as ERR:
            print(ERR)
            print('Firebird Connection UNSUCCESSFUL')
            Module.log(COMPANYID, MSYSTEMID, "'FirebirdConnection'", 0, ERR)

        return connFire


if __name__ == '__main__':
    pass
