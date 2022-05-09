from pyspark.sql import SparkSession
import psycopg2
from influxdb_client import InfluxDBClient
import datetime

class Module:

    spark = SparkSession \
        .builder \
        .master("local[*]") \
        .appName("module") \
        .getOrCreate()


    @classmethod
    def msSqlRead(self, dbtable, teleskopip, teleskopport, teleskopdbname, teleskopuser, teleskoppwd):
        #sqlServerUrl = "jdbc:sqlserver://ip:port/svname;database=dbname"
        sqlServerUrl = "jdbc:sqlserver://" + teleskopip + ":" + teleskopport + ";database=" + teleskopdbname
        data = self.spark.read.format('jdbc') \
                   .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
                   .option("url", sqlServerUrl) \
                   .option("dbtable", dbtable) \
                   .option("user", teleskopuser) \
                   .option("password", teleskoppwd) \
                   .load()

        return data

    @classmethod
    def msSqlReadQuery(self, query, teleskopip, teleskopport, teleskopdbname, teleskopuser, teleskoppwd):
        #sqlServerUrl = "jdbc:sqlserver://ip:port/svname;database=dbname"
        sqlServerUrl = "jdbc:sqlserver://" + teleskopip + ":" + teleskopport + ";database=" + teleskopdbname
        data = self.spark.read.format('jdbc') \
            .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
            .option("url", sqlServerUrl) \
            .option("query", query) \
            .option("user", teleskopuser) \
            .option("password", teleskoppwd) \
            .load()

        return data

    @classmethod
    def msSqlDmRead(self, dbtable, dmip, dmport, dmdbname, dmuser, dmpwd):
        #sqlServerUrl = "jdbc:sqlserver://ip:port;database=dbname"
        sqlServerUrl = "jdbc:sqlserver://" + dmip + ":" + dmport + ";database=" + dmdbname
        data = self.spark.read.format('jdbc') \
            .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
            .option("url", sqlServerUrl) \
            .option("dbtable", dbtable) \
            .option("user", dmuser) \
            .option("password", dmpwd) \
            .load()

        return data

    @classmethod
    def msSqlDmReadQuery(self, query, dmip, dmport, dmdbname, dmuser, dmpwd):
        #sqlServerUrl = "jdbc:sqlserver://ip:port;database=dbname"
        sqlServerUrl = "jdbc:sqlserver://" + dmip + ":" + dmport + ";database=" + dmdbname
        data = self.spark.read.format('jdbc') \
            .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
            .option("url", sqlServerUrl) \
            .option("query", query) \
            .option("user", dmuser) \
            .option("password", dmpwd) \
            .load()

        return data

    @classmethod
    def postgreAppend(self, data, dbtable):
        #postgreUrl = 'jdbc:postgresql://ip:port/dbname'
        postgreUrl = 'jdbc:postgresql://localhost:port/dbname'
        data.write.format('jdbc') \
            .mode('append') \
            .option('driver', 'org.postgresql.Driver') \
            .option("url", postgreUrl) \
            .option('dbtable', 'dbschema.' + dbtable) \
            .option('user', 'dbuser') \
            .option('password', 'dbpwd') \
            .option('mapreduce.fileoutputcommitter.algorithm.version', '2') \
            .save()
        return data


    @classmethod
    def postgrePsycopg2(self):
        # DB CONNECTION
        dn = 'dbname'
        du = 'dbuser'
        dp = 'dbpwd'
        dh = 'localhost'
        dbp = 'port'
        cs = "dbname=%s user=%s password=%s host=%s port=%s" % (dn, du, dp, dh, dbp)
        self.conn = psycopg2.connect(cs)
        conn = self.conn
        self.cursor = conn.cursor()
        cursor = self.cursor
        return cursor, conn


    @classmethod
    def postgreWrite(self, data, dbtable):
        #postgreUrl = 'jdbc:postgresql://ip:port/dbname'
        postgreUrl = 'jdbc:postgresql://localhost:port/dbname'
        data.write.format('jdbc') \
        .mode('overwrite') \
        .option('driver', 'org.postgresql.Driver') \
        .option("url", postgreUrl) \
        .option('dbtable', 'dwh.'+dbtable) \
        .option('user', 'dbuser') \
        .option('password', 'dbpwd') \
        .save()

    @classmethod
    def postgreRead(self, dbtable):
        #postgreUrl = 'jdbc:postgresql://ip:port/dbname'
        postgreUrl = 'jdbc:postgresql://localhost:port/dbname'
        data = self.spark.read.format('jdbc') \
            .option('driver', 'org.postgresql.Driver') \
            .option("url", postgreUrl) \
            .option('dbtable', 'dwh.' + dbtable) \
            .option('user', 'dbuser') \
            .option('password', 'dbpwd') \
            .load()
        return data

    @classmethod
    def influxRead(self, influxIP, influxPort, influxToken, influxOrg):
        client = InfluxDBClient(url='{}:{}'.format(influxIP, influxPort),
                                token='{}'.format(influxToken),
                                org='{}'.format(influxOrg),
                                timeout=60000)
        #print("InfluxDBClient(url='{}:{}',token='{}',org='{}',timeout=60000)".format(influxIP, influxPort,influxToken,influxOrg))
        return client

    @classmethod
    def log(self, FACTORYID, FUNCTIONNAME, ISSUCCESS, ERR, NEWCOUNT):
        if ISSUCCESS==0:
            self.conn.rollback()
        ERR = str(ERR)
        ERR = ERR.replace("'", '')
        ERR = "'" + ERR + "'"
        JOBDATE = "'" + str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")) + "'"
        SQL = 'INSERT INTO dwh.LOGS(JOBDATE,factoryid,FUNCTIONNAME,ISSUCCESS,ERR,NEW) VALUES ({},{},{},{},{},{})'.format(JOBDATE, FACTORYID, FUNCTIONNAME, ISSUCCESS, ERR, NEWCOUNT)
        self.cursor.execute(SQL)
        self.conn.commit()