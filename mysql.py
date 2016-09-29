#!/usr/bin/env python

import os
import sys

import MySQLdb
import _mysql_exceptions as DB_EXC
import Queue

class MysqlClient(object):
    def __init__(self, host, user, password, name, port = 3306, charset = 'utf8'):
        self.host = host
        self.user = user
        self.password = password
        self.name = name
        self.port = port
        self.charset = charset
        self.cxn = None
        self.cur = None

    def connect(self):
        try:
            self.cxn = MySQLdb.connect(self.host, self.user, self.password, self.name, port = self.port, charset = self.charset)
            if not self.cxn:
                print('MYSQL server connection faild...')
                return False
            self.cur = self.cxn.cursor()
            return True
        except Exception as e:
            print('MYSQL server connection error:', e)
            return False

    def getCursor(self):
        return self.cur

    def commit(self):
        return self.cxn.commit()

    def rollback(self):
        return self.cxn.rollback()

    def close(self):
        self.cur.close()
        self.cxn.close()

    def query(self, sql, args=None, many=False):
        affected_rows = 0
        if not many:
            if args == None:
                affected_rows = self.cur.execute(sql)
            else:
                affected_rows = self.cur.execute(sql, args)
        else:
            if args==None:
                affected_rows = self.cur.executemany(sql)
            else:
                affected_rows = self.cur.executemany(sql, args)
        return affected_rows

    def fetchAll(self):
        return self.cur.fetchall()

class MysqlPool(object):
    def __init__(self, num):
        self.num = num
        self.queue = Queue.Queue(self.num)

        for i in range(num):
            self.createConnection()

    def get(self):
        if not self.queue.qsize():
            self.createConnection()
        return self.queue.get(1)

    def free(self, conn):
        self.queue.put(conn, 1)

    def createConnection(self):
        conn = MysqlClient(MYSQL_HOST, MYSQL_USER, MYSQL_PASSWD, MYSQL_DB)
        if not conn.connect():
            print('connect to mysql error...')
            return None
        self.queue.put(conn, 1)

    def clear(self):
        while self.queue.size():
            conn = self.queue.get(1)
            conn.close()
        return None

