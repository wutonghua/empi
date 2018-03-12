#!/usr/bin/env python
# -*-coding:utf-8 -*-

import sys
import contextlib
import pymysql as pm
import time


reload(sys)
sys.setdefaultencoding('utf8')


def conn_db():
    try:
        conn = pm.connect(host='123.232.38.99', port=3306, user='zmap_dadev', passwd='Jth2016', db='zmap_product_dy', charset='utf8')
    except pm.OperationalError:
        print '数据库连接异常，尝试重连...'
        time.sleep(20)
        conn = pm.connect(host='123.232.38.99', port=3306, user='zmap_dadev', passwd='Jth2016', db='zmap_product_dy', charset='utf8')

    return conn


@contextlib.contextmanager
def execute_sql():
    conn = conn_db()
    cur = conn.cursor()
    try:
        yield cur
        conn.commit()
    except pm.OperationalError, e:
        print 'in mysql_conf/ execute sql'
        print e.args



