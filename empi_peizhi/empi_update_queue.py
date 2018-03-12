#-*- coding: utf-8 -*-
import time
import sys
import logging
import stomp
import pymysql
import uuid

logging.basicConfig( level=logging.DEBUG)

s = set()


def makeid():  # 使用uuid来初始化empi_id
    while True:
        a = str(uuid.uuid1())
        b = a[:12]
        if b not in s:
            onlyid = b
            s.add(b)
            break

    return onlyid

def empiUpdate():
    conn = pymysql.connect(host='123.232.38.99', user='root', passwd='Jth2016', db='zmap_empi', charset='utf8')
    cur = conn.cursor()

    sql_update = "select patient_name,SUBSTR(birthday FROM 1 FOR 10) from zmap_r_patient_empi_ys where empi_id is null and CHAR_LENGTH(patient_name) < 5"  # 加上限制姓名长度这个条件防止姓名过长造成模型计算错误
    cur.execute(sql_update)
    res_new = cur.fetchall()  # 存放更新的数据（empi_id为空）

    for i in range(len(res_new)):
        sql_find = "select patient_name,SUBSTR(birthday FROM 1 FOR 10),empi_id from zmap_r_patient_empi_ys where patient_name =" + "'" + \
                   res_new[i][0] + "'" + "and birthday like" + "'" + str(res_new[i][1]) + "%'"
        cur.execute(sql_find)
        res_mid = cur.fetchall()

        # empi_id用已有的，还是用新的？
        res_mid_list = []
        for j in range(len(res_mid)):
            res_mid_list.append(res_mid[j][2])
        res_mid_list.sort()

        if len(res_mid_list) == 0:  # 这是一条新的记录，赋值新的empi_id
            name = res_new[i][0]
            birthday = res_new[i][1]
            empi_id = None
        elif res_mid_list[len(res_mid_list) - 1] != None:  # 说明表中有查询的这个人的记录，用旧的empi_id
            name = res_new[i][0]
            birthday = res_new[i][1]
            empi_id = res_mid_list[len(res_mid_list) - 1]
        else:
            name = res_new[i][0]
            birthday = res_new[i][1]
            empi_id = None

        if empi_id == None:
            # 新患者，表中没有之前的记录。
            sql_update_no = "UPDATE zmap_r_patient_empi_ys set empi_id=" + "'" + str(
                uuid.uuid1()) + "'" + "where patient_name =" + "'" + name + "'" + "and birthday like" + "'" + str(
                birthday) + "%'"
            cur.execute(sql_update_no)
            conn.commit()
        else:
            # 旧患者，表中有之前的记录。
            sql_update_has = "UPDATE zmap_r_patient_empi_ys set empi_id=" + "'" + str(
                empi_id) + "'" + "where patient_name =" + "'" + name + "'" + "and birthday like" + "'" + str(
                birthday) + "%'"
            cur.execute(sql_update_has)
            conn.commit()


class MyListener(object):
    def on_error(self, headers, message):
        print('received an error %s' % message)

    def on_message(self, headers, message):
        # print headers
        print('received a message %s' % message)
        if str(message) == 'empi_demo_finsh':
            empiUpdate()


conn = stomp.Connection([('123.232.38.100', 61613)])

conn.start()
conn.connect()

# 发送消息到主题
# conn.send(body='this is message', destination = '/topic/testTopic')

# 从队列接受消息
res = conn.subscribe(destination = 'empi.data.queue', id=1, ack='auto')
# conn.send(body='hello activemq!', destination = '/empi/data/queue')
print '***********************'
# 从主题接受消息
# conn.subscribe(destination='/topic/testTopic', id=1, ack='auto')
conn.set_listener('', MyListener())

# while 1:
#     time.sleep(2)

conn.disconnect()

