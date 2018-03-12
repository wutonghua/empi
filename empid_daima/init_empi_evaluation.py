#!/usr/bin/env python
# -*-coding:utf-8 -*-
import time
import pymysql

from mysql import execute_sql
from basic_f import data_set, update_empi_id, counter, find_none


def empi_init(table):
    with execute_sql() as cursor:
        try:
            cursor.execute("alter table %s drop PRIMARY KEY" % table)
        except pymysql.err.InternalError:
            pass
        cursor.execute("delete from zmap_r_patient_backup")
        cursor.execute("insert into zmap_r_patient_backup select * from %s" % table)
        cursor.execute("update %s set confidence=''" % table)
        cursor.execute("update %s set confirm_status=''" % table)
        cursor.execute("update %s set confirm_person=''" % table)
        cursor.execute("alter table %s drop column empi_id" % table)
        cursor.execute("alter table %s add empi_id bigint PRIMARY KEY  AUTO_INCREMENT" % table)
        cursor.execute("Alter table %s change empi_id empi_id  VARCHAR(200)" % table)
        cursor.execute("alter table %s drop PRIMARY KEY" % table)
        cursor.execute("update %s set empi_id=LPAD(empi_id,9,'0')" % table)
        cursor.execute("alter table %s add PRIMARY KEY (patient_id)" % table)
        try:
            cursor.execute("CREATE INDEX index_id_card ON %s (id_card)" % table)
        except pymysql.err.InternalError:
            pass
        try:
            cursor.execute("CREATE INDEX index_birthday ON %s (patient_name,birthday)" % table)
        except pymysql.err.InternalError:
            pass
        try:
            update_empi_id("CREATE INDEX index_empi_id ON %s (empi_id)" % table)
        except pymysql.err.InternalError:
            pass


def id_card(table, sql_execute, id_card_counter):
    label = 0
    for fields in data_set(sql_execute):
        time.sleep(0.01)
        label += 1
        print 'ID_CARD总量：', id_card_counter, '当前：', label
        empi_id = data_set("select empi_id from %s where id_card='%s' limit 1 " % (table, fields[0])).next()[0]
        update_empi_id("update %s set empi_id='%s',confidence='1' where id_card='%s'" % (table, str(empi_id), fields[0]))


def insure_id(table, sql_execute, insure_counter):
    label = 0
    for fields in data_set(sql_execute):
        time.sleep(0.01)
        label += 1
        print 'INSURE_ID总量：', insure_counter, '当前：', label
        try:
            empi_id = data_set("select empi_id from %s where patient_name='%s' and insure_id='%s' and "
                               "case when id_card REGEXP '[0-9]{15,}' then id_card else '' end !='' order by "
                               "case when id_card REGEXP '[0-9]{15,}' then id_card else '' end desc limit 1"
                               % (table, fields[0], fields[1])).next()[0]
        except StopIteration:
            empi_id = data_set("select empi_id from %s where patient_name='%s' "
                               "and insure_id='%s' order by case when id_card REGEXP '[0-9]{15,}' "
                               "then id_card else '' end desc limit 1" % (table, fields[0], fields[1])).next()[0]
            update_empi_id("update %s set empi_id='%s',confidence='0.6' where patient_name='%s' and insure_id='%s' "
                           "and  (id_card REGEXP '[0-9]{15,}'='' or id_card REGEXP '[0-9]{15,}' is null)"
                           % (table, str(empi_id), fields[0], fields[1]))
        else:
            update_empi_id("update %s set empi_id='%s', confidence='0.9' where patient_name='%s' and insure_id='%s' "
                           "and  (id_card REGEXP '[0-9]{15,}'='' or id_card REGEXP '[0-9]{15,}' is null)"
                           % (table, str(empi_id), fields[0], fields[1]))


def birthday(table, sql_execute, birthday_counter):
    label = 0
    for fields in data_set(sql_execute):
        time.sleep(0.01)
        label += 1
        print 'BRITHDAY总量：', birthday_counter, '当前：', label
        try:
            empi_id = data_set("select empi_id from %s where patient_name='%s' and left(birthday,10)='%s' "
                               "and case when id_card REGEXP '[0-9]{15,}' then id_card else '' end!='' "
                               "order by case when id_card REGEXP '[0-9]{15,}' then id_card else '' end desc limit 1"
                               % (table, fields[0], fields[1])).next()[0]
        except StopIteration:
            empi_id = data_set("select empi_id from %s where patient_name='%s' "
                               "and left(birthday,10)='%s' order by case when id_card REGEXP '[0-9]{15,}' "
                               "then id_card else '' end desc,case when insure_id REGEXP '[0-9]{7,}' "
                               "then insure_id else '' end desc limit 1" % (table, fields[0], fields[1])).next()[0]
            max_confidence = data_set("select max(confidence) from %s where patient_name='%s' and "
                                      "left(birthday,10)='%s' " % (table, fields[0], fields[1])).next()[0]
            if max_confidence == '0.9':
                update_empi_id("update %s set empi_id='%s',confidence='0.8' "
                               "where patient_name='%s' and left(birthday,10)='%s' "
                               "and  (id_card REGEXP '[0-9]{15,}'='' or id_card REGEXP '[0-9]{15,}' is null) and "
                               "(insure_id REGEXP '[0-9]{7,}'='' or insure_id REGEXP '[0-9]{,}' is null)"
                               % (table, str(empi_id), fields[0], fields[1]))
            else:
                update_empi_id("update %s set empi_id='%s',confidence='0.4' "
                               "where patient_name='%s' and left(birthday,10)='%s' "
                               "and  (id_card REGEXP '[0-9]{15,}'='' or id_card REGEXP '[0-9]{15,}' is null) and "
                               "(insure_id REGEXP '[0-9]{7,}'='' or insure_id REGEXP '[0-9]{,}' is null)"
                               % (table, str(empi_id), fields[0], fields[1]))
        else:
            update_empi_id("update %s set empi_id='%s',confidence='0.8' where patient_name='%s' and "
                           "left(birthday,10)='%s' and  (id_card REGEXP '[0-9]{15,}'='' or id_card REGEXP '[0-9]{15,}' "
                           "is null) and (insure_id REGEXP '[0-9]{7,}'='' or insure_id REGEXP '[0-9]{,}' is null)"
                           % (table, str(empi_id), fields[0], fields[1]))


def evaluation(table):
    sql_id = "select  id_card_new from (select id_card_new from (select case when id_card REGEXP '[0-9]{15,}' " \
             "then id_card else '' end id_card_new, empi_id from %s " \
             "where id_card!='' and id_card is not null)t where id_card_new !='')t2 " \
             "group by id_card_new having count(id_card_new)>1" % table
    sql_insure = "select patient_name,insure_id from (select patient_name,insure_id from %s " \
                 "where (patient_name!='' and patient_name is not null and insure_id!='' and insure_id is not null))t " \
                 "group by patient_name,insure_id having count(*)!=1" % table
    sql_bn = "select patient_name,birthday from (select patient_name,left(birthday,10) birthday from %s " \
             "where (patient_name!='' and patient_name is not null and birthday!='' and birthday is not null))t " \
             "group by patient_name,birthday having count(*)!=1" % table
    id_card_counter = counter("select count(*) from (%s)t" % sql_id)
    insure_counter = counter("select count(*) from (%s)t" % sql_insure)
    birthday_counter = counter("select count(*) from (%s)t" % sql_bn)
    id_card(table, sql_id, id_card_counter)
    insure_id(table, sql_insure, insure_counter)
    birthday(table, sql_bn, birthday_counter)
    update_empi_id("update %s set confidence='1' where (confidence is null or confidence='')  and case when id_card "
                   "REGEXP '[0-9]{15,}' then id_card else '' end!=''" % table)
    update_empi_id("update %s set confidence='0.6' where (confidence is null or confidence='') and (patient_name!='' "
                   "and patient_name is not null and insure_id!='' and insure_id is not null) and case when id_card "
                   "REGEXP '[0-9]{15,}' then id_card else '' end=''" % table)
    update_empi_id("update %s set confidence='0.4' where (confidence is null or confidence='') and (patient_name!='' "
                   "and patient_name is not null and birthday!='' and birthday is not null)" % table)
    update_empi_id("update %s set confidence='0.2' where (confidence is null or confidence='') and patient_name!='' "
                   "and patient_name is not null " % table)
    update_empi_id("update %s set confidence='0' where confidence is null or confidence=''" % table)


def merge(table):
    flag = 0
    empi_id_all = set(field[0] for field in data_set(
        "select empi_id from %s group by empi_id HAVING count(empi_id)>1" % table))
    total = len(empi_id_all)
    for items in empi_id_all:
        flag += 1
        time.sleep(0.01)
        print '合并总量：', total, '当前：' , flag
        data = [data for data in data_set("select * from %s where empi_id='%s' order by confidence desc " % (table, items))]
        temp = [counter('select uuid()')] + list(data[0][1:])
        for j in [i for i in [2, 3, 4, 5, 6, 7, 8, 10, 11] if not data[0][i]]:
            for i in range(1, len(data)):
                if data[i][j]:
                    temp[j] = data[i][j]
                    break
                else:
                    pass
        temp[42], temp[43] = '', '未确认'
        update_empi_id("insert into %s values('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s',"
                       "'%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s',"
                       "'%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s')" % tuple([table] + map(find_none, temp)))

if __name__ == '__main__':
    empi_init('zmap_r_patient')
    evaluation('zmap_r_patient')

