#!/usr/bin/env python
# -*-coding:utf-8 -*-
import time

from basic_f import update_empi_id, data_set, counter, find_none


def update(table, sql_execute):
    label = 0
    max_id_sql = "select LPAD(max(cast(empi_id as signed)) + 1,9,'0')  from %s" % table
    for fields in data_set(sql_execute):
        time.sleep(0.01)
        label += 1
        print label
        if fields[2]:
            id = max([line[1] for line in data_set("select  id_card,empi_id from %s where id_card='%s' and "
                                                   "(confirm_status='' or confirm_status is null)" % (table, fields[2]))])
            if not id:
                empi_id = counter(max_id_sql)
            else:
                empi_id = id
            update_empi_id(
                "update %s set empi_id='%s',confidence='1'  where patient_id='%s'" % (table, str(empi_id), fields[5]))
        elif fields[0] and fields[3]:
            try:
                id = data_set("select empi_id from %s where patient_name='%s' and insure_id='%s' and (empi_id is not "
                              "null and empi_id!='') and (confirm_status='' or confirm_status is null) order by "
                              "case when id_card REGEXP '[0-9]{15,}' then id_card else '' end desc limit 1"
                              % (table, fields[0], fields[3])).next()[0]
                empi_id = id
            except StopIteration:
                empi_id = counter(max_id_sql)
                update_empi_id(
                    "update %s set empi_id='%s',confidence='0.6'  where patient_id='%s'" % (table, str(empi_id), fields[5]))
            else:
                max_confidence = data_set("select max(confidence) from %s where patient_name='%s' and insure_id='%s' "
                                          "and (confirm_status='' or confirm_status is null) " % (table, fields[0], fields[3])).next()[0]
                if max_confidence == '1' or max_confidence == '0.9':
                    update_empi_id(
                        "update %s set empi_id='%s',confidence='0.9' where patient_id='%s'" % (table, str(empi_id), fields[5]))
                else:
                    update_empi_id(
                        "update %s set empi_id='%s',confidence='0.6' where patient_id='%s'" % (table, str(empi_id), fields[5]))

        elif fields[0] and fields[1]:
            try:
                id = data_set("select empi_id from %s where patient_name='%s' and left(birthday,10)='%s' and "
                              "(empi_id is not null and empi_id!='') and (confirm_status='' or confirm_status is null) "
                              "order by case when id_card REGEXP '[0-9]{15,}' then id_card else '' end desc,"
                              "case when insure_id REGEXP '[0-9]{7,}' then insure_id else '' end desc limit 1"
                              % (table, fields[0], str(fields[1])[:10])).next()[0]
                empi_id = id
            except StopIteration:
                empi_id = counter(max_id_sql)
                update_empi_id(
                    "update %s set empi_id='%s',confidence='0.4' where patient_id='%s'" % (table, str(empi_id), fields[5]))
            else:

                max_confidence = data_set("select max(confidence) from %s where patient_name='%s' and "
                                          "left(birthday,10)='%s' and (confirm_status='' or confirm_status is null) "
                                          % (table, fields[0], str(fields[1])[:10])).next()[0]
                if max_confidence == '1' or max_confidence == '0.9':
                    update_empi_id("update %s set empi_id='%s',confidence='0.8' where patient_id='%s'"
                                   % (table, str(empi_id), fields[5]))
                else:
                    update_empi_id(
                        "update %s set empi_id='%s',confidence='0.4' where patient_id='%s'" % (table, str(empi_id), fields[5]))
        else:
            empi_id = counter(max_id_sql)
            if fields[0]:
                update_empi_id(
                    "update %s set empi_id='%s',confidence='0.2' where patient_id='%s'" % (table, str(empi_id), fields[5]))
            else:
                update_empi_id(
                    "update %s set empi_id='%s',confidence='0' where patient_id='%s'" % (table, str(empi_id), fields[5]))


def merge(table, u_empi_id):
    flag = 0
    update_empi_id("delete from %s where confirm_status='未确认' and empi_id in (%s)" % (table, ','.join(u_empi_id)))
    u_empi_id = [empi_id for empi_id in u_empi_id if counter("select count(*) from %s where empi_id=%s" % (table, empi_id)) > 1]
    # total = len(u_empi_id)
    for items in u_empi_id:
        time.sleep(0.01)
        flag += 1
        # print '合并总量：', total, '当前：', flag
        data = [data for data in data_set(
            "select * from %s where empi_id='%s' order by confirm_status desc,confidence DESC " % (table, items))]
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




