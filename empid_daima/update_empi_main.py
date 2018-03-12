#!/usr/bin/env python
# -*-coding:utf-8 -*-
import time

from basic_f import data_set
from update_empi_evaluation import update, merge


def main(table):
    sql = "select patient_name,birthday,case when id_card REGEXP '[0-9]{15,}'  " \
          "then id_card else '' end id_card,insure_id,empi_id,patient_id from %s " \
          "where empi_id=''or empi_id is null order by id_card desc,case when insure_id REGEXP '[0-9]{7,}' " \
          "then insure_id else '' end desc" % table
    u_patient_id = ["'%s'" % data[5] for data in data_set(sql)]
    update(table, sql)
    if u_patient_id:
        u_empi_id = [data[0] for data in data_set("select distinct empi_id from %s where patient_id in (%s)"
                                                  % (table, ','.join(u_patient_id)))]
        merge(table, u_empi_id)
    else:
        pass

if __name__ == '__main__':
    start = time.time()
    main('zmap_r_patient')
    end = time.time()
    print end - start
