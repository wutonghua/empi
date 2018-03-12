#!/usr/bin/env python
# -*-coding:utf-8 -*-
import re

from mysql import execute_sql


def data_set(sql):
    with execute_sql() as cursor:
        cursor.execute(sql)
        row = cursor.fetchone()
        while row:
            yield row
            row = cursor.fetchone()


def update_empi_id(sql):
    with execute_sql() as cursor_update:
        cursor_update.execute(sql)


def counter(sql):
    with execute_sql() as cursor_counter:
        cursor_counter.execute(sql)
        return cursor_counter.fetchall()[0][0]


def find_none(first_data):
    if first_data:
        s = first_data * 2
        if re.search(r'^\\+$', s):
            return first_data * 2
        elif first_data == '\'':
            return ''
        else:
            return first_data
    else:
        return ''






