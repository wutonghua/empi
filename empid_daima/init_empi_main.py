#!/usr/bin/env python
# -*-coding:utf-8 -*-
"""初始化"""
import time

from init_empi_evaluation import empi_init, evaluation, merge


def main(table):
    print '数据初始化中......'
    empi_init(table)
    print '数据初始化完毕,赋值empi_id以及confidence....'
    time.sleep(1)
    evaluation(table)
    print 'empi_id以及confidence赋值完毕，开始计算合并...'
    merge(table)
    print '合并完毕'

if __name__ == '__main__':
    start = time.time()
    main('zmap_r_patient')
    end = time.time()
    print end - start
