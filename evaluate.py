#!/usr/bin/env python
# -*- coding: utf-8 -*-
################################################################################
#
# Copyright (c) 2019 ***.com, Inc. All Rights Reserved
# The NSH Anti-Plugin Project
################################################################################
"""
NSH通用评估模块，对主线、三环以及图谱模型的预测结果进行评估，并上传结果至MySQL

Usage: python MySQLUtils.py --ds_pred_start 2019-01-04 --ds_pred_end 2019-01-10 --tablename zhuxiangua
                            --ds_ban_start 2019-01-04 --ds_ban_end 2019-01-17'
Authors: Zhou Jialiang
Email: zjl_sempre@163.com
Date: 2019/02/13
"""
import os
import argparse
import logging
import pymysql
from datetime import datetime, timedelta
import log

# 项目目录
PROJECT_DIR = '/home/zhoujialiang/online_evaluate'

# MySQL Config
MySQL_HOST_IP = '***.***.***.***'
MySQL_HOST_PORT = 3306
MySQL_HOST_USER = '***'
MySQL_HOST_PASSWORD = '***'
MySQL_TARGET_DB = '***'

# 查询预测id
QUERY_SQL_PRED = """
select distinct role_id
from anti_plugin.nsh_{tablename}
where {FIELD_DS} >= '{ds_pred_start}'
    and {FIELD_DS} <= '{ds_pred_end}' {other_conditions}
"""

# 查询封停id
QUERY_SQL_BAN = """
select distinct role_id
from anti_plugin.nsh_account_ban
where ds >= '{ds_ban_start}' 
    and ds <= '{ds_ban_end}'
"""

INSERT = """
insert into anti_plugin.nsh_evaluate(method, prec, recall, cnt_pos_true, cnt_pos, cnt_true, ds_eval, ds_pred_start, ds_pred_end, ds_ban_start, ds_ban_end) 
values ('{method}', {prec}, {recall}, {cnt_pos_true}, {cnt_pos}, {cnt_true}, '{ds_eval}', '{ds_pred_start}', '{ds_pred_end}', '{ds_ban_start}', '{ds_ban_end}')
"""

FIELD_DS = {
    'sanhuangua': 'left(start_time, 10)',
    'zhuxiangua': 'left(start_time, 10)',
    'graph': 'ds'
}

CONDITIONS = {
    'sanhuangua': 'and suspect_score >= 0.8',
    'zhuxiangua': 'and suspect_score >= 0.9 and method = \'mlp_41_window\'',
    'graph': ''
}

# MySQL类
class MysqlDB(object):
    """MySQL数据读取或上传

    Attributes:
        _conn: MySQLl连接
    """
    def __init__(self, host, port, user, passwd, db, ):
        self._conn = pymysql.connect(host=host, port=port, user=user, password=passwd, database=db)
        logging.info('Init MySQL connection')

    def __del__(self):
        self._conn.close()

    def get_results(self, sql):
        """MySQL查询结果

        Args:
            sql: 查询语句

        Returns:
            results: 查询结果
        """
        logging.info('Pulling data from MySQL...')
        cursor = self._conn.cursor()
        cursor.execute(sql)
        results = cursor.fetchall()
        return results

    def insert_row(self, sql):
        """插入一行至MySQL

        Args:
            sql: 插入语句
        """
        cursor = self._conn.cursor()
        try:
            cursor.execute(sql)
            self._conn.commit()
        except:
            self._conn.rollback()


class Evaluator(object):
    """MySQL数据读取或上传

    Attributes:
        _conn: MySQLl连接
    """
    def __init__(self, ids_pred, ids_ban):
        self._ids_pred = set(ids_pred)
        self._ids_ban = set(ids_ban)

    def prec_recall(self):
        """评估准确率和召回率
        获取预测外挂数，实际封停数，预测外挂且封停数；
        并计算预测准确率和召回率

        Returns:
            results: 评估结果字典，包含 {
                cnt_pos: 预测外挂数
                cnt_true: 实际封停数
                cnt_pos_true: 预测正确数
                prec: 准确率
                recall: 召回率
            } 五个key

        """
        results = dict()
        results['cnt_pos'] = len(self._ids_pred)
        results['cnt_true'] = len(self._ids_ban)
        results['cnt_pos_true'] = results['cnt_pos'] + results['cnt_true'] - len(set(list(self._ids_ban) + list(self._ids_pred)))
        try:
            results['prec'] = results['cnt_pos_true'] / results['cnt_pos']
        except Exception as e:
            logging.error('No predicted results! {}'.format(e))
        try:
            results['recall'] = results['cnt_pos_true'] / results['cnt_true']
        except Exception as e:
            logging.error('No banned role_ids! {}'.format(e))

        return results


if __name__ == '__main__':

    # 输入参数
    parser = argparse.ArgumentParser('Evaluate Plugin Models'
                                     'Usage: python3 MySQLUtils.py --ds_pred_start 2019-01-04 --ds_pred_end 2019-01-10 '
                                     '--tablename zhuxiangua --ds_ban_start 2019-01-04 --ds_ban_end 2019-01-17')
    parser.add_argument('--tablename', help='\'zhuxiangua\', \'sanhuangua\' or \'graph\'')
    parser.add_argument('--ds_pred_start', help='time format: \'%Y-%m-%d\'')
    parser.add_argument('--ds_pred_end', help='time format: \'%Y-%m-%d\'')
    parser.add_argument('--ds_ban_start', help='time format: \'%Y-%m-%d\'')
    parser.add_argument('--ds_ban_end', help='time format: \'%Y-%m-%d\'')
    args = parser.parse_args()
    tablename = args.tablename
    ds_pred_start = args.ds_pred_start
    ds_pred_end = args.ds_pred_end
    ds_ban_start = args.ds_ban_start
    ds_ban_end = args.ds_ban_end
    ds_eval = (datetime.strptime(ds_ban_end, '%Y-%m-%d') + timedelta(days=1)).strftime('%Y-%m-%d')

    # 日志
    log.init_log(os.path.join(PROJECT_DIR, 'logs', 'evaluate'))
    logging.info('Start evaluating {}...'.format(tablename))

    # 连接MySQL
    db = MysqlDB(host=MySQL_HOST_IP, port=MySQL_HOST_PORT, user=MySQL_HOST_USER, passwd=MySQL_HOST_PASSWORD, db=MySQL_TARGET_DB)

    # 获取预测id
    ids_pred = set()
    sql_pred = QUERY_SQL_PRED.format(FIELD_DS=FIELD_DS[tablename], tablename=tablename, ds_pred_start=ds_pred_start, ds_pred_end=ds_pred_end,
                                     other_conditions=CONDITIONS[tablename])
    for row in db.get_results(sql_pred):
        role_id = row[0]
        ids_pred.add(str(role_id))

    # 获取封停id
    ids_ban = set()
    sql_ban = QUERY_SQL_BAN.format(ds_ban_start=ds_ban_start, ds_ban_end=ds_ban_end)
    for row in db.get_results(sql_ban):
        role_id = row[0]
        ids_ban.add(str(role_id))

    # 评估结果
    evaluator = Evaluator(ids_pred=ids_pred, ids_ban=ids_ban)
    results = evaluator.prec_recall()
    logging.info(results)

    # 上传评估结果
    query = INSERT.format(method=tablename, prec=results['prec'], recall=results['recall'], ds_eval=ds_eval,
                      cnt_pos_true=results['cnt_pos_true'], cnt_pos=results['cnt_pos'], cnt_true=results['cnt_true'],
                      ds_pred_start=ds_pred_start, ds_pred_end=ds_pred_end, ds_ban_start=ds_ban_start, ds_ban_end=ds_ban_end)
    logging.info('Upload evaluation results...')
    db.insert_row(query)
