# -*- coding: utf-8 -*-

import argparse
import os
import sys
import string
import time
import json

import settings
from settings import DATABASES

from mysql import MysqlClient

# Synonym sets

def getStartTime():
    startTime = settings.startTime
    if os.path.exists(settings.startTimeFile):
        f = open(settings.startTimeFile, 'r')
        startTime = f.read().strip()
        f.close()
    return startTime

def saveCacheTime(startTime):
    f = open(settings.startTimeFile, 'w')
    f.write(startTime)
    f.close()
    return None

def tag(id, desp, nodeAttributes, synonymsDic):
    tags = {}
    despDict = json.loads(desp)

    attrs = nodeAttributes[str(id)]
    for attr in attrs:
        attr_n = attr['name']
        tags[attr_n] = []

        attr_t = despDict.get(attr_n)
        if attr_t is None:
            continue

        for attr_v in attr['values']:
            synonyms = synonymsDic.get(attr_v)
            if synonyms is None:
                synonyms = [attr_v]
            else:
                synonyms.append(attr_v)

            for synonym in synonyms:
                if string.find(string.lower(attr_t), string.lower(synonym)) <> -1:
                    tags[attr_n].append(attr_v)
                    break
    return tags

def process(dataType):
    db = DATABASES['default']
    con = MysqlClient(db['HOST'], db['USER'], db['PASSWORD'], db['NAME'], db['PORT'])
    if not con.connect():
        print('MySQL connection error...')
        return None

    ''' Node attributes '''
    nodeAttributes = {}
    query_rows = con.query('SELECT `cid`, `pid`, `typename` FROM `tb_category` WHERE `isattribute` = 1')
    for cate in con.fetchAll():
        nodeAttributes[str(cate[0])] = []
        con.query('SELECT `cid`, `pid`, `typename` FROM `tb_category` WHERE `isattribute` = 2 AND `pid` = %s',
            [cate[0]])
        for attr in con.fetchAll():
            attr_dic = {}
            attr_dic['name'] = attr[2]
            attr_dic['values'] = []
            con.query('SELECT `cid`, `pid`, `typename` FROM `tb_category` WHERE `isattribute` = 3 AND `pid` = %s',
                [attr[0]])
            for val in con.fetchAll():
                attr_dic['values'].append(val[2])
            nodeAttributes[str(cate[0])].append(attr_dic)

    print(json.dumps(nodeAttributes, encoding='UTF-8', ensure_ascii=False, 
        sort_keys=True, 
        indent=4, 
        separators=(',', ': ')))

    ''' synonyms '''
    synonyms = {}

    last_time = getStartTime()

    while True:
        con.query('SELECT now()')
        now_time = str(con.fetchAll()[0][0])
        con.commit()

        start = 0
        step = 1000

        while True:
            query_rows = con.query('SELECT `srcid`, `title`, `srclink`, `desp`, `createtime` '
                'FROM `tb_goods_info` '
                'WHERE `cid` = %s AND `createtime` >= %s AND `createtime` < %s '
                'limit %s, %s',
                [dataType, last_time, now_time, start, step])

            print('[%s - %s]: query %s %s rows...' 
                % (last_time, now_time, start, query_rows))

            for data in con.fetchAll():
                tags = tag(dataType, data[3], nodeAttributes, synonyms)

                affected_rows = con.query('UPDATE `tb_goods_info` SET `attrs` = %s, `updatetime` = now() WHERE `srcid` = %s',
                    [json.dumps(tags, encoding='UTF-8', ensure_ascii=False), data[0]])
                if affected_rows <> 1:
                    print('srcid (%s) update faild...', [srcid])

            con.commit();

            start += query_rows
            if query_rows <> step:
                break

        last_time = now_time
        saveCacheTime(last_time)
        time.sleep(settings.interval)

    return None

def main():
    parser = argparse.ArgumentParser("Bookish-Tagger")
    parser.add_argument('-c', action='store', dest='dataType', required=True, type=int, help='Store the type of Taobao data')
    parser.add_argument('-i', action='store', dest='interval', required=True, type=int, help='Store the interval value')
    parser.add_argument('-t', action='store', dest='startTime', help='Store the processing starting time')
    parser.add_argument('--verbose', action='store_true', dest='verboseMode', help='Verbose mode')
    parser.add_argument('--version', action='version', version='%(prog)s 1.0')
    args = parser.parse_args()

    if args.startTime:
        settings.startTime = args.startTime
    if args.interval:
        settings.interval = args.interval
    if args.verboseMode:
        settings.DEBUG = True

    if args.dataType:
        process(args.dataType)
    return None

if __name__ == '__main__':
    main()

