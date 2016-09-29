# -*- coding: utf-8 -*-

import os
import sys
import string
import time
import json

import settings
from settings import DATABASES

from mysql import MysqlClient

# Synonym sets

synonyms_config = {
        u'通勤': [
            'AAAA',
            'BBBB',
            'CCCC',
            'DDDD'
        ],
        u'A字裙': [
            'EEEE',
            'FFFF',
            'GGGG',
            'HHHH'
        ]
}

def getStartTime():
    start_time = settings.cacheTime
    if os.path.exists(settings.cacheTimeFile):
        f = open(settings.cacheTimeFile, 'r')
        start_time = f.read().strip()
        f.close()
    return start_time

def saveCacheTime(cacheTime):
    f = open(settings.cacheTimeFile, 'w')
    f.write(cacheTime)
    f.close()
    return None

def process(id, desp, nodeAttributes):
    tags = {}
    despDict = json.loads(desp)

    attrs = nodeAttributes[id]
    for attr in attrs:
        attr_n = attr['name']
        tags[attr_n] = []

        attr_t = despDict.get(attr_n)
        if attr_t is None:
            continue

        for attr_v in attr['values']:
            synonyms = synonyms_config.get(attr_v)
            if synonyms is None:
                synonyms = [attr_v]
            else:
                synonyms.append(attr_v)

            for synonym in synonyms:
                if string.find(string.lower(attr_t), string.lower(synonym)) <> -1:
                    tags[attr_n].append(attr_v)
                    break
    return tags

def main():
    db = DATABASES['default']
    con = MysqlClient(db['HOST'], db['USER'], db['PASSWORD'], db['NAME'], db['PORT'])
    if not con.connect():
        print('MySQL connection error...')
        return None

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

    last_time = getStartTime()

    while True:
        con.query('SELECT now()')
        now_time = str(con.fetchAll()[0][0])
        con.commit()

        start = 0
        step = 1000
        cid = '2'

        while True:
            query_rows = con.query('SELECT `srcid`, `title`, `srclink`, `desp`, `createtime` '
                'FROM `tb_goods_info` '
                'WHERE `cid` = %s AND `createtime` >= %s AND `createtime` < %s '
                'limit %s, %s',
                [cid, last_time, now_time, start, step])

            print('[%s - %s]: query %s %s rows...' % (last_time, now_time, start, query_rows))

            for data in con.fetchAll():
                tags = process(cid, data[3], nodeAttributes)

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
        time.sleep(60)

    return None

if __name__ == '__main__':
	main()
