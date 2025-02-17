#-*- coding: utf-8 -*-
"""
    功能: 使用whoosh库构建索引
"""

import os
import time
import argparse
from whoosh.index import create_in, open_dir, exists_in
from whoosh.fields import Schema, TEXT
from jieba.analyse.analyzer import ChineseAnalyzer

def build_index(args):
    """ 构建索引 """
    if not os.path.exists(args.path_index):
        os.mkdir(args.path_index)
    if exists_in(args.path_index):
        index = open_dir(args.path_index)
        print('index exists, opening...')
    else:
        analyzer = ChineseAnalyzer()
        keyword = TEXT(stored=True, analyzer=analyzer, multitoken_query='or')
        schema = Schema(keyword=keyword)
        index = create_in(args.path_index, schema)

    writer = index.writer()
    with open(args.path_keyword, 'r') as f:
        for i, line in enumerate(f):
            if ((i + 1) % 100000 == 0):
                print((i + 1), 'keywords added.')
            keyword = line.strip()
            if len(keyword) < 2:
                continue
            writer.add_document(keyword=keyword)
        print('all keywords added: {}'.format(i + 1))
    writer.commit()
    print('index built successfully.')

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='build index for yitu search')
    parser.add_argument('--path_index', type=str, default='./index', help='path to index')
    parser.add_argument('--path_keyword', type=str, default='./data2/keywords.txt', help='path to title')
    args = parser.parse_args()
    start_time = time.time()
    build_index(args)
    print('耗时: {}秒'.format(round(time.time() - start_time, 6)))

