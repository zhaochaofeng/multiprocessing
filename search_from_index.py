'''
    功能：从index中检索相关keywords（用ProcessPoolExecutor实现）
'''

import time
import logging
import argparse
import multiprocessing as mp
from concurrent.futures import ProcessPoolExecutor
from whoosh.index import open_dir
from whoosh import scoring
from whoosh.qparser import QueryParser
import atexit
logging.basicConfig(level=logging.INFO, format='%(asctime)s : %(levelname)s : %(message)s')

# 全局变量
index = None
searcher = None

def init_worker(index_path):
    """ 用于每个进程的初始化 """
    print('init index: {}'.format(index_path))
    global index, searcher
    index = open_dir(index_path)
    searcher = index.searcher(weighting=scoring.BM25F)
    atexit.register(close_worker) # 注册退出函数

def close_worker():
    """ 用于每个进程的关闭 """
    print('close index ...')
    global searcher
    if searcher:
        searcher.close()

def worker(param):
    ''' 进程函数：检索 '''
    args, line, i = param
    global searcher
    if (i + 1) % 10 == 0:
        print('processing line: {}'.format(i + 1))
    try:
        parser = QueryParser("keyword", index.schema)
        query = line.strip()
        if len(query) < 2:
            return None
        print("query: {}".format(query))
        query_parse = parser.parse(query)
        results = searcher.search(query_parse, limit=args.top_n)
        keywords = set()
        if len(results) > 0:
            for hit in results:
                keyword = hit['keyword']
                score = hit.score
                print("response: ", keyword, score)
                if score < args.score_thre:
                    continue
                keywords.add(keyword)
        if keywords:
            return query, keywords
    except Exception as e:
        print(f"Error processing line: {e}")
    return None

def main(args):
    ''' 主逻辑 '''
    with open(args.path_in, encoding='utf-8') as f:
        lines = f.readlines()
    logging.info('The number of lines: [%d]', len(lines))
    if len(lines) == 0:
        logging.error("No input files found.")
        exit(-1)
    # 动态设置进程数
    num_worker = min(args.num_worker, len(lines), mp.cpu_count())
    logging.info('num_worker: [%d]', num_worker)
    writer = open(args.path_out, 'w')

    params = [(args, line, i) for i, line in enumerate(lines)]
    # 创建进程池
    with ProcessPoolExecutor(max_workers=num_worker, initializer=init_worker, initargs=(args.path_index, )) as executor:
        for res in executor.map(worker, params):
            if res:
                query, keywords = res
                writer.write('{}\t{}\n'.format(query, ' '.join(keywords)))
    writer.close()
    logging.info('output: {}'.format(args.path_out))

if __name__ == '__main__':
    start_time = time.time()
    parser = argparse.ArgumentParser(description='multi_process_independence_onefile')
    parser.add_argument('--path_index', type=str, help='索引路径', default='./index')
    parser.add_argument('--path_in', type=str, help='query路径', default='./data2/query.txt')
    parser.add_argument('--path_out', type=str, help='查询输出路径', default='./data2/out.txt')
    parser.add_argument('--num_worker', type=int, help='进程数', default=2)
    parser.add_argument('--top_n', type=int, help='取关联的topN个关键词', default=100)
    parser.add_argument('--score_thre', type=float, help='得分阈值', default=1)

    args = parser.parse_args()
    logging.info('Argument: [%s]', args)
    main(args)
    logging.info('Consume time: [%s]', round(time.time() - start_time, 4))
