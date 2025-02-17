'''
    功能：分词(排除停用词)
'''

import os
import time
import jieba
import logging
import argparse
import multiprocessing as mp
from multiprocessing import get_context
logging.basicConfig(level=logging.INFO, format='%(asctime)s : %(levelname)s : %(message)s')

stopword = None
def worker(filepath):
    ''' 进程函数：分词 '''
    logging.info('Processing file: [%s]', filepath)
    path_out = filepath + '.seg'
    try:
        with open(filepath, 'r') as f, open(path_out, 'w') as writer:
            for line in f.readlines():
                line = line.strip()
                if len(line) == 0:
                    continue
                words = jieba.lcut(line)
                words = [word for word in words if word not in stopword]
                writer.write('{}\n'.format(' '.join(words)))
        logging.info('Process finish. path_out [%s]', path_out)
    except Exception as e:
        logging.error('Error process file: [%s]:[%s]', filepath, e)

def get_filepath(path_in):
    ''' 获取待处理的文件路径集合 '''
    file_list = []
    if os.path.isfile(path_in):
        file_list.append(path_in)
    elif os.path.isdir(path_in):
        files = os.listdir(path_in)
        for file in files:
            file_list.append(os.path.join(path_in, file))
    else:
        logging.error('Input path is not file or dir: [%s]', path_in)
    return file_list

def main(args):
    ''' 主逻辑 '''
    file_list = get_filepath(args.path_in)
    logging.info('The number of process files: [%d]', len(file_list))
    if len(file_list) == 0:
        logging.error("No input files found.")
        exit(-1)
    if not os.path.isfile(args.path_sw):
        logging.error('No stopword file found: [%s]', args.path_sw)
        exit(-1)
    global stopword
    stopword = set([line.strip() for line in open(args.path_sw).readlines()])

    print('stopword size: ', len(stopword))
    print(stopword)

    # 动态设置进程数
    num_worker = min(args.num_worker, len(file_list), mp.cpu_count())
    logging.info('num_worker: [%d]', num_worker)

    # 使用fork方式创建进程池
    with get_context('fork').Pool(processes=num_worker) as pool:
        for _ in pool.imap_unordered(func=worker, iterable=file_list):
            # 由于worker没有返回值，所以这里用pass
            pass

if __name__ == '__main__':
    start_time = time.time()
    parser = argparse.ArgumentParser(description='multi_process_relate2')
    parser.add_argument('--path_in', type=str, help='输入路径', default='./data')
    parser.add_argument('--path_sw', type=str, help='停用词路径', default='./stopwords.txt')
    parser.add_argument('--num_worker', type=int, help='进程数', default=2)
    args = parser.parse_args()
    logging.info('Argument: [%s]', args)
    main(args)
    logging.info('Consume time: [%s]', round(time.time() - start_time, 4))
