'''
    功能：分词后统计词频
'''

import os
import time
import jieba
import logging
import argparse
import multiprocessing as mp
logging.basicConfig(level=logging.INFO, format='%(asctime)s : %(levelname)s : %(message)s')

def worker(params):
    ''' 进程函数：分词 '''
    filepath, d = params
    logging.info('Processing file: [%s]', filepath)
    try:
        with open(filepath, 'r') as f:
            for line in f.readlines():
                line = line.strip()
                if len(line) == 0:
                    continue
                words = jieba.lcut(line)
                for word in words:
                    d[word] = d.get(word, 0) + 1
        logging.info('Process Completed: [%s]', filepath)
    except Exception as e:
        logging.error('Process Error: [%s]:[%s]', filepath, e)

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
    # 动态设置进程数
    num_worker = min(args.num_worker, len(file_list), mp.cpu_count())
    logging.info('num_worker: [%d]', num_worker)

    with mp.Manager() as manager:
        d = manager.dict()  # 创建共享字典
        # d是被引用，不会被复制
        params = [(file, d) for file in file_list]
        with mp.Pool(processes=num_worker) as pool:
            for _ in pool.imap_unordered(func=worker, iterable=params):
                # 由于worker没有返回值，所以这里用pass
                pass
        # 将字典写出到输出路径
        with open(args.path_out, 'w') as f:
            for k, v in d.items():
                f.write(k + '\t' + str(v) + '\n')
        logging.info('Output path: [%s]', args.path_out)

if __name__ == '__main__':
    start_time = time.time()
    parser = argparse.ArgumentParser(description='multi_process_relate')
    parser.add_argument('--path_in', type=str, help='输入路径', default='./data')
    parser.add_argument('--path_out', type=str, help='输出路径', default='./data/wordcount.txt')
    parser.add_argument('--num_worker', type=int, help='进程数', default=2)
    args = parser.parse_args()
    logging.info('Argument: [%s]', args)
    main(args)
    logging.info('Consume time: [%s]', round(time.time() - start_time, 4))
