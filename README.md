# multiprocessing

## 背景
    只有单台机器可用的时候，可以通过python标准库中的multiprocessing模块实现多进程计算，提高处理数据的效率。

## 场景
    通常情况下，遇到的计算类型有如下3种：
```
1、进程间不相关。每条数据单独处理，互不影响，类似于mapreduce中的map操作。比如，分词、模型特征构建、数据预处理等。例子：
    python seg_for_alone.py

2、进程间共享写对象。单条数据处理之后，需要对处理的结果做某些合并操作，类似mapreduce中的reduce操作。例子：
    python seg_and_wordcount.py

3、进程间共享读对象。如果进程在计算的时候，需要引入外部信息，那么需要把外部信息写入到一个共享对象，让各个进程可以读取。类似于mapreduce中的cache。例子：
    分词：python seg_exclude_stopwords.py
    检索：
        构建索引：python construct_index.py
        检索：python search_from_index.py
```

## 参考资料
https://docs.python.org/3/library/concurrency.html