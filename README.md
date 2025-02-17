# multiprocessing

## 背景
    只有单台机器可用的时候，可以通过python标准库中的multiprocessing模块实现多进程计算，提高处理数据的效率。

## 场景
    通常情况下，遇到的计算类型有如下3种：
```
1、进程间不相关。每条数据单独处理，互不影响。比如，分词、模型特征构建、数据预处理等。例子：
    python seg_for_alone.py

2、进程间共享写对象。进程间需要共同更新一个数据对象。例子：
    python seg_and_wordcount.py

3、进程间共享读对象。各进程需要从一个对象中读取数据。例子：
    分词：python seg_exclude_stopwords.py
    检索：
        构建索引：python construct_index.py
        检索：python search_from_index.py
```