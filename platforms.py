import random
from json.encoder import JSONEncoder

class Platform:
    def __init__(self, name, **args):
        self.name = name
        self.paradigms = args.get('paradigms', ['batch', 'streaming', 'linear', 'graph', 'sql', 'nosql'])
        self.scale = args.get('scale', ['local', 'small', 'medium', 'large'])
        self.args = args # 版本号/是否分布式


all_plt = [
Platform("spark", paradigms=["batch", "streaming", "sql", "linear", "graph"], scale=['medium', 'large']),
Platform("storm", paradigms=["batch", "streaming"], scale=['medium', 'large']),
Platform("flink", paradigms=["batch", "streaming"], scale=['medium', 'large']),
Platform("javastream", paradigms=["batch"], scale=['local', 'small']),
Platform("hadoop", paradigms=["batch"], scale=['medium', 'large']),
Platform("samza", paradigms=["batch", "streaming"], scale=['local', 'small', 'medium']),
Platform("kafka", paradigms=["streaming"], scale=['medium', 'large']),
Platform("apex", paradigms=["streaming"], scale=['local', 'small', 'medium']),
Platform("flume", paradigms=["streaming"], scale=['local', 'small', 'medium']),
Platform("postgre", paradigms=["sql"], scale=['local','small', 'medium']),
Platform("mysql", paradigms=["sql"], scale=['local', 'small', 'medium']),
Platform("hive", paradigms=["sql"], scale=['medium', 'large']), # 列式的，怎么表示？
Platform("hbase", paradigms=["sql"], scale=['medium', 'large']),
Platform("monetdb", paradigms=["sql"], scale=['small', 'medium']),
Platform("redis", paradigms=["nosql", "streaming"], scale=['small', 'medium', 'large']),
Platform("mongodb", paradigms=["nosql"], scale=['medium', 'large']),
Platform("cassandra", paradigms=["nosql"], scale=['small', 'medium']),
Platform("rocksdb", paradigms=["nosql"], scale=['small', 'medium']), # k-v 
Platform("pytorch", paradigms=["linear"], scale=['local', 'small', 'medium']),
Platform("scikit", paradigms=["linear"], scale=['local']),
Platform("tensorflow", paradigms=["linear"], scale=['medium', 'large']),
Platform("theano", paradigms=["linear"], scale=['local', 'small', 'medium']),
Platform("caffe", paradigms=["linear"], scale=['local', 'small']),
Platform("keras", paradigms=["linear"], scale=['local', 'small', 'medium']),
Platform("mxnet", paradigms=["linear"], scale=['local', 'small', 'medium']),
Platform("graphchi", paradigms=["graph"], scale=['local', 'small']),
Platform("giraph", paradigms=["graph"], scale=['local', 'small', 'medium']),
]
all_plt_dic = {p.name: p for p in all_plt}
plts_with_paradigm = {
    'batch' : [plt for plt in all_plt if 'batch' in plt.paradigms],
    'streaming':  [plt for plt in all_plt if 'streaming' in plt.paradigms],
    'sql':  [plt for plt in all_plt if 'sql' in plt.paradigms],
    'nosql':  [plt for plt in all_plt if 'nosql' in plt.paradigms],
    'linear':  [plt for plt in all_plt if 'linear' in plt.paradigms],
    'graph':  [plt for plt in all_plt if 'graph' in plt.paradigms],
}

plts_with_scale = {
    'local': [plt for plt in all_plt if 'local' in plt.scale],
    'small': [plt for plt in all_plt if 'small' in plt.scale],
    'medium': [plt for plt in all_plt if 'medium' in plt.scale],
    'large': [plt for plt in all_plt if 'large' in plt.scale],
}
# spark = Platform("spark", paradigms=["batch", "streaming", "sql", "linear", "graph"])
# storm = Platform("storm", paradigms=["batch", "streaming"])
# flink = Platform("flink", paradigms=["batch", "streaming"])
# javastream = Platform("javastream", paradigms=["batch"])
# hadoop = Platform("hadoop", paradigms=["batch"])
# samza = Platform("samza", paradigms=["batch", "streaming"])
# kafka = Platform("kafka", paradigms=["streaming"])
# apex = Platform("apex", paradigms=["streaming"])
# flume = Platform("flume", paradigms=["streaming"])
# postgre = Platform("postgre", paradigms=["sql"])
# mysql = Platform("mysql", paradigms=["sql"])
# hive = Platform("hive", paradigms=["sql"]) # 列式的，怎么表示？
# hbase = Platform("hbase", paradigms=["sql"])
# monetdb = Platform("monetdb", paradigms=["sql"])
# redis = Platform("redis", paradigms=["nosql", "streaming"])
# mongodb = Platform("mongodb", paradigms=["nosql"])
# cassandra = Platform("cassandra", paradigms=["nosql"])
# rocksdb = Platform("rocksdb", paradigms=["nosql"]) # k-v 
# pytorch = Platform("pytorch", paradigms=["linear"])
# scikit = Platform("scikit", paradigms=["linear"])
# tensorflow = Platform("tensorflow", paradigms=["linear"])
# theano = Platform("theano", paradigms=["linear"])
# caffe = Platform("caffe", paradigms=["linear"])
# keras = Platform("keras", paradigms=["linear"])
# mxnet = Platform("mxnet", paradigms=["linear"])
# graphchi = Platform("graphchi", paradigms=["graph"])
# giraph = Platform("giraph", paradigms=["graph"])

# all_plt = set([spark,storm, flink, javastream, hadoop, samza, kafka, apex, flume, 
# postgre, mysql, hive, hbase, monetdb, 
# redis, mongodb, cassandra, rocksdb,
# pytorch, tensorflow, scikit, theano, caffe, keras, mxnet, graphchi, giraph])



# batch_plt = set([plt for plt in all_plt if "batch" in plt.paradigms])
# streaming_plt = set([plt for plt in all_plt if "streaming" in plt.paradigms])
# sql_plt = set([plt for plt in all_plt if "sql" in plt.paradigms])
# nosql_plt = set([plt for plt in all_plt if "nosql" in plt.paradigms])
# linear_plt  = set([plt for plt in all_plt if "linear" in plt.paradigms])
# graph_plt = set([plt for plt in all_plt if "graph" in plt.paradigms])



