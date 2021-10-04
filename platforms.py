import random
from json.encoder import JSONEncoder

class Platform:
    def __init__(self, name, paradigms, **args):
        self.name = name

        self.paradigm = paradigms if isinstance(paradigms, list)  else [paradigms]
        self.args = args # 版本号/是否分布式

    # def setID(self, id):
    #     if (self.id is None):
    #         self.id = id

def serialize_to_json(plt):
    return plt.__dict__

def deserialize_from_json(dct):
    if 'name' in dct and 'paradigm' in dct:
        return Platform(dct['name'], dct['paradigm'])
    return dct
    
spark = Platform("spark", ["batch", "streaming", "sql", "linear", "graph"])
storm = Platform("storm", ["batch", "streaming"])
flink = Platform("flink", ["batch", "streaming"])
javastream = Platform("javastream", "batch")
hadoop = Platform("hadoop", "batch")
samza = Platform("samza", ["batch", "streaming"])
kafka = Platform("kafka", "streaming")
apex = Platform("apex", "streaming")
flume = Platform("flume", "streaming")
postgre = Platform("postgre", "sql")
mysql = Platform("mysql", "sql")
hive = Platform("hive", "sql") # 列式的，怎么表示？
hbase = Platform("hbase", "sql")
monetdb = Platform("monetdb", "sql")
redis = Platform("redis", ["nosql", "streaming"])
mongodb = Platform("mongodb", "nosql")
cassandra = Platform("cassandra", "nosql")
rocksdb = Platform("rocksdb", "nosql") # k-v 
pytorch = Platform("pytorch", "linear")
scikit = Platform("scikit", "linear")
tensorflow = Platform("tensorflow", "linear")
theano = Platform("theano", "linear")
caffe = Platform("caffe", "linear")
keras = Platform("keras", "linear")
mxnet = Platform("mxnet", "linear")
graphchi = Platform("graphchi", "graph")
giraph = Platform("giraph", "graph")

all_plt = set([spark,storm, flink, javastream, hadoop, samza, kafka, apex, flume, 
postgre, mysql, hive, hbase, monetdb, 
redis, mongodb, cassandra, rocksdb,
pytorch, tensorflow, scikit, theano, caffe, keras, mxnet, graphchi, giraph])

all_plt_dic = {p.name: p for p in all_plt}

batch_plt = set([plt for plt in all_plt if "batch" in plt.paradigm])
streaming_plt = set([plt for plt in all_plt if "streaming" in plt.paradigm])
sql_plt = set([plt for plt in all_plt if "sql" in plt.paradigm])
nosql_plt = set([plt for plt in all_plt if "nosql" in plt.paradigm])
linear_plt  = set([plt for plt in all_plt if "linear" in plt.paradigm])
graph_plt = set([plt for plt in all_plt if "graph" in plt.paradigm])

plts_with_paradigm = {
    'batch' : batch_plt,
    'streaming': streaming_plt,
    'sql': sql_plt,
    'nosql': nosql_plt,
    'linear': linear_plt,
    'graph': graph_plt
}

