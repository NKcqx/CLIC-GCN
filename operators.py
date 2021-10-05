from json.encoder import JSONEncoder
from enum import Enum

import platforms


class OperatorKind(int, Enum):
    SOURCE      = 1
    CALCULATOR  = 2
    DISPATCHER  = 3
    LOCATOR     = 4
    SAMPLER     = 5
    ACTION      = 6


class Operator:
    def __init__(self, name, paradigms=None, **args):
        self.name = name
        self.paradigms = paradigms

        if('supported_platforms' in args):
            self.supported_platforms = args.get('supported_platforms')
        else:
            self.supported_platforms = [plt for plt in platforms.all_plt if len(set(plt.paradigms) & set(paradigms)) > 0 ]

        self.kind = args.get('kind', OperatorKind.CALCULATOR)
        self.num_input = args.get('num_input', 1)
        self.num_output = args.get('num_output', 1)
    
    def addPlatform(self, p):
        self.supported_platforms.extend(p)

all_opts = [
Operator("save", ["batch", "streaming", "linear", "graph"], kind=OperatorKind.ACTION),
## basic transformer (改变元素类型)
Operator("map", ["batch", "streaming"], kind=OperatorKind.CALCULATOR),
Operator("flatmap", ["batch", "streaming"], kind=OperatorKind.CALCULATOR),
Operator("filter", ["batch", "streaming", "linear"], kind=OperatorKind.CALCULATOR),
Operator("distinct", ["batch", "streaming", "linear"], kind=OperatorKind.CALCULATOR),
## keyed (输出数据按key分类), sum、min也是一种keyed聚合
Operator("keyby", ["batch", "streaming"], kind=OperatorKind.CALCULATOR, num_output=2),
# cogroup    = Operator("cogroup", ['batch'], kind=OperatorKind.CALCULATOR, num_input=2) # 等价于 connect（已在streaming中定义）
Operator("groupby", ['batch', 'streaming', 'sql', 'nosql'],  kind=OperatorKind.CALCULATOR, num_output=2),
Operator("sortby", ["batch", "streaming", "linear"], kind=OperatorKind.CALCULATOR),
Operator("join", ["batch", "streaming", "sql", "nosql", "linear"], kind=OperatorKind.CALCULATOR, num_input=2),
Operator("union", ["batch", "streaming", "sql", "nosql"], kind=OperatorKind.CALCULATOR, num_input=2),
Operator("maptopair",['batch', 'streaming'], kind=OperatorKind.CALCULATOR, num_output=2),
Operator("zip",["batch", "streaming"], kind=OperatorKind.CALCULATOR, num_output=2),

Operator("count", ["batch", "linear", "sql", "streaming"], kind=OperatorKind.ACTION), # 默认num_input=1, num_output=1
Operator("first", ["batch", "streaming"], kind=OperatorKind.ACTION),
Operator("max", ["batch", "linear", "sql", "streaming"], kind=OperatorKind.ACTION),
Operator("min", ["batch", "linear", "sql", "streaming"], kind=OperatorKind.ACTION),
Operator("median", ['batch', 'linear', 'streaming'], kind=OperatorKind.ACTION),
Operator("sum", ["batch", "linear", "sql", "streaming"], kind=OperatorKind.ACTION),
Operator("top", ["batch", "streaming"], kind=OperatorKind.ACTION),
Operator("reduce", ["batch", "streaming"], kind=OperatorKind.ACTION),
Operator("collect", ["batch", "streaming"], kind=OperatorKind.ACTION),

Operator("subtract", ["batch", "streaming", "linear"], kind=OperatorKind.CALCULATOR, num_input=2), # 从第一个数据中提取第二个不存在的元素（差集）

Operator('broadcast', ['batch'], kind=OperatorKind.DISPATCHER),

Operator("concat", ['batch', 'sql', 'nosql', 'linear'],  kind=OperatorKind.CALCULATOR, num_output=2),
Operator("sample", ["batch", "streaming", "linear", "graph"], kind=OperatorKind.CALCULATOR),

#############
# linear (mainly)
#############
Operator('add', ['linear'], kind=OperatorKind.CALCULATOR, num_input=2, num_output=1),
Operator('product', ['linear'], kind=OperatorKind.CALCULATOR, num_input=2, num_output=1),
Operator('divid', ['linear'], kind=OperatorKind.CALCULATOR, num_input=2, num_output=1),
Operator('matmul', ['linear'], kind=OperatorKind.CALCULATOR, num_input=2, num_output=1),
Operator('transpose', ['linear'], kind=OperatorKind.CALCULATOR, num_input=1, num_output=1),
Operator('pow', ['linear'], kind=OperatorKind.CALCULATOR, num_input=1, num_output=1),
Operator('sqrt', ['linear'], kind=OperatorKind.CALCULATOR, num_input=1, num_output=1),
Operator('exp', ['linear'], kind=OperatorKind.CALCULATOR, num_input=1, num_output=1),
Operator('log', ['linear'], kind=OperatorKind.CALCULATOR, num_input=1, num_output=1),
Operator('round', ['linear'], kind=OperatorKind.CALCULATOR, num_input=1, num_output=1),
Operator('floor', ['linear'], kind=OperatorKind.CALCULATOR, num_input=1, num_output=1),
Operator('ceil', ['linear'], kind=OperatorKind.CALCULATOR, num_input=1, num_output=1),
Operator('abs', ['linear'], kind=OperatorKind.CALCULATOR),
Operator('cos', ['linear'], kind=OperatorKind.CALCULATOR),
Operator('sin', ['linear'], kind=OperatorKind.CALCULATOR),
Operator('tan', ['linear'], kind=OperatorKind.CALCULATOR),
Operator('frac', ['linear'], kind=OperatorKind.CALCULATOR),

Operator('tensor', ['linear'], kind=OperatorKind.SAMPLER, num_input=0),
Operator('rand', ['linear'], kind=OperatorKind.SAMPLER, num_input=0),
Operator('ones', ['linear'], kind=OperatorKind.SAMPLER, num_input=0),
Operator('zeros', ['linear'], kind=OperatorKind.SAMPLER, num_input=0),
Operator('range', ['linear'], kind=OperatorKind.SAMPLER, num_input=0),
Operator('uniform', ['linear'], kind=OperatorKind.SAMPLER, num_input=0),
Operator('loc', ['linear'], kind=OperatorKind.LOCATOR),
Operator('reshape', ['linear'], kind=OperatorKind.LOCATOR),
Operator('squeeze', ['linear'], kind=OperatorKind.LOCATOR),
Operator('topk', ['linear'], kind=OperatorKind.LOCATOR),

#############
# streaming (mainly)
#############
Operator('connect', ['streaming', 'batch'], kind=OperatorKind.CALCULATOR, num_input=2),  # 不同于union只能聚合相同类型的数据（流），connect能连接不同类型的数据
Operator('comap', ['streaming'], kind=OperatorKind.CALCULATOR, num_input=2), # 对union后的两个流施加不同的func
Operator('coflatmap', ['streaming'], kind=OperatorKind.CALCULATOR, num_input=2),
Operator('split', ['streaming', 'batch'], kind=OperatorKind.CALCULATOR, num_output=2), # 一条流根据predicate分成多个流
Operator('select', ['streaming', 'batch'], kind=OperatorKind.LOCATOR), # 从split返回的流中选择一条流

## dispatcher 数据分区相关的
Operator('shuffle', ['streaming', 'batch'], kind=OperatorKind.DISPATCHER),
Operator('rebalance', ['streaming'], kind=OperatorKind.DISPATCHER),
Operator('global', ['streaming'], kind=OperatorKind.DISPATCHER),
Operator('rescale', ['streaming'], kind=OperatorKind.DISPATCHER),
Operator('broadcast', ['streaming', 'batch'], kind=OperatorKind.DISPATCHER),

#############
# source
#############
Operator("srcfromtxtfile", ['batch', 'sql', 'nosql', 'linear'],  kind=OperatorKind.SOURCE, num_input=0),
Operator('srcfrommem', ['streaming', 'sql', 'nosql', 'linear', 'batch', 'graph'], kind=OperatorKind.SOURCE, num_input=0),
Operator('srcfromdb', ['sql', 'nosql'], kind=OperatorKind.SOURCE, num_input=0),

]

opts_with_paradigm = {
    'batch': set([opt for opt in all_opts if "batch" in opt.paradigms]),
    'streaming': set([opt for opt in all_opts if "streaming" in opt.paradigms]),
    'sql': set([opt for opt in all_opts if "sql" in opt.paradigms]),
    'nosql': set([opt for opt in all_opts if "nosql" in opt.paradigms]),
    'linear': set([opt for opt in all_opts if "linear" in opt.paradigms]),
    'graph': set([opt for opt in all_opts if "graph" in opt.paradigms])
}

# #############
# # batch(mainly): 基于单个时间的基本转换、针对相同键值时间的转换、将多个数据合并 or 将一组数据拆分为多个、对事件重新组织的分发转换
# #############
# save       = Operator("save", ["batch", "streaming", "linear", "graph"], kind=OperatorKind.ACTION)
# ## basic transformer (改变元素类型)
# map        = Operator("map", ["batch", "streaming"], kind=OperatorKind.CALCULATOR)
# flatmap    = Operator("flatmap", ["batch", "streaming"], kind=OperatorKind.CALCULATOR)
# filter     = Operator("filter", ["batch", "streaming", "linear"], kind=OperatorKind.CALCULATOR)
# distinct   = Operator("distinct", ["batch", "streaming", "linear"], kind=OperatorKind.CALCULATOR)
# ## keyed (输出数据按key分类), sum、min也是一种keyed聚合
# keyBy      = Operator("keyby", ["batch", "streaming"], kind=OperatorKind.CALCULATOR, num_output=2)
# # cogroup    = Operator("cogroup", ['batch'], kind=OperatorKind.CALCULATOR, num_input=2) # 等价于 connect（已在streaming中定义）
# groupby    = Operator("groupby", ['batch', 'streaming', 'sql', 'nosql'],  kind=OperatorKind.CALCULATOR, num_output=2)
# sortBy     = Operator("sortby", ["batch", "streaming", "linear"], kind=OperatorKind.CALCULATOR)
# join       = Operator("join", ["batch", "streaming", "sql", "nosql", "linear"], kind=OperatorKind.CALCULATOR, num_input=2)
# union      = Operator("union", ["batch", "streaming", "sql", "nosql"], kind=OperatorKind.CALCULATOR, num_input=2)
# maptopair  = Operator("maptopair",['batch', 'streaming'], kind=OperatorKind.CALCULATOR, num_output=2)
# zip        = Operator("zip",["batch", "streaming"], kind=OperatorKind.CALCULATOR, num_output=2)

# count      = Operator("count", ["batch", "linear", "sql", "streaming"], kind=OperatorKind.ACTION) # 默认num_input=1, num_output=1
# first      = Operator("first", ["batch", "streaming"], kind=OperatorKind.ACTION)
# max        = Operator("max", ["batch", "linear", "sql", "streaming"], kind=OperatorKind.ACTION)
# min        = Operator("min", ["batch", "linear", "sql", "streaming"], kind=OperatorKind.ACTION)
# median     = Operator("median", ['batch', 'linear', 'streaming'], kind=OperatorKind.ACTION)
# sum        = Operator("sum", ["batch", "linear", "sql", "streaming"], kind=OperatorKind.ACTION)
# top        = Operator("top", ["batch", "streaming"], kind=OperatorKind.ACTION) 
# reduce     = Operator("reduce", ["batch", "streaming"], kind=OperatorKind.ACTION)
# collect    = Operator("collect", ["batch", "streaming"], kind=OperatorKind.ACTION)

# subtract   = Operator("subtract", ["batch", "streaming", "linear"], kind=OperatorKind.CALCULATOR, num_input=2) # 从第一个数据中提取第二个不存在的元素（差集）

# repartition= Operator('broadcast', ['batch'], kind=OperatorKind.DISPATCHER)

# concat     = Operator("concat", ['batch', 'sql', 'nosql', 'linear'],  kind=OperatorKind.CALCULATOR, num_output=2)
# sample     = Operator("sample", ["batch", "streaming", "linear", "graph"], kind=OperatorKind.CALCULATOR)

# #############
# # linear (mainly)
# #############
# add         = Operator('add', ['linear'], kind=OperatorKind.CALCULATOR, num_input=2, num_output=1)
# product     = Operator('product', ['linear'], kind=OperatorKind.CALCULATOR, num_input=2, num_output=1)
# divid       = Operator('divid', ['linear'], kind=OperatorKind.CALCULATOR, num_input=2, num_output=1)
# matmul      = Operator('matmul', ['linear'], kind=OperatorKind.CALCULATOR, num_input=2, num_output=1)
# transpose   = Operator('transpose', ['linear'], kind=OperatorKind.CALCULATOR, num_input=1, num_output=1)
# pow         = Operator('pow', ['linear'], kind=OperatorKind.CALCULATOR, num_input=1, num_output=1)
# sqrt        = Operator('sqrt', ['linear'], kind=OperatorKind.CALCULATOR, num_input=1, num_output=1)
# exp         = Operator('exp', ['linear'], kind=OperatorKind.CALCULATOR, num_input=1, num_output=1)
# log         = Operator('log', ['linear'], kind=OperatorKind.CALCULATOR, num_input=1, num_output=1)
# round       = Operator('round', ['linear'], kind=OperatorKind.CALCULATOR, num_input=1, num_output=1)
# floor       = Operator('floor', ['linear'], kind=OperatorKind.CALCULATOR, num_input=1, num_output=1)
# ceil        = Operator('ceil', ['linear'], kind=OperatorKind.CALCULATOR, num_input=1, num_output=1)
# abs         = Operator('abs', ['linear'], kind=OperatorKind.CALCULATOR)
# cos         = Operator('cos', ['linear'], kind=OperatorKind.CALCULATOR)
# sin         = Operator('sin', ['linear'], kind=OperatorKind.CALCULATOR)
# tan         = Operator('tan', ['linear'], kind=OperatorKind.CALCULATOR)
# frac        = Operator('frac', ['linear'], kind=OperatorKind.CALCULATOR)

# tensor      = Operator('tensor', ['linear'], kind=OperatorKind.SAMPLER, num_input=0)
# rand        = Operator('rand', ['linear'], kind=OperatorKind.SAMPLER, num_input=0)
# ones        = Operator('ones', ['linear'], kind=OperatorKind.SAMPLER, num_input=0)
# zeros       = Operator('zeros', ['linear'], kind=OperatorKind.SAMPLER, num_input=0)
# _range       = Operator('range', ['linear'], kind=OperatorKind.SAMPLER, num_input=0)
# uniform     = Operator('uniform', ['linear'], kind=OperatorKind.SAMPLER, num_input=0)
# loc         = Operator('loc', ['linear'], kind=OperatorKind.LOCATOR)
# reshape     = Operator('reshape', ['linear'], kind=OperatorKind.LOCATOR)
# squeeze     = Operator('squeeze', ['linear'], kind=OperatorKind.LOCATOR)
# topk        = Operator('topk', ['linear'], kind=OperatorKind.LOCATOR)

# #############
# # streaming (mainly)
# #############
# connect     = Operator('connect', ['streaming', 'batch'], kind=OperatorKind.CALCULATOR, num_input=2)  # 不同于union只能聚合相同类型的数据（流），connect能连接不同类型的数据
# comap       = Operator('comap', ['streaming'], kind=OperatorKind.CALCULATOR, num_input=2) # 对union后的两个流施加不同的func
# coflatmap   = Operator('coflatmap', ['streaming'], kind=OperatorKind.CALCULATOR, num_input=2)
# split       = Operator('split', ['streaming', 'batch'], kind=OperatorKind.CALCULATOR, num_output=2) # 一条流根据predicate分成多个流
# select      = Operator('select', ['streaming', 'batch'], kind=OperatorKind.LOCATOR) # 从split返回的流中选择一条流

# ## dispatcher 数据分区相关的
# shuffle     = Operator('shuffle', ['streaming', 'batch'], kind=OperatorKind.DISPATCHER)
# rebalance   = Operator('rebalance', ['streaming'], kind=OperatorKind.DISPATCHER)
# global_     = Operator('global', ['streaming'], kind=OperatorKind.DISPATCHER)
# rescale     = Operator('rescale', ['streaming'], kind=OperatorKind.DISPATCHER)
# broadcast   = Operator('broadcast', ['streaming', 'batch'], kind=OperatorKind.DISPATCHER)

# #############
# # source
# #############
# srcfromtxtfile = Operator("srcfromtxtfile", ['batch', 'sql', 'nosql', 'linear'],  kind=OperatorKind.SOURCE, num_input=0)
# srcfrommem = Operator('srcfrommem', ['streaming', 'sql', 'nosql', 'linear', 'batch', 'graph'], kind=OperatorKind.SOURCE, num_input=0)
# srcfromdb  = Operator('srcfromdb', ['sql', 'nosql'], kind=OperatorKind.SOURCE, num_input=0)

# all_opt = set([count, first, map, min, sum,top,reduce,collect,save,sample,sortBy,
# subtract,map,flatmap,distinct,filter,join,union,maptopair,zip,keyBy,groupby,
# srcfromtxtfile, srcfrommem, srcfromdb,
# add, product, divid, matmul, transpose, pow,sqrt, exp, log, round, floor, ceil, abs, cos, sin, tan, frac, tensor,
# rand, ones, zeros, _range, uniform, loc, reshape, squeeze, topk, connect, comap, coflatmap, split, select, shuffle, rebalance, global_,
# rescale, broadcast, repartition
# ])


