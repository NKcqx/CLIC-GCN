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


class Operator(JSONEncoder):
    def __init__(self, name, paradigms=None, **args):
        self.name = name
        self.paradigm = paradigms if isinstance(paradigms, list)  else [paradigms]
        if(args.get('supported_platforms')):
            self.supported_platforms = args.get('supported_platforms')
        else:
            self.supported_platforms = []
            for pdm in self.paradigm:
                self.supported_platforms.extend(platforms.plts_with_paradigm.get(pdm))
            # self.supported_platforms = set(self.supported_platforms)

        self.kind = args.get('kind', OperatorKind.CALCULATOR)
        self.num_input = args.get('num_input', 1)
        self.num_output = args.get('num_output', 1)
         # 输入/输出参数个数、paradigm中的运算类型
    
    # def setID(self, id):
    #     if (self.id is None):
    #         self.id = id
    
    # def execute(self):
    #     return random.choice(self.supported_platforms)
    
    def addPlatform(self, p):
        self.supported_platforms.extend(p)
    
    def default(self, o):
        return o.__dict__ 

def serialize_to_json(opt):
    res = opt.__dict__
    if(res.get('supported_platforms', -1) != -1):
        del res['supported_platforms']
    # a = []
    # for i in range(len(res['supported_platforms'])):
    #     plt = res['supported_platforms'][i]
    #     if  isinstance(plt, platforms.Platform):
    #         a.append(platforms.serialize_to_json(plt))
    # if (len(a)>0):
    #     res['supported_platforms'] = a
    return res

def deserialize_from_json(dct):
    if 'name' in dct and 'paradigm' in dct:
        opt = Operator(dct['name'], dct['paradigm'])
        # if 'supported_platforms' in dct:
            # opt.supported_platforms = [ platforms.deserialize_from_json(plt) for plt in dct['supported_platforms'] ]
        if 'kind' in dct:
            opt.kind = OperatorKind(dct['kind']) 
        if 'num_input' in dct:
            opt.num_input = dct['num_input']
        if 'num_output' in dct:
            opt.num_output = dct['num_output']
        return opt
    return dct





#############
# batch(mainly): 基于单个时间的基本转换、针对相同键值时间的转换、将多个数据合并 or 将一组数据拆分为多个、对事件重新组织的分发转换
#############
save       = Operator("save", ["batch", "streaming", "linear", "graph"], kind=OperatorKind.ACTION)
## basic transformer (改变元素类型)
map        = Operator("map", ["batch", "streaming"], kind=OperatorKind.CALCULATOR)
flatmap    = Operator("flatmap", ["batch", "streaming"], kind=OperatorKind.CALCULATOR)
filter     = Operator("filter", ["batch", "streaming", "linear"], kind=OperatorKind.CALCULATOR)
distinct   = Operator("distinct", ["batch", "streaming", "linear"], kind=OperatorKind.CALCULATOR)
## keyed (输出数据按key分类), sum、min也是一种keyed聚合
keyBy      = Operator("keyby", ["batch", "streaming"], kind=OperatorKind.CALCULATOR, num_output=2)
# cogroup    = Operator("cogroup", ['batch'], kind=OperatorKind.CALCULATOR, num_input=2) # 等价于 connect（已在streaming中定义）
groupby    = Operator("groupby", ['batch', 'streaming', 'sql', 'nosql'],  kind=OperatorKind.CALCULATOR, num_output=2)
sortBy     = Operator("sortby", ["batch", "streaming", "linear"], kind=OperatorKind.CALCULATOR)
join       = Operator("join", ["batch", "streaming", "sql", "nosql", "linear"], kind=OperatorKind.CALCULATOR, num_input=2)
union      = Operator("union", ["batch", "streaming", "sql", "nosql"], kind=OperatorKind.CALCULATOR, num_input=2)
maptopair  = Operator("maptopair",['batch', 'streaming'], kind=OperatorKind.CALCULATOR, num_output=2)
zip        = Operator("zip",["batch", "streaming"], kind=OperatorKind.CALCULATOR, num_output=2)

count      = Operator("count", ["batch", "linear", "sql", "streaming"], kind=OperatorKind.ACTION) # 默认num_input=1, num_output=1
first      = Operator("first", ["batch", "streaming"], kind=OperatorKind.ACTION)
max        = Operator("max", ["batch", "linear", "sql", "streaming"], kind=OperatorKind.ACTION)
min        = Operator("min", ["batch", "linear", "sql", "streaming"], kind=OperatorKind.ACTION)
median     = Operator("median", ['batch', 'linear', 'streaming'], kind=OperatorKind.ACTION)
sum        = Operator("sum", ["batch", "linear", "sql", "streaming"], kind=OperatorKind.ACTION)
top        = Operator("top", ["batch", "streaming"], kind=OperatorKind.ACTION) 
reduce     = Operator("reduce", ["batch", "streaming"], kind=OperatorKind.ACTION)
collect    = Operator("collect", ["batch", "streaming"], kind=OperatorKind.ACTION)

subtract   = Operator("subtract", ["batch", "streaming", "linear"], kind=OperatorKind.CALCULATOR, num_input=2) # 从第一个数据中提取第二个不存在的元素（差集）

repartition= Operator('broadcast', ['batch'], kind=OperatorKind.DISPATCHER)

concat     = Operator("concat", ['batch', 'sql', 'nosql', 'linear'],  kind=OperatorKind.CALCULATOR, num_output=2)
sample     = Operator("sample", ["batch", "streaming", "linear", "graph"], kind=OperatorKind.CALCULATOR)

#############
# linear (mainly)
#############
add         = Operator('add', ['linear'], kind=OperatorKind.CALCULATOR, num_input=2, num_output=1)
product     = Operator('product', ['linear'], kind=OperatorKind.CALCULATOR, num_input=2, num_output=1)
divid       = Operator('divid', ['linear'], kind=OperatorKind.CALCULATOR, num_input=2, num_output=1)
matmul      = Operator('matmul', ['linear'], kind=OperatorKind.CALCULATOR, num_input=2, num_output=1)
transpose   = Operator('transpose', ['linear'], kind=OperatorKind.CALCULATOR, num_input=1, num_output=1)
pow         = Operator('pow', ['linear'], kind=OperatorKind.CALCULATOR, num_input=1, num_output=1)
sqrt        = Operator('sqrt', ['linear'], kind=OperatorKind.CALCULATOR, num_input=1, num_output=1)
exp         = Operator('exp', ['linear'], kind=OperatorKind.CALCULATOR, num_input=1, num_output=1)
log         = Operator('log', ['linear'], kind=OperatorKind.CALCULATOR, num_input=1, num_output=1)
round       = Operator('round', ['linear'], kind=OperatorKind.CALCULATOR, num_input=1, num_output=1)
floor       = Operator('floor', ['linear'], kind=OperatorKind.CALCULATOR, num_input=1, num_output=1)
ceil        = Operator('ceil', ['linear'], kind=OperatorKind.CALCULATOR, num_input=1, num_output=1)
abs         = Operator('abs', ['linear'], kind=OperatorKind.CALCULATOR)
cos         = Operator('cos', ['linear'], kind=OperatorKind.CALCULATOR)
sin         = Operator('sin', ['linear'], kind=OperatorKind.CALCULATOR)
tan         = Operator('tan', ['linear'], kind=OperatorKind.CALCULATOR)
frac        = Operator('frac', ['linear'], kind=OperatorKind.CALCULATOR)

tensor      = Operator('tensor', ['linear'], kind=OperatorKind.SAMPLER, num_input=0)
rand        = Operator('rand', ['linear'], kind=OperatorKind.SAMPLER, num_input=0)
ones        = Operator('ones', ['linear'], kind=OperatorKind.SAMPLER, num_input=0)
zeros       = Operator('zeros', ['linear'], kind=OperatorKind.SAMPLER, num_input=0)
_range       = Operator('range', ['linear'], kind=OperatorKind.SAMPLER, num_input=0)
uniform     = Operator('uniform', ['linear'], kind=OperatorKind.SAMPLER, num_input=0)
loc         = Operator('loc', ['linear'], kind=OperatorKind.LOCATOR)
reshape     = Operator('reshape', ['linear'], kind=OperatorKind.LOCATOR)
squeeze     = Operator('squeeze', ['linear'], kind=OperatorKind.LOCATOR)
topk        = Operator('topk', ['linear'], kind=OperatorKind.LOCATOR)

#############
# streaming (mainly)
#############
connect     = Operator('connect', ['streaming', 'batch'], kind=OperatorKind.CALCULATOR, num_input=2)  # 不同于union只能聚合相同类型的数据（流），connect能连接不同类型的数据
comap       = Operator('comap', ['streaming'], kind=OperatorKind.CALCULATOR, num_input=2) # 对union后的两个流施加不同的func
coflatmap   = Operator('coflatmap', ['streaming'], kind=OperatorKind.CALCULATOR, num_input=2)
split       = Operator('split', ['streaming', 'batch'], kind=OperatorKind.CALCULATOR, num_output=2) # 一条流根据predicate分成多个流
select      = Operator('select', ['streaming', 'batch'], kind=OperatorKind.LOCATOR) # 从split返回的流中选择一条流

## dispatcher 数据分区相关的
shuffle     = Operator('shuffle', ['streaming', 'batch'], kind=OperatorKind.DISPATCHER)
rebalance   = Operator('rebalance', ['streaming'], kind=OperatorKind.DISPATCHER)
global_     = Operator('global', ['streaming'], kind=OperatorKind.DISPATCHER)
rescale     = Operator('rescale', ['streaming'], kind=OperatorKind.DISPATCHER)
broadcast   = Operator('broadcast', ['streaming', 'batch'], kind=OperatorKind.DISPATCHER)

#############
# source
#############
srcfromtxtfile = Operator("srcfromtxtfile", ['batch', 'sql', 'nosql', 'linear'],  kind=OperatorKind.SOURCE, num_input=0)
srcfrommem = Operator('srcfrommem', ['streaming', 'sql', 'nosql', 'linear', 'batch', 'graph'], kind=OperatorKind.SOURCE, num_input=0)
srcfromdb  = Operator('srcfromdb', ['sql', 'nosql'], kind=OperatorKind.SOURCE, num_input=0)

all_opt = set([count, first, map, min, sum,top,reduce,collect,save,sample,sortBy,
subtract,map,flatmap,distinct,filter,join,union,maptopair,zip,keyBy,groupby,
srcfromtxtfile, srcfrommem, srcfromdb,
add, product, divid, matmul, transpose, pow,sqrt, exp, log, round, floor, ceil, abs, cos, sin, tan, frac, tensor,
rand, ones, zeros, _range, uniform, loc, reshape, squeeze, topk, connect, comap, coflatmap, split, select, shuffle, rebalance, global_,
rescale, broadcast, repartition
])

batch_opt = set([opt for opt in all_opt if "batch" in opt.paradigm])
streaming_opt = set([opt for opt in all_opt if "streaming" in opt.paradigm])
sql_opt = set([opt for opt in all_opt if "sql" in opt.paradigm])
nosql_opt = set([opt for opt in all_opt if "nosql" in opt.paradigm])
linear_opt = set([opt for opt in all_opt if "linear" in opt.paradigm])
graph_opt  = set([opt for opt in all_opt if "graph" in opt.paradigm])

opts_with_paradigm = {
    'batch': batch_opt,
    'streaming': streaming_opt,
    'sql': sql_opt,
    'nosql': nosql_opt,
    'linear': linear_opt,
    'graph': graph_opt
}



# data = serialize_to_json(save)