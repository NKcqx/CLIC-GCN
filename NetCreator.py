import os
import os.path as osp
import networkx as nx
import matplotlib.pyplot as plt
import copy
import random
import time

from numpy.random import choice
from enum import Enum

import operators, platforms, generator



def getTails(G):
    nodes = G.out_degree
    res = [n[0] for n in nodes if n[1] == 0]
    return res

def getHeads(G):
    nodes = G.in_degree
    res = [n[0] for n in nodes if n[1] == 0]
    return res

def chooseFSM(paradigm='linear'):  
    d = {
        'batch': generator.batchFSM,
        'streaming': generator.streamFSM,
        'linear': generator.linearFSM
    }
    return d[paradigm]

def UpForkNet(paradigm, platform, size='small', left=None, right=None, junc = None, loop=0):
    '''
        把两个分支用一个juncture连接
        left：左分支，没有的话就随机造，默认包含source
        right：右分支，没有的话就随机造，默认包含source
        junc：juncture
    '''
    batch_juncture_up =  [opt for opt in  operators.batch_opt if opt.num_input == 2 and opt.num_output==1]
    left_graph = PathNet(paradigm, platform, loop=loop) if left is None else left
    right_graph = PathNet(paradigm,platform, loop=loop) if right is None else right
    G  = nx.compose(left_graph['graph'], right_graph['graph'])
    
    junc = choice(batch_juncture_up, 1)[0] if junc is None else junc
    junc = copy.deepcopy(junc)
    G.add_node(junc, paradigm=paradigm, platform=platform)

    left_head = getHeads(left_graph['graph'])[0]
    right_head = getHeads(right_graph['graph'])[0]
    G.add_edges_from([(left_head, junc), (right_head, junc)])
    # if(len(list(nx.simple_cycles(G))) > 0):
    #     print('#######  Circle!  ########', 'Upfork', left_head.name, ' -> ', junc.name, ';', right_head.name, ' -> ', junc.name)
    return {
        'graph': G,
        'head': [left_head, right_head],
        'tail': [junc]
    }

def PathNet(paradigm, platform, loop=0, size='small'):
    fsm = chooseFSM(paradigm)
    pipeline = fsm(with_source=False, with_sink=False, loop=loop)['opts']
    G = nx.DiGraph()
    for opt in pipeline:
        G.add_node(opt, paradigm=paradigm, platform=platform)
    for i in range(0, len(pipeline)-1):
        G.add_edge(pipeline[i], pipeline[i+1])
        # if(len(list(nx.simple_cycles(G))) > 0):
        #     print('#######  Circle!  ########', 'PathNet', pipeline[i].name, ' -> ' , pipeline[i+1].name)
    
    return {
        'graph': G,
        'head': [pipeline[0]],
        'tail': [pipeline[-1]],
    }

def DownForkNet(paradigm, platform, left=None, right=None, junc = None, loop=0, size='small'):
    '''
        把两个分支用一个juncture连接
        left：左分支，没有的话就随机造，默认包含source
        right：右分支，没有的话就随机造，默认包含source
        junc：juncture
    '''
    batch_juncture_down = [opt for opt in  operators.batch_opt if opt.num_input == 1 and opt.num_output==2] 
    left_graph = PathNet(paradigm, platform, loop=loop) if left is None else left
    right_graph = PathNet(paradigm, platform, loop=loop) if right is None else right
    G  = nx.compose(left_graph['graph'], right_graph['graph'])

    junc = choice(batch_juncture_down, 1)[0] if junc is None else junc
    junc = copy.deepcopy(junc)
    G.add_node(junc, paradigm=paradigm, platform=platform)
    left_head = getHeads(left_graph['graph'])[0]
    right_head = getHeads(right_graph['graph'])[0]
    G.add_edges_from([(junc, left_head), (junc, right_head)])
    # if(len(list(nx.simple_cycles(G))) > 0):
    #     print('#######  Circle!  ########', 'DownFork',  junc.name, ' -> ', left_head.name, ';', junc.name, ' -> ', right_head.name)
    return {
        'graph': G,
        'head': [junc],
        'tail': [left_head, right_head]
    }

def SourceNet(paradigm, platform, loop=0, size='small'):
    source_opt = [opt for opt in  operators.all_opt if opt.kind == operators.OperatorKind.SOURCE] 
    node = choice(source_opt)
    node = copy.deepcopy(node)
    G = nx.DiGraph()
    G.add_node(node, paradigm=paradigm, platform=platform)
    return {
        'graph': G,
        'head': [node],
        'tail': [node]
    }


def ActionNet(paradigm, platform, loop=0, size='small'):
    action_opt = [opt for opt in  operators.all_opt if opt.kind == operators.OperatorKind.ACTION] 
    node = choice(action_opt)
    node = copy.deepcopy(node)
    G = nx.DiGraph()
    G.add_node(node, paradigm=paradigm, platform=platform)
    return {
        'graph': G,
        'head': [node],
        'tail': [node]
    }

def JoinNetworks(A, B, tail, head):
    G = nx.compose(A, B)
    G.add_edge(tail, head)
    # if(len(list(nx.simple_cycles(G))) > 0):
    #     print('#######  Circle!  ########', 'Join',  tail.name, ' -> ', head.name)

    return G

def completeNet(Net=None, concat_head='*', concat_tail='*', level=1, paradigm='*', platform='*', loop=0, size='small'):
    if (paradigm == '*'):
        paradigm = random.choice(['batch', 'streaming', 'linear'])
    if (platform == '*'):
        platform = random.choice(list(platforms.plts_with_paradigm[paradigm]))
    
    if(Net is None):
        p = {
            'small': [0.6, 0.2, 0.2],
            'medium': [0.4, 0.3, 0.3],
            'large': [0.2, 0.4, 0.4]
        }
        Net = choice([PathNet, UpForkNet, DownForkNet], p=p[size])(paradigm, platform, loop=loop)
    
    p = {
            'small': [0.8, 0.15, 0.05],
            'medium': [0.7, 0.2, 0.1],
            'large': [0.6, 0.2, 0.2]
        }
    if(concat_head is None):
        pass
    else:
        heads = Net['head'] if concat_head == '*' else concat_head
        # print('--'*level + '更新了{}个heads中的{}个' .format(len(Net['head']), len(heads))  )
        
        for h in heads:
            Net['head'].remove(h) # 这个h将要被替换为新网络的head，所以先删除它
            N = choice([SourceNet, PathNet, UpForkNet], p=p[size])
            subNet = N(paradigm, platform, loop=loop)
            
            Net['graph'] = JoinNetworks(subNet['graph'], Net['graph'], subNet['tail'][0], h)
            Net['head'].extend(subNet['head'])
            if (N != SourceNet):
                Net = completeNet(Net, subNet['head'], None, level=level+1, size=size)


    if(concat_tail is None):
        pass
    else:
        tails = Net['tail'] if concat_tail == '*' else concat_tail
        for t in tails:
            Net['tail'].remove(t) # 这个t将要被替换为新网络的tail，所以先删除它
            N = choice([ActionNet, PathNet, DownForkNet], p=p[size])
            subNet = N(paradigm, platform, loop=loop)
            Net['graph'] = JoinNetworks(Net['graph'], subNet['graph'], t, subNet['head'][0])
            Net['tail'].extend(subNet['tail'])
            if (N != ActionNet):
                Net = completeNet(Net, None, subNet['tail'], level=level+1, size=size)

    return Net


if __name__ == '__main__':
    H = completeNet()
    nx.draw(H['graph'])
    plt.show()
    