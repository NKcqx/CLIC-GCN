import os
import os.path as osp
import networkx as nx
import matplotlib.pyplot as plt
import copy
import random
import time
import argparse
from numpy.random import choice
from datetime import datetime

import operators, platforms, FSM



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
        'batch': FSM.BatchFSM(),
        'streaming': FSM.StreamingFSM(),
        'linear': FSM.LinearFSM()
    }
    return d[paradigm]

def PathNet(paradigm, platform, loop=0, size='small'):
    fsm = chooseFSM(paradigm)
    pipeline = fsm.produce(scale=size, platform=platform.name, with_source=False, with_sink=False, loop=loop)['opt_seq']
    G = nx.DiGraph()
    for opt in pipeline:
        G.add_node(opt, paradigm=paradigm, platform=platform)
    for i in range(0, len(pipeline)-1):
        G.add_edge(pipeline[i], pipeline[i+1])
        
    return {
        'graph': G,
        'head': [pipeline[0]],
        'tail': [pipeline[-1]],
    }

def UpForkNet(paradigm, platform, size='small', left=None, right=None, junc = None, loop=0):
    '''
        把两个分支用一个juncture连接
        left：左分支，没有的话就随机造，默认包含source
        right：右分支，没有的话就随机造，默认包含source
        junc：juncture
    '''
    upfork_juncture =  [opt for opt in  operators.opts_with_paradigm[paradigm] if opt.num_input == 2 and opt.num_output==1]
    left_graph = PathNet(paradigm, platform, loop=loop) if left is None else left
    right_graph = PathNet(paradigm,platform, loop=loop) if right is None else right
    G  = nx.compose(left_graph['graph'], right_graph['graph'])
    
    junc = choice(upfork_juncture, 1)[0] if junc is None else junc
    junc = copy.deepcopy(junc)
    G.add_node(junc, paradigm=paradigm, platform=platform)

    left_head = getHeads(left_graph['graph'])[0]
    right_head = getHeads(right_graph['graph'])[0]
    G.add_edges_from([(left_head, junc), (right_head, junc)])


    return {
        'graph': G,
        'head': [left_head, right_head],
        'tail': [junc]
    }



def DownForkNet(paradigm, platform, left=None, right=None, junc = None, loop=0, size='small'):
    '''
        把两个分支用一个juncture连接
        left：左分支，没有的话就随机造，默认包含source
        right：右分支，没有的话就随机造，默认包含source
        junc：juncture
    '''
    
    downfork_juncture = [opt for opt in  operators.opts_with_paradigm[paradigm] if opt.num_input == 1 and opt.num_output==2] 
    left_graph = PathNet(paradigm, platform, loop=loop) if left is None else left
    right_graph = PathNet(paradigm, platform, loop=loop) if right is None else right
    G  = nx.compose(left_graph['graph'], right_graph['graph'])

    junc = choice(downfork_juncture, 1)[0] if junc is None else junc
    junc = copy.deepcopy(junc)
    G.add_node(junc, paradigm=paradigm, platform=platform)
    left_head = getHeads(left_graph['graph'])[0]
    right_head = getHeads(right_graph['graph'])[0]
    G.add_edges_from([(junc, left_head), (junc, right_head)])

    return {
        'graph': G,
        'head': [junc],
        'tail': [left_head, right_head]
    }

def SourceNet(paradigm, platform, loop=0, size='small'):
    source_opt = [opt for opt in  operators.all_opts if opt.kind == operators.OperatorKind.SOURCE] 
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
    action_opt = [opt for opt in  operators.all_opts if opt.kind == operators.OperatorKind.ACTION] 
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

    return G

def completeNet(Net=None, concat_head='*', concat_tail='*', level=1, paradigm='*', platform='*', loop=0, size='small'):
    if (paradigm == '*'):
        paradigm = random.choice(['batch', 'streaming', 'linear'])
    if (platform == '*'):
        platform = random.choice(list(platforms.plts_with_paradigm[paradigm]))

    if(Net is None):
        seed_p = { 
            # 起始 Net，pathnet 仅 1 个 head, tail ，以其作为初始网络最终构造的网络规模小
            'local':[0.5, 0.25, 0.25],
            'small': [0.5, 0.25, 0.25],
            'medium': [0.4, 0.3, 0.3],
            'large': [0.2, 0.4, 0.4]
        }
        Net = choice([PathNet, UpForkNet, DownForkNet], p=seed_p[size])(paradigm, platform, loop=loop)
    
    p = {
        # 选择使用哪个 Net 递归合成 新Net 时， source/action, path, upfork
            'local':[0.6, 0.3, 0.1], 
            'small': [0.6, 0.2, 0.2],
            'medium': [0.5, 0.3, 0.2],
            'large': [0.5, 0.25, 0.25]
        }
    if(concat_head != None):
        heads = Net['head'] if concat_head == '*' else concat_head

        for h in heads:
            if(h.num_input != 0 and  h.num_output != 0):
                Net['head'].remove(h) # 这个h将要被替换为新网络的head，所以先删除它
                N = choice([SourceNet, PathNet, UpForkNet], p=p[size])
                subNet = N(paradigm, platform, loop=loop)
                
                Net['graph'] = JoinNetworks(subNet['graph'], Net['graph'], subNet['tail'][0], h)
                Net['head'].extend(subNet['head'])
                if (N != SourceNet):
                    Net = completeNet(Net, subNet['head'], None, level=level+1, size=size)

    if(concat_tail != None):
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

def create():
    parser = argparse.ArgumentParser(description='Generate all kinds of Networks.')
    parser.add_argument('paradigm', metavar='paradigm', type=str, nargs='+',
                help='network paradigms: [`batch`, `streaming`, `linear`].')
    parser.add_argument('size', metavar='size', type=str, nargs='+',
                help='network size: [`local`, `small`, `medium`, `large`].')
    parser.add_argument('amount', metavar='amount', type=int, nargs='+',
                help='the number of generated networks in total')
    
    args = parser.parse_args()
    
    paradigm= args.paradigm[0]
    size= args.size[0]
    amount = args.amount[0]
    t = time.strftime("%Y-%m-%d", time.localtime())
    path = osp.join(os.getcwd(), 'data', 'Logical Plans', 'generated', paradigm, t+'_'+size)
    for id in range(amount):
            G = completeNet()['graph']
            nx.write_gpickle(G, path+'_'+str(id))


def sample():
    H = completeNet(size='small')
    nx.draw(H['graph'])
    plt.show()


if __name__ == '__main__':
    create()
    