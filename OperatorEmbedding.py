from genericpath import isdir
import numpy as np
import gensim
import random
import networkx as nx
import matplotlib.pyplot as plt
import os
import os.path as osp


from gensim.models.word2vec import Word2Vec

import platforms, NetCreator

embedding_size = 8
window = 3
min_count = 1
initial_lr = 0.0001

def train_model(model_path, emb_dim=8, window=3, min_count=1, initial_lr=0.0001):
    sentences = []
    plans_dir = osp.join(os.getcwd(), 'data', 'Logical Plans' , 'generated')
    for paradigm in os.listdir(plans_dir):
        if(osp.isdir(osp.join(plans_dir, paradigm))):
            pp = osp.join(plans_dir, paradigm)
            plans = [f for f in os.listdir(pp) if osp.isfile(osp.join(pp, f))]
            print("正在加载 {} 类型的数据".format(paradigm))
            count = 0
            for plan in plans:
                graph_path = osp.join(pp, plan)
                # print(graph_path)
                g = nx.read_gpickle(graph_path)
                l = list(nx.topological_sort(g))
                sentence = [o.name for o in l]
                sentences.append(sentence)
                count += 1

    print("数据加载完毕, 开始训练模型...")
    
    model = gensim.models.Word2Vec(
        sentences=sentences, 
        size=emb_dim, 
        window=window, 
        min_count=min_count,
        negative=0,
        alpha=initial_lr
        )
    print('模型训练完成, 保存路径: ', os.path.abspath(model_path))
    model.save(model_path)
    return model

def getModel(path):
    if (os.path.exists(path)):
        model = Word2Vec.load(path)
        return model
    else:
        print('未找到预训练模型，准备记载数据并重新训练...')
        return train_model(path, embedding_size, window, min_count, initial_lr)


def get_embedding(names):
    path = os.path.join(os.getcwd(), 'data', 'operator_embedding.ebd') 
    if (os.path.exists(path)):
        model = Word2Vec.load(path)
        return [model[name] for name in names]
    else:
        raise 'No W2V model found in specific dir {}'.format(path)

if __name__ == '__main__':
    getModel(os.path.join(os.getcwd(), 'data', 'operator_embedding8.ebd') )
