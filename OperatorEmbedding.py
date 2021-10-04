import numpy as np
import gensim
import random
import networkx as nx
import matplotlib.pyplot as plt
import os

from gensim.models.word2vec import Word2Vec

import platforms, NetCreator

embedding_size = 8
window = 5
min_count = 1
initial_lr = 0.001

def getModel(path):
    if (os.path.exists(path)):
        model = Word2Vec.load(path)
        return model
    else:
        print('未找到预训练模型，准备生成数据并重新训练...')
        sentences = []
        print('正在生成Batch Processing数据...')
        for times in range(1, 300):
            G = NetCreator.completeNet(paradigm='batch', platform='*', size='small', loop=random.randint(0, 10))['graph']
            l = list(nx.topological_sort(G))
            sentence = [o.name for o in l]
            sentences.append(sentence)

        for times in range(1, 600):
            plts = [platforms.storm, platforms.spark, platforms.flink, platforms.hadoop]
            plt = np.random.choice(plts)
            G = NetCreator.completeNet(paradigm='batch', platform=plt, size='medium', loop=random.randint(10, 30))['graph']
            l = list(nx.topological_sort(G))
            sentence = [o.name for o in l]
            sentences.append(sentence)

        for times in range(1, 300):
            plts = [platforms.spark, platforms.flink]
            plt = np.random.choice(plts)
            G = NetCreator.completeNet(paradigm='batch', platform=plt, size='large', loop=random.randint(30, 50))['graph']
            l = list(nx.topological_sort(G))
            sentence = [o.name for o in l]
            sentences.append(sentence)
        print('done.')
        ################
        ##  Streaming
        ################
        
        print('正在生成Streaming Processing数据...')
        for times in range(1, 300):
            G = NetCreator.completeNet(paradigm='streaming', platform='*', size='small', loop=random.randint(0, 10))['graph']
            l = list(nx.topological_sort(G))
            sentence = [o.name for o in l]
            sentences.append(sentence)

        for times in range(1, 600):
            plts = [platforms.flink, platforms.samza, platforms.storm, platforms.kafka, platforms.flume, platforms.spark]
            plt = np.random.choice(plts)
            G = NetCreator.completeNet(paradigm='streaming', platform=plt, size='medium', loop=random.randint(10, 30))['graph']
            l = list(nx.topological_sort(G))
            sentence = [o.name for o in l]
            sentences.append(sentence)

        for times in range(1, 300):
            plts = [platforms.flink, platforms.storm, platforms.kafka, platforms.spark]
            plt = np.random.choice(plts)
            G = NetCreator.completeNet(paradigm='streaming', platform=plt, size='large', loop=random.randint(30, 50))['graph']
            l = list(nx.topological_sort(G))
            sentence = [o.name for o in l]
            sentences.append(sentence)
        print('done.')
        ################
        ##  Linear
        ################
        print('正在生成Linear Algebra数据...')
        for times in range(1, 200):
            G = NetCreator.completeNet(paradigm='linear', platform='*', size='small', loop=random.randint(0, 100))['graph']
            l = list(nx.topological_sort(G))
            sentence = [o.name for o in l]
            sentences.append(sentence)

        for times in range(1, 400):
            plts = [platforms.tensorflow, platforms.pytorch, platforms.theano , platforms.mxnet, platforms.keras]
            plt = np.random.choice(plts)
            G = NetCreator.completeNet(paradigm='linear', platform=plt, size='medium', loop=random.randint(100, 500))['graph']
            l = list(nx.topological_sort(G))
            sentence = [o.name for o in l]
            sentences.append(sentence)

        for times in range(1, 200):
            plts = [platforms.tensorflow, platforms.pytorch]
            plt = np.random.choice(plts)
            G = NetCreator.completeNet(paradigm='linear', platform=plt, size='large', loop=random.randint(500, 1000))['graph']
            l = list(nx.topological_sort(G))
            sentence = [o.name for o in l]
            sentences.append(sentence)
        print('done.')
        
        print('开始训练模型...')
        model = gensim.models.Word2Vec(
            sentences=sentences, 
            size=embedding_size, 
            window=window, 
            min_count=min_count,
            negative=0,
            alpha=initial_lr
            )
        print('模型训练完成: ', os.path.abspath(path))
        model.save(path)
        return model

def get_embedding(names):
    path = os.path.join(os.getcwd(), 'data', 'operator_embedding.ebd') 
    if (os.path.exists(path)):
        model = Word2Vec.load(path)
        return [model[name] for name in names]
    else:
        raise 'No W2V model found in specific dir {}'.format(path)

if __name__ == '__main__':
    getModel(os.path.join(os.getcwd(), 'data', 'operator_embedding.ebd') )
