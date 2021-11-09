# -*- coding: utf-8 -*-
import os.path as osp
import os
import argparse
import random
import pandas as pd
import math
import logging
import time
import torch
import torch.nn.functional as F
import torch_geometric.transforms as T
import MyGCN, MyLR, NetDataSet, GraphVectorDataSet, Transformer

from torch import utils
from sklearn import metrics
from torch.utils.data import DataLoader
from torch.utils.tensorboard import SummaryWriter


logger = logging.Logger('training logger')
logger.setLevel(10)


parser = argparse.ArgumentParser(description='Generate all kinds of Networks.')

parser.add_argument('-b', '--batch_size', type=int, default=16,
            help='Training batch size.')
parser.add_argument('-emb', '--emb_dim', type=int, default=512,
            help='Embedding Dimension used in the GCN Network.')
parser.add_argument('-e', '--epoch', type=int, default=512,
            help='Training epoch.')   
parser.add_argument('-lr', '--learning_rate', type=float, default=0.0001,
            help='Learning rate.')                
parser.add_argument('-mp', '--model_path', type=str, default=os.path.join(os.getcwd(),'data', 'lr.pt') ,
            help='The path where the trained gcn model will be stored.')
parser.add_argument('-dp', '--data_path', type=str, default=os.path.join(os.getcwd(), 'data', 'Logical Plans') ,
            help='The train/val/test dataset path.')
args = parser.parse_args()


batch_size = args.batch_size
learning_rate = args.learning_rate
num_epoch = args.epoch
embedding_size = args.emb_dim
opt_embedding_size = 16
model_path = args.model_path
data_folder_path = args.data_path

checkpoint_interval = 500 # 每 500 个 epoch save 一次

transform = Transformer.Graph2Vec()
dataset = GraphVectorDataSet.GraphVectorDataset(data_folder_path, transform)
input_size = dataset.num_feature
output_size = dataset.num_label
model = MyLR.LinearRegression(input_dim=input_size, output_dim=output_size)
criterion = torch.nn.MSELoss()
optimizer = torch.optim.Adam(model.parameters(), lr=learning_rate)

# load checkpoint
if(os.path.exists(model_path)):
    checkpoint = torch.load(model_path)
    model.load_state_dict(torch.load(model_path))
    optimizer.load_state_dict(checkpoint['optimizer_state_dict'])
    epoch = checkpoint['epoch']
    loss = checkpoint['loss']

if torch.cuda.is_available():
    model.cuda()

t = time.strftime("%m.%d", time.localtime())
writer = SummaryWriter(os.path.join(os.getcwd(), 'log/{}_{}emb_{}gcn_{}epoch'.format(t, opt_embedding_size, embedding_size, num_epoch)) )

train_val_ratio = 0.8
train_dataset = dataset[:math.floor(len(dataset) * train_val_ratio)]
test_dataset = dataset[math.floor(len(dataset) * train_val_ratio): ]
# train_dataset = dataset
train_loader = DataLoader(train_dataset, batch_size=batch_size, shuffle=True)
test_loader = DataLoader(test_dataset, batch_size=batch_size, shuffle=True)



def train():
    model.train()
    loss_all = 0
    count = 0
    for data in train_loader:
        if torch.cuda.is_available():
            data.cuda()
        optimizer.zero_grad()
        output = model(data['graph_vec'])
        label = data['label']
        if torch.cuda.is_available():
            label.cuda()
        loss = criterion(output, label)
        loss.backward()
        loss_all += loss.item()
        count +=1
        optimizer.step()
    
    return loss_all / count

def evaluate():
    model.eval()
    total_loss = 0
    count = 0
    with torch.no_grad():
        for data in test_loader:
            output = model(data['graph_vec'])
            label = data['label']
            loss = criterion(output, label)
            total_loss += loss.item()
            count += 1
    
    return total_loss / count 

for epoch in range(num_epoch):
    loss = train()
    eval_loss = evaluate()
    print('Epoch:{}, Train Loss: {}, Test Loss: {}'.format(epoch, loss, eval_loss))
    writer.add_scalar('train_loss', loss, epoch)
    if epoch % checkpoint_interval == 0: 
        print("Saving check point to {}".format(model_path))
        torch.save({
                'epoch': epoch,
                'model_state_dict': model.state_dict(),
                'optimizer_state_dict': optimizer.state_dict(),
                'loss': loss,
                }, model_path)


writer.close()

torch.save({
            'epoch': num_epoch,
            'model_state_dict': model.state_dict(),
            'optimizer_state_dict': optimizer.state_dict(),
            'loss': loss,
            }, model_path)