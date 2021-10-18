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
from torch import utils
import torch.nn.functional as F
import torch_geometric.transforms as T



from sklearn import metrics
from torch_geometric.data import DataLoader
from torch.utils.tensorboard import SummaryWriter

import MyGCN, NetDataSet

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
parser.add_argument('-mp', '--model_path', type=str, default=os.path.join(os.getcwd(),'data', 'gcn.pt') ,
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

dataset = NetDataSet.NetDataset(data_folder_path)
dataset = dataset.shuffle()
input_size = dataset.num_features
num_label = dataset.num_classes
model = MyGCN.MyGCN(input_dim=input_size, num_classes=num_label, embedding_dim=embedding_size)
# model.load_state_dict(torch.load(model_path))

t = time.strftime("%m.%d", time.localtime())
writer = SummaryWriter(os.path.join(os.getcwd(), 'log/{}_{}emb_{}gcn_{}epoch'.format(t, opt_embedding_size, embedding_size, num_epoch)) )

logger.info('训练数据集大小：', len(dataset))


# train_dataset = dataset[:math.floor(len(dataset) * train_val_ratio)]
# val_dataset = dataset[math.floor(len(dataset) * train_val_ratio): ]
train_dataset = dataset
train_loader = DataLoader(train_dataset, batch_size=batch_size)

device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
model = model.to(device)

optimizer = torch.optim.Adam([
    dict(params=model.conv1.parameters(), weight_decay=5e-4),
    dict(params=model.conv2.parameters(), weight_decay=0)
], lr=learning_rate)

def train():
    model.train()

    loss_all = 0
    for data in train_loader:
        data = data.to(device)
        optimizer.zero_grad()
        output = model(data)
        label = data.y.to(device)
        loss = F.nll_loss(output, label)
        # auc_roc = metrics.roc_auc_score(output, label, multi_class='ovr')
        loss.backward()
        loss_all += data.num_graphs * loss.item()
        optimizer.step()
    
    return loss_all / len(train_dataset)

for epoch in range(num_epoch):
    loss = train()
    print('Epoch:{}, Loss:{}'.format(epoch, loss))
    writer.add_scalar('train_loss', loss, epoch)


writer.close()

torch.save(model.state_dict(), model_path)
