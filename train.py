# -*- coding: utf-8 -*-
import os.path as osp
import os
import argparse
import random
import pandas as pd
import math
import logging
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
model_path = os.path.join(os.getcwd(),'data', 'gcn.pt') 
data_folder_path = os.path.join(os.getcwd(), 'data', 'Logical Plans') 

# if utils.is_main_process():
writer = SummaryWriter(os.path.join(os.getcwd(), 'log/10.13_16emb_512gcn_4096epoch') )



batch_size = 16
train_val_ratio = 0.8
learning_rate = 0.0001
num_epoch = 4096
embedding_size = 512 # GCN 中的 embedding 大小，非 operator embedding
dataset = NetDataSet.NetDataset(data_folder_path)
dataset = dataset.shuffle()

input_size = dataset.num_features
num_label = dataset.num_classes
model = MyGCN.MyGCN(input_dim=input_size, num_classes=num_label, embedding_dim=embedding_size)
# model.load_state_dict(torch.load(model_path))

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
