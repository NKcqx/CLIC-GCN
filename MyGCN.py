
import argparse
import random
import pandas as pd


import torch
import torch.nn.functional as F
import torch_geometric.transforms as T
import torch_geometric.utils as tg_utils

from torch_geometric.nn import GCNConv, ChebConv



class MyGCN(torch.nn.Module):
    def __init__(self, input_dim, num_classes, embedding_dim=16):
        super(MyGCN, self).__init__()
        self.conv1 = GCNConv(input_dim, embedding_dim,
                             normalize=True)
        self.conv2 = GCNConv(embedding_dim, num_classes,
                             normalize=True)

    def forward(self, data):
        # try:
            x, edge_index, edge_weight = data.x, data.edge_index, data.edge_attr
            conv1_res = self.conv1(x, edge_index, edge_weight)
            x = F.relu(conv1_res)
            x = F.dropout(x, training=self.training)
            x = self.conv2(x, edge_index, edge_weight)
            return F.log_softmax(x, dim=1)
        # except IndexError as e:
        #     print(e.with_traceback)




# model = MyGCN()
# model.load_state_dict(torch.load("/Users/jason/Documents/Study_Study/DASLab/Cross_Platform_Compute/practice/CLIC-GCN/myGCN.pt")))

# dataset = MyDataset("/Users/jason/Documents/Study_Study/DASLab/Cross_Platform_Compute/practice/CLIC-GCN/data/Logical Plans")
# dataset = dataset.shuffle()
# train_dataset = dataset[:8]
# val_dataset = dataset[8:9]
# test_dataset = dataset[9:]
# len(train_dataset), len(val_dataset), len(test_dataset)

# train_loader = DataLoader(train_dataset, batch_size=2)
# device = torch.device('cpu')
# model = MyGCN().to(device)
# optimizer = torch.optim.Adam([
#     dict(params=model.conv1.parameters(), weight_decay=5e-4),
#     dict(params=model.conv2.parameters(), weight_decay=0)
# ], lr=0.01)  # Only perform weight-decay on first convolution.


# for epoch in range(10):
#     print(train())

# torch.save(model.state_dict(), "/Users/jason/Documents/Study_Study/DASLab/Cross_Platform_Compute/practice/CLIC-GCN/myGCN.pt")