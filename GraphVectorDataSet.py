import os
import torch
import pandas as pd
import numpy as np
import os.path as osp
import networkx as nx   
from torch.utils.data import Dataset, DataLoader

from NetCreator import UpForkNet

class GraphVectorDataset(Dataset):
    """Face Landmarks dataset."""

    def __init__(self, root_dir, transform):
        self.transform = transform
        self.datafile_list = []
        plans_dir = osp.join(root_dir, 'generated')
        paradigms = [d for d in os.listdir(plans_dir) if osp.isdir(osp.join(plans_dir, d)) ]
        self.num_feature = transform.num_feature
        self.num_label = transform.num_label
        for para in paradigms:
            pp = osp.join(plans_dir, para)
            # 所有的待读取文件的全路径
            for f in os.listdir(pp):
                path = osp.join(pp, f)
                if (osp.isfile(path)):
                    self.datafile_list.append(path)

    def __len__(self):
        return len(self.datafile_list)

    def __getitem__(self, idx):
        if torch.is_tensor(idx):
            idx = idx.tolist()
        graph_paths = self.datafile_list[idx]
        graph_vecs = []
        for graph in graph_paths:
            with open(graph, 'rb') as f:
                g = nx.read_gpickle(f)
            if(not nx.is_weakly_connected(g)):
                raise RuntimeError('the deserialized DAG is not connected.')
            if self.transform:
                g = self.transform(g)
            graph_vecs.append(g)
        return graph_vecs