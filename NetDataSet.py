from logging import debug
import os
import os.path as osp

import torch
import networkx as nx

from torch_geometric.data import InMemoryDataset, Data, DataLoader,Dataset
from sklearn.preprocessing import LabelEncoder

from Encoder import operator_encoder, platform_encoder

class NetDataset(InMemoryDataset):
    def __init__(self, root, transform=None, pre_transform=None):
        super(NetDataset, self).__init__(root, transform, pre_transform)
        # 遍历root路径中的每个文件夹，文件夹名为Logical Plan名字，.node为节点， .edge 为边
        self.data, self.slices = torch.load(self.processed_paths[0])

    @property
    def raw_file_names(self):
        return []
    @property
    def processed_file_names(self):
        return ['myData.pt']

    def download(self):
        pass
    
    def process(self): 
        # 读入 cqx 自定义格式的 图文件，转为 Data Object
        data_list = []
        plans_dir = osp.join(self.root, 'generated')
        plans = [f for f in os.listdir(plans_dir) if osp.isfile(osp.join(plans_dir, f))]
        for plan in plans:
            g = nx.read_gpickle(osp.join(plans_dir, plan))
            if(not nx.is_weakly_connected(g)):
                raise RuntimeError('the deserialized DAG is not connected.')
            node_map = {}
            x, y = [], []
            sources, targets = [], []
            idx = 0
            for n in g.nodes:
                node_map[n] = idx
                idx+=1
                code = operator_encoder.encode([n], method='embedding')[0]
                label = platform_encoder.encode([g.nodes[n]['platform']])[0]
                x.append(code)
                y.append(label)
            
            debug_edge_count = 0
            for n1, n2, _ in list(g.edges(data=True)):
                sources.append(node_map[n1])
                targets.append(node_map[n2])
                debug_edge_count += 1

            if(g.number_of_edges() is not debug_edge_count):
                raise RuntimeError('加载边的数量与预期不一致，预期：{}, 加载了{}'.format(g.number_of_edges(), debug_edge_count))

            # for n1, n2, _ in list(g.edges(data=True)):
            #     if (n1 not in node_map): 
            #         n1_code = operator_encoder.encode([n1])
            #         n1_label = platform_encoder.encode([g.nodes[n1]['platform']])
            #         node_map[n1] = idx
            #         x.append(n1_code)
            #         y.append(n1_label)
            #         idx+=1

            #     if (n2 not in node_map): 
            #         n2_code = operator_encoder.encode([n2])
            #         n2_label = platform_encoder.encode([g.nodes[n2]['platform']])
            #         node_map[n2] = idx
            #         x.append(n2_code)
            #         y.append(n2_label)
            #         idx+=1

            #     sources.append(node_map[n1])
            #     targets.append(node_map[n2])

            x = torch.tensor(x, dtype=torch.float)
            y = torch.tensor(y, dtype=torch.long) 

            edge_index = torch.tensor([sources, targets], dtype=torch.long)

            data = Data(x=x, edge_index=edge_index, y=y)
            data_list.append(data)

        data, slices = self.collate(data_list)
        torch.save((data, slices), self.processed_paths[0])