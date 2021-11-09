import torch
import platforms, operators
import networkx as nx



class Graph2Vec(object):

    def __init__(self):
        self.operator_features = {}
        for opt in operators.all_opts:
            self.operator_features[opt.name] = {
                'topo': [0, 0, 0], # pipeline, upfork, downfork
                'plt': [0 for _ in range(len(platforms.all_plts))],
                'context': 0
            }
        # 3 是 'topo' 种类数，前面的是 global 的 topo, 后面的是每个运算符的 topo
        self.num_feature = 3 + (len(platforms.all_plts) + 3)* len(operators.all_opts)
        self.num_label = len(platforms.all_plts) 
    
    def __call__(self, graph:nx.DiGraph):

        global_topo_feature = [0,0,0] # pipeline, upfork, downfork
        
        for n in graph.nodes:
            plt = graph.nodes[n]['platform']
            opt_feature = self.operator_features.get(n.name)
            topo_type = 0 if n.num_input == 1 and n.num_output == 1 else 1 if n.num_input == 1 and n.num_output == 2 else 2
            plt_type = 0 if plt.name == 'spark' else 1 # TODO:
            # 理论上要使用 platform_encoder 的one-hot 来编码，但实验中只有两个平台，因此直接spark=0, Hadoop=1
            # plt_code = platform_encoder.encode([graph.nodes[n]['platform']], method='onehot')[0]
            opt_feature['topo'][topo_type] += 1
            global_topo_feature[topo_type] += 1
            opt_feature['topo'][plt_type]  += 1

            label = [0, 1] if plt.name == 'spark' else [1, 0] # TODO:

        # union operator_freatures to a flat vector
        graph_vector = global_topo_feature
        for _, v in self.operator_features.items():
            graph_vector.extend(v['topo'])
            graph_vector.extend(v['plt'])

        return {'graph_vec': torch.tensor(graph_vector, dtype=torch.float32), 'label': torch.tensor(label, dtype=torch.float32)}
