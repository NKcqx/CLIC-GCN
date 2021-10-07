import os
import scipy as sp
import torch

from sklearn import preprocessing

import operators, platforms, OperatorEmbedding

class OperatorEncoder:

    def __init__(self) -> None:
        self.label_encoder = preprocessing.LabelEncoder()
        self.label_encoder.fit([o.name for o in operators.all_opts])

        self.onehot_encoder = preprocessing.OneHotEncoder(sparse=False)
        self.onehot_encoder.fit([[o.name] for o in operators.all_opts])
        p = os.path.join(os.getcwd(), 'data', 'operator_embedding.ebd') 
        self.embedding_model = OperatorEmbedding.getModel(p)


    def encode(self, opts, method='onehot'):
        if type(opts) != type([]):
            raise TypeError
        
        if (method == 'onehot'):
            return self.onehot_encoder.transform([[o.name] for o in opts])
        elif (method == 'embedding'):
            return [self.embedding_model[o.name] for o in opts]

class PlatformEncoder:
    
    def __init__(self) -> None:
        self.label_encoder = preprocessing.LabelEncoder()
        self.label_encoder.fit([p.name for p in platforms.all_plt])

        self.onehot_encoder = preprocessing.OneHotEncoder(sparse=False)
        self.onehot_encoder.fit([[p.name] for p in platforms.all_plt])



    def encode(self, plts, method='onehot'):
        if type(plts) != type([]):
            raise TypeError
        return self.label_encoder.transform([p.name for p in plts])
       


operator_encoder = OperatorEncoder()
platform_encoder = PlatformEncoder()

