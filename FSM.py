import random
import copy

from numpy.random import choice
from enum import Enum

import operators, platforms
from operators import OperatorKind

class MachineState(Enum): # ML FSM
    START       = 0
    END         = 1
    SOURCE      = 2
    CALCULATOR  = 3
    DISPATCHER  = 4
    LOCATOR     = 5
    SAMPLER     = 6
    ACTION      = 7
    LOOP        = 8

class FSM:

    def __init__(self, name="fsm"):
        self.name = name

    def getRandomOpt(self, opt_list, statement = lambda x : True):
        valid_opts = [copy.deepcopy(opt) for opt in opt_list if statement(opt)]
        res = random.choice(valid_opts)
        return res

    def getOptRandomPlt(self, opt, plt_list):
        plt_list_names = [p.name for p in plt_list]
        while(True):
            plt = random.choice(opt.supported_platforms)
            if plt.name in plt_list_names:
                return plt
    
    def supportedPltNOpt(self, paradigm, platform_list='*'):
        # 先找到所有属于batch的、且受 platform_list 中的平台支持的运算符
        if (platform_list=='*'):
            supported_opts = operators.opts_with_paradigm[paradigm]
            supported_plts = platforms.plts_with_paradigm[paradigm]
        else:
            supported_plts = [p for p in platforms.plts_with_paradigm[paradigm] if p.name in platform_list]
            supported_opts = [op for op in operators.opts_with_paradigm[paradigm] if len(set(supported_plts) & set(op.supported_platforms)) > 0]

        return supported_opts, supported_plts

    def produce(self):
        raise NotImplementedError('无法直接调用基类 produce 方法.')

class BatchFSM(FSM):
    def __init__(self):
        super().__init__("BatchFSM")
        self.scale = {
            'local': {
                'transition': {
                    MachineState.CALCULATOR:[0.7, 0.1, 0.2], # calculator -> calculator, loop, action
                    MachineState.LOOP:[0.4, 0.6], # loop -> calculator, action
                    },
                'platforms': platforms.plts_with_scale['local']
            },
            'small': {
                'transition': {
                    MachineState.CALCULATOR:[0.7, 0.2, 0.1], # calculator -> calculator, loop, action
                    MachineState.LOOP:[0.5, 0.5], # loop -> calculator, action
                    },
                'platforms': platforms.plts_with_scale['small']
            },
            'medium': {
                'transition': {
                    MachineState.CALCULATOR:[0.9, 0.05, 0.05], # calculator -> calculator, loop, action
                    MachineState.LOOP:[0.6, 0.4], # loop -> calculator, action
                    },
                'platforms': platforms.plts_with_scale['medium']
            },
            'large': {
                'transition': {
                    MachineState.CALCULATOR:[0.9, 0.09, 0.01], # calculator -> calculator, loop, action
                    MachineState.LOOP:[0.7, 0.3], # loop -> calculator, action
                    },
                'platforms': platforms.plts_with_scale['large']
            },
        }
        # self.supported_opts, self.supported_plts = self.supportedPltNOpt()

    def supportedPltNOpt(self, platform_list='*'):
        return super(BatchFSM, self).supportedPltNOpt('batch', platform_list)
    
    def getRandomOpt(self, statement = lambda x : True):
        return super(BatchFSM, self).getRandomOpt(operators.opts_with_paradigm['batch'], statement)
    


    def produce(self, scale ='local', platform='*', **args):
        if(platform == '*'):
            plts = [p for p in self.supportedPltNOpt()[1] if scale in p.scale] 
            platform = random.choice(plts)
        else:
            platform  = platforms.all_plt_dic.get(platform)

        opt_seq = []
        
        state = MachineState.START ## 初始状态
        while(True):
            if (state == MachineState.START):
                state = MachineState.SOURCE
                continue
            if (state == MachineState.SOURCE):
                if (not args.get('with_source', False)):
                    state = MachineState.CALCULATOR
                    continue
                opt = self.getRandomOpt(statement= lambda op : op.kind == OperatorKind.SOURCE)
                opt_seq.append(opt)
                state = MachineState.CALCULATOR
                continue

            if (state == MachineState.CALCULATOR):
                opt = self.getRandomOpt(statement= lambda op : op.kind == OperatorKind.CALCULATOR and op.num_input==1 and op.num_output==1 )
                opt_seq.append(opt)
                state_transition_p = self.scale[scale]['transition'][MachineState.CALCULATOR]
                state = choice([MachineState.CALCULATOR, MachineState.LOOP, MachineState.ACTION], 1, p=state_transition_p)[0]
                continue

            if  (state == MachineState.LOOP):
                p = self.scale[scale]['transition'][MachineState.CALCULATOR]
                p[0]-=0.01
                p[2]+=0.01
                self.scale[scale]['transition'][MachineState.CALCULATOR] = p
                loop_body = self.produce(scale, platform, with_source = False, with_sink = False)['opt_seq']
                # batchFSM(platform_list, loop=0, with_source = False, with_sink = False)
                
                for _ in range(args.get('loop', 1)):
                    opt_seq.extend(copy.deepcopy(opt) for opt in loop_body)
                
                state_transition_p = self.scale[scale]['transition'][MachineState.LOOP]
                state = choice([MachineState.CALCULATOR, MachineState.ACTION], 1, p=state_transition_p)[0]
                continue
            
            if  (state == MachineState.ACTION):
                if (not args.get('with_sink', False)):
                    state = MachineState.END
                    continue
                opt = self.getRandomOpt(statement=lambda op: op.kind==OperatorKind.ACTION)
                opt_seq.append(opt)
                state = MachineState.END
                continue
            if (state == MachineState.END):
                break
        return {
            'opt_seq': opt_seq,
            'platform': platform
        } 


class StreamingFSM(FSM):
    def __init__(self):
        super().__init__("StreamingFSM")
        self.scale = {
            'local': {
                'transition': { 
                    MachineState.CALCULATOR:[0.2, 0.4, 0.1, 0.3], # calculator -> calculator, locator,  loop, dispatcher
                    MachineState.LOOP:[0.2, 0.8], # loop -> calculator, action
                    MachineState.LOCATOR: [0.4, 0.6] # locator -> calculator, dispatcher
                    },
                'platforms': platforms.plts_with_scale['local']
            },
            'small': {
                'transition': {
                    MachineState.CALCULATOR:[0.45, 0.4, 0.1, 0.05], # calculator -> calculator, locator,  loop, dispatcher
                    MachineState.LOOP:[0.3, 0.7], # loop -> calculator, action
                    MachineState.LOCATOR: [0.5, 0.5] # locator -> calculator, dispatcher
                    },
                'platforms': platforms.plts_with_scale['small']
            },
            'medium': {
                'transition': {
                    MachineState.CALCULATOR:[0.6, 0.3, 0.09, 0.01], # calculator -> calculator, locator,  loop, dispatcher
                    MachineState.LOOP:[0.4, 0.6], # loop -> calculator, action
                    MachineState.LOCATOR: [0.5, 0.5] # locator -> calculator, dispatcher
                    },
                'platforms': platforms.plts_with_scale['medium']
            },
            'large': {
                'transition': {
                    MachineState.CALCULATOR:[0.8, 0.1, 0.09, 0.01], # calculator -> calculator, locator,  loop, dispatcher
                    MachineState.LOOP:[0.8, 0.2], # loop -> calculator, action
                    MachineState.LOCATOR: [0.8, 0.2] # locator -> calculator, dispatcher
                    },
                'platforms': platforms.plts_with_scale['large']
            },
        }

    def supportedPltNOpt(self, platform_list='*'):
        return super(StreamingFSM, self).supportedPltNOpt('streaming', platform_list)
    
    def getRandomOpt(self, statement = lambda x : True):
        return super(StreamingFSM, self).getRandomOpt(operators.opts_with_paradigm['streaming'], statement)

    def produce(self, scale ='local', platform='*', **args):
        if(platform == '*'):
            plts = [p for p in self.supportedPltNOpt()[1] if scale in p.scale] 
            platform = random.choice(plts)
        else:
            platform = platforms.all_plt_dic.get(platform)

        opt_seq = []
        
        state = MachineState.START ## 初始状态
        while(True):
            if (state == MachineState.START):
                state = MachineState.SOURCE
                continue
            if (state == MachineState.SOURCE):
                if (not args.get('with_source', False)):
                    state = MachineState.CALCULATOR
                    continue
                opt = self.getRandomOpt(statement= lambda op : op.kind == OperatorKind.SOURCE)
                opt_seq.append(opt)
                state = MachineState.CALCULATOR
                continue

            if (state == MachineState.CALCULATOR):
                opt = self.getRandomOpt(
                    statement= lambda op: op.kind == operators.OperatorKind.CALCULATOR 
                    and op.num_input==1 
                    and op.num_output==1
                    )
                opt_seq.append(opt)
                state_transition_p = self.scale[scale]['transition'][MachineState.CALCULATOR]
                state = choice([
                    MachineState.CALCULATOR, MachineState.LOCATOR, MachineState.LOOP,  MachineState.DISPATCHER], 
                    1, 
                    p=state_transition_p)[0]
                continue
            
            if  (state == MachineState.LOOP):
                p = self.scale[scale]['transition'][MachineState.CALCULATOR]
                p[0]-=0.01
                p[-1]+=0.01
                loop_body = self.produce(scale, platform, with_source = False, with_sink = False)['opt_seq']
                for _ in range(args.get('loop', 1)):
                    opt_seq.extend(copy.deepcopy(opt) for opt in loop_body)
                
                state_transition_p = self.scale[scale]['transition'][MachineState.LOOP]
                state = choice([MachineState.CALCULATOR, MachineState.ACTION], 1, p=state_transition_p)[0]
                continue


            if (state == MachineState.LOCATOR):
                opt = self.getRandomOpt(statement= lambda op : op.kind == OperatorKind.LOCATOR)
                opt_seq.append(opt)
                state_transition_p = self.scale[scale]['transition'][MachineState.LOCATOR]
                state = choice([MachineState.CALCULATOR, MachineState.ACTION], 1, p=state_transition_p)[0]
                continue

            if (state == MachineState.DISPATCHER):
                opt = self.getRandomOpt(statement= lambda op : op.kind == OperatorKind.DISPATCHER)
                opt_seq.append(opt)
                state = MachineState.ACTION
                continue
            
            if  (state == MachineState.ACTION):
                if (not args.get('with_sink', False)):
                    state = MachineState.END
                    continue
                opt = self.getRandomOpt(statement= lambda op : op.kind == OperatorKind.ACTION)
                opt_seq.append(opt)
                state = MachineState.END
                continue
            if (state == MachineState.END):
                break
        
        return {
            'opt_seq': opt_seq,
            'platform': platform
        } 

class LinearFSM(FSM):
    def __init__(self):
        super().__init__("LinearFSM")
        self.scale = {
            'local': {
                'transition': {
                    MachineState.SAMPLER:[0.2, 0.4, 0.4], # sampler -> sampler, locator, calculator
                    MachineState.CALCULATOR:[0.6, 0.2, 0.1, 0.1], # calculator -> calculator, locator, loop, action
                    MachineState.LOOP:[0.6, 0.4], # loop -> calculator, action
                    MachineState.LOCATOR: [0.5, 0.5] # locator -> calculator, action
                    },
                'platforms': platforms.plts_with_scale['local']
            },
            'small': {
                'transition': {
                    MachineState.SAMPLER:[0.2, 0.4, 0.4], # sampler -> sampler, locator, calculator
                    MachineState.CALCULATOR:[0.7, 0.15, 0.05, 0.1], # calculator -> calculator, locator, loop, action
                    MachineState.LOOP:[0.7, 0.3], # loop -> calculator, action
                    MachineState.LOCATOR: [0.5, 0.5] # locator -> calculator, action
                    },
                'platforms': platforms.plts_with_scale['small']
            },
            'medium': {
                'transition': {
                    MachineState.SAMPLER:[0.2, 0.4, 0.4], # sampler -> sampler, locator, calculator
                    MachineState.CALCULATOR:[0.7, 0.09, 0.2, 0.01], # calculator -> calculator, locator, loop, action
                    MachineState.LOOP:[0.8, 0.2], # loop -> calculator, action
                    MachineState.LOCATOR: [0.8, 0.2] # locator -> calculator, action
                    },
                'platforms': platforms.plts_with_scale['medium']
            },
            'large': {
                'transition': {
                    MachineState.SAMPLER:[0.2, 0.4, 0.4], # sampler -> sampler, locator, calculator
                    MachineState.CALCULATOR:[0.7, 0.09, 0.2, 0.01], # calculator -> calculator, locator, loop, action
                    MachineState.LOOP:[0.9, 0.1], # loop -> calculator, action
                    MachineState.LOCATOR: [0.9, 0.1] # locator -> calculator, action
                    },
                'platforms': platforms.plts_with_scale['large']
            },
        }

    def supportedPltNOpt(self, platform_list='*'):
        return super(LinearFSM, self).supportedPltNOpt('linear', platform_list)
    
    def getRandomOpt(self, statement = lambda x : True):
        return super(LinearFSM, self).getRandomOpt(operators.opts_with_paradigm['linear'], statement)

    def produce(self, scale ='local', platform='*', **args):
        if(platform == '*'):
            plts = [p for p in self.supportedPltNOpt()[1] if scale in p.scale] 
            platform = random.choice(plts)
        else:
            platform = platforms.all_plt_dic.get(platform)

        opt_seq = []
        
        state = MachineState.START ## 初始状态
        while(True):
            if (state == MachineState.START):
                state = MachineState.SOURCE
                continue
            if (state == MachineState.SOURCE):
                if (not args.get('with_source', False)):
                    state = MachineState.CALCULATOR
                    continue
                opt = self.getRandomOpt(statement= lambda op : op.kind == OperatorKind.SOURCE)
                opt_seq.append(opt)
                state = MachineState.CALCULATOR
                continue

            if (state == MachineState.CALCULATOR):
                opt = self.getRandomOpt(
                    statement= lambda op: op.kind == operators.OperatorKind.CALCULATOR 
                    and op.num_input==1 
                    and op.num_output==1
                    )
                opt_seq.append(opt)
                state_transition_p = self.scale[scale]['transition'][MachineState.CALCULATOR]
                state = choice([
                    MachineState.CALCULATOR, MachineState.LOCATOR, MachineState.LOOP,  MachineState.ACTION], 
                    1, 
                    p=state_transition_p)[0]
                continue
            
            if  (state == MachineState.LOOP):
                p = self.scale[scale]['transition'][MachineState.CALCULATOR]
                p[0]-=0.01
                p[-1]+=0.01
                loop_body = self.produce(scale, platform, with_source = False, with_sink = False)['opt_seq']
                for _ in range(args.get('loop', 1)):
                    opt_seq.extend(copy.deepcopy(opt) for opt in loop_body)
                state_transition_p = self.scale[scale]['transition'][MachineState.LOOP]
                state = choice([MachineState.CALCULATOR, MachineState.ACTION], 1, p=state_transition_p)[0]
                continue


            if (state == MachineState.LOCATOR):
                opt = self.getRandomOpt(statement= lambda op : op.kind == OperatorKind.LOCATOR)
                opt_seq.append(opt)
                state_transition_p = self.scale[scale]['transition'][MachineState.LOCATOR]
                state = choice([MachineState.CALCULATOR, MachineState.ACTION], 1, p=state_transition_p)[0]
                continue
            
            if  (state == MachineState.ACTION):
                if (not args.get('with_sink', False)):
                    state = MachineState.END
                    continue
                opt = self.getRandomOpt(statement= lambda op : op.kind == OperatorKind.ACTION)
                opt_seq.append(opt)
                state = MachineState.END
                continue
            if (state == MachineState.END):
                break
        
        return {
            'opt_seq': opt_seq,
            'platform': platform
        } 
 

#############
# Test Code
#############
if __name__ == '__main__':
    # sentences = []

    machines = [BatchFSM(), LinearFSM(), StreamingFSM()]
    scales = ['local', 'small', 'medium', 'large']
    for machine in machines:
        print("********* paradigm: {} ************".format(machine.name))
        for scale in scales:
            print("======== scale: {} ========".format(scale))
            for _ in range(10):
                res = machine.produce(scale = scale)
                print("target platform: {}, sequnce length: {}".format(res['platform'].name, len(res['opt_seq'])) )

    



# for _ in range(1, 30): # 生成30个 [500,3000) 随机大小的workflow
#     res = generateStreamingPipeline(random.randint(500, 3000), ['storm', 'flink', 'samza', 'sparkstreaming'])['plt_vec']
#     if not set(res).issubset(tmp):
#         print(1)
#         break
#     sentences.append(res)

