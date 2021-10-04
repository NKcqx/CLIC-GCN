import random
import copy

from numpy.random import choice
from enum import Enum

import operators, platforms

class MachineState(Enum):
    START       = 0
    END         = 1
    SOURCE      = 2
    CALCULATOR  = 3
    DISPATCHER  = 4
    LOCATOR     = 5
    SAMPLER     = 6
    ACTION      = 7
    LOOP        = 8


def getValidPltNOpt(paradigm, platform_list='*'):
    # 先找到所有属于batch的、且受 platform_list 中的平台支持的运算符
    if (platform_list=='*'):
        supported_opts = operators.opts_with_paradigm[paradigm]
        supported_plts = set(platforms.plts_with_paradigm[paradigm])
    else:
        # 找到用户指定的 plt 的 Platform 实例
        supported_plts = [p for p in platforms.plts_with_paradigm[paradigm] if p.name in platform_list]
        # 找到所有可用于batch的运算符
        supported_opts = [op for op in operators.opts_with_paradigm[paradigm] if len(set(supported_plts) & set(op.supported_platforms)) is not 0]

    return supported_opts, supported_plts

#TODO: 使用随机游走
def getRandomOpt(opt_list, plt_list, k=1, statement = lambda x : True):
    if (k<1):
        k=1
    valid_opts = [opt for opt in opt_list if statement(opt)]
    res = []
    for _ in range(0, k):
        while(True):
            opt = random.choice(valid_opts)
            random_supported_plts = set(opt.supported_platforms) & set(plt_list) # opt支持的平台 和 可用batch平台的交集
            if(len(random_supported_plts) is not 0):
                break
        #res.append(opt)
        res.append(copy.deepcopy(opt))
    return res

def getRandomPlt(opt, plt_list):
    plt_list_names = [p.name for p in plt_list]
    while(True):
        plt = random.choice(opt.supported_platforms)
        if plt.name in plt_list_names:
            return plt


def batchFSM(platform_list='*', **args):
    used_platforms = [] # 统计本次pipeline涉及到的所有平台
    used_opts = []
    state = MachineState.START
    supported_opts, supported_plts = getValidPltNOpt('batch', platform_list)
    while(True):
        if (state == MachineState.START):
            state = MachineState.SOURCE
            continue
        if (state == MachineState.SOURCE):
            if (not args.get('with_source', False)):
                state = MachineState.CALCULATOR
                continue
            opts = getRandomOpt(supported_opts, supported_plts, statement= lambda op : op.kind == operators.OperatorKind.SOURCE)
            opt=opts[0]
            opt_plt = getRandomPlt(opt, supported_plts).name
            # src_plt = random.choice(list(src.supported_platforms)).name
            used_platforms.append(opt_plt)
            used_opts.append(opt)

            state = MachineState.CALCULATOR
            continue

        if (state == MachineState.CALCULATOR):
            opts = getRandomOpt(supported_opts, supported_plts, statement= lambda op: op.kind == operators.OperatorKind.CALCULATOR and op.num_input==1 and op.num_output==1)
            opt=opts[0]
            opt_plt = getRandomPlt(opt, supported_plts).name
            # opt_plt = random.choice(list(opt.supported_platforms)).name
            used_platforms.append(opt_plt)
            used_opts.append(opt)
            P_loop   = 0.1 if args.get('loop', 0) is 0 else 0.3
            P_action = args.get('toAction', (1 - P_loop)/2.0)
            P_stay   = 1 - P_action - P_loop
            state = choice([MachineState.ACTION, MachineState.LOOP, MachineState.CALCULATOR], 1, p=[P_action, P_loop, P_stay])[0]
            continue

        if  (state == MachineState.LOOP):
            loop_body = batchFSM(platform_list, loop=0, with_source = False, with_sink = False)
            
            for _ in range(args.get('loop', 1)):
                for plt in loop_body['plt_vec']:
                    used_platforms.append(copy.deepcopy(plt) )
                for opt in loop_body['opts']:
                    used_opts.append(copy.deepcopy(opt))
            
            P_action = args.get('toAction', 0.6)
            P_stay   = 1 - P_action
            state = choice([MachineState.ACTION, MachineState.CALCULATOR], 1, p=[P_action, P_stay])[0]
            continue
        
        if  (state == MachineState.ACTION):
            if (not args.get('with_sink', False)):
                state = MachineState.END
                continue
            opts = getRandomOpt(supported_opts, supported_plts, statement=lambda op: op.kind==operators.OperatorKind.ACTION)
            opt = opts[0]
            opt_plt = getRandomPlt(opt, supported_plts).name
            used_platforms.append(opt_plt)
            used_opts.append(opt)
            state = MachineState.END
            continue
        if (state == MachineState.END):
            break
    return {
        'opts': used_opts,
        'plt_vec': used_platforms
    }  
 

def streamFSM(platform_list='*', **args):
    def getStreamOpt(kind):
        opts = getRandomOpt(supported_opts, supported_plts, 
            statement= lambda op : op.kind == kind and 'streaming' in op.paradigm
            )
        opt=opts[0]
        opt_plt = getRandomPlt(opt, supported_plts).name
        # src_plt = random.choice(list(src.supported_platforms)).name
        return opt, opt_plt

    used_platforms = [] # 统计本次pipeline涉及到的所有平台
    used_opts = []
    state = MachineState.START
    supported_opts, supported_plts = getValidPltNOpt('streaming', platform_list)
    while(True):
        if (state == MachineState.START):
            state = MachineState.SOURCE
            continue
        if (state == MachineState.SOURCE):
            if (not args.get('with_source', False)):
                state = MachineState.CALCULATOR
                continue
            opt, opt_plt = getStreamOpt(operators.OperatorKind.SOURCE)
            used_platforms.append(opt_plt)
            used_opts.append(opt)

            state = MachineState.CALCULATOR
            continue

        if (state == MachineState.CALCULATOR):

            opts = getRandomOpt(supported_opts, supported_plts, 
            statement= lambda op: op.kind == operators.OperatorKind.CALCULATOR 
                and op.num_input==1 
                and op.num_output==1)
            opt=opts[0]
            opt_plt = getRandomPlt(opt, supported_plts).name
            # opt_plt = random.choice(list(opt.supported_platforms)).name
            used_platforms.append(opt_plt)
            used_opts.append(opt)
            P_loop   = 0.1 if args.get('loop', 0) is 0 else 0.3
            # P_action = args.get('toAction', (1 - P_loop)/4.0)
            P_Locator = args.get('toLocator', (1 - P_loop)/3.0)
            P_Dispatcher = P_Locator
            P_stay = P_Locator
            state = choice([
                MachineState.LOOP, MachineState.LOCATOR, MachineState.DISPATCHER, MachineState.CALCULATOR], 
                1, 
                p=[P_loop, P_Locator, P_Dispatcher, P_stay])[0]
            continue
        
        if  (state == MachineState.LOOP):
            loop_body = streamFSM(platform_list, loop=0, with_source = False, with_sink = False)
            
            for _ in range(args.get('loop', 1)):
                for plt in loop_body['plt_vec']:
                    used_platforms.append(copy.deepcopy(plt) )
                for opt in loop_body['opts']:
                    used_opts.append(copy.deepcopy(opt))
            
            P_action = args.get('toAction', 0.6)
            P_stay   = 1 - P_action
            state = choice([MachineState.ACTION, MachineState.CALCULATOR], 1, p=[P_action, P_stay])[0]
            continue

        if (state == MachineState.LOCATOR):
            opt, opt_plt = getStreamOpt(operators.OperatorKind.LOCATOR)

            used_platforms.append(opt_plt)
            used_opts.append(opt)

            state = choice([MachineState.CALCULATOR, MachineState.DISPATCHER], 1, p=[0.5, 0.5])[0]
            continue

        if (state == MachineState.DISPATCHER):
            opt, opt_plt = getStreamOpt(operators.OperatorKind.DISPATCHER)
            used_platforms.append(opt_plt)
            used_opts.append(opt)
            state = MachineState.ACTION
            continue
        
        if  (state == MachineState.ACTION):
            if (not args.get('with_sink', False)):
                state = MachineState.END
                continue
            opt, opt_plt = getStreamOpt(operators.OperatorKind.ACTION)
            used_platforms.append(opt_plt)
            used_opts.append(opt)
            state = MachineState.END
            continue
        if (state == MachineState.END):
            break
    return {
        'opts': used_opts,
        'plt_vec': used_platforms
    } 

def linearFSM(platform_list='*', **args):
    def getLinearOpt(kind):
        opts = getRandomOpt(supported_opts, supported_plts, 
            statement= lambda op : op.kind == kind and 'linear' in op.paradigm
            )
        opt=opts[0]
        opt_plt = getRandomPlt(opt, supported_plts).name
        # src_plt = random.choice(list(src.supported_platforms)).name
        return opt, opt_plt

    used_platforms = [] # 统计本次pipeline涉及到的所有平台
    used_opts = []
    state = MachineState.START
    supported_opts, supported_plts = getValidPltNOpt('linear', platform_list)
    while(True):
        if (state == MachineState.START):
            state = MachineState.SOURCE
            continue
        if (state == MachineState.SOURCE):
            if (not args.get('with_source', False)):
                state = MachineState.CALCULATOR
                continue
            opt, opt_plt = getLinearOpt(operators.OperatorKind.SOURCE)
            used_opts.append(opt)
            used_platforms.append(opt_plt)
            state = MachineState.SAMPLER
            continue

        if (state == MachineState.SAMPLER):

            opt, opt_plt = getLinearOpt(operators.OperatorKind.SAMPLER)
            used_opts.append(opt)
            used_platforms.append(opt_plt)

            P_locator = 0.4
            P_calculator = P_locator
            P_sampler = 1 - P_locator - P_calculator
            state = choice(
                [MachineState.LOCATOR, MachineState.CALCULATOR, MachineState.SAMPLER], 
                1, 
                p=[P_locator, P_calculator, P_sampler])[0]
            continue
        
        if (state == MachineState.LOCATOR):
            opt, opt_plt = getLinearOpt(operators.OperatorKind.LOCATOR)
            used_opts.append(opt)
            used_platforms.append(opt_plt)

            state = choice([MachineState.CALCULATOR, MachineState.ACTION], 1, p=[0.8, 0.2])[0]
            continue
        
        if (state == MachineState.CALCULATOR):
            opts = getRandomOpt(supported_opts, supported_plts, 
                statement= lambda op: op.kind == operators.OperatorKind.CALCULATOR 
                and op.num_input==1 
                and op.num_output==1 
                and 'linear' in op.paradigm)
            opt=opts[0]
            opt_plt = getRandomPlt(opt, supported_plts).name
            # opt_plt = random.choice(list(opt.supported_platforms)).name
            used_platforms.append(opt_plt)
            used_opts.append(opt)
            P_loop   = 0.1 if args.get('loop', 0) is 0 else 0.3
            P_action = args.get('toLocator', (1 - P_loop)/5.0)
            P_stay = 2*P_action
            P_locator = 2*P_action
            state = choice([
                MachineState.LOOP, MachineState.LOCATOR, MachineState.CALCULATOR, MachineState.ACTION], 
                1, 
                p=[P_loop, P_locator, P_stay, P_action])[0]
            continue
        
        if  (state == MachineState.LOOP):
            loop_body = linearFSM(platform_list, loop=0, with_source = False, with_sink = False)
            
            for _ in range(args.get('loop', 1)):
                for plt in loop_body['plt_vec']:
                    used_platforms.append(copy.deepcopy(plt) )
                for opt in loop_body['opts']:
                    used_opts.append(copy.deepcopy(opt))
            
            P_action = args.get('toAction', 0.6)
            P_calculator   = 1 - P_action
            state = choice([MachineState.CALCULATOR, MachineState.ACTION], 1, p=[P_calculator, P_action])[0]
            continue
        
        if  (state == MachineState.ACTION):
            if (not args.get('with_sink', False)):
                state = MachineState.END
                continue
            opt, opt_plt = getLinearOpt(operators.OperatorKind.ACTION)
            used_platforms.append(opt_plt)
            used_opts.append(opt)
            state = MachineState.END
            continue
        if (state == MachineState.END):
            break
    return {
        'opts': used_opts,
        'plt_vec': used_platforms
    } 

#############
# Test Code
#############
# sentences = []
# tmp = set(['storm', 'flink', 'samza'])
# res = linearFSM(['pytorch', 'spark', 'tensorflow'])
# print(res['plt_vec'])
# for r in res['opts']:
#     print(r.name)

# for _ in range(1, 30): # 生成30个 [500,3000) 随机大小的workflow
#     res = generateStreamingPipeline(random.randint(500, 3000), ['storm', 'flink', 'samza', 'sparkstreaming'])['plt_vec']
#     if not set(res).issubset(tmp):
#         print(1)
#         break
#     sentences.append(res)

