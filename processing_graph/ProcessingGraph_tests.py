import json
import unittest
import random
from processing_graph.BaseProcessor import BaseProcessor
from processing_graph.ProcessingNode import ProcessingNode, ExecutionNode, val_ref
import inspect
from typing import Callable, List, Optional, Any,Dict



from functools import wraps
from dataclasses import dataclass

def attach_parameters(**params):
    """
    Decorator to attach parameter descriptors to a function.

    Args:
        **params: Keyword arguments representing parameter names and their descriptors.
                  The descriptors can be any object, such as strings, enums, or custom classes.
    """
    def decorator(func):
        @dataclass(frozen=True)
        class Parameters:
            pass

        for name, value in params.items():
            setattr(Parameters, name, value)

        # Attach the Parameters class to the function
        setattr(func, 'parameters', Parameters)

        @wraps(func)
        def wrapper(*args, **kwargs):
            return func(*args, **kwargs)
        
        return wrapper
    return decorator



from functools import wraps


def fallback_to_str(func):
    """
    Decorator for json.dumps–style functions that ensures
    anything not JSON‐serializable gets passed through str().
    """
    @wraps(func)
    def wrapper(obj: Any, *args, **kwargs):
        # If the caller didn't supply a `default=` arg, use str()
        kwargs.setdefault("default", lambda o: str(o))
        return func(obj, *args, **kwargs)
    return wrapper
safe_dumps = fallback_to_str(json.dumps) ##

class TestNewProcessingPipeline(unittest.TestCase):

    def test_schema_tests(self):
        # 1. Generate data
        random.seed(0)
        data = [(random.random(),) for _ in range(150)]

        class PointBuffer:
            def __init__(self, in_dict):
                assert type(in_dict) == dict 
                self.settings = in_dict
                self.settings['buffer'] = []


            def generate(self, point,**kwargs):
                buf = self.settings['buffer']
                buf.append(point)
                
                # trim to max length
                max_len = self.settings['buffer_size']
                while len(buf) > max_len:
                    buf.pop(0)
                    
                return {
                    'test_val_out': kwargs['test_val'],
                    'test_nested': kwargs['test_nested'],
                    'data': point,
                    'buffer': list(buf)
                }
                
        class MovingAverage(dict):
            def hardcode_func(self,param):
                return param

            def calc(self, point):
                point = point
                if 'past_number' not in self:
                    self['past_number'] = point
                self['past_number'] = 0.9 * self['past_number'] + 0.1 * point
                return self['past_number']
            
            def buffer_average(self, buffer):
                return sum(buffer) / len(buffer)

        # 3. Build the graph definition
        p_def = {}
        # DELEGATE TO THE AI!
        # when you dont know what you want you have reached a state of desirelessness
        # ??? -> Search -> guess .... -> Nothing interesting idea

        p_def['PointBuffer'] = ProcessingNode.ExecutionNode({
            'name': 'PointBuffer',
            'clas':PointBuffer,
            'settings': {'buffer_size': 10, 'input': 'number'}, 
            'dependencies': {'generate': {'point':['__ref', 'point_in'],
                                          'test_val':7,
                                           'test_nested':{'k':['__ref', 'point_in']}
                                            }}
            })

        
        dNode = ProcessingNode.ExecutionNode
        iParam = ProcessingNode.ValueDict
        oField = ProcessingNode.OutputField
        p_def['MovingAverage'] = dNode({
            'name': 'MovingAverage',
            'clas':MovingAverage,
            'settings': ProcessingNode.NodeSettings({}),
            'dependencies': ProcessingNode.ServiceMap(
            {
                'calc': iParam({'point': ['__ref', 'PointBuffer','generate', 'data']}),
                'buffer_average': iParam({'buffer': ['__ref', 'PointBuffer','generate', 'buffer']}),
                'hardcode_func':  iParam({'param':10})                            
                                                
            })
        })


        # 4. Instantiate and run
        pn = BaseProcessor(p_def)
        features = []
        for p in data:
            feature = {'point_in': p[0]}
            out = pn.process(feature)
            features.append(out)
            
        
        sample = features[-1]
        self.assertIn('MovingAverage', sample)
        self.assertIn('calc', sample['MovingAverage'])
        self.assertTrue(sample['MovingAverage']['calc'] > 0.45 and  sample['MovingAverage']['calc'] < 0.47)
        self.assertTrue(sample['MovingAverage']['buffer_average'] > 0.40 and  sample['MovingAverage']['buffer_average'] < 0.5)


    def test_sexy_interface(self):
            
            # 1. Generate data
            random.seed(0)
            data = [(random.random(),) for _ in range(150)]


            class PointBuffer:
                p_point = 'point'
                def __init__(self, in_dict):
                    assert type(in_dict) == dict 
                    self.settings = in_dict
                    self.settings['buffer'] = []

                def generate(self, point,**kwargs):
                    buf = self.settings['buffer']
                    buf.append(point)
                    
                    # trim to max length
                    max_len = self.settings['buffer_size']
                    while len(buf) > max_len:
                        buf.pop(0)
                        
                    return {
                        'test_val_out': kwargs['test_val'],
                        'test_nested': kwargs['test_nested'],
                        'data': point,
                        'buffer': list(buf)
                    }
                
            class MovingAverage(dict):
                def hardcode_func(self,param,param2,param_nest):
                    return param, param2,param_nest

                def calc(self, point):
                    point = point
                    if 'past_number' not in self:
                        self['past_number'] = point
                    self['past_number'] = 0.9 * self['past_number'] + 0.1 * point
                    return self['past_number']
                
                def buffer_average(self, buffer):
                    return sum(buffer) / len(buffer)


            # 3. Build the graph definition
            p_def = {}
            # DELEGATE TO THE AI!
            # when you dont know what you want you have reached a state of desirelessness
            # ??? -> Search -> guess .... -> Nothing interesting idea
            ExecutionNode = ProcessingNode.ExecutionNode
            ValueDict = ProcessingNode.ValueDict
            oField = ProcessingNode.OutputField
            ServiceMap = ProcessingNode.ServiceMap
            # Just sketching out the many, many, valid techniques
            p_def['PointBuffer'] = ProcessingNode.ExecutionNode({
                'name': 'PointBuffer',
                'clas':PointBuffer,
                'settings': {'buffer_size': 10, 'input': 'number'}, 
                'dependencies': {ServiceMap.func_name(PointBuffer.generate): 
                                            {   
                                              'point':['__ref', 'point_in'],
                                              'test_val':7,
                                              'test_nested':{'k':['__ref', 'point_in']}
                                            }
                                }
                })
            

            p_def['PointBuffer'] = ProcessingNode.ExecutionNode({
                'name': 'PointBuffer',
                'clas':PointBuffer,
                'settings': {'buffer_size': 10, 'input': 'number'}, 
                'dependencies': {ServiceMap.func_name(PointBuffer.generate): 
                                            {
                                                **{'point':['__ref', 'point_in']},  
                                                **{'test_val':7},  
                                                **{'test_nested':{'k':['__ref', 'point_in']}}
                                            }
                                }
                })
            p_def['PointBuffer'] = ProcessingNode.ExecutionNode({
                'name': 'PointBuffer',
                'clas':PointBuffer,
                'settings': {'buffer_size': 10, 'input': 'number'}, 
                'dependencies': ServiceMap({ServiceMap.func_name(PointBuffer.generate): ServiceMap.mk_dict([
                                                {'point':ServiceMap.val_ref(pth='point_in')},  
                                                {'test_val':7},  
                                                {'test_nested':{'k':ServiceMap.val_ref(pth='point_in')}}
                                            ])
                                })
                })
            p_def['PointBuffer'] = ProcessingNode.ExecutionNode({
                'name': 'PointBuffer',
                'clas':PointBuffer,
                'settings': {'buffer_size': 10, 'input': 'number'}, 
                'dependencies': {ServiceMap.func_name(PointBuffer.generate): 
                                    {   
                                        'point':ServiceMap.val_ref(pth='point_in'),
                                        'test_val':7,
                                        'test_nested':{'k':ServiceMap.val_ref(pth='point_in')}
                                    }
                                }
                })
            p_def['MovingAverage'] = ProcessingNode.ExecutionNode({
                'name': 'MovingAverage',
                'clas':MovingAverage,
                'settings': ProcessingNode.NodeSettings({}),
                'dependencies': ServiceMap({ 
                    ServiceMap.func_name(MovingAverage.calc): ValueDict({'point': ServiceMap.val_ref(node_id='PointBuffer',func=PointBuffer.generate, pth='data')}),
                    ServiceMap.func_name(MovingAverage.buffer_average): ValueDict({'buffer': ServiceMap.val_ref(node_id='PointBuffer',func=PointBuffer.generate, pth='buffer')}),
                    ServiceMap.func_name(MovingAverage.hardcode_func):  ValueDict({'param':ServiceMap.val_ref(node_id='PointBuffer',func=PointBuffer.generate, pth='data'),
                                                                     'param2':7,
                                                                     'param_nest':ValueDict({'point_in':ServiceMap.val_ref(pth='point_in')}),
                                                                     
                                                                     
                                                                     })                            
                })
            })
            # 
            F = ServiceMap.func_name
            graph_ref =  ServiceMap.val_ref
            p_def['MovingAverage'] = ProcessingNode.ExecutionNode({
                'name': 'MovingAverage',
                'clas':MovingAverage,
                'settings': ProcessingNode.NodeSettings({}),
                'dependencies': ServiceMap({ 
                    F(MovingAverage.calc): {
                            'point': graph_ref(node_id='PointBuffer',
                                               func=PointBuffer.generate, 
                                               pth='data')
                        },
                    F(MovingAverage.buffer_average): {
                            'buffer': graph_ref(node_id='PointBuffer',
                                                func=PointBuffer.generate, 
                                                pth='buffer')
                        },
                    F(MovingAverage.hardcode_func): {
                            'param':graph_ref(node_id='PointBuffer',
                                              func=PointBuffer.generate, 
                                              pth='data'),
                            'param2':7,
                            'param_nest':{
                                        'point_in':graph_ref(pth='point_in')  
                                          },
                         }                            
                })
            })
            
            Fid = ServiceMap.func_name
            Vref = ServiceMap.val_ref
            class Farg(str):
                pass
            # [] - Clean up value interface
            # [] - Clean up value interface
            
            p_def['MovingAverage'] = ExecutionNode({
                'name': 'MovingAverage',
                'clas':MovingAverage,
                'settings': ProcessingNode.NodeSettings({}),
                'dependencies': ServiceMap(
                { 
                    Fid(MovingAverage.calc): ValueDict(
                                    {
                                        Farg('point'): Vref(node_id='PointBuffer',func=PointBuffer.generate, 
                                                                pth='data') # (1) change each into factories (2) SHOULD TAKE A STRING OR A LIST
                                    }),
                    Fid(MovingAverage.buffer_average): ValueDict(
                                    {
                                        Farg('buffer'): Vref(node_id='PointBuffer',func=PointBuffer.generate, 
                                                                 pth='buffer')
                                    }),
                    Fid(MovingAverage.hardcode_func): ValueDict(
                                    {
                                        Farg('param'): Vref(node_id='PointBuffer',func=PointBuffer.generate,pth='data'),
                                        Farg('param2'):7,
                                        Farg('param_nest'):ValueDict({'point_in':Vref(pth='point_in')}),
                                    })                            
                })
            })
            print("printing test .... ")
            graph_dump = safe_dumps(p_def,indent=4);
            print(safe_dumps(p_def,indent=4))
            with open("./dump.json", 'w') as f:
                f.write(graph_dump)

            # 4. Instantiate and run
            pn = BaseProcessor(p_def)
            features = []
            for p in data:
                feature = {'point_in': p[0]}
                out = pn.process(feature)
                features.append(out)
                #break
            
            sample = features[-1]
            self.assertIn('MovingAverage', sample)
            self.assertIn('calc', sample['MovingAverage'])
            self.assertTrue(sample['MovingAverage']['calc'] > 0.45 and  sample['MovingAverage']['calc'] < 0.47)
            self.assertTrue(sample['MovingAverage']['buffer_average'] > 0.40 and  sample['MovingAverage']['buffer_average'] < 0.5)
            self.assertTrue(sample['PointBuffer']['generate']['test_val_out'] == 7)
            self.assertTrue(sample['PointBuffer']['generate']['test_nested']['k'] >0 )


    def test_sexy_standard_interface(self):
            
            random.seed(0)
            data = [(random.random(),) for _ in range(150)]

            class PointBuffer:
                p_point = 'point'
                def __init__(self, in_dict):
                    assert type(in_dict) == dict 
                    self.settings = in_dict
                    self.settings['buffer'] = []

                def generate(self, point,**kwargs):
                    buf = self.settings['buffer']
                    buf.append(point)
                    
                    # trim to max length
                    max_len = self.settings['buffer_size']
                    while len(buf) > max_len:
                        buf.pop(0)
                        
                    return {
                        'test_val_out': kwargs['test_val'],
                        'test_nested': kwargs['test_nested'],
                        'data': point,
                        'buffer': list(buf)
                    }
                
            class MovingAverage(dict):
                def hardcode_func(self,param,param2,param_nest):
                    return param, param2,param_nest

                def calc(self, point):
                    point = point
                    if 'past_number' not in self:
                        self['past_number'] = point
                    self['past_number'] = 0.9 * self['past_number'] + 0.1 * point
                    return self['past_number']
                
                def buffer_average(self, buffer):
                    return sum(buffer) / len(buffer)


            p_def = {}
            ExecutionNode = ProcessingNode.ExecutionNode
            ValueDict = ProcessingNode.ValueDict
            oField = ProcessingNode.OutputField
            ServiceMap = ProcessingNode.ServiceMap
            F = ServiceMap.func_name
            graph_ref =  ServiceMap.val_ref


            p_def['PointBuffer'] = ProcessingNode.ExecutionNode({
                'name': 'PointBuffer',
                'clas':PointBuffer,
                'settings': {'buffer_size': 10, 'input': 'number'}, 
                'dependencies': {F(PointBuffer.generate): 
                                    {   
                                        'point':graph_ref(pth='point_in'),
                                        'test_val':7,
                                        'test_nested':{'k':graph_ref(pth='point_in')}
                                    }
                                }
                })

            p_def['MovingAverage'] = ProcessingNode.ExecutionNode({
                'name': 'MovingAverage',
                'clas':MovingAverage,
                'settings': ProcessingNode.NodeSettings({}),
                'dependencies': ServiceMap({ 
                    F(MovingAverage.calc): {
                            'point': graph_ref(node_id='PointBuffer',func=PointBuffer.generate, pth='data')
                        },
                    F(MovingAverage.buffer_average): {
                            'buffer': graph_ref(node_id='PointBuffer',func=PointBuffer.generate,  pth='buffer')
                        },
                    F(MovingAverage.hardcode_func): {
                            'param':graph_ref(node_id='PointBuffer',func=PointBuffer.generate,  pth='data'),
                            'param2':7,
                            'param_nest':{
                                        'point_in':graph_ref(pth='point_in')  
                                          },
                         }                            
                })
            })
            

            pn = BaseProcessor(p_def)
            features = []
            for p in data:
                feature = {'point_in': p[0]}
                out = pn.process(feature)
                features.append(out)
                #break
            
            sample = features[-1]
            self.assertIn('MovingAverage', sample)
            self.assertIn('calc', sample['MovingAverage'])
            self.assertTrue(sample['MovingAverage']['calc'] > 0.45 and  sample['MovingAverage']['calc'] < 0.47)
            self.assertTrue(sample['MovingAverage']['buffer_average'] > 0.40 and  sample['MovingAverage']['buffer_average'] < 0.5)
            self.assertTrue(sample['PointBuffer']['generate']['test_val_out'] == 7)
            self.assertTrue(sample['PointBuffer']['generate']['test_nested']['k'] >0 )



    def test_slim_interface(self):
        
        random.seed(0)
        data = [(random.random(),) for _ in range(150)]

        class PointBuffer:
            p_point = 'point'
            def __init__(self, in_dict):
                assert type(in_dict) == dict 
                self.settings = in_dict
                self.settings['buffer'] = []

            def generate(self, point,**kwargs):
                buf = self.settings['buffer']
                buf.append(point)
                
                # trim to max length
                max_len = self.settings['buffer_size']
                while len(buf) > max_len:
                    buf.pop(0)
                    
                return {
                    'test_val_out': kwargs['test_val'],
                    'test_nested': kwargs['test_nested'],
                    'data': point,
                    'buffer': list(buf)
                }
            
        class MovingAverage(dict):
            def hardcode_func(self,param,param2,param_nest):
                return param, param2,param_nest

            def calc(self, point):
                point = point
                if 'past_number' not in self:
                    self['past_number'] = point
                self['past_number'] = 0.9 * self['past_number'] + 0.1 * point
                return self['past_number']
            
            def buffer_average(self, buffer):
                return sum(buffer) / len(buffer)

        # ——— aliases ———
        ExecutionNode = ProcessingNode.ExecutionNode
        ServiceMap    = ProcessingNode.ServiceMap
        vref          = ServiceMap.func_name
        param_ref     = ServiceMap.param_ref
        val_ref       = ServiceMap.val_ref

        # ——— build p_def ———
        p_def = {}

        # 1) PointBuffer node
        pb_node = ExecutionNode({
            ExecutionNode.f_name:    'PointBuffer',
            ExecutionNode.f_clas:   PointBuffer,
            ExecutionNode.f_settings:{ 'buffer_size': 10 },
            ExecutionNode.f_dependencies:{ },

        })

        pb_node.set_input(
            param_ref(PointBuffer.generate, 'point'),
            val_ref(pth='point_in')
        )
        pb_node.set_input(
            param_ref(PointBuffer.generate, 'test_nested', 'k'),
            val_ref(pth='point_in')
        )
        pb_node.set_input(
            param_ref(PointBuffer.generate, 'test_val'),
            7
        )

        p_def['PointBuffer'] = pb_node
        print(safe_dumps(pb_node, indent=4))

        # 2) MovingAverage node
        ma_node = ExecutionNode({
            ExecutionNode.f_name:    'MovingAverage',
            ExecutionNode.f_clas:   MovingAverage,
            ExecutionNode.f_settings:{},
            ExecutionNode.f_dependencies:{ },

        })

        ma_node.set_input(
            param_ref(MovingAverage.calc, 'point'),
            val_ref(node_id='PointBuffer', func=PointBuffer.generate, pth='data')
        ) #35 LOC
        ma_node.set_input(
            param_ref(MovingAverage.buffer_average, 'buffer'),
            val_ref(node_id='PointBuffer', func=PointBuffer.generate, pth='buffer')
        )
        ma_node.set_input(
            param_ref(MovingAverage.hardcode_func, 'param'),
            val_ref(node_id='PointBuffer', func=PointBuffer.generate, pth='data')
        )
        ma_node.set_input(
            param_ref(MovingAverage.hardcode_func, 'param2'),
            7
        )
        ma_node.set_input(
            param_ref(MovingAverage.hardcode_func, 'param_nest'),
            { 'point_in': val_ref(pth='point_in') }
        )

        p_def['MovingAverage'] = ma_node
        
        # ——— execute the graph ———
        pn = BaseProcessor(p_def)
        features = []
        for (val,) in data:
            features.append(pn.process({'point_in': val}))

        sample = features[-1]

        # ——— assertions ———
        self.assertIn('MovingAverage', sample)
        self.assertIn('calc', sample['MovingAverage'])
        self.assertTrue(0.45 < sample['MovingAverage']['calc'] < 0.47)
        self.assertTrue(0.40 < sample['MovingAverage']['buffer_average'] < 0.50)

        pb_out = sample['PointBuffer']['generate']
        self.assertEqual(pb_out['test_val_out'], 7)
        self.assertTrue(pb_out['test_nested']['k'] > 0)               


    def test_slimmer_interface(self):
        
        random.seed(0)
        data = [(random.random(),) for _ in range(150)]

        class PointBuffer:
            p_point = 'point'
            def __init__(self, in_dict):
                assert type(in_dict) == dict 
                self.settings = in_dict
                self.settings['buffer'] = []

            def generate(self, point:int,**kwargs):
                buf = self.settings['buffer']
                buf.append(point)
                
                # trim to max length
                max_len = self.settings['buffer_size']
                while len(buf) > max_len:
                    buf.pop(0)
                    
                return {
                    'test_val_out': kwargs['test_val'],
                    'test_nested': kwargs['test_nested'],
                    'data': point,
                    'buffer': list(buf)
                }
            
        class SumNode(dict):
            def __init__(self, in_dict):
                assert type(in_dict) == dict 
                self.settings = in_dict
                self.settings['sum'] = 0

            def sumit(self, point):
                print("SumNode" + str(point))
                point = point
                self.settings['sum'] =   self.settings['sum'] + point
                return  self.settings['sum']
            
        # ——— build p_def ———
        p_def = {}
        # 1) PointBuffer node -- Still hate that params are strings
        pb_node = ExecutionNode.Create('PointBuffer',PointBuffer,
            settings={ 'buffer_size': 10 },
            dependencies={ 
                    PointBuffer.generate: {
                            "point":         val_ref(pth='point_in'),        
                            "test_nested":   {"k": val_ref(pth='point_in')},  
                            "test_val":      7                               
                            }
            },
        )
        p_def[pb_node.name] = pb_node
        outreff = pb_node.outref( [PointBuffer.generate,"data"])
        sum_node = ExecutionNode.Create('SumNode',SumNode,
            dependencies= { 
                    SumNode.sumit: { "point": pb_node.outref( [PointBuffer.generate,"data"]) },
            }
        )
        p_def[sum_node.name] = sum_node 
        
        # ——— execute the graph ———
        pn = BaseProcessor(p_def)
        features = []
        for (val,) in data:
            features.append(pn.process({'point_in': val}))
            print(features[-1]["SumNode"])
        sum_node_correct = features[-1]["SumNode"]

        pn = BaseProcessor(p_def)
        state = pn.export_state()
        features = []
        for (val,) in data:
            pn = BaseProcessor(p_def)
            state["SumNode"]["TEST"] = 1
            pn.import_state(state)
            print("PRE StATE")
            print(json.dumps(state,indent=3))
            features.append(pn.process({'point_in': val}))
            state = pn.export_state()
            print("POST STATE")
            print(json.dumps(state,indent=3))
            print(features[-1]["SumNode"])
        assert sum_node_correct == features[-1]["SumNode"]
        print("SAME SUM!")

    # NEXT TESTS
    # Update node independently - can invoke sum node several times


if __name__ == '__main__':
    # unittest.main()
    # unittest.main(defaultTest='TestNewProcessingPipeline.test_slimmer_interface')
    unittest.main(defaultTest='TestNewProcessingPipeline.test_sexy_interface')



'''
{
    "PointBuffer": {
        "name": "PointBuffer",
        "clas": "PointBuffer",
        "settings": {
            "buffer_size": 10,
            "input": "number",
            "cache_path": "PointBuffer"
        },
        "dependencies": {
            "generate": {
                "point": [
                    "__ref",
                    "point_in"
                ],
                "test_val": 7,
                "test_nested": {
                    "k": [
                        "__ref",
                        "point_in"
                    ]
                }
            }
        }
    },
    "MovingAverage": {
        "name": "MovingAverage",
        "clas": "MovingAverage",
        "settings": {
            "cache_path": "MovingAverage"
        },
        "dependencies": {
            "calc": {
                "point": [
                    "__ref",
                    "PointBuffer",
                    "generate",
                    "data"
                ]
            },
            "buffer_average": {
                "buffer": [
                    "__ref",
                    "PointBuffer",
                    "generate",
                    "buffer"
                ]
            },
            "hardcode_func": {
                "param": [
                    "__ref",
                    "PointBuffer",
                    "generate",
                    "data"
                ],
                "param2": 7,
                "param_nest": {
                    "point_in": [
                        "__ref",
                        "point_in"
                    ]
                }
            }
        }
    }
}

'''