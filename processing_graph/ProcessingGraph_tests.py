import json
import unittest
import random
from ProcessingGraph import ProcessingGraph
from ProcessingNode import ProcessingNode
import inspect
from typing import Callable, List, Optional, Any,Dict

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
            'class':PointBuffer,
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
            'class':MovingAverage,
            'settings': ProcessingNode.NodeSettings({}),
            'dependencies': ProcessingNode.ServiceMap(
            {
                'calc': iParam({'point': ['__ref', 'PointBuffer','generate', 'data']}),
                'buffer_average': iParam({'buffer': ['__ref', 'PointBuffer','generate', 'buffer']}),
                'hardcode_func':  iParam({'param':10})                            
                                                
            })
        })


        # 4. Instantiate and run
        pn = ProcessingGraph(p_def)
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

            # 2. Define two ProcessingNode subclasses
            # ***** [ ] - refactor how settings work to be passed on init not managed by graph
            # [X] - Should be able to just pass whatever class, not ProcessingNode
            # [X] - Should dynamically map into, and provide, named params via kwargs
            # [X] - should test dict base class and __init__
            # [X] - Should use harder type mapping dict
            # [ ] - 'dependencies' should rename to 'registry'
            # [ ] - Should test settings
            # [ ] - should test raw function binding
            # [ ] - should test upstream dependencies (previous cycle -- support for cycles)

            # [ ] - Add in BaseData to harden interface


            # Usage examples:
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
                'class':PointBuffer,
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
                'class':PointBuffer,
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
                'class':PointBuffer,
                'settings': {'buffer_size': 10, 'input': 'number'}, 
                'dependencies': ServiceMap({ServiceMap.func_name(PointBuffer.generate): ServiceMap.mk_dict([
                                                {'point':ServiceMap.build_ref(field_path='point_in')},  
                                                {'test_val':7},  
                                                {'test_nested':{'k':ServiceMap.build_ref(field_path='point_in')}}
                                            ])
                                })
                })
            p_def['PointBuffer'] = ProcessingNode.ExecutionNode({
                'name': 'PointBuffer',
                'class':PointBuffer,
                'settings': {'buffer_size': 10, 'input': 'number'}, 
                'dependencies': {ServiceMap.func_name(PointBuffer.generate): 
                                    {   
                                        'point':ServiceMap.build_ref(field_path='point_in'),
                                        'test_val':7,
                                        'test_nested':{'k':ServiceMap.build_ref(field_path='point_in')}
                                    }
                                }
                })
            p_def['MovingAverage'] = ProcessingNode.ExecutionNode({
                'name': 'MovingAverage',
                'class':MovingAverage,
                'settings': ProcessingNode.NodeSettings({}),
                'dependencies': ServiceMap({ 
                    ServiceMap.func_name(MovingAverage.calc): ValueDict({'point': ServiceMap.build_ref(func=PointBuffer.generate, field_path='data')}),
                    ServiceMap.func_name(MovingAverage.buffer_average): ValueDict({'buffer': ServiceMap.build_ref(func=PointBuffer.generate, field_path='buffer')}),
                    ServiceMap.func_name(MovingAverage.hardcode_func):  ValueDict({'param':ServiceMap.build_ref(func=PointBuffer.generate, field_path='data'),
                                                                     'param2':7,
                                                                     'param_nest':ValueDict({'point_in':ServiceMap.build_ref(field_path='point_in')}),
                                                                     
                                                                     
                                                                     })                            
                })
            })
            # 
            F = ServiceMap.func_name
            graph_ref =  ServiceMap.build_ref
            p_def['MovingAverage'] = ProcessingNode.ExecutionNode({
                'name': 'MovingAverage',
                'class':MovingAverage,
                'settings': ProcessingNode.NodeSettings({}),
                'dependencies': ServiceMap({ 
                    F(MovingAverage.calc): {
                            'point': graph_ref(func=PointBuffer.generate, 
                                               field_path='data')
                        },
                    F(MovingAverage.buffer_average): {
                            'buffer': graph_ref(func=PointBuffer.generate, 
                                                field_path='buffer')
                        },
                    F(MovingAverage.hardcode_func): {
                            'param':graph_ref(func=PointBuffer.generate, 
                                              field_path='data'),
                            'param2':7,
                            'param_nest':{
                                        'point_in':graph_ref(field_path='point_in')  
                                          },
                         }                            
                })
            })
            
            Fid = ServiceMap.func_name
            Vref = ServiceMap.build_ref
            class Farg(str):
                pass
            # [] - Clean up value interface
            # [] - Clean up value interface
            
            p_def['MovingAverage'] = ExecutionNode({
                'name': 'MovingAverage',
                'class':MovingAverage,
                'settings': ProcessingNode.NodeSettings({}),
                'dependencies': ServiceMap(
                { 
                    Fid(MovingAverage.calc): ValueDict(
                                    {
                                        Farg('point'): Vref(func=PointBuffer.generate, 
                                                                field_path='data') # (1) change each into factories (2) SHOULD TAKE A STRING OR A LIST
                                    }),
                    Fid(MovingAverage.buffer_average): ValueDict(
                                    {
                                        Farg('buffer'): Vref(func=PointBuffer.generate, 
                                                                 field_path='buffer')
                                    }),
                    Fid(MovingAverage.hardcode_func): ValueDict(
                                    {
                                        Farg('param'): Vref(func=PointBuffer.generate,field_path='data'),
                                        Farg('param2'):7,
                                        Farg('param_nest'):ValueDict({'point_in':Vref(field_path='point_in')}),
                                    })                            
                })
            })

            # 4. Instantiate and run
            pn = ProcessingGraph(p_def)
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


if __name__ == '__main__':
    unittest.main()
