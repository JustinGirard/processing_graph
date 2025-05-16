
#try:
from decelium_wallet.commands.BaseData import BaseData
#except:
#    # Base Data expressed as optional upgrade, not hard requirement.
#    class BaseData(dict): pass
#
import inspect
from typing import Callable, List, Optional, Any,Dict,Union


class ProcessingNode():

    class ExecutionNode(BaseData):
        f_name = "name"
        f_class = "class"
        f_settings = "settings"
        f_dependencies = "dependencies"

        def get_keys(self):
            required = {
                self.f_name: str,
                self.f_class: type,
                self.f_settings: dict,
                self.f_dependencies: ProcessingNode.ServiceMap,
            }
            optional = {
            }
            return required, optional

        def get_defaults(self):
            return {
                self.f_settings: {},
                self.f_dependencies: ProcessingNode.ServiceMap({}),
            }
        
    class NodeSettings (BaseData): pass #Might add requirements one day
    class ServiceMap(BaseData):
        def get_keys(self):
            return {'*':ProcessingNode.ValueDict}, {}
        
        @classmethod
        def mk_dict(cls,lst: list) -> dict:
            merged_dic = {}
            for dic in lst:
                assert type(dic) == dict
                merged_dic.update(dic)
            return merged_dic
        
        def do_validate(self,key,value):
            pass
        
        @classmethod
        def func_name(cls,func: Union[Callable|str]) -> List[Any]:
            if type(func) == str:
                return func
            # Bound method: grab its instance’s class
            if inspect.ismethod(func):
                cls_name = func.__self__.__class__.__name__
                func_name = func.__name__
            # Unbound function defined on a class: infer class from __qualname__
            elif inspect.isfunction(func):
                func_name = func.__name__
                qual = func.__qualname__               # e.g. "PointBuffer.generate"
                parts = qual.split('.')
                cls_name = parts[-2] if len(parts) >= 2 else None
            else:
                raise TypeError("`func` must be a function or method")
            return func_name
        @classmethod
        def class_func_name(cls,func: Callable) -> List[Any]:
            # Bound method: grab its instance’s class
            if inspect.ismethod(func):
                cls_name = func.__self__.__class__.__name__
                func_name = func.__name__
            # Unbound function defined on a class: infer class from __qualname__
            elif inspect.isfunction(func):
                func_name = func.__name__
                qual = func.__qualname__               # e.g. "PointBuffer.generate"
                parts = qual.split('.')
                cls_name = parts[-2] if len(parts) >= 2 else None
            else:
                raise TypeError("`func` must be a function or method")
            return cls_name,func_name
        
        #@classmethod
        #def make_ref(cls,func: Callable, field: Optional[str] = None) -> List[Any]:
        #    cls_name, func_name = cls.class_func_name(func)
        #    ref = ["__ref", cls_name, func_name]
        #    if field:
        #        ref.append(field)
        #    return ref
        @classmethod
        def build_ref(cls,func: Optional[Callable]=None, field_path: Optional[str] = None) -> List[Any]:
            ref = ["__ref"]
            if func:
                cls_name, func_name = cls.class_func_name(func)
                ref.append(cls_name)
                ref.append(func_name)
            if field_path:
                ref.append(field_path)
            return ref
                
        @classmethod
        def make_func_bind(cls,tofunc:Union[Callable,str],toparam:str, fromfunc: Callable, field: Optional[str] = None) -> Dict[Any,Any]:
            raise Exception("Turned Off")
            if type(tofunc) != str:
                _, tofunc = cls.class_func_name(func=tofunc)
                print(f"FOUND tofunc: {tofunc}")
            ref = cls.build_ref(func=fromfunc, field_path=field)
            return ProcessingNode.BoundValue({
                    tofunc: ProcessingNode.ValueDict  ({toparam: ref})
            })

        @classmethod
        def make_val_bind(cls,tofunc:str,toparam:str, fromval: Any):
            raise Exception("Turned Off")
            return ProcessingNode.BoundValue({
                    tofunc: ProcessingNode.ValueDict  ({toparam: fromval})
            })

        

    
    class ValueDict(BaseData): # Problem -- Base data doesnt support list()
        @staticmethod
        def is_mapping(cls,in_dict):
            raise Exception("not needed?")
            if  in_dict != None  \
                    and type(in_dict) == dict \
                    and len(in_dict) >0 \
                    and type(in_dict[list(in_dict.keys())[0]]) == list \
                    and in_dict[list(in_dict.keys())[0]][0] == '__ref':
                return True
            return False
        
        @staticmethod
        def is_list_mapping(lst):
            if type(lst) != list:
                return False
            if len(lst) <= 0:
                return False
            if lst[0] == '__ref':
                return True
            return False
        
        def __init__(self, in_dict=None, trim=False, **kwargs):
            #print(f"VALIDATING DICT {in_dict}")
            super().__init__(in_dict, trim, **kwargs)
        def do_every_validation(self, key, value):
            if type(value) in [str,int,float]:
                #print(f"---checking val {key}:{value}")
                return value,""
            if type(value) == list and self.is_list_mapping(value) :
                #print(f"---checking mapping  {key}:{value}")
                return value,""
            if type(value) == dict or issubclass(type(value),dict) :
                #print(f"---checking dict  {key}:{value}")
                
                value = self.__class__(value)
                return value,""
            
            return super().do_validation(key, value)
        #def get_keys(self):
        #    return {'*':list}, {}
        
        pass
    class OutputField(list): # Problem -- Base data doesnt support list()
        pass


    def __init__(self,in_dict=None,trim=False):
        if in_dict is None:
            in_dict = {}
        # Extract our named parameters (or default)
        # - Static Instance
        settings              = in_dict.pop("settings", {})
        dependencies          = in_dict.pop("dependencies", {})
        dependency_list       = in_dict.pop("dependency_list", {})
        upstream_dependency_list = in_dict.pop("upstream_dependency_list", {})
        inner_class = in_dict.pop("class", None)
        assert isinstance(inner_class, type), f"class must be a type(), got {inner_class}"    

        self.dependencies = dependencies
        self.dependency_list=dependency_list

        self.upstream_dependency_list=upstream_dependency_list
        self.settings = {}
        self.settings.update(settings)
        self.retVal=None
        self.inner_class = inner_class
        self.inner_instance = inner_class(self.settings)
        self.do_init() 
        #assert 'name' in self.settings

    def do_init(self):
        #raise Exception('Not Implemented')
        pass
    
    def process(self,feature,lastFeature={}):
        #print(self.settings['name']+".process()")
        self.lastFeature = lastFeature
        self.feature = feature
        for k in self.dependencies.keys():
            if self.dependencies[k].settings['name'] not in feature: 
                self.dependencies[k].process(feature)
        
        # Re initalize and locate features
        features = {}
        for key in self.settings:
                features[key] = self.settings[key]
        for key in self.dependency_list:
                features[key] = self.get_dependency_value(key)
        for key in self.upstream_dependency_list:
            features[key] = self.get_upstream_dependency_value(key)
        for key in features:
            self.settings[key] = features[key]

        #print('SETTINGS:')
        #print('features:',features)
        #print('self.settings:',self.settings)
        #print('SETTINGS END:')
        try:
            feature[self.settings['name']] =  self.do_process(features,self.settings)
        except Exception as e:
            print(self.settings)
            print(self.settings['name'])
            print(features )
            print(self.settings )
            #print(feature[self.settings['name']])
            raise e

        self.retVal=feature
        return self.retVal

    def getValueForSetting(self,dependency):
            if(isinstance(dependency,list) and  len(dependency) > 0 and dependency[0]=='__ref'):
                dependency = tuple(dependency)
                dependency=dependency[1:]
            if(isinstance(dependency,tuple)):
                breadcrumb = list(dependency)
                className =breadcrumb[0] 
                breadcrumb.pop(0)
                try:
                    valueKey = self.feature[className] #First look for a value from the network
                except:
                    return None
                for k in breadcrumb:
                    try:
                        valueKey = valueKey[k] # recursively seek the value
                    except:
                        valueKey = None
                        break
            else:
                d = {}
                if(isinstance(dependency,dict)):
                    for dkey in dependency.keys():
                        d[dkey] = self.getValueForSetting(dependency[dkey])
                else:
                    d = dependency
                valueKey = d # If the value is not a tuple, it is a hard coded setting
            return valueKey 

    def get_dependency_value(self,key):
        valueKey = None
        dependency = self.dependency_list [key]
        if(isinstance(dependency,dict)):
            d = {}
            for dkey in dependency.keys():
                d[dkey] = self.getValueForSetting(dependency[dkey])
            
            return d
        else:
            return self.getValueForSetting(dependency)

    def get_upstream_dependency_value(self,key):
        valueKey = None
        try:
            if(isinstance(self.upstream_dependency_list [key],list) and self.upstream_dependency_list [key][0]=='__ref'):
                self.upstream_dependency_list [key] = tuple(self.upstream_dependency_list [key])
                self.upstream_dependency_list [key]= self.upstream_dependency_list [key][1:]
            if(isinstance(self.upstream_dependency_list [key],tuple)):
                breadcrumb = list(self.upstream_dependency_list [key])
                className =breadcrumb[0] 
                breadcrumb.pop(0)
                valueKey = self.lastFeature[className] #First look for a value from the network
                for k in breadcrumb:
                    valueKey = valueKey[k] # recursively seek the value
            else:
                valueKey = self.settings[k] # If the value is not a tuple, it is a hard coded setting
        except:
            pass
        return valueKey 

    #def get_dependency_instance(self,key):
    #    if self.dependency_list:
    #        valueKey = self.dependencies[key]
    #        return valueKey 
    #    return None

    def do_input(self,features,settings):
        raise Exception ("Not implemented")

    #def do_process(self,features,settings):
    #    try:
    #        return self.do_input(features['input'],settings)
    #    except Exception as e:
    #        import traceback
    #        err_str =  traceback.format_exc(limit=50)
    #        print(err_str)
    #        print('missing ["input"]--------------')
    #        #print(features)
    #        print('--------------')
    #        raise e
    def do_process(self, features: dict, settings: dict):
        """
        For each entry in `features`, if this ProcessingNode subclass
        defines a method named after that key, invoke it as:
            result = self.<key>(features[key], settings)
        and collect all such results into a dict.
        """
        results = {}
        for name, kwargs in features.items():
            if hasattr(self.inner_instance, name) and callable(getattr(self.inner_instance, name)):
                try:
                    method = getattr(self.inner_instance, name)
                    results[name] = method(**kwargs)
                except Exception as e:
                    import traceback
                    tb = traceback.format_exc(limit=50)
                    print(f"Error in {self.__class__.__name__}.{name}:\n{tb}")
                    raise e
        return results


    def setSetting(self,k,val):
        self.settings[k]=val
   
    def getSetting(self,k):
        return self.settings[k]

    def getSettings(self):
        return self.settings

    def setValue(self,dictData={}):
        self.feature[self.settings['name']] = dictData

    def set(self,key='',dictData={}):
        if key == '':
            self.setValue(dictData)
        else:
            self.feature[self.settings['name']][key] = dictData

    def getDependencies(self):
        return self.dependencies
    
    def setDependency(self,did,dependency):
        self.dependencies[did] = dependency 