# try:
from nodejobs.dependencies.BaseData import BaseData
from functools import wraps
import json
import time

# except:
#    # Base Data expressed as optional upgrade, not hard requirement.
#    class BaseData(dict): pass
#
import inspect
from typing import Callable, List, Optional, Any, Dict, Union


# Global alias to simplify code
def val_ref(
    domain: str = None,
    node_id: str = None,
    func: Optional[Callable] = None,
    pth: Optional[Union[str, List[str]]] = None,
) -> List[Any]:
    return ProcessingNode.ServiceMap.val_ref(
        domain=domain, node_id=node_id, func=func, pth=pth
    )


# Global alias to simplify code
def func_name(func: Union[Callable | str]) -> List[Any]:
    return ProcessingNode.ServiceMap.func_name(func)


class ServiceMap(BaseData):
    """
    COMMENT ME
    """

    def get_keys(self):
        return {"*": ProcessingNode.ValueDict}, {}

    @classmethod
    def mk_dict(cls, lst: list) -> dict:
        merged_dic = {}
        for dic in lst:
            assert type(dic) == dict
            merged_dic.update(dic)
        return merged_dic

    def do_pre_process(self, in_dict: dict):
        for k in list(in_dict.keys()):
            if type(k) != str:
                # print(f"******************* Evaluating --------------- > {k}")
                in_dict[self.func_name(k)] = in_dict[k]
                del in_dict[k]
        return super().do_pre_process(in_dict)

    @classmethod
    def param_ref(cls, func: Union[Callable | str], *args) -> List[Any]:
        c, f = cls.class_func_name(func)
        ref = [f]
        ref.extend(args)
        return ref

    @classmethod
    def class_name(cls, class_var) -> str:
        if not inspect.isclass(class_var):
            raise TypeError(f"Expected a class, got {type(class_var)}")
        return class_var.__name__

    @classmethod
    def class_func_name(cls, func: Callable) -> List[Any]:
        # Bound method: grab its instance’s class
        if inspect.ismethod(func):
            cls_name = func.__self__.__class__.__name__
            func_name = func.__name__
        # Unbound function defined on a class: infer class from __qualname__
        elif inspect.isfunction(func):
            func_name = func.__name__
            qual = func.__qualname__  # e.g. "PointBuffer.generate"
            parts = qual.split(".")
            cls_name = parts[-2] if len(parts) >= 2 else None
        else:
            raise TypeError("`func` must be a function or method")
        return cls_name, func_name

    # Like -- kiiinda - Readable. Also shit tho
    @classmethod
    def func_name(cls, func: Union[Callable | str]) -> List[Any]:
        if type(func) == str:
            return func
        # print("func_name TYPE", type(func))
        c, f = cls.class_func_name(func)
        assert type(f) == str
        return f

    @classmethod
    def val_ref(
        cls,
        domain: str = None,
        node_id: str = None,
        func: Optional[Callable] = None,
        pth: Optional[Union[str | list[str]]] = None,
    ) -> List[Any]:
        ref = ["__ref"]
        if domain != None:
            ref.append(domain)
        if node_id != None:
            ref.append(node_id)
        if func:
            if type(func) != str:
                cls_name, func_name = cls.class_func_name(func)
            else:
                func_name = func
            ref.append(func_name)
        if pth:
            if type(pth) == str:
                pth = [pth]
            for field_segment in pth:
                ref.append(field_segment)
        clean_ref = [cls.func_name(field_segment) for field_segment in ref]

        return clean_ref

    # Insert a raw reference, as a series of args. Can be elegant looking
    @classmethod
    def raw_ref(cls, *args):
        ref = ["__ref"]
        ref = ref.extend(args)
        return ref

    @classmethod
    def make_func_bind(
        cls,
        tofunc: Union[Callable, str],
        toparam: str,
        fromfunc: Callable,
        field: Optional[str] = None,
    ) -> Dict[Any, Any]:
        raise Exception("Turned Off")
        if type(tofunc) != str:
            _, tofunc = cls.class_func_name(func=tofunc)
        ref = cls.val_ref(func=fromfunc, pth=field)
        return ProcessingNode.BoundValue(
            {tofunc: ProcessingNode.ValueDict({toparam: ref})}
        )

    @classmethod
    def make_val_bind(cls, tofunc: str, toparam: str, fromval: Any):
        raise Exception("Turned Off")
        return ProcessingNode.BoundValue(
            {tofunc: ProcessingNode.ValueDict({toparam: fromval})}
        )


class ExecutionNode(BaseData):
    """
    COMMENT ME - NO
    """

    (
        f_name,
        f_clas,
        f_typ,
        f_settings,
        f_dependencies,
        f_outtypes,
        f_intypes,
        p_cache_path,
    ) = (
        "name",
        "clas",
        "type",
        "settings",
        "dependencies",
        "outtypes",
        "intypes",
        "cache_path",
    )

    name: str
    clas: type
    type: (type, None)
    settings: (dict, {})
    dependencies: (ServiceMap, {})

    def outref(self, func: Union[Callable, str, list]) -> List[str]:
        """'
        Simple method to pull out node references given a path within a nodes key space
        """
        assert BaseData.valid_type(value=func, annotation=Union[Callable, str, list]), (
            f"Not a valid func ref [{func}]"
        )
        if type(func) == list:
            return val_ref(node_id=self[self.f_name], pth=func)
        return val_ref(node_id=self[self.f_name], pth=[func_name(func)])

    def do_pre_process(self, in_dict):
        if self.f_settings not in in_dict or in_dict[self.f_settings] == None:
            in_dict[self.f_settings] = {}
        else:
            in_dict[self.f_settings] = in_dict[self.f_settings].copy()
        if self.p_cache_path not in in_dict[self.f_settings]:
            in_dict[self.f_settings][self.p_cache_path] = in_dict[self.f_name]

        return super().do_pre_process(in_dict)

    def to_safe_value(self, key, val):
        # print(f"ExecutionNode key {key}")
        if key == self.f_clas and isinstance(val, type):
            return val.__name__
        return super().to_safe_value(key, val)

    def bind(self, func: Callable):
        """
        Start a little context‐manager for wiring up inputs to `func`.
        Usage:
            with node.bind(Foo.bar) as b:
                b.arg('a',   'Loader.output.data')
                b.arg('b',   some_ref)
        """
        return ProcessingNode.MethodBinder(self, func)

    @classmethod
    def create_exe_node(cls, name, bound_class, settings=None, dependencies=None):
        if settings == None:
            settings = {}
        else:
            settings = settings.copy()
        if dependencies == None:
            dependencies = {}
        else:
            dependencies = dependencies.copy()
        if cls.p_cache_path not in settings:
            settings[cls.p_cache_path] = name
        print(
            "Create dump "
            + str(
                {
                    cls.f_name: name,  ####
                    cls.f_clas: bound_class,
                    cls.f_typ: ProcessingNode,
                    cls.f_settings: settings,
                    cls.f_dependencies: ProcessingNode.ServiceMap(dependencies),
                }
            )
        )
        return cls(
            {
                cls.f_name: name,  ####
                cls.f_clas: bound_class,
                cls.f_typ: ProcessingNode,
                cls.f_settings: settings,
                cls.f_dependencies: ProcessingNode.ServiceMap(dependencies),
            }
        )

    def get_defaults(self):
        return {
            self.f_settings: {},
            self.f_dependencies: ProcessingNode.ServiceMap({}),
        }

    def set_input(self, key_path: list, source: Any):
        assert type(key_path) == list and len(key_path) >= 2, (
            " At least need a function and param name for an entry"
        )
        deps: ProcessingNode.ValueDict = self[self.f_dependencies]
        assert isinstance(key_path, list) and len(key_path) >= 2, (
            "key_path must be a list of at least [method, param, ...]"
        )

        deps = self[self.f_dependencies]
        node = deps
        for part in key_path[:-1]:
            if part not in node:
                node[part] = ProcessingNode.ValueDict({})
            node = node[part]

        node[key_path[-1]] = source
        self[self.f_dependencies] = deps

    def __init__(self, in_dict=None, trim=False, **kwargs):
        print("INSIDE INIT EXE")
        # raise hell
        super().__init__(in_dict, trim, **kwargs)


class ProcessingNode:
    class MethodBinder:
        def __init__(self, node, func):
            self._node = node
            self._func = func

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            # don't swallow exceptions
            return False

        def arg(self, param_name, source):
            # param_ref is imported or referenced appropriately
            key = ProcessingNode.ServiceMap.param_ref(self._func, param_name)
            self._node.set_input(key, source)
            return self  # allow chaining if you like

    class ExecutionNode(ExecutionNode):
        pass

    class NodeSettings(BaseData):
        pass  # Might add requirements one day

    class ServiceMap(ServiceMap):
        pass

    class ValueDict(BaseData):  # Problem -- Base data doesnt support list()
        @staticmethod
        def is_list_mapping(lst):
            if type(lst) != list:
                return False
            if len(lst) <= 0:
                return False
            if lst[0] == "__ref":
                return True
            return False

        def __init__(self, in_dict=None, trim=False, **kwargs):
            super().__init__(in_dict, trim, **kwargs)

        def do_every_validation(self, key, value):
            if type(value) in [str, int, float]:
                return value, ""
            if type(value) == list and self.is_list_mapping(value):
                return value, ""
            if type(value) == list:
                return value, ""
            if type(value) == dict or issubclass(type(value), dict):
                value = self.__class__(value)
                return value, ""

            return super().do_validation(key, value)

        # def get_keys(self):
        #    return {'*':list}, {}

        pass

    class OutputField(list):  # Problem -- Base data doesnt support list()
        pass

    def __init__(self, in_dict=None, trim=False):
        if in_dict is None:
            in_dict = {}
        # Extract our named parameters (or default)
        # - Static Instance
        settings = in_dict.pop("settings", {})
        dependencies = in_dict.pop("dependencies", {})
        dependency_list = in_dict.pop("dependency_list", {})
        upstream_dependency_list = in_dict.pop("upstream_dependency_list", {})
        inner_class = in_dict.pop("clas", None)
        assert isinstance(inner_class, type), (
            f"class must be a type(), got {inner_class}"
        )
        # self.globalsContext

        self.dependencies = dependencies

        self.dependency_list = dependency_list
        # print (f"INITING DEPENDENCIES fdfdf {self.dependency_list}")
        self.upstream_dependency_list = upstream_dependency_list
        self.settings = {}
        self.settings.update(settings)
        self.retVal = None
        self.inner_class = inner_class
        try:
            self.inner_instance = inner_class(self.settings)
        except:
            print(
                "could not init a class in ProcessingNode.  class and settings are below"
            )
            print(inner_class)
            print(self._debug_safe_dump(self.settings))
            raise
        self.do_init()
        # assert 'name' in self.settings

    def do_init(self):
        # raise Exception('Not Implemented')
        pass

    # def process(self,feature,lastFeature={},for_graph=None):
    #     self.lastFeature = lastFeature
    #     self.feature = feature

    #     # Process Dependencies Recursively
    #     for k in self.dependencies.keys():
    #         print(f"MY DEP KEYS {self.dependencies.keys()}")
    #         if self.dependencies[k].settings['name'] not in feature:
    #             self.dependencies[k].process(feature,lastFeature,for_graph)

    #     features = {}
    #     # Build your personal processing feature
    #     for key in self.settings: # It has your dependencies
    #             features[key] = self.settings[key]
    #     for key in self.dependency_list: # It resolves any values
    #             features[key] = self.get_dependency_value(key)
    #     for key in self.upstream_dependency_list: # Also pulls in any forward references
    #         features[key] = self.get_upstream_dependency_value(key)
    #     for key in features:
    #         self.settings[key] = features[key]
    #     feature[self.settings['name']] =  self.do_process(features,self.settings,for_graph)

    #     self.retVal=feature
    #     return self.retVal

    def getValueForSetting(self, dependency):
        if (
            isinstance(dependency, list)
            and len(dependency) > 0
            and dependency[0] == "__ref"
        ):
            dependency = tuple(dependency)
            dependency = dependency[1:]
        if isinstance(dependency, tuple):
            breadcrumb = list(dependency)
            className = breadcrumb[0]
            breadcrumb.pop(0)
            try:
                valueKey = self.feature[
                    className
                ]  # First look for a value from the network
            except:
                return None
            for k in breadcrumb:
                try:
                    valueKey = valueKey[k]  # recursively seek the value
                except:
                    # raise KeyError(f"Missing ref root '{className}:{str(breadcrumb)}' for path __ref/{'/'.join(map(str, dependency))}")
                    valueKey = None
                    # break
        else:
            d = {}
            if isinstance(dependency, dict):
                for dkey in dependency.keys():
                    d[dkey] = self.getValueForSetting(dependency[dkey])
            elif isinstance(dependency, list):
                d = [self.getValueForSetting(item) for item in dependency]
            else:
                d = dependency
            valueKey = d  # If the value is not a tuple, it is a hard coded setting
        return valueKey

    def get_dependency_value(self, key):
        valueKey = None
        dependency = self.dependency_list[key]
        if isinstance(dependency, dict):
            d = {}
            for dkey in dependency.keys():
                d[dkey] = self.getValueForSetting(dependency[dkey])

            return d
        else:
            return self.getValueForSetting(dependency)

    def get_upstream_dependency_value(self, key):
        valueKey = None
        try:
            if (
                isinstance(self.upstream_dependency_list[key], list)
                and self.upstream_dependency_list[key][0] == "__ref"
            ):
                self.upstream_dependency_list[key] = tuple(
                    self.upstream_dependency_list[key]
                )
                self.upstream_dependency_list[key] = self.upstream_dependency_list[key][
                    1:
                ]
            if isinstance(self.upstream_dependency_list[key], tuple):
                breadcrumb = list(self.upstream_dependency_list[key])
                className = breadcrumb[0]
                breadcrumb.pop(0)
                valueKey = self.lastFeature[
                    className
                ]  # First look for a value from the network
                for k in breadcrumb:
                    valueKey = valueKey[k]  # recursively seek the value
            else:
                valueKey = self.settings[
                    k
                ]  # If the value is not a tuple, it is a hard coded setting
        except:
            pass
        return valueKey

    @staticmethod
    def _debug_safe_dump(d, indent=4):
        def shorten_dict_strings(d, max_len=35, attrition=5, _depth=0):
            """
            Recursively shorten all string values in a dict (or nested dict/list) to
            (max_len - attrition * depth) characters.
            """
            this_len = max(1, max_len - attrition * _depth)
            if isinstance(d, dict):
                return {
                    k: shorten_dict_strings(v, max_len, attrition, _depth + 1)
                    for k, v in d.items()
                }
            elif isinstance(d, list):
                return [
                    shorten_dict_strings(item, max_len, attrition, _depth + 1)
                    for item in d
                ]
            elif isinstance(d, str):
                return d[:this_len] + f"len({len(d)})"
            else:
                return d

        def fallback_to_str(func):
            @wraps(func)
            def wrapper(obj: Any, *args, **kwargs):
                # If the caller didn't supply a `default=` arg, use str()
                kwargs.setdefault("default", lambda o: str(o))
                return func(obj, *args, **kwargs)

            return wrapper

        safe_dumps = fallback_to_str(json.dumps)
        short_dic = shorten_dict_strings(d, max_len=60, attrition=2)
        string = safe_dumps(short_dic, indent=4)
        return string

    def do_input(self, features, settings):
        raise Exception("Not implemented")

    def do_process(self, features: dict, settings: dict, for_graph):
        results = {}
        logger = getattr(for_graph, "log", None)
        for f_name, kwargs in features.items():
            # print(f"do_process {f_name}")
            if hasattr(self.inner_instance, f_name) and callable(
                getattr(self.inner_instance, f_name)
            ):
                gstring = f"{self.settings['name']}.{f_name}({list(kwargs.keys())})"
                try:
                    t0 = time.perf_counter()
                    if logger:
                        logger(
                            f"--------------- start node={self.settings['name']} func={f_name} keys={list(kwargs.keys())} cache={self.settings.get('cache_path')}"
                        )
                    results[f_name] = self.do_process_function(
                        f_name, kwargs, self.settings
                    )
                    if logger:
                        ms = int((time.perf_counter() - t0) * 1000)
                        r = results[f_name]
                        if isinstance(r, dict):
                            shape = f"type=dict keys={list(r.keys())}"
                        elif isinstance(r, list):
                            shape = f"type=list len={len(r)}"
                        else:
                            shape = f"type={type(r).__name__}"
                        logger(
                            f"--------------- done  node={self.settings['name']} func={f_name} ms={ms} {shape}"
                        )
                    for_graph.executed_nodes.append(gstring)

                except Exception as e:
                    if logger:
                        logger(
                            f"--------------- error node={self.settings['name']} func={f_name} keys={list(kwargs.keys())} cache={self.settings.get('cache_path')} err={e}",
                            is_error=True,
                        )

                    #      safe_dumps = fallback_to_str(json.dumps) ##
                    def shorten_dict_strings(d, max_len=25, attrition=5, _depth=0):
                        """
                        Recursively shorten all string values in a dict (or nested dict/list) to
                        (max_len - attrition * depth) characters.
                        """
                        this_len = max(1, max_len - attrition * _depth)
                        if isinstance(d, dict):
                            return {
                                k: shorten_dict_strings(
                                    v, max_len, attrition, _depth + 1
                                )
                                for k, v in d.items()
                            }
                        elif isinstance(d, list):
                            return [
                                shorten_dict_strings(
                                    item, max_len, attrition, _depth + 1
                                )
                                for item in d
                            ]
                        elif isinstance(d, str):
                            return d[:this_len]
                        else:
                            return d

                    def fallback_to_str(func):
                        @wraps(func)
                        def wrapper(obj: Any, *args, **kwargs):
                            # If the caller didn't supply a `default=` arg, use str()
                            kwargs.setdefault("default", lambda o: str(o))
                            return func(obj, *args, **kwargs)

                        return wrapper

                    safe_dumps = fallback_to_str(json.dumps)
                    err_str = f"Failed Node :{gstring}\n\n"
                    # err_str = err_str +' \n\n --- feature:\n'+safe_dumps(kwargs,indent=3)
                    features = shorten_dict_strings(features, max_len=50, attrition=2)
                    err_str = (
                        err_str
                        + " \n\n --- feature:\n"
                        + safe_dumps(features, indent=3)
                    )
                    for_graph.executed_nodes.append(err_str)
                    raise e
        return results

    def do_process_function(self, f_name, kwargs, settings):
        # print(f"-~-~-~-~-~-~running-~-~> {self.settings['name']}.{f_name}(...)")
        method = getattr(self.inner_instance, f_name)
        return method(**kwargs)

    def getInnerInstance(self):
        return self.inner_instance

    def setSetting(self, k, val):
        self.settings[k] = val

    def getSetting(self, k):
        return self.settings[k]

    def getSettings(self):
        return self.settings

    def setValue(self, dictData={}):
        self.feature[self.settings["name"]] = dictData

    def set(self, key="", dictData={}):
        if key == "":
            self.setValue(dictData)
        else:
            self.feature[self.settings["name"]][key] = dictData

    def getDependencies(self):
        return self.dependencies

    def setDependency(self, did, dependency):
        self.dependencies[did] = dependency
