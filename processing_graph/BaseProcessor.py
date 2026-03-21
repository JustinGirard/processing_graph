"""
A Processing Network is a facade to a collection of processing nodes.
A Processing Network (PN) is given a json description of a processing network.
The PN constructs this network, and then gives access to the central 'process' method.
JG: Wrote this fucking thing 5 years ago. Barely remeber how it works, but it works really, really, well.
JG-2: True
JG-3: Its nice now
JG-4: Its not tho... no input/output declarations
}
"""

import sys
import inspect, sys

from .ProcessingNode import ProcessingNode
from nodejobs.dependencies.BaseData import BaseData, BaseField
from typing import Any


class ProcessingGraph(BaseData):
    def add_node(self, node: dict) -> bool:
        node = ProcessingNode.ExecutionNode(node)
        self[node[node.f_name]] = node
        return True

    def add_nodes(self, nodes: list):
        return [self.add_node(n) for n in nodes]


# TODO - Add in a proper Field type, and back integrate it into BaseData.
# For now the tuple method is good, minimal, and expressive
class Commit(BaseData):
    graph: (ProcessingGraph, {})
    execution_query: dict
    root_name: (str, None)
    result_path: (list, None)
    from_ref: (list, None)

    def do_pre_process(self, in_dict):
        c = Commit
        if c.from_ref in in_dict and in_dict[c.from_ref] != None:
            in_dict[c.root_name] = in_dict[c.from_ref][1]
            in_dict[c.result_path] = in_dict[c.from_ref][2:]
            del in_dict[c.from_ref]
        return super().do_pre_process(in_dict)


class CommitExecution(BaseData):
    result: Any
    output: dict
    error: (Any, None)


class BaseProcessor:
    class ProcessingGraph(ProcessingGraph):
        pass

    def __init__(self, networkDef, root="", context=None):
        assert isinstance(networkDef, dict), f"Totally invalid graph {networkDef}"
        self.executed_nodes = []
        self.networkTemplate = networkDef.copy()
        self.networkDef = networkDef
        self.instanceMap = {}
        self.root = root
        self.globalsContext = context
        for instanceName in networkDef:
            if instanceName == "__outputs":
                continue
            networkDef[instanceName]["name"] = instanceName
            # TODO fix this -- but I dont understand the invisible dependencies impacted by rebuilding the keys.
            # lol -- so the name field gets demolished? -- TODO -- should be the other way around. The name field should be the important one

            self.createNodeRecursive(networkDef[instanceName])
        self.lastFeature = {}

    @staticmethod
    def run_graph(graph, feature, val_path):
        assert isinstance(graph, dict), f"Invalid graph found {graph}"
        pn = BaseProcessor(graph)
        out = pn.process(feature, rootIn=val_path[0])
        exec_val = out
        try:
            for key in val_path:
                exec_val = exec_val[key]
        except:
            raise Exception(
                f"No Results in val_path {val_path}, last_valid_obj: {exec_val}"
            )
        try:
            commit_execution = CommitExecution(
                {
                    CommitExecution.output: out,
                    CommitExecution.result: exec_val,
                    CommitExecution.error: None,
                }
            )
        except Exception as e:
            commit_execution = CommitExecution(
                {
                    CommitExecution.output: out,
                    CommitExecution.result: [],
                    CommitExecution.error: e,
                }
            )

        return commit_execution

    @staticmethod
    def run_commit(commit) -> CommitExecution:
        commit = Commit(commit)
        EG = Commit
        graph = commit.graph
        root_id = commit.root_name
        result_path = commit.result_path
        feature = commit.execution_query
        # print(graph)
        # raise("WHAT IS THE GRAPH")
        val_path = [root_id, *result_path]
        ce = BaseProcessor.run_graph(graph, feature, val_path)
        cme = CommitExecution(ce)
        return cme

    @staticmethod  # NEW
    def build_context(mod=None, ns=None, predicate=None):
        """
        Return {ClassName: ClassObj} from a module or namespace.
        - mod: module object (e.g., sys.modules[__name__])
        - ns:  dict namespace (e.g., globals())
        - predicate: optional filter: (cls) -> bool
        """
        if ns is None and mod is None:
            frm = inspect.stack()[1].frame
            mod = sys.modules.get(frm.f_globals.get("__name__"))

        items = ns.items() if ns is not None else inspect.getmembers(mod)
        ctx = {name: obj for name, obj in items if inspect.isclass(obj)}
        if mod is not None and ns is None:
            ctx = {
                n: c
                for n, c in ctx.items()
                if getattr(c, "__module__", None) == mod.__name__
            }
        if predicate is not None:
            ctx = {n: c for n, c in ctx.items() if predicate(c)}
        return ctx

    def getNetworkTemplate(self):
        return self.networkTemplate

    def export_state(self):
        # Extract state information comprehensively
        state_data = {
            node_name: node_instance.settings
            for node_name, node_instance in self.instanceMap.items()
        }
        return state_data

    def import_state(self, state_data):
        for node_name, settings in state_data.items():
            if node_name in self.instanceMap:
                for key, val in settings.items():
                    self.instanceMap[node_name].setSetting(key, val)
            else:
                # Handle cases where nodes are not initially present
                print(f"Warning: Node {node_name} not found in instanceMap.")

    def createNodeRecursive(self, instanceDict):
        # TODO -- all methods, use schema validator
        instanceDict["settings"] = ProcessingNode.NodeSettings(instanceDict["settings"])
        iName = instanceDict["name"]
        if not iName in self.instanceMap:
            # if 'settings' in self.networkDef[iName]:
            #    settings=self.networkDef[iName]['settings']
            # else:
            #    settings=None
            upstream_dependency_list = {}
            dependency_list = {}
            settings = instanceDict["settings"]
            if "upstream_dependencies" in self.networkDef[iName]:
                input_list = self.networkDef[iName]["upstream_dependencies"]
                for ik in input_list.keys():
                    iItem = input_list[ik]
                    if isinstance(iItem, tuple):
                        upstream_dependency_list[ik] = iItem
                    elif isinstance(iItem, list) and iItem[0] == "__ref":
                        iItem = tuple(iItem)
                        iItem = iItem[1:]
                        upstream_dependency_list[ik] = iItem

                    else:  # TODO figure out why settings are populated here. idk
                        settings[ik] = iItem

            if "dependencies" in self.networkDef[iName]:
                input_list = self.networkDef[iName]["dependencies"]
                for ik in input_list.keys():
                    iItem = input_list[ik]
                    dependency_list[ik] = iItem

            if (
                "type" not in self.networkDef[iName]
                or self.networkDef[iName]["type"] == None
            ):
                self.networkDef[iName]["type"] = ProcessingNode
            typeVar = self.networkDef[iName]["type"]
            # print(self.globalsContext.keys())
            # raise hell
            if type(self.networkDef[iName]["clas"]) == str:
                clas = self.networkDef[iName]["clas"]
                assert self.networkDef[iName]["clas"] in self.globalsContext, (
                    f"If you are passing string as a class: ({clas}), you must have it in the context"
                )
                self.networkDef[iName]["clas"] = self.globalsContext[
                    self.networkDef[iName]["clas"]
                ]

            try:
                self.instanceMap[iName] = typeVar(
                    {
                        "settings": settings,
                        "dependency_list": dependency_list,
                        "upstream_dependency_list": upstream_dependency_list,
                        "clas": self.networkDef[iName]["clas"],
                    }
                )

            except Exception as e:
                import traceback

                err_str = traceback.format_exc(limit=50)
                print(err_str)
                print("Trouble instancing a ProcessingNode, here is some info:")
                print("-----------------------------------")
                print("iName", iName)
                print("settings", settings)
                print("dependency_list", dependency_list)
                print("upstream_dependency_list", upstream_dependency_list)
                print("type", typeVar)
                print("class", self.networkDef[iName]["clas"])
                print("-----------Re-raising error---------")
                raise e
            instance = self.instanceMap[iName]
            instanceDict["instance"] = self.instanceMap[iName]

            instance.setSetting("name", iName)
            for depParameter in instanceDict["dependencies"].keys():
                depName = instanceDict["dependencies"][depParameter]
                refrences_unprocessed = [depName]
                ind = 0
                while len(refrences_unprocessed) > 0:
                    ind = ind + 1
                    refrence = refrences_unprocessed.pop(0)

                    # Recurse and search for sub refrences if you find a dict
                    if isinstance(refrence, dict):
                        for sub_refrence in list(refrence.values()):
                            refrences_unprocessed.append(sub_refrence)
                    elif isinstance(refrence, list) and (
                        len(refrence) == 0 or refrence[0] != "__ref"
                    ):
                        refrences_unprocessed.extend(refrence)
                    # If you find an instance (Tuple) register the dependency
                    # DUDE TODO Fix this horrible code
                    # Expects :  ['__ref', 'PointBuffer','generate', 'data']
                    # Which is ref (flag), class, function, inner field
                    if (
                        isinstance(refrence, list)
                        and len(refrence) > 0
                        and refrence[0] == "__ref"
                    ):
                        refrence = tuple(refrence)
                        refrence = refrence[1:]

                    if isinstance(refrence, tuple):
                        try:
                            depInstance = self.createNodeRecursive(
                                self.networkDef[refrence[0]]
                            )
                            instance.setDependency(depParameter + str(ind), depInstance)
                        except:
                            pass
        return self.instanceMap[iName]

    def getInstance(self, iName):
        return self.instanceMap[iName]

    def getInnerInstance(self, iName):
        pn: ProcessingNode = self.instanceMap[iName]
        return pn.getInnerInstance()

    def do_preprocess(self):  # LOL I thought these would be useful
        pass

    def do_postprocess(self):
        pass

    def process_node(
        self, proc_node: ProcessingNode, feature, lastFeature={}, for_graph=None
    ):
        proc_node.lastFeature = lastFeature
        proc_node.feature = feature
        if proc_node.settings["name"] in feature:
            return feature[proc_node.settings["name"]]

        # Process Dependencies Recursively
        for k in proc_node.dependencies.keys():
            # Because I totally forgot before:
            # proc_node.dependencies[k], k is NOT the node or ref name.
            # This is because dependencies are pooled ina giant dict. What happens is that when
            # duplicate EDGES are detected leading to the same reference, they are disambiguated
            # by appending an int (1...N) to ref name. Like "write_files_2". These edges may lead
            # to the same cached nodes, and each runtime edge is hot computed when the node is instanced
            # So these cached edges are never seen in any output files / stored. They are runtime linkages
            # Overall I find this lazy loading and instancing of edges very functional, and performant, but
            # they lack any real ability to declare, analyze, or debug
            # Overall this whole scheme could use a refactor, however as it is such a deep feature, it
            # could easly take 10-20 hours of dev to just fix this up, and since it works VERY WELL,
            # it just seems like something to leave alone for now.
            # The only reason to really refactor this, would be if one wanted to do some real-time visualization
            # and debugging of the graph at an edge level. I have not had this issue ever, over dozens of projects
            # So even though I hate it, I will opt to just "leave it alone"
            # However, I want to leave this comment about this none issue.
            if proc_node.dependencies[k].settings["name"] not in feature:
                self.process_node(
                    proc_node.dependencies[k], feature, lastFeature, for_graph
                )
                assert proc_node.dependencies[k].settings["name"] in feature, (
                    "The node did not correctly deposit results"
                )

        features = {}
        # Build your personal processing feature
        for key in proc_node.settings:  # It has your dependencies
            features[key] = proc_node.settings[key]
        for key in proc_node.dependency_list:  # It resolves any values
            features[key] = proc_node.get_dependency_value(key)
        for (
            key
        ) in proc_node.upstream_dependency_list:  # Also pulls in any forward references
            features[key] = proc_node.get_upstream_dependency_value(key)
        for key in features:
            proc_node.settings[key] = features[key]
        feature[proc_node.settings["name"]] = proc_node.do_process(
            features, proc_node.settings, for_graph
        )

        proc_node.retVal = feature
        return proc_node.retVal

    def process(self, feature=None, rootIn=""):
        # We have to set every node in the network to it's "unprocessed" state
        self.do_preprocess()
        if feature == None:
            feature = {}
        targetNode = rootIn
        if rootIn == "":
            targetNode = self.root

        if targetNode == "":
            # raise Exception("Never need to blindly execute the graph like this")
            for instanceName in self.networkDef.keys():
                if instanceName == "__outputs":
                    continue
                if not instanceName in feature or not feature[instanceName]:
                    # feature = self.instanceMap[instanceName].process(feature,self.lastFeature,self)
                    feature = self.process_node(
                        self.instanceMap[instanceName], feature, self.lastFeature, self
                    )
        else:
            try:
                self.executed_nodes.append("Root Entry: " + targetNode)
                # feature = self.instanceMap[targetNode].process(feature,self.lastFeature,self)
                feature = self.process_node(
                    self.instanceMap[targetNode], feature, self.lastFeature, self
                )
            except Exception as exc:
                # raise exc
                # print succinct graph trace
                print("\nGraphError:")
                if len(self.executed_nodes) > 1:
                    for n in self.executed_nodes[:-1]:
                        print(f"  - {n}")

                if len(self.executed_nodes) > 0:
                    print(f"  - {self.executed_nodes[-1]} < ---- Error")
                else:
                    print("\n - Empty graph?")

                _, _, tb = sys.exc_info()
                # skip until the first frame *not* in ProcessingNode
                while tb is not None:
                    mod = tb.tb_frame.f_globals.get("__name__", "")
                    # print(mod)
                    if mod.startswith(
                        "processing_graph.ProcessingNode"
                    ) or mod.startswith("processing_graph.BaseProcessor"):
                        tb = tb.tb_next
                    else:
                        break

                # re-raise, attaching only the remaining traceback
                # raise exc.with_traceback(tb)
                raise exc
        self.lastFeature = feature
        self.do_postprocess()
        return feature

    def __str__(self):
        return self.networkDef
