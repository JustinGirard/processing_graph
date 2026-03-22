## 🧠 Why This Graph Processing Library?

This library provides a **type-safe**, **schema-enforced**, and **extensible** graph processing engine built around declarative node definitions and runtime-validated data structures.

Unlike traditional graph systems that prioritize general-purpose DAG execution (e.g. Dask, Graphtik, Airflow), this system is optimized for **modular, strongly typed dataflow composition** with built-in validation, introspection, and configuration hygiene.

---

### ✅ Key Benefits

#### 1. **Schema-Enforced Nodes (via `BaseData`)**

Each node, port, dependency, and graph is defined using `BaseData` — a typed dictionary-like class that:

* Enforces required and optional fields
* Validates type correctness (recursively)
* Applies transformation, coercion, and field defaults
* Allows structured, safe nesting (e.g., `PortDependency → PortGroup → PortRef`)

> This enables full introspection, serialization, and editor tooling — without hand-written validation.

---

#### 2. **Readable, Declarative Graph Definitions**

Graph definitions are concise and explicit:

```python
ProcessingNode({
    "name": "MovingAverage",
    "type": SomeProcessor,
    "settings": {"window": 5},
    "dependencies": PortDependency({
        "input": PortGroup({
            "point": PortRef({"ref": ["__ref", "PointBuffer", "data"]})
        })
    })
})
```

This creates a clear mental model:

* `ProcessingNode` = processor with inputs
* `PortRef` = symbolic dependency resolution
* Graphs can be composed from plain dicts, JSON, or programmatically

---

#### 3. **No Global Side Effects or Magic**

* No decorators
* No monkey-patching
* No global state
* No string-based function binding

> This makes the system easy to trace, debug, and control — even at runtime or in test environments.

---

#### 4. **Runtime Safety Without Compile-Time Rigidness**

Graph topology is validated at runtime using explicit dependency resolution (e.g. topological sort).

* Detects missing inputs
* Detects cycles
* Validates node structure

> This preserves Python’s flexibility while giving you structural guarantees you'd expect from static languages.

---

#### 5. **Modular, Replaceable Execution Engine**

Execution happens through a simple, readable `ProcessingNetwork`:

* Processes node-by-node in topological order
* Resolves input dependencies dynamically
* Returns a merged output with all node results

You can replace it with:

* An async version
* A multiprocessing scheduler
* An external engine (e.g. Graphtik) if flat execution is preferred

---

### 🆚 How It Compares

| Feature                     | This Library          | Graphtik / Dask      | Airflow / Luigi     |
| --------------------------- | --------------------- | -------------------- | ------------------- |
| Declarative Input Schema    | ✅ Full (`BaseData`)   | ❌ (free-form kwargs) | ❌ (task-level only) |
| Recursive Field Validation  | ✅                     | ❌                    | ❌                   |
| Dynamic Input Resolution    | ✅ PortRef / PortGroup | ❌ Flat only          | ❌ Static            |
| Built-in Topological Sort   | ✅                     | ✅                    | ✅                   |
| Extensible Node Format      | ✅                     | ✅                    | ❌                   |
| Designed for Runtime Graphs | ✅                     | ❌ Mostly static      | ❌ Batch Only        |

---

### 💡 Ideal Use Cases

* Game logic graphs (AI, ability chains, simulations)
* Modular feature extraction / preprocessing
* Dynamic pipelines with user-defined graphs
* Low-overhead knowledge graphs or agent graphs
* Typed orchestration of modular Python components

---

## 🧩 TL;DR

This library is a **minimalist**, **type-safe**, and **declarative** graph execution engine — made for senior engineers who need:

* Runtime graph safety
* Structured validation
* Pluggable execution models
* Editor- and test-friendly data layouts

It’s **not** trying to replace general DAG runners — it’s for when you want **typed, programmatic, embeddable graph logic** inside a real system.

License:
- Today, free for personal use only
- Can not be included as part of any paid service
- Free licence can be revoked
- Experimental
- All rights reserved
- Use at own risk
