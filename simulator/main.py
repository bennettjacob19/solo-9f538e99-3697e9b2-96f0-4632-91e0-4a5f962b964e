"""A small, stateful Kubernetes API look-alike for rehearsing node drains.

It is not a Kubernetes distribution.  It implements only the handful of
endpoints the drain UI uses, but it implements their *semantics* faithfully
where the exercise depends on them:

* ``PATCH /api/v1/nodes/{name}`` for cordoning (merge patch).
* ``GET /api/v1/pods?fieldSelector=spec.nodeName=...`` for listing.
* ``POST /api/v1/namespaces/{ns}/pods/{name}/eviction`` implementing the
  ``policy/v1`` Eviction subresource, including the UID precondition.
* ``GET /apis/policy/v1/poddisruptionbudgets`` with live budget accounting.

It also implements just enough of the discovery + paginated list surface for
the namespace residue scanner:

* ``GET /api/v1``, ``GET /apis``, ``GET /apis/{group}/{version}`` discovery
  documents (including a subresource, a cluster-scoped type, a type without
  ``list``, and a group served at two versions).
* ``GET .../namespaces/{ns}/{resource}?limit=&continue=`` with continue
  tokens that can be expired on demand (HTTP 410).
* ``GET /api/v1/namespaces/{name}`` so a scan can pin the namespace UID.

Eviction decisions are made under one lock against freshly computed budget
status, so two nodes drained concurrently cannot jointly exceed a shared
PodDisruptionBudget.

The ``/api/sim`` endpoints and the page at ``/`` let an operator change a
budget, replace a pod under the same name (new UID), make a replacement ready,
let an evicted pod finish terminating, and arm the two scan drills: removing
the first page of a list while expiring its continue token, and failing one
API group's discovery.
"""

from __future__ import annotations

import threading
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from fastapi import FastAPI, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse, Response
from fastapi.templating import Jinja2Templates

BASE_DIR = Path(__file__).resolve().parent
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))

NODE_NAMES = ["node-a", "node-b", "node-c"]


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _new_uid() -> str:
    return uuid.uuid4().hex


@dataclass
class SimPod:
    name: str
    namespace: str
    node: str
    uid: str = field(default_factory=_new_uid)
    labels: dict[str, str] = field(default_factory=dict)
    owner_kind: str | None = None
    static: bool = False
    ready: bool = True
    phase: str = "Running"
    terminating: bool = False
    deletion_timestamp: str | None = None
    finalizers: list[str] = field(default_factory=list)
    resource_version: int = 1

    @property
    def key(self) -> tuple[str, str]:
        return (self.namespace, self.name)

    @property
    def healthy(self) -> bool:
        return self.phase == "Running" and self.ready and not self.terminating


@dataclass
class SimPDB:
    name: str
    namespace: str
    selector: dict[str, str]
    min_available: int | None = None
    max_unavailable: int | None = None
    finalizers: list[str] = field(default_factory=list)
    generation: int = 1
    resource_version: int = 1

    def budget(self, pods: list[SimPod]) -> dict[str, int]:
        matching = [
            pod
            for pod in pods
            if pod.namespace == self.namespace
            and all(pod.labels.get(k) == v for k, v in self.selector.items())
        ]
        expected = len(matching)
        healthy = sum(1 for pod in matching if pod.healthy)
        if self.min_available is not None:
            desired = self.min_available
        else:
            desired = max(0, expected - (self.max_unavailable or 0))
        return {
            "disruptionsAllowed": max(0, healthy - desired),
            "currentHealthy": healthy,
            "desiredHealthy": desired,
            "expectedPods": expected,
        }


@dataclass
class SimNode:
    name: str
    unschedulable: bool = False
    ready: bool = True
    uid: str = field(default_factory=_new_uid)
    resource_version: int = 1


@dataclass
class SimNamespace:
    name: str
    uid: str = field(default_factory=_new_uid)
    phase: str = "Active"
    deletion_timestamp: str | None = None
    finalizers: list[str] = field(default_factory=list)
    resource_version: int = 1


@dataclass
class SimObject:
    """A generic namespaced object (ConfigMap, Rollout, ...) for scan drills."""

    group: str  # "" 表示核心组
    resource: str  # 复数名，如 configmaps
    namespace: str
    name: str
    kind: str
    api_version: str
    uid: str = field(default_factory=_new_uid)
    finalizers: list[str] = field(default_factory=list)
    resource_version: int = 1

    @property
    def key(self) -> tuple[str, str, str, str]:
        return (self.group, self.resource, self.namespace, self.name)


#: Generic resource types the simulator can store and serve, and the
#: apiVersion/kind they are serialised with.  Versions are views over the
#: same objects, like a real API server.
GENERIC_KINDS: dict[tuple[str, str], tuple[str, str]] = {
    ("", "configmaps"): ("v1", "ConfigMap"),
    ("", "secrets"): ("v1", "Secret"),
    ("workloads.example.io", "rollouts"): ("workloads.example.io/v1", "Rollout"),
    ("workloads.example.io", "experiments"): ("workloads.example.io/v1alpha1", "Experiment"),
}

_LIST_VERBS = ["create", "delete", "get", "list", "patch", "update", "watch"]

#: Discovery documents.  Deliberately includes a subresource, a type without
#: the list verb, cluster-scoped types, and a group at two versions, so the
#: scanner's filtering rules are all exercised.
CORE_RESOURCES: list[dict[str, Any]] = [
    {"name": "pods", "namespaced": True, "kind": "Pod", "verbs": _LIST_VERBS},
    {"name": "pods/eviction", "namespaced": True, "kind": "Eviction", "verbs": ["create"]},
    {"name": "pods/status", "namespaced": True, "kind": "Pod", "verbs": ["get", "patch", "update"]},
    {"name": "configmaps", "namespaced": True, "kind": "ConfigMap", "verbs": _LIST_VERBS},
    {"name": "secrets", "namespaced": True, "kind": "Secret", "verbs": _LIST_VERBS},
    {"name": "bindings", "namespaced": True, "kind": "Binding", "verbs": ["create"]},
    {"name": "namespaces", "namespaced": False, "kind": "Namespace", "verbs": _LIST_VERBS},
    {"name": "nodes", "namespaced": False, "kind": "Node", "verbs": ["get", "list", "patch", "watch"]},
]

GROUP_VERSIONS: dict[str, dict[str, Any]] = {
    "policy": {
        "versions": ["v1"],
        "preferred": "v1",
        "resources": {
            "v1": [
                {"name": "poddisruptionbudgets", "namespaced": True, "kind": "PodDisruptionBudget", "verbs": _LIST_VERBS},
                {"name": "poddisruptionbudgets/status", "namespaced": True, "kind": "PodDisruptionBudget", "verbs": ["get", "patch", "update"]},
            ],
        },
    },
    "workloads.example.io": {
        "versions": ["v1", "v1alpha1"],
        "preferred": "v1",
        "resources": {
            "v1": [
                {"name": "rollouts", "namespaced": True, "kind": "Rollout", "verbs": _LIST_VERBS},
            ],
            "v1alpha1": [
                {"name": "rollouts", "namespaced": True, "kind": "Rollout", "verbs": _LIST_VERBS},
                {"name": "experiments", "namespaced": True, "kind": "Experiment", "verbs": _LIST_VERBS},
            ],
        },
    },
}


class ClusterState:
    def __init__(self) -> None:
        self.lock = threading.RLock()
        self.nodes: dict[str, SimNode] = {}
        self.pods: dict[tuple[str, str], SimPod] = {}
        self.pdbs: dict[tuple[str, str], SimPDB] = {}
        self.namespaces: dict[str, SimNamespace] = {}
        self.objects: dict[tuple[str, str, str, str], SimObject] = {}
        self.events: list[dict[str, Any]] = []
        # Continue tokens embed this epoch; bumping it expires every token.
        self.page_epoch = 1
        # One-shot drill: remove the first page of this list and expire tokens.
        self.drill_vanish: dict[str, str] | None = None
        # Groups whose discovery document should fail.
        self.failing_groups: set[str] = set()
        self.reset()

    # -- seed --------------------------------------------------------------
    def reset(self) -> None:
        with self.lock:
            self.nodes = {name: SimNode(name=name) for name in NODE_NAMES}
            self.pods = {}
            self.pdbs = {}
            self.namespaces = {}
            self.objects = {}
            self.events = []
            self.page_epoch += 1
            self.drill_vanish = None
            self.failing_groups = set()

            for name in ("default", "shop", "kube-system"):
                self.namespaces[name] = SimNamespace(name=name)
            # A namespace stuck in Terminating, with leftovers of several
            # resource types — the scan drill's subject.
            self.namespaces["stuck-ns"] = SimNamespace(
                name="stuck-ns",
                phase="Terminating",
                deletion_timestamp="2026-01-01T00:10:00Z",
                finalizers=["kubernetes"],
            )

            def add(
                namespace: str,
                name: str,
                node: str,
                labels: dict[str, str],
                owner_kind: str | None = None,
                static: bool = False,
            ) -> None:
                pod = SimPod(
                    namespace=namespace,
                    name=name,
                    node=node,
                    labels=labels,
                    owner_kind=owner_kind,
                    static=static,
                )
                self.pods[pod.key] = pod

            # A ReplicaSet-like workload spread over all three nodes.
            for index, node in enumerate(["node-a", "node-a", "node-b", "node-b", "node-c"]):
                add("shop", f"web-{index}", node, {"app": "web"}, owner_kind="ReplicaSet")
            # A two-replica cache, one pod on each of the two target nodes.
            add("shop", "cache-0", "node-a", {"app": "cache"}, owner_kind="StatefulSet")
            add("shop", "cache-1", "node-b", {"app": "cache"}, owner_kind="StatefulSet")
            # DaemonSets mirror nodes and must never be evicted.
            for suffix, node in zip("abc", ["node-a", "node-b", "node-c"]):
                add(
                    "kube-system",
                    f"log-agent-{suffix}",
                    node,
                    {"app": "log-agent"},
                    owner_kind="DaemonSet",
                )
            # A static pod owned by the kubelet.
            add("kube-system", "kube-vip-a", "node-a", {"app": "kube-vip"}, static=True)

            self.pdbs[("shop", "web-pdb")] = SimPDB(
                name="web-pdb", namespace="shop", selector={"app": "web"}, min_available=3
            )
            self.pdbs[("shop", "cache-pdb")] = SimPDB(
                name="cache-pdb", namespace="shop", selector={"app": "cache"}, min_available=1
            )

            # -- residue inside the Terminating namespace -------------------
            # Eight terminating pods, six of them held by a finalizer.  They
            # sit on no node, so they never interfere with drain rounds.
            for index in range(8):
                pod = SimPod(
                    namespace="stuck-ns",
                    name=f"stuck-pod-{index}",
                    node="",
                    labels={"app": "stuck"},
                    owner_kind="ReplicaSet",
                    ready=False,
                    phase="Pending",
                    terminating=True,
                    deletion_timestamp="2026-01-01T00:10:00Z",
                    finalizers=["example.com/cleanup"] if index < 6 else [],
                )
                self.pods[pod.key] = pod
            self.pdbs[("stuck-ns", "stuck-pdb")] = SimPDB(
                name="stuck-pdb",
                namespace="stuck-ns",
                selector={"app": "stuck"},
                min_available=1,
                finalizers=["example.com/pdb-guard"],
            )

            def put(
                group: str,
                resource: str,
                name: str,
                finalizers: list[str] | None = None,
            ) -> None:
                api_version, kind = GENERIC_KINDS[(group, resource)]
                obj = SimObject(
                    group=group,
                    resource=resource,
                    namespace="stuck-ns",
                    name=name,
                    kind=kind,
                    api_version=api_version,
                    finalizers=list(finalizers or []),
                )
                self.objects[obj.key] = obj

            put("", "configmaps", "stuck-cm-0", ["example.com/cleanup"])
            put("", "configmaps", "stuck-cm-1")
            put("", "configmaps", "stuck-cm-2")
            put("workloads.example.io", "rollouts", "stuck-rollout-0", ["example.com/traffic-drain"])
            put("workloads.example.io", "rollouts", "stuck-rollout-1")
            put("workloads.example.io", "experiments", "stuck-exp-0", ["example.com/cleanup"])

            self._event("reset", "演练环境已恢复到初始状态")

    def _event(self, kind: str, message: str, **extra: Any) -> None:
        self.events.insert(
            0,
            {"at": _now_iso(), "kind": kind, "message": message, **extra},
        )
        del self.events[200:]

    # -- serialisation -----------------------------------------------------
    def node_json(self, node: SimNode) -> dict[str, Any]:
        return {
            "apiVersion": "v1",
            "kind": "Node",
            "metadata": {
                "name": node.name,
                "uid": node.uid,
                "resourceVersion": str(node.resource_version),
                "creationTimestamp": "2026-01-01T00:00:00Z",
                "labels": {"kubernetes.io/hostname": node.name},
            },
            "spec": {"unschedulable": node.unschedulable},
            "status": {
                "conditions": [
                    {
                        "type": "Ready",
                        "status": "True" if node.ready else "False",
                        "reason": "KubeletReady" if node.ready else "KubeletNotReady",
                    }
                ],
                "capacity": {"cpu": "8", "memory": "32Gi", "pods": "110"},
            },
        }

    def pod_json(self, pod: SimPod) -> dict[str, Any]:
        metadata: dict[str, Any] = {
            "name": pod.name,
            "namespace": pod.namespace,
            "uid": pod.uid,
            "resourceVersion": str(pod.resource_version),
            "creationTimestamp": "2026-01-01T00:00:00Z",
            "labels": dict(pod.labels),
        }
        if pod.terminating and pod.deletion_timestamp:
            metadata["deletionTimestamp"] = pod.deletion_timestamp
            metadata["deletionGracePeriodSeconds"] = 30
        if pod.finalizers:
            metadata["finalizers"] = list(pod.finalizers)
        if pod.owner_kind:
            metadata["ownerReferences"] = [
                {
                    "apiVersion": "apps/v1",
                    "kind": pod.owner_kind,
                    "name": f"{pod.name.rsplit('-', 1)[0]}-owner",
                    "uid": _new_uid(),
                    "controller": True,
                }
            ]
        if pod.static:
            metadata["annotations"] = {
                "kubernetes.io/config.mirror": _new_uid(),
                "kubernetes.io/config.source": "file",
            }
        return {
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": metadata,
            "spec": {
                "nodeName": pod.node,
                "containers": [{"name": "app", "image": "registry.local/app:1.0"}],
            },
            "status": {
                "phase": pod.phase,
                "conditions": [
                    {
                        "type": "Ready",
                        "status": "True" if pod.healthy else "False",
                    }
                ],
                "containerStatuses": [
                    {
                        "name": "app",
                        "ready": pod.healthy,
                        "restartCount": 0,
                        "image": "registry.local/app:1.0",
                        "imageID": "registry.local/app@sha256:"
                        + ("0" * 64),
                        "containerID": f"containerd://{pod.uid[:16]}",
                        "started": True if pod.healthy else False,
                        "state": {"running": {}} if pod.healthy else {"waiting": {"reason": "ContainerCreating"}},
                    }
                ],
            },
        }

    def pdb_json(self, pdb: SimPDB) -> dict[str, Any]:
        status = pdb.budget(list(self.pods.values()))
        spec: dict[str, Any] = {"selector": {"matchLabels": dict(pdb.selector)}}
        if pdb.min_available is not None:
            spec["minAvailable"] = pdb.min_available
        else:
            spec["maxUnavailable"] = pdb.max_unavailable or 0
        metadata: dict[str, Any] = {
            "name": pdb.name,
            "namespace": pdb.namespace,
            "uid": f"pdb-{pdb.namespace}-{pdb.name}",
            "generation": pdb.generation,
            "resourceVersion": str(pdb.resource_version),
            "creationTimestamp": "2026-01-01T00:00:00Z",
        }
        if pdb.finalizers:
            metadata["finalizers"] = list(pdb.finalizers)
        return {
            "apiVersion": "policy/v1",
            "kind": "PodDisruptionBudget",
            "metadata": metadata,
            "spec": spec,
            "status": {
                "observedGeneration": pdb.generation,
                **status,
                "disruptionsAllowed": status["disruptionsAllowed"],
            },
        }

    def namespace_json(self, ns: SimNamespace) -> dict[str, Any]:
        metadata: dict[str, Any] = {
            "name": ns.name,
            "uid": ns.uid,
            "resourceVersion": str(ns.resource_version),
            "creationTimestamp": "2026-01-01T00:00:00Z",
        }
        if ns.deletion_timestamp:
            metadata["deletionTimestamp"] = ns.deletion_timestamp
        if ns.finalizers:
            metadata["finalizers"] = list(ns.finalizers)
        return {
            "apiVersion": "v1",
            "kind": "Namespace",
            "metadata": metadata,
            "spec": {"finalizers": list(ns.finalizers)},
            "status": {"phase": ns.phase},
        }

    def object_json(self, obj: SimObject) -> dict[str, Any]:
        metadata: dict[str, Any] = {
            "name": obj.name,
            "namespace": obj.namespace,
            "uid": obj.uid,
            "resourceVersion": str(obj.resource_version),
            "creationTimestamp": "2026-01-01T00:00:00Z",
        }
        if obj.finalizers:
            metadata["finalizers"] = list(obj.finalizers)
        return {
            "apiVersion": obj.api_version,
            "kind": obj.kind,
            "metadata": metadata,
            "spec": {},
        }

    # -- pagination ----------------------------------------------------------
    def paginate(
        self, items: list[dict[str, Any]], limit: int | None, token: str | None
    ) -> tuple[list[dict[str, Any]], str | None] | None:
        """Slice one page out of a sorted listing.

        Returns ``None`` when the continue token is stale (the caller then
        answers 410).  Tokens embed the page epoch, so any drill that bumps
        the epoch expires every outstanding token at once.
        """
        offset = 0
        if token:
            parts = token.split(":")
            if len(parts) != 3 or parts[0] != "sim" or parts[1] != str(self.page_epoch):
                return None
            try:
                offset = int(parts[2])
            except ValueError:
                return None
        if limit and limit > 0:
            page = items[offset : offset + limit]
            nxt = offset + limit
            next_token = f"sim:{self.page_epoch}:{nxt}" if nxt < len(items) else None
        else:
            page, next_token = items[offset:], None
        return page, next_token

    def delete_listed(
        self, group: str, resource: str, namespace: str, page: list[dict[str, Any]]
    ) -> None:
        """Remove the just-listed objects, for the vanish-after-first-page drill."""
        for item in page:
            name = (item.get("metadata") or {}).get("name")
            if not name:
                continue
            if group == "" and resource == "pods":
                self.pods.pop((namespace, name), None)
            elif group == "policy" and resource == "poddisruptionbudgets":
                self.pdbs.pop((namespace, name), None)
            else:
                self.objects.pop((group, resource, namespace, name), None)


STATE = ClusterState()
app = FastAPI(title="drain rehearsal simulator", docs_url=None, redoc_url=None)


# -- helpers ---------------------------------------------------------------
def status_response(code: int, reason: str, message: str) -> JSONResponse:
    kind = "Status"
    return JSONResponse(
        status_code=code,
        content={
            "apiVersion": "v1",
            "kind": kind,
            "status": "Failure" if code >= 400 else "Success",
            "message": message,
            "reason": reason,
            "code": code,
        },
        media_type="application/json",
    )


def _find_pod(namespace: str, name: str) -> SimPod | None:
    return STATE.pods.get((namespace, name))


def _field_selector_matches(pod: SimPod, selector: str) -> bool:
    if not selector:
        return True
    for term in selector.split(","):
        if "=" not in term:
            continue
        key, _, value = term.partition("=")
        key = key.strip()
        value = value.lstrip("=")
        if key == "spec.nodeName" and pod.node != value:
            return False
        if key == "metadata.namespace" and pod.namespace != value:
            return False
        if key == "metadata.name" and pod.name != value:
            return False
        if key == "status.phase" and pod.phase != value:
            return False
    return True


def _eviction_precondition_uid(body: dict[str, Any]) -> str | None:
    options = body.get("deleteOptions") or {}
    preconditions = options.get("preconditions") or {}
    return preconditions.get("uid")


# -- discovery ---------------------------------------------------------------
@app.get("/api/v1")
def core_api_resources() -> dict[str, Any]:
    return {
        "kind": "APIResourceList",
        "apiVersion": "v1",
        "groupVersion": "v1",
        "resources": CORE_RESOURCES,
    }


@app.get("/apis")
def api_groups() -> dict[str, Any]:
    groups = []
    for name, doc in GROUP_VERSIONS.items():
        groups.append(
            {
                "name": name,
                "versions": [
                    {"groupVersion": f"{name}/{v}", "version": v} for v in doc["versions"]
                ],
                "preferredVersion": {
                    "groupVersion": f"{name}/{doc['preferred']}",
                    "version": doc["preferred"],
                },
            }
        )
    return {"kind": "APIGroupList", "apiVersion": "v1", "groups": groups}


@app.get("/apis/{group}/{version}")
def group_api_resources(group: str, version: str) -> Response:
    doc = GROUP_VERSIONS.get(group)
    if doc is None or version not in doc["versions"]:
        return status_response(
            404, "NotFound", f"the server could not find the requested resource"
        )
    with STATE.lock:
        failing = group in STATE.failing_groups
    if failing:
        return status_response(
            500, "InternalError", f"discovery for group {group} is unavailable"
        )
    return JSONResponse(
        {
            "kind": "APIResourceList",
            "apiVersion": "v1",
            "groupVersion": f"{group}/{version}",
            "resources": doc["resources"][version],
        }
    )


# -- namespaces ---------------------------------------------------------------
@app.get("/api/v1/namespaces/{name}")
def read_namespace(name: str) -> Response:
    with STATE.lock:
        ns = STATE.namespaces.get(name)
        if ns is None:
            return status_response(404, "NotFound", f'namespaces "{name}" not found')
        return JSONResponse(STATE.namespace_json(ns))


# -- generic paginated namespaced lists ---------------------------------------
def _namespaced_listing(
    group: str,
    version: str,
    resource: str,
    namespace: str,
    limit: int | None,
    token: str | None,
) -> Response:
    with STATE.lock:
        if group == "" and version == "v1" and resource == "pods":
            kind = "Pod"
            items = [
                STATE.pod_json(pod)
                for pod in STATE.pods.values()
                if pod.namespace == namespace
            ]
        elif group == "policy" and version == "v1" and resource == "poddisruptionbudgets":
            kind = "PodDisruptionBudget"
            items = [
                STATE.pdb_json(pdb)
                for pdb in STATE.pdbs.values()
                if pdb.namespace == namespace
            ]
        elif (group, resource) in GENERIC_KINDS:
            doc = GROUP_VERSIONS.get(group) if group else None
            if group:
                advertised = (doc or {}).get("resources", {}).get(version)
                if advertised is None or resource not in {r["name"] for r in advertised}:
                    return status_response(
                        404, "NotFound", "the server could not find the requested resource"
                    )
            elif version != "v1":
                return status_response(
                    404, "NotFound", "the server could not find the requested resource"
                )
            kind = GENERIC_KINDS[(group, resource)][1]
            items = [
                STATE.object_json(obj)
                for obj in STATE.objects.values()
                if obj.group == group
                and obj.resource == resource
                and obj.namespace == namespace
            ]
        else:
            return status_response(
                404, "NotFound", "the server could not find the requested resource"
            )
        api_version = f"{group}/{version}" if group else version
        for item in items:
            item["apiVersion"] = api_version
        items.sort(key=lambda item: item["metadata"]["name"])

        result = STATE.paginate(items, limit, token)
        if result is None:
            return status_response(
                410,
                "Expired",
                "continue token expired; relist to obtain a fresh snapshot",
            )
        page, next_token = result

        # One-shot drill: the first page of this list goes out, then its
        # objects vanish and every continue token (including this one) dies.
        if (
            STATE.drill_vanish
            and not token
            and next_token
            and STATE.drill_vanish.get("group") == group
            and STATE.drill_vanish.get("resource") == resource
        ):
            STATE.delete_listed(group, resource, namespace, page)
            STATE.page_epoch += 1
            STATE.drill_vanish = None
            STATE._event(
                "drill",
                f"预设触发：{group or 'core'}/{resource} 第一页 {len(page)} 个对象已移除，"
                "续页令牌已过期（410）",
            )

        metadata: dict[str, Any] = {"resourceVersion": "1"}
        if next_token:
            metadata["continue"] = next_token
        return JSONResponse(
            {
                "apiVersion": api_version,
                "kind": f"{kind}List",
                "metadata": metadata,
                "items": page,
            }
        )


@app.get("/api/v1/namespaces/{namespace}/{resource}")
def list_core_namespaced(
    namespace: str,
    resource: str,
    limit: int | None = None,
    continue_: str | None = Query(default=None, alias="continue"),
) -> Response:
    return _namespaced_listing("", "v1", resource, namespace, limit, continue_)


@app.get("/apis/{group}/{version}/namespaces/{namespace}/{resource}")
def list_group_namespaced(
    group: str,
    version: str,
    namespace: str,
    resource: str,
    limit: int | None = None,
    continue_: str | None = Query(default=None, alias="continue"),
) -> Response:
    return _namespaced_listing(group, version, resource, namespace, limit, continue_)


# -- Kubernetes API surface ------------------------------------------------
@app.get("/api/v1/nodes")
def list_nodes() -> dict[str, Any]:
    with STATE.lock:
        items = [STATE.node_json(node) for node in STATE.nodes.values()]
    return {
        "apiVersion": "v1",
        "kind": "NodeList",
        "metadata": {"resourceVersion": "1"},
        "items": items,
    }


@app.get("/api/v1/nodes/{name}")
def read_node(name: str) -> Response:
    with STATE.lock:
        node = STATE.nodes.get(name)
        if node is None:
            return status_response(404, "NotFound", f'nodes "{name}" not found')
        return JSONResponse(STATE.node_json(node))


@app.patch("/api/v1/nodes/{name}")
async def patch_node(name: str, request: Request) -> Response:
    body = await request.json()
    with STATE.lock:
        node = STATE.nodes.get(name)
        if node is None:
            return status_response(404, "NotFound", f'nodes "{name}" not found')
        if isinstance(body, dict):
            desired = body.get("spec", {}).get("unschedulable")
            if desired is not None:
                node.unschedulable = bool(desired)
                node.resource_version += 1
                STATE._event(
                    "cordon" if node.unschedulable else "uncordon",
                    f"节点 {name} 已{'封锁' if node.unschedulable else '解除封锁'}",
                )
        return JSONResponse(STATE.node_json(node))


@app.get("/api/v1/pods")
def list_pods(fieldSelector: str | None = None, labelSelector: str | None = None) -> dict[str, Any]:
    with STATE.lock:
        pods = [
            STATE.pod_json(pod)
            for pod in STATE.pods.values()
            if _field_selector_matches(pod, fieldSelector or "")
        ]
    if labelSelector:
        # The drain UI does not use this, but accept the usual k=v form.
        required = {}
        for term in labelSelector.split(","):
            if "=" in term:
                key, _, value = term.partition("=")
                required[key.strip()] = value.strip()
        pods = [
            pod
            for pod in pods
            if all(pod["metadata"].get("labels", {}).get(k) == v for k, v in required.items())
        ]
    return {
        "apiVersion": "v1",
        "kind": "PodList",
        "metadata": {"resourceVersion": "1"},
        "items": pods,
    }


@app.get("/api/v1/namespaces/{namespace}/pods/{name}")
def read_pod(namespace: str, name: str) -> Response:
    with STATE.lock:
        pod = _find_pod(namespace, name)
        if pod is None:
            return status_response(
                404, "NotFound", f'pods "{name}" not found in namespace "{namespace}"'
            )
        return JSONResponse(STATE.pod_json(pod))


@app.get("/api/v1/pods/{name}")
def read_pod_any_namespace(name: str) -> Response:
    with STATE.lock:
        matches = [pod for pod in STATE.pods.values() if pod.name == name]
        if not matches:
            return status_response(404, "NotFound", f'pods "{name}" not found')
        return JSONResponse(STATE.pod_json(matches[0]))


@app.post("/api/v1/namespaces/{namespace}/pods/{name}/eviction")
async def evict_pod(namespace: str, name: str, request: Request) -> Response:
    body = await request.json()
    precondition_uid = _eviction_precondition_uid(body)
    with STATE.lock:
        pod = _find_pod(namespace, name)
        if pod is None:
            return status_response(
                404, "NotFound", f'pods "{name}" not found in namespace "{namespace}"'
            )
        if precondition_uid and precondition_uid != pod.uid:
            STATE._event(
                "eviction-rejected",
                f"{namespace}/{name} 的 UID 前置条件不匹配（请求 {precondition_uid[:8]}，当前 {pod.uid[:8]}）",
            )
            return status_response(
                409,
                "Conflict",
                f'Eviction of pod "{name}" rejected: UID precondition failed',
            )
        if pod.static:
            return status_response(
                500, "InternalError", f'cannot evict static pod "{namespace}/{name}"'
            )
        if pod.terminating:
            STATE._event("eviction-idempotent", f"{namespace}/{name} 已在退出中")
            return JSONResponse(
                {"apiVersion": "policy/v1", "kind": "Eviction", "metadata": {"name": name}},
                status_code=201,
            )
        matching_pdbs = [
            pdb
            for pdb in STATE.pdbs.values()
            if pdb.namespace == namespace
            and all(pod.labels.get(k) == v for k, v in pdb.selector.items())
        ]
        # Kubernetes counts the candidate pod as healthy when deciding; the
        # eviction is allowed while disruptionsAllowed is positive and the
        # status recomputes once the pod starts terminating.
        for pdb in matching_pdbs:
            allowed = pdb.budget(list(STATE.pods.values()))["disruptionsAllowed"]
            if allowed <= 0:
                STATE._event(
                    "eviction-blocked",
                    f"{namespace}/{name} 受 {pdb.namespace}/{pdb.name} 阻挡",
                )
                return status_response(
                    429,
                    "TooManyRequests",
                    "Cannot evict pod as it would violate the pod's disruption budget.",
                )
        pod.terminating = True
        pod.deletion_timestamp = _now_iso()
        pod.resource_version += 1
        STATE._event("eviction", f"{namespace}/{name} 获准驱逐")
        return JSONResponse(
            {
                "apiVersion": "policy/v1",
                "kind": "Eviction",
                "metadata": {"name": name, "namespace": namespace},
            },
            status_code=201,
        )


@app.get("/apis/policy/v1/poddisruptionbudgets")
def list_pdbs() -> dict[str, Any]:
    with STATE.lock:
        items = [STATE.pdb_json(pdb) for pdb in STATE.pdbs.values()]
    return {
        "apiVersion": "policy/v1",
        "kind": "PodDisruptionBudgetList",
        "metadata": {"resourceVersion": "1"},
        "items": items,
    }


@app.get("/apis/policy/v1/namespaces/{namespace}/poddisruptionbudgets/{name}")
def read_pdb(namespace: str, name: str) -> Response:
    with STATE.lock:
        pdb = STATE.pdbs.get((namespace, name))
        if pdb is None:
            return status_response(404, "NotFound", f'poddisruptionbudgets "{name}" not found')
        return JSONResponse(STATE.pdb_json(pdb))


@app.get("/healthz")
def healthz() -> dict[str, str]:
    return {"status": "ok"}


# -- rehearsal controls ----------------------------------------------------
@app.get("/api/sim/state")
def sim_state() -> dict[str, Any]:
    with STATE.lock:
        pods = list(STATE.pods.values())
        return {
            "nodes": [
                {
                    "name": node.name,
                    "unschedulable": node.unschedulable,
                    "ready": node.ready,
                }
                for node in sorted(STATE.nodes.values(), key=lambda n: n.name)
            ],
            "namespaces": [
                {
                    "name": ns.name,
                    "uid": ns.uid,
                    "phase": ns.phase,
                    "finalizers": ns.finalizers,
                }
                for ns in sorted(STATE.namespaces.values(), key=lambda n: n.name)
            ],
            "objects": [
                {
                    "group": obj.group,
                    "resource": obj.resource,
                    "namespace": obj.namespace,
                    "name": obj.name,
                    "kind": obj.kind,
                    "finalizers": obj.finalizers,
                }
                for obj in sorted(
                    STATE.objects.values(),
                    key=lambda o: (o.group, o.resource, o.namespace, o.name),
                )
            ],
            "drills": {
                "vanish_after_first_page": STATE.drill_vanish,
                "failing_groups": sorted(STATE.failing_groups),
            },
            "pods": [
                {
                    "namespace": pod.namespace,
                    "name": pod.name,
                    "uid": pod.uid,
                    "node": pod.node,
                    "labels": pod.labels,
                    "owner_kind": pod.owner_kind,
                    "static": pod.static,
                    "ready": pod.ready,
                    "phase": pod.phase,
                    "terminating": pod.terminating,
                    "healthy": pod.healthy,
                }
                for pod in sorted(pods, key=lambda p: (p.namespace, p.name))
            ],
            "budgets": [
                {
                    "namespace": pdb.namespace,
                    "name": pdb.name,
                    "selector": pdb.selector,
                    "minAvailable": pdb.min_available,
                    "maxUnavailable": pdb.max_unavailable,
                    "generation": pdb.generation,
                    **pdb.budget(pods),
                }
                for pdb in sorted(STATE.pdbs.values(), key=lambda p: (p.namespace, p.name))
            ],
            "events": STATE.events[:40],
        }


@app.post("/api/sim/budgets/{namespace}/{name}")
async def update_budget(namespace: str, name: str, request: Request) -> Response:
    body = await request.json()
    with STATE.lock:
        pdb = STATE.pdbs.get((namespace, name))
        if pdb is None:
            return status_response(404, "NotFound", f'poddisruptionbudgets "{name}" not found')
        if "minAvailable" in body:
            value = body["minAvailable"]
            if value is None:
                pdb.min_available = None
            else:
                value = int(value)
                if value < 0:
                    return status_response(422, "Invalid", "minAvailable 不能为负")
                pdb.min_available = value
                pdb.max_unavailable = None
        elif "maxUnavailable" in body:
            value = int(body["maxUnavailable"])
            if value < 0:
                return status_response(422, "Invalid", "maxUnavailable 不能为负")
            pdb.max_unavailable = value
            pdb.min_available = None
        else:
            return status_response(422, "Invalid", "需要 minAvailable 或 maxUnavailable")
        pdb.generation += 1
        pdb.resource_version += 1
        STATE._event(
            "budget",
            f"预算 {namespace}/{name} 调整为 "
            + (
                f"minAvailable={pdb.min_available}"
                if pdb.min_available is not None
                else f"maxUnavailable={pdb.max_unavailable}"
            ),
        )
        return JSONResponse(STATE.pdb_json(pdb))


@app.post("/api/sim/pods/{namespace}/{name}/replace")
async def replace_pod(namespace: str, name: str, request: Request) -> Response:
    """Re-create the pod under the same name with a new UID.

    This is how a StatefulSet behaves when its pod is evicted: the name is
    reused, the object identity is not.
    """
    body: dict[str, Any] = {}
    if request.headers.get("content-length") not in (None, "0"):
        body = await request.json()
    with STATE.lock:
        old = _find_pod(namespace, name)
        if old is None:
            return status_response(404, "NotFound", f'pods "{name}" not found')
        node = body.get("node") or old.node
        if node not in STATE.nodes:
            return status_response(422, "Invalid", f"节点 {node} 不存在")
        ready = bool(body.get("ready", True))
        replacement = SimPod(
            namespace=namespace,
            name=name,
            node=node,
            labels=dict(old.labels),
            owner_kind=old.owner_kind,
            static=old.static,
            ready=ready,
        )
        STATE.pods[replacement.key] = replacement
        STATE._event(
            "replace",
            f"{namespace}/{name} 同名重建：UID {old.uid[:8]} → {replacement.uid[:8]}，节点 {node}",
        )
        return JSONResponse(
            {
                "previous_uid": old.uid,
                "pod": {
                    "namespace": namespace,
                    "name": name,
                    "uid": replacement.uid,
                    "node": node,
                    "ready": ready,
                },
            },
            status_code=201,
        )


@app.post("/api/sim/pods")
async def create_pod(request: Request) -> Response:
    """Add a pod directly, for scenarios such as a finished Job pod.

    This is a rehearsal convenience, not a Kubernetes API endpoint.
    """
    body = await request.json()
    namespace = str(body.get("namespace") or "default")
    name = str(body.get("name") or "")
    node = body.get("node") or NODE_NAMES[0]
    if not name:
        return status_response(422, "Invalid", "需要 name")
    with STATE.lock:
        if node not in STATE.nodes:
            return status_response(422, "Invalid", f"节点 {node} 不存在")
        if (namespace, name) in STATE.pods:
            return status_response(
                409, "AlreadyExists", f'pods "{name}" already exists'
            )
        pod = SimPod(
            namespace=namespace,
            name=name,
            node=node,
            labels=dict(body.get("labels") or {}),
            owner_kind=body.get("owner_kind"),
            static=bool(body.get("static", False)),
            ready=bool(body.get("ready", True)),
            phase=str(body.get("phase") or "Running"),
        )
        STATE.pods[pod.key] = pod
        STATE.namespaces.setdefault(namespace, SimNamespace(name=namespace))
        STATE._event("create", f"创建 Pod {namespace}/{name} 于 {node}")
        return JSONResponse(STATE.pod_json(pod), status_code=201)


@app.post("/api/sim/pods/{namespace}/{name}/ready")
async def set_ready(namespace: str, name: str, request: Request) -> Response:
    body = await request.json()
    with STATE.lock:
        pod = _find_pod(namespace, name)
        if pod is None:
            return status_response(404, "NotFound", f'pods "{name}" not found')
        pod.ready = bool(body.get("ready", True))
        pod.resource_version += 1
        STATE._event(
            "ready",
            f"{namespace}/{name} 标记为{'就绪' if pod.ready else '未就绪'}",
        )
        return JSONResponse(STATE.pod_json(pod))


@app.post("/api/sim/pods/{namespace}/{name}/exit")
async def finish_terminating(namespace: str, name: str) -> Response:
    """Let a pod that already has a deletion timestamp finish terminating."""
    with STATE.lock:
        pod = _find_pod(namespace, name)
        if pod is None:
            return status_response(404, "NotFound", f'pods "{name}" not found')
        if not pod.terminating:
            return status_response(409, "Conflict", "Pod 尚未进入退出流程，拒绝直接移除")
        del STATE.pods[pod.key]
        STATE._event("exit", f"{namespace}/{name} 已退出节点 {pod.node}")
        return JSONResponse({"exited": True, "namespace": namespace, "name": name})


@app.post("/api/sim/pods/exit-terminating")
def finish_all_terminating() -> dict[str, Any]:
    with STATE.lock:
        finished = [
            f"{pod.namespace}/{pod.name}"
            for pod in list(STATE.pods.values())
            if pod.terminating
        ]
        for pod in [p for p in STATE.pods.values() if p.terminating]:
            STATE.pods.pop(pod.key, None)
        if finished:
            STATE._event("exit", f"{len(finished)} 个待退出 Pod 已退出")
        return {"exited": finished}


@app.post("/api/sim/drills/vanish-after-first-page")
async def arm_vanish_drill(request: Request) -> Response:
    """One-shot: the next first page of this list loses its objects and its
    continue token expires (the follow-up page gets a 410)."""
    body = await request.json()
    group = str(body.get("group") or "")
    resource = str(body.get("resource") or "")
    if not resource:
        return status_response(422, "Invalid", "需要 resource")
    with STATE.lock:
        STATE.drill_vanish = {"group": group, "resource": resource}
        STATE._event(
            "drill",
            f"已预设：{group or 'core'}/{resource} 第一页返回后移除页内对象并让续页过期（一次性）",
        )
        return JSONResponse({"armed": True, "group": group, "resource": resource})


@app.post("/api/sim/drills/discovery-failure")
async def arm_discovery_failure(request: Request) -> Response:
    """Make one API group's discovery document fail until cleared."""
    body = await request.json()
    group = str(body.get("group") or "")
    if not group:
        return status_response(422, "Invalid", "需要 group")
    if group not in GROUP_VERSIONS:
        return status_response(404, "NotFound", f"未知 API 组 {group}")
    with STATE.lock:
        STATE.failing_groups.add(group)
        STATE._event("drill", f"已预设：API 组 {group} 的发现将失败")
        return JSONResponse({"armed": True, "group": group})


@app.post("/api/sim/drills/clear")
def clear_drills() -> dict[str, bool]:
    with STATE.lock:
        STATE.drill_vanish = None
        STATE.failing_groups.clear()
        STATE._event("drill", "已清除所有扫描演练预设")
    return {"cleared": True}


@app.post("/api/sim/namespaces/{name}/remove")
def remove_namespace(name: str) -> Response:
    with STATE.lock:
        if name not in STATE.namespaces:
            return status_response(404, "NotFound", f'namespaces "{name}" not found')
        del STATE.namespaces[name]
        STATE._event("namespace", f"命名空间 {name} 已删除")
        return JSONResponse({"removed": name})


@app.post("/api/sim/namespaces/{name}/recreate")
def recreate_namespace(name: str) -> Response:
    """Delete and re-create the namespace under the same name (new UID)."""
    with STATE.lock:
        old = STATE.namespaces.get(name)
        STATE.namespaces[name] = SimNamespace(name=name)
        # A re-created namespace starts empty; sweep the leftovers.
        STATE.pods = {k: v for k, v in STATE.pods.items() if k[0] != name}
        STATE.pdbs = {k: v for k, v in STATE.pdbs.items() if k[0] != name}
        STATE.objects = {k: v for k, v in STATE.objects.items() if k[2] != name}
        new = STATE.namespaces[name]
        STATE._event(
            "namespace",
            f"命名空间 {name} 同名重建："
            + (f"UID {old.uid[:8]} → " if old else "（此前不存在）")
            + f"{new.uid[:8]}，残留已清空",
        )
        return JSONResponse({"recreated": name, "uid": new.uid})


@app.post("/api/sim/objects")
async def create_object(request: Request) -> Response:
    """Add a generic namespaced object (rehearsal convenience)."""
    body = await request.json()
    group = str(body.get("group") or "")
    resource = str(body.get("resource") or "")
    namespace = str(body.get("namespace") or "")
    name = str(body.get("name") or "")
    finalizers = [str(f) for f in (body.get("finalizers") or []) if str(f)]
    if (group, resource) not in GENERIC_KINDS:
        return status_response(422, "Invalid", f"未知的资源类型 {group}/{resource}")
    if not namespace or not name:
        return status_response(422, "Invalid", "需要 namespace 与 name")
    with STATE.lock:
        api_version, kind = GENERIC_KINDS[(group, resource)]
        obj = SimObject(
            group=group,
            resource=resource,
            namespace=namespace,
            name=name,
            kind=kind,
            api_version=api_version,
            finalizers=finalizers,
        )
        if obj.key in STATE.objects:
            return status_response(409, "AlreadyExists", f"{resource} \"{name}\" already exists")
        STATE.objects[obj.key] = obj
        STATE.namespaces.setdefault(namespace, SimNamespace(name=namespace))
        STATE._event(
            "create",
            f"创建对象 {group or 'core'}/{resource} {namespace}/{name}"
            + (f"（finalizers: {', '.join(finalizers)}）" if finalizers else ""),
        )
        return JSONResponse(STATE.object_json(obj), status_code=201)


@app.post("/api/sim/reset")
def reset_cluster() -> dict[str, str]:
    STATE.reset()
    return {"status": "reset"}


@app.get("/", response_class=HTMLResponse)
def dashboard(request: Request) -> HTMLResponse:
    return templates.TemplateResponse(request, "sim.html", {"request": request})


if __name__ == "__main__":  # pragma: no cover
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8080)
