"""Behavioural tests for the namespace residue scanner.

The HTTP-level tests run against the rehearsal simulator; the void / 410
edge cases use a scripted fake client so the failure can be injected at
exactly the right moment.
"""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from app.kube import KubeError, KubeGone
from app.main import create_app
from app.models import NamespaceInfo
from app.nsscan import ScanError, ScanManager

# -- helpers -----------------------------------------------------------------


def type_of(report, resource):
    return next(t for t in report.types if t.ref.resource == resource)


class FakeKube:
    """Minimal stand-in for KubeClient: queued namespace reads, queued docs."""

    def __init__(self):
        self.ns_reads: list = []
        self.docs: dict[str, list] = {}
        self.list_calls: list[tuple[str, list]] = []

    def get_namespace(self, name):
        result = self.ns_reads.pop(0)
        if isinstance(result, Exception):
            raise result
        return result

    def get_json(self, path, params=None):
        self.list_calls.append((path, list(params or [])))
        queue = self.docs[path]
        result = queue.pop(0) if len(queue) > 1 else queue[0]
        if isinstance(result, Exception):
            raise result
        return result


def fake_discovery(resources):
    return {
        "/api/v1": [{"resources": resources}],
        "/apis": [{"groups": []}],
    }


PODS_RESOURCE = {"name": "pods", "namespaced": True, "kind": "Pod", "verbs": ["get", "list"]}
EMPTY_PAGE = {"items": [], "metadata": {}}


# -- discovery + counting against the simulator -------------------------------


def test_scan_counts_residue_by_type(scanner):
    report = scanner.scan("stuck-ns")

    assert not report.void
    assert report.phase == "Terminating"
    assert report.gaps == []
    assert all(t.complete for t in report.types)
    assert not report.all_clear

    by_resource = {t.ref.resource: t for t in report.types}
    assert by_resource["pods"].count == 8
    assert by_resource["configmaps"].count == 3
    assert by_resource["secrets"].count == 0
    assert by_resource["poddisruptionbudgets"].count == 1
    assert by_resource["rollouts"].count == 2
    assert by_resource["experiments"].count == 1
    assert report.total == 15


def test_discovery_skips_subresources_cluster_scoped_and_no_list(scanner):
    report = scanner.scan("stuck-ns")
    names = {t.ref.resource for t in report.types}

    assert "pods/eviction" not in names  # 子资源
    assert "pods/status" not in names
    assert "poddisruptionbudgets/status" not in names
    assert "namespaces" not in names  # 集群级
    assert "nodes" not in names
    assert "bindings" not in names  # 没有 list 动词


def test_same_group_resource_scanned_once_at_preferred_version(scanner):
    report = scanner.scan("stuck-ns")

    rollouts = [t for t in report.types if t.ref.resource == "rollouts"]
    assert len(rollouts) == 1
    assert rollouts[0].ref.group == "workloads.example.io"
    assert rollouts[0].ref.version == "v1"  # 首选版本，而不是 v1alpha1
    # v1alpha1 独有的资源仍然会被发现
    experiments = type_of(report, "experiments")
    assert experiments.ref.version == "v1alpha1"


def test_finalizer_samples_are_capped_at_five(scanner):
    report = scanner.scan("stuck-ns")

    pods = type_of(report, "pods")
    assert pods.count == 8
    assert pods.finalizer_objects == 6
    assert len(pods.samples) == 5
    for sample in pods.samples:
        assert sample.name.startswith("stuck-pod-")
        assert sample.finalizers == ["example.com/cleanup"]


def test_all_clear_requires_zero_and_complete(scanner):
    report = scanner.scan("default")  # 空的 Active 命名空间

    assert report.fully_scanned
    assert report.total == 0
    assert report.all_clear


def test_scan_rejects_unknown_or_blank_namespace(scanner):
    with pytest.raises(ScanError):
        scanner.scan("no-such-namespace")
    with pytest.raises(ScanError):
        scanner.scan("   ")


# -- drills against the simulator ---------------------------------------------


def test_vanish_after_first_page_drill_forces_410_rescan(scanner, sim):
    sim.post(
        "/api/sim/drills/vanish-after-first-page",
        json={"group": "", "resource": "pods"},
    )
    report = scanner.scan("stuck-ns")

    pods = type_of(report, "pods")
    # 第一页 3 个对象被移除、续页 410：丢弃已读结果从头再扫，扫到剩下的 5 个。
    assert pods.restarts == 1
    assert pods.complete
    assert pods.count == 5
    # 一次性预设触发后自动解除
    assert sim.get("/api/sim/state").json()["drills"]["vanish_after_first_page"] is None


def test_discovery_failure_marks_gap_and_keeps_other_results(scanner, sim):
    sim.post("/api/sim/drills/discovery-failure", json={"group": "workloads.example.io"})
    report = scanner.scan("stuck-ns")

    assert not report.void
    assert not report.all_clear
    gap_scopes = {gap.scope for gap in report.gaps}
    assert "workloads.example.io/v1" in gap_scopes
    assert "workloads.example.io/v1alpha1" in gap_scopes
    # 该组的类型缺失，但其他组的结果保留
    names = {t.ref.resource for t in report.types}
    assert "rollouts" not in names
    assert "experiments" not in names
    assert type_of(report, "pods").count == 8
    assert type_of(report, "configmaps").complete


# -- void scans (scripted fake client) ----------------------------------------


def _ns(uid, phase="Terminating"):
    return NamespaceInfo(name="demo", uid=uid, phase=phase)


def _fake_with_empty_scan():
    kube = FakeKube()
    kube.docs = fake_discovery([PODS_RESOURCE])
    kube.docs["/api/v1/namespaces/demo/pods"] = [EMPTY_PAGE]
    return kube


def test_namespace_disappearing_mid_scan_voids_the_report():
    kube = _fake_with_empty_scan()
    kube.ns_reads = [_ns("uid-1"), None]  # 扫描结束时命名空间已消失
    report = ScanManager(kube).scan("demo")

    assert report.void
    assert "已消失" in report.void_reason
    assert not report.all_clear


def test_same_name_recreate_mid_scan_voids_the_report():
    kube = _fake_with_empty_scan()
    kube.ns_reads = [_ns("uid-1"), _ns("uid-2")]  # UID 变化 = 同名重建
    report = ScanManager(kube).scan("demo")

    assert report.void
    assert "同名重建" in report.void_reason
    assert not report.all_clear


def test_failed_namespace_recheck_voids_the_report():
    kube = _fake_with_empty_scan()
    kube.ns_reads = [_ns("uid-1"), KubeError("connection reset")]
    report = ScanManager(kube).scan("demo")

    assert report.void
    assert "复核失败" in report.void_reason


def test_repeated_410_marks_type_incomplete_and_discards_partials():
    kube = FakeKube()
    kube.ns_reads = [_ns("uid-1"), _ns("uid-1")]
    kube.docs = fake_discovery([PODS_RESOURCE])
    page_one = {
        "items": [{"metadata": {"name": "a", "finalizers": ["f"]}}],
        "metadata": {"continue": "t1"},
    }
    # 第一页 → 续页 410 → 重扫第一页 → 续页再次 410（超过允许的重扫次数）
    kube.docs["/api/v1/namespaces/demo/pods"] = [
        page_one,
        KubeGone("410"),
        page_one,
        KubeGone("410"),
    ]
    report = ScanManager(kube, page_size=1, max_restarts=1).scan("demo")

    pods = report.types[0]
    assert not pods.complete
    assert pods.restarts == 1
    assert "410" in pods.error
    # 已读的计数与样本被丢弃，不作为结果展示
    assert pods.count == 0
    assert pods.samples == []
    assert not report.all_clear


def test_single_410_recovers_with_fresh_rescan():
    kube = FakeKube()
    kube.ns_reads = [_ns("uid-1"), _ns("uid-1")]
    kube.docs = fake_discovery([PODS_RESOURCE])
    page_one = {
        "items": [{"metadata": {"name": "a", "finalizers": ["f"]}}],
        "metadata": {"continue": "t1"},
    }
    final_page = {"items": [{"metadata": {"name": "b"}}], "metadata": {}}
    kube.docs["/api/v1/namespaces/demo/pods"] = [page_one, KubeGone("410"), final_page]
    report = ScanManager(kube, page_size=1, max_restarts=1).scan("demo")

    pods = report.types[0]
    assert pods.complete
    assert pods.restarts == 1
    # 只保留重扫后的结果：第一页的 "a" 已被丢弃
    assert pods.count == 1
    assert pods.samples == []


def test_list_failure_marks_one_type_and_keeps_the_rest():
    kube = FakeKube()
    kube.ns_reads = [_ns("uid-1"), _ns("uid-1")]
    kube.docs = fake_discovery(
        [
            PODS_RESOURCE,
            {"name": "configmaps", "namespaced": True, "kind": "ConfigMap", "verbs": ["list"]},
        ]
    )
    kube.docs["/api/v1/namespaces/demo/pods"] = [KubeError("500 boom")]
    kube.docs["/api/v1/namespaces/demo/configmaps"] = [
        {"items": [{"metadata": {"name": "cm-0"}}, {"metadata": {"name": "cm-1"}}], "metadata": {}}
    ]
    report = ScanManager(kube).scan("demo")

    pods = type_of(report, "pods")
    assert not pods.complete
    assert "500" in pods.error
    configmaps = type_of(report, "configmaps")
    assert configmaps.complete and configmaps.count == 2
    assert report.total == 2  # 只统计已查全的类型
    assert not report.fully_scanned
    assert not report.all_clear


def test_core_discovery_failure_is_a_gap_not_a_crash():
    kube = FakeKube()
    kube.ns_reads = [_ns("uid-1"), _ns("uid-1")]
    kube.docs = {
        "/api/v1": [KubeError("500 discovery broken")],
        "/apis": [{"groups": []}],
    }
    report = ScanManager(kube).scan("demo")

    assert report.types == []
    assert len(report.gaps) == 1
    assert "core" in report.gaps[0].scope
    assert not report.all_clear  # 计数为零也不许显示「未发现残留」


# -- web pages -----------------------------------------------------------------


@pytest.fixture()
def client(simulator_url, monkeypatch):
    monkeypatch.setenv("DRAIN_KUBE_MODE", "host")
    monkeypatch.setenv("DRAIN_KUBE_HOST", simulator_url)
    monkeypatch.setenv("DRAIN_SCAN_PAGE_SIZE", "3")
    return TestClient(create_app())


def start_scan(client: TestClient, namespace: str) -> str:
    response = client.post(
        "/ns-scan", data={"namespace": namespace}, follow_redirects=False
    )
    assert response.status_code == 303
    return response.headers["location"]


def test_scan_page_lists_types_and_finalizer_samples(client):
    location = start_scan(client, "stuck-ns")
    page = client.get(location).text

    assert "stuck-ns" in page
    assert "Terminating" in page
    assert "rollouts" in page
    assert "workloads.example.io" in page
    assert "stuck-pod-0" in page
    assert "example.com/cleanup" in page
    assert "本次未发现残留" not in page

    payload = client.get(location.replace("/ns-scan/", "/api/ns-scan/")).json()
    assert payload["total"] == 15
    assert payload["all_clear"] is False
    assert payload["void"] is False
    pods = next(t for t in payload["types"] if t["resource"] == "pods")
    assert pods["count"] == 8
    assert pods["finalizer_objects"] == 6
    assert len(pods["samples"]) == 5
    assert pods["samples"][0]["finalizers"] == ["example.com/cleanup"]


def test_all_clear_page_shows_the_phrase_only_when_fully_scanned(client):
    location = start_scan(client, "default")
    page = client.get(location).text

    assert "本次未发现残留" in page
    payload = client.get(location.replace("/ns-scan/", "/api/ns-scan/")).json()
    assert payload["all_clear"] is True
    assert payload["total"] == 0


def test_discovery_failure_page_marks_gap_and_hides_all_clear(client, sim):
    sim.post("/api/sim/drills/discovery-failure", json={"group": "policy"})
    location = start_scan(client, "default")
    page = client.get(location).text

    assert "未查全" in page
    assert "policy/v1" in page
    assert "本次未发现残留" not in page


def test_scan_of_missing_namespace_redirects_with_error(client):
    response = client.post(
        "/ns-scan", data={"namespace": "no-such"}, follow_redirects=False
    )
    assert response.status_code == 303
    assert response.headers["location"].startswith("/?error=")


def test_unknown_scan_id_renders_notice(client):
    response = client.get("/ns-scan/nope")
    assert response.status_code == 200
    assert "不存在" in response.text


def test_index_lists_recent_scans(client):
    start_scan(client, "stuck-ns")
    page = client.get("/").text
    assert "命名空间残留扫描" in page
    assert "stuck-ns" in page
