"""命名空间残留扫描：命名空间卡在 Terminating 时，查出还有哪些资源没清掉。

工作方式：

1. 发现——读取 ``/api/v1`` 与 ``/apis``，再按组读取 ``/apis/{group}/{version}``
   （首选版本优先）。只保留**命名空间级、动词含 list、且不是子资源**（名称不
   含 ``/``）的资源；同组同一资源只保留一个版本，首选版本优先。
2. 扫描——每种资源独立分页列表（``limit`` + ``continue``）。内存里只保留
   该类型的计数器和最多 5 个带 finalizer 的样本对象，因此扫描内存只随单页
   大小与资源类型数增长，与对象总数无关。
3. 续页 410——令牌过期时丢弃该类型已读的计数与样本，从头再扫一次；再次
   410 则把该类型标为「未查全」。
4. 失败隔离——发现或列取失败只把对应的组或类型标为「未查全」，其余已查全
   的结果保留。只有所有范围都查全且计数为零，页面才会显示「本次未发现残留」。
5. 作废——扫描前后各读一次命名空间对象：扫描期间命名空间消失、被同名重建
   （UID 变化）或复核失败，本次结果一律作废。
"""

from __future__ import annotations

import threading
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone

from .kube import KubeClient, KubeError, KubeGone

#: 每类资源最多展示多少个带 finalizer 的对象
MAX_SAMPLES = 5


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


class ScanError(RuntimeError):
    pass


@dataclass(frozen=True)
class ResourceRef:
    """One listable namespaced resource type, pinned to a single version."""

    group: str  # "" 表示核心组
    version: str
    resource: str  # 复数名，如 pods
    kind: str

    def list_path(self, namespace: str) -> str:
        if self.group:
            return f"/apis/{self.group}/{self.version}/namespaces/{namespace}/{self.resource}"
        return f"/api/v1/namespaces/{namespace}/{self.resource}"

    @property
    def display(self) -> str:
        group = self.group or "core"
        return f"{self.resource} · {group}/{self.version}"


@dataclass
class FinalizerSample:
    name: str
    finalizers: list[str]


@dataclass
class TypeReport:
    """Scan outcome for one resource type."""

    ref: ResourceRef
    complete: bool = False
    count: int = 0
    finalizer_objects: int = 0
    samples: list[FinalizerSample] = field(default_factory=list)
    restarts: int = 0  # 410 后从头重扫的次数
    error: str = ""


@dataclass
class DiscoveryGap:
    """A discovery document that could not be read; its types were not scanned."""

    scope: str  # 例如 "core (/api/v1)"、"apps/v1"、"全部 API 组 (/apis)"
    error: str


@dataclass
class ScanReport:
    scan_id: str
    namespace: str
    namespace_uid: str
    phase: str
    started_at: str
    finished_at: str = ""
    types: list[TypeReport] = field(default_factory=list)
    gaps: list[DiscoveryGap] = field(default_factory=list)
    void: bool = False
    void_reason: str = ""

    @property
    def total(self) -> int:
        """残留对象总数（仅统计已查全的类型）。"""
        return sum(t.count for t in self.types if t.complete)

    @property
    def incomplete_types(self) -> list[TypeReport]:
        return [t for t in self.types if not t.complete]

    @property
    def fully_scanned(self) -> bool:
        return not self.void and not self.gaps and not self.incomplete_types

    @property
    def all_clear(self) -> bool:
        """只有所有范围查全且计数为零，才允许显示「本次未发现残留」。"""
        return self.fully_scanned and self.total == 0


class ScanManager:
    """Runs namespace scans and keeps the most recent reports in memory."""

    def __init__(
        self,
        kube: KubeClient,
        page_size: int = 500,
        max_restarts: int = 1,
        keep: int = 50,
    ):
        self.kube = kube
        self.page_size = max(1, page_size)
        self.max_restarts = max(0, max_restarts)
        self._keep = max(1, keep)
        self._reports: dict[str, ScanReport] = {}
        self._order: list[str] = []
        self._lock = threading.RLock()

    # -- 查询 ---------------------------------------------------------------
    def get(self, scan_id: str) -> ScanReport | None:
        with self._lock:
            return self._reports.get(scan_id)

    def list(self) -> list[ScanReport]:
        with self._lock:
            return sorted(
                self._reports.values(), key=lambda r: r.started_at, reverse=True
            )

    def _remember(self, report: ScanReport) -> None:
        with self._lock:
            self._reports[report.scan_id] = report
            self._order.append(report.scan_id)
            while len(self._order) > self._keep:
                self._reports.pop(self._order.pop(0), None)

    # -- 发现 ---------------------------------------------------------------
    @staticmethod
    def _listable_namespaced(resource: dict) -> bool:
        name = resource.get("name") or ""
        return (
            bool(resource.get("namespaced"))
            and "/" not in name  # 跳过 pods/status 这类子资源
            and "list" in (resource.get("verbs") or [])
        )

    def _discover(self) -> tuple[list[ResourceRef], list[DiscoveryGap]]:
        refs: dict[tuple[str, str], ResourceRef] = {}
        gaps: list[DiscoveryGap] = []

        def take(doc: dict, group: str, version: str) -> None:
            for res in doc.get("resources") or []:
                if not self._listable_namespaced(res):
                    continue
                key = (group, res["name"])
                if key not in refs:  # 同组同一资源只保留先到的（首选）版本
                    refs[key] = ResourceRef(
                        group=group,
                        version=version,
                        resource=res["name"],
                        kind=res.get("kind") or "",
                    )

        try:
            take(self.kube.get_json("/api/v1"), "", "v1")
        except KubeError as exc:
            gaps.append(DiscoveryGap("core (/api/v1)", str(exc)))

        groups: list[dict] = []
        try:
            groups = (self.kube.get_json("/apis") or {}).get("groups") or []
        except KubeError as exc:
            gaps.append(DiscoveryGap("全部命名 API 组 (/apis)", str(exc)))

        for group_doc in groups:
            group_name = group_doc.get("name") or ""
            preferred = (group_doc.get("preferredVersion") or {}).get("groupVersion")
            versions = [v.get("groupVersion") for v in group_doc.get("versions") or []]
            ordered = [gv for gv in versions if gv]
            if preferred in ordered:
                ordered.remove(preferred)
                ordered.insert(0, preferred)  # 首选版本优先
            for gv in ordered:
                _, _, version = gv.partition("/")
                try:
                    take(self.kube.get_json(f"/apis/{gv}"), group_name, version)
                except KubeError as exc:
                    gaps.append(DiscoveryGap(gv, str(exc)))
        refs_sorted = sorted(refs.values(), key=lambda r: (r.group, r.resource))
        return refs_sorted, gaps

    # -- 扫描 ---------------------------------------------------------------
    def _scan_type(self, ref: ResourceRef, namespace: str) -> TypeReport:
        """分页统计一种资源；410 时丢弃已读结果从头再扫（最多 max_restarts 次）。"""
        report = TypeReport(ref=ref)
        token: str | None = None
        gones = 0
        while True:
            params: list[tuple[str, object]] = [("limit", self.page_size)]
            if token:
                params.append(("continue", token))
            try:
                doc = self.kube.get_json(ref.list_path(namespace), params)
            except KubeGone:
                # 续页令牌已过期：已读的计数与样本不再可信，丢弃后从头再扫。
                gones += 1
                if gones > self.max_restarts:
                    report.error = "续页令牌反复过期（410），本类型未查全"
                    report.count = 0
                    report.finalizer_objects = 0
                    report.samples = []
                    return report
                report.restarts += 1
                report.count = 0
                report.finalizer_objects = 0
                report.samples = []
                token = None
                continue
            except KubeError as exc:
                report.error = str(exc)
                return report
            items = doc.get("items") or []
            report.count += len(items)
            for item in items:
                meta = item.get("metadata") or {}
                finalizers = meta.get("finalizers") or []
                if not finalizers:
                    continue
                report.finalizer_objects += 1
                if len(report.samples) < MAX_SAMPLES:
                    report.samples.append(
                        FinalizerSample(
                            name=meta.get("name") or "?",
                            finalizers=[str(f) for f in finalizers],
                        )
                    )
            token = (doc.get("metadata") or {}).get("continue") or None
            if not token:
                report.complete = True
                return report

    def scan(self, namespace: str) -> ScanReport:
        namespace = (namespace or "").strip()
        if not namespace:
            raise ScanError("请输入命名空间名称")
        before = self.kube.get_namespace(namespace)
        if before is None:
            raise ScanError(f"命名空间 {namespace} 不存在（或已被删除）。")
        report = ScanReport(
            scan_id=uuid.uuid4().hex[:12],
            namespace=before.name,
            namespace_uid=before.uid,
            phase=before.phase,
            started_at=_now(),
        )
        refs, report.gaps = self._discover()
        for ref in refs:
            report.types.append(self._scan_type(ref, namespace))

        # 复核：扫描期间命名空间消失或同名重建（UID 变化），本次扫描作废。
        try:
            after = self.kube.get_namespace(namespace)
        except KubeError as exc:
            report.void = True
            report.void_reason = f"命名空间复核失败（{exc}），本次扫描作废。"
        else:
            if after is None:
                report.void = True
                report.void_reason = "扫描期间命名空间已消失，本次扫描作废。"
            elif after.uid != before.uid:
                report.void = True
                report.void_reason = (
                    "扫描期间命名空间被同名重建（UID 变化），本次扫描作废。"
                )
        report.finished_at = _now()
        self._remember(report)
        return report
