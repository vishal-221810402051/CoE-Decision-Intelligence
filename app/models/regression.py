from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any


@dataclass
class DriftItem:
    artifact: str
    field_path: str
    severity: str  # critical | soft
    expected: Any
    actual: Any
    message: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class ArtifactComparisonResult:
    artifact_name: str
    pass_status: bool
    critical_drift_count: int
    soft_drift_count: int
    metrics: dict[str, Any] = field(default_factory=dict)
    drift_items: list[DriftItem] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["drift_items"] = [item.to_dict() for item in self.drift_items]
        return payload


@dataclass
class RepeatRunReport:
    meeting_id: str
    runs: int
    pass_status: bool
    artifact_results: list[ArtifactComparisonResult] = field(default_factory=list)
    summary_metrics: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["artifact_results"] = [item.to_dict() for item in self.artifact_results]
        return payload


@dataclass
class MeetingRegressionResult:
    meeting_id: str
    category: str
    difficulty: str
    pass_status: bool
    artifact_results: list[ArtifactComparisonResult] = field(default_factory=list)
    summary_metrics: dict[str, Any] = field(default_factory=dict)
    notes: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["artifact_results"] = [item.to_dict() for item in self.artifact_results]
        return payload


@dataclass
class RegressionSuiteReport:
    total_meetings: int
    passed_meetings: int
    failed_meetings: int
    summary_metrics: dict[str, Any] = field(default_factory=dict)
    meeting_results: list[MeetingRegressionResult] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["meeting_results"] = [item.to_dict() for item in self.meeting_results]
        return payload

