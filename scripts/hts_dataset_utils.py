from __future__ import annotations

import math
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import numpy as np


_FRAME_RE = re.compile(r"\bf\s*=\s*(\d+)")
_TIMESTAMP_RE = re.compile(r"\bt\s*=\s*(\d+)")


def resolve_recording_dir(output_root: str | Path, name: str, session: str | None = None) -> Path:
    dataset_dir = Path(output_root) / name
    if session:
        return dataset_dir / session
    return dataset_dir


def resolve_replay_dir(output_root: str | Path, name: str, session: str | None = None) -> Path:
    dataset_dir = Path(output_root) / name
    if session:
        session_dir = dataset_dir / session
        if not session_dir.exists():
            raise SystemExit(f"Session not found: {session_dir}")
        return session_dir

    if not dataset_dir.exists():
        raise SystemExit(f"Dataset not found: {dataset_dir}")

    session_dirs = [
        child
        for child in sorted(dataset_dir.iterdir())
        if child.is_dir() and child.name != "__MACOSX" and (child / "aligned_frames.parquet").exists()
    ]
    if len(session_dirs) == 1:
        return session_dirs[0]
    if len(session_dirs) > 1:
        session_names = ", ".join(child.name for child in session_dirs)
        raise SystemExit(
            f"Dataset {dataset_dir} contains multiple sessions ({session_names}). Please choose one with --session."
        )
    return dataset_dir


@dataclass
class ParsedTelemetryLine:
    stream: str
    kind: str
    frame_id: int | None
    timestamp_ns: int | None
    values: list[float]
    raw_label: str


def parse_hts_line(line: str) -> ParsedTelemetryLine | None:
    parts = [part.strip() for part in line.split(",")]
    if not parts:
        return None

    raw_label = parts[0].rstrip(":").strip()
    label = raw_label.lower()
    if "landmarks" in label:
        kind = "landmarks"
    elif "wrist" in label:
        kind = "wrist"
    elif "head" in label and "pose" in label:
        kind = "pose"
    else:
        return None

    if "left" in label:
        stream = "left"
    elif "right" in label:
        stream = "right"
    elif "head" in label:
        stream = "head"
    else:
        return None

    frame_match = _FRAME_RE.search(raw_label)
    time_match = _TIMESTAMP_RE.search(raw_label)
    values: list[float] = []
    for part in parts[1:]:
        if not part:
            continue
        try:
            values.append(float(part))
        except ValueError:
            continue

    return ParsedTelemetryLine(
        stream=stream,
        kind=kind,
        frame_id=int(frame_match.group(1)) if frame_match else None,
        timestamp_ns=int(time_match.group(1)) if time_match else None,
        values=values,
        raw_label=raw_label,
    )


def quat_normalize(quat: Iterable[float]) -> np.ndarray:
    arr = np.asarray(list(quat), dtype=np.float64)
    if arr.shape != (4,):
        raise ValueError("Quaternion must contain exactly 4 values.")
    norm = np.linalg.norm(arr)
    if norm <= 0.0:
        return np.array([0.0, 0.0, 0.0, 1.0], dtype=np.float64)
    return arr / norm


def quat_conjugate(quat: Iterable[float]) -> np.ndarray:
    q = quat_normalize(quat)
    return np.array([-q[0], -q[1], -q[2], q[3]], dtype=np.float64)


def quat_multiply(lhs: Iterable[float], rhs: Iterable[float]) -> np.ndarray:
    x1, y1, z1, w1 = quat_normalize(lhs)
    x2, y2, z2, w2 = quat_normalize(rhs)
    return quat_normalize(
        np.array(
            [
                w1 * x2 + x1 * w2 + y1 * z2 - z1 * y2,
                w1 * y2 - x1 * z2 + y1 * w2 + z1 * x2,
                w1 * z2 + x1 * y2 - y1 * x2 + z1 * w2,
                w1 * w2 - x1 * x2 - y1 * y2 - z1 * z2,
            ],
            dtype=np.float64,
        )
    )


def quat_rotate(points: np.ndarray, quat: Iterable[float]) -> np.ndarray:
    q = quat_normalize(quat)
    pts = np.asarray(points, dtype=np.float64)
    q_xyz = q[:3]
    q_w = q[3]
    t = 2.0 * np.cross(q_xyz, pts)
    return pts + q_w * t + np.cross(q_xyz, t)


def euler_deg_to_quat(pitch_deg: float, yaw_deg: float, roll_deg: float) -> np.ndarray:
    pitch = math.radians(pitch_deg) * 0.5
    yaw = math.radians(yaw_deg) * 0.5
    roll = math.radians(roll_deg) * 0.5
    cp, sp = math.cos(pitch), math.sin(pitch)
    cy, sy = math.cos(yaw), math.sin(yaw)
    cr, sr = math.cos(roll), math.sin(roll)
    return quat_normalize(
        np.array(
            [
                sr * cp * cy - cr * sp * sy,
                cr * sp * cy + sr * cp * sy,
                cr * cp * sy - sr * sp * cy,
                cr * cp * cy + sr * sp * sy,
            ],
            dtype=np.float64,
        )
    )


def landmarks_local_to_world(
    wrist_position: Iterable[float], wrist_quaternion: Iterable[float], landmarks_local: Iterable[float]
) -> np.ndarray:
    wrist = np.asarray(list(wrist_position), dtype=np.float64)
    quat = quat_normalize(wrist_quaternion)
    local = np.asarray(list(landmarks_local), dtype=np.float64)
    if local.size % 3 != 0:
        local = local[: local.size - (local.size % 3)]
    points = local.reshape((-1, 3))
    return quat_rotate(points, quat) + wrist


def default_camera_offset(camera_eye: str) -> np.ndarray:
    eye = camera_eye.lower()
    if eye == "right":
        return np.array([0.032, 0.0, 0.015], dtype=np.float64)
    return np.array([-0.032, 0.0, 0.015], dtype=np.float64)


def intrinsics_from_fov(width: int, height: int, fov_x_deg: float, fov_y_deg: float) -> dict[str, float]:
    cx = width * 0.5
    cy = height * 0.5
    fx = cx / math.tan(math.radians(fov_x_deg) * 0.5)
    fy = cy / math.tan(math.radians(fov_y_deg) * 0.5)
    return {
        "fx": float(fx),
        "fy": float(fy),
        "cx": float(cx),
        "cy": float(cy),
    }


def camera_pose_from_head(
    head_position: Iterable[float],
    head_quaternion: Iterable[float],
    camera_offset_local: Iterable[float],
    camera_rotation_offset: Iterable[float] | None = None,
) -> tuple[np.ndarray, np.ndarray]:
    head_pos = np.asarray(list(head_position), dtype=np.float64)
    head_quat = quat_normalize(head_quaternion)
    offset = np.asarray(list(camera_offset_local), dtype=np.float64)
    camera_pos = head_pos + quat_rotate(offset.reshape(1, 3), head_quat)[0]
    camera_quat = head_quat
    if camera_rotation_offset is not None:
        camera_quat = quat_multiply(head_quat, camera_rotation_offset)
    return camera_pos, camera_quat


def calibration_to_projection_defaults(calibration: dict | None) -> dict[str, float] | None:
    if not calibration:
        return None
    focal = calibration.get("focal_length")
    principal = calibration.get("principal_point")
    if focal is None or principal is None:
        return None
    return {
        "fx": float(focal[0]),
        "fy": float(focal[1]),
        "cx": float(principal[0]),
        "cy": float(principal[1]),
    }


def project_world_to_image(
    world_points: np.ndarray,
    camera_position: Iterable[float],
    camera_quaternion: Iterable[float],
    fx: float,
    fy: float,
    cx: float,
    cy: float,
    min_depth: float = 1e-4,
) -> tuple[np.ndarray, np.ndarray]:
    points = np.asarray(world_points, dtype=np.float64).reshape((-1, 3))
    cam_pos = np.asarray(list(camera_position), dtype=np.float64)
    cam_quat = quat_normalize(camera_quaternion)
    camera_space = quat_rotate(points - cam_pos, quat_conjugate(cam_quat))

    zs = camera_space[:, 2]
    valid = zs > min_depth
    projected = np.full((points.shape[0], 2), np.nan, dtype=np.float64)
    if np.any(valid):
        projected[valid, 0] = fx * (camera_space[valid, 0] / zs[valid]) + cx
        projected[valid, 1] = cy - fy * (camera_space[valid, 1] / zs[valid])
    return projected, valid


def project_world_to_image_with_calibration(
    world_points: np.ndarray,
    camera_position: Iterable[float],
    camera_quaternion: Iterable[float],
    calibration: dict,
    image_width: int,
    image_height: int,
    min_depth: float = 1e-4,
) -> tuple[np.ndarray, np.ndarray]:
    focal = np.asarray(calibration["focal_length"], dtype=np.float64)
    principal = np.asarray(calibration["principal_point"], dtype=np.float64)
    sensor_resolution = np.asarray(calibration["sensor_resolution"], dtype=np.float64)
    current_resolution = np.asarray(
        calibration.get("current_resolution") or [image_width, image_height],
        dtype=np.float64,
    )

    points = np.asarray(world_points, dtype=np.float64).reshape((-1, 3))
    cam_pos = np.asarray(list(camera_position), dtype=np.float64)
    cam_quat = quat_normalize(camera_quaternion)
    camera_space = quat_rotate(points - cam_pos, quat_conjugate(cam_quat))

    scale = current_resolution / sensor_resolution
    scale /= max(scale[0], scale[1])
    crop_xy = sensor_resolution * (1.0 - scale) * 0.5
    crop_wh = sensor_resolution * scale

    zs = camera_space[:, 2]
    valid = zs > min_depth
    projected = np.full((points.shape[0], 2), np.nan, dtype=np.float64)
    if np.any(valid):
        sensor_x = (camera_space[valid, 0] / zs[valid]) * focal[0] + principal[0]
        sensor_y = (camera_space[valid, 1] / zs[valid]) * focal[1] + principal[1]
        viewport_x = (sensor_x - crop_xy[0]) / crop_wh[0]
        viewport_y = (sensor_y - crop_xy[1]) / crop_wh[1]
        projected[valid, 0] = viewport_x * image_width
        projected[valid, 1] = (1.0 - viewport_y) * image_height
    return projected, valid


def finger_segment_indices(num_landmarks: int) -> list[tuple[int | str, int]]:
    if num_landmarks >= 21:
        thumb = (1, 2, 3, 4)
        index = (5, 6, 7, 8)
        middle = (9, 10, 11, 12)
        ring = (13, 14, 15, 16)
        little = (17, 18, 19, 20)
    else:
        thumb = (0, 1, 2, 3)
        index = (4, 5, 6, 7)
        middle = (8, 9, 10, 11)
        ring = (12, 13, 14, 15)
        little = (16, 17, 18, 19)

    return [
        ("wrist", thumb[0]),
        (thumb[0], thumb[1]),
        (thumb[1], thumb[2]),
        (thumb[2], thumb[3]),
        ("wrist", index[0]),
        (index[0], index[1]),
        (index[1], index[2]),
        (index[2], index[3]),
        ("wrist", middle[0]),
        (middle[0], middle[1]),
        (middle[1], middle[2]),
        (middle[2], middle[3]),
        ("wrist", ring[0]),
        (ring[0], ring[1]),
        (ring[1], ring[2]),
        (ring[2], ring[3]),
        ("wrist", little[0]),
        (little[0], little[1]),
        (little[1], little[2]),
        (little[2], little[3]),
    ]
