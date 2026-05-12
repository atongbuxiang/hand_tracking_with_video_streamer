"""Detect AprilTags in a recorded Quest dataset and rewrite poses into tag space.

This script:
  - reads dataset intrinsics from session.json
  - detects all AprilTags in camera.mp4 with OpenCV
  - solves each tag pose in the camera frame
  - converts detected tag poses into the dataset world frame
  - chooses a reference tag and records the fixed world <-> tag transform
  - rewrites head, hand, and camera poses into tag space

Usage:
    python ./scripts/align_quest_dataset_apriltag.py --name demo --marker-length-m 0.05
    python ./scripts/align_quest_dataset_apriltag.py --name demo --segment 3 --marker-length-m 0.05
    python ./scripts/align_quest_dataset_apriltag.py --name demo --segment 3 --marker-length-m 0.05 --reference-tag-id 3
"""

from __future__ import annotations

import argparse
import json
import logging
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq


DEFAULT_OUTPUT_ROOT = "./data"
DEFAULT_DICTIONARY = "DICT_APRILTAG_36h11"
DEFAULT_DETECTIONS_OUTPUT = "apriltag_detections.parquet"
DEFAULT_ALIGNED_OUTPUT = "aligned_frames_tag.parquet"
PROGRESS_LOG_INTERVAL = 1000

TAG_FIELD_RENAMES = {
    "head_position": "head_position_tag",
    "head_quaternion": "head_quaternion_tag",
    "left_wrist_position": "left_wrist_position_tag",
    "left_wrist_quaternion": "left_wrist_quaternion_tag",
    "right_wrist_position": "right_wrist_position_tag",
    "right_wrist_quaternion": "right_wrist_quaternion_tag",
    "left_landmarks_world": "left_landmarks_tag",
    "right_landmarks_world": "right_landmarks_tag",
    "camera_position_world": "camera_position_tag",
    "camera_quaternion_world": "camera_quaternion_tag",
}

_UNITY_TO_OPENCV = np.array(
    [
        [1.0, 0.0, 0.0],
        [0.0, -1.0, 0.0],
        [0.0, 0.0, 1.0],
    ],
    dtype=np.float64,
)
_UNITY_TO_OPENCV_4X4 = np.eye(4, dtype=np.float64)
_UNITY_TO_OPENCV_4X4[:3, :3] = _UNITY_TO_OPENCV


def _import_cv2():
    try:
        import cv2  # type: ignore
    except ImportError as exc:  # pragma: no cover
        raise SystemExit(
            "Missing dependency. Install opencv-contrib-python-headless to detect AprilTags."
        ) from exc
    return cv2


def _quat_to_matrix(quat: list[float] | np.ndarray) -> np.ndarray:
    q = np.asarray(quat, dtype=np.float64)
    if q.shape != (4,):
        raise ValueError("Quaternion must contain exactly four values.")
    norm = np.linalg.norm(q)
    if norm <= 0.0:
        return np.eye(3, dtype=np.float64)
    x, y, z, w = q / norm
    xx, yy, zz = x * x, y * y, z * z
    xy, xz, yz = x * y, x * z, y * z
    wx, wy, wz = w * x, w * y, w * z
    return np.array(
        [
            [1 - 2 * (yy + zz), 2 * (xy - wz), 2 * (xz + wy)],
            [2 * (xy + wz), 1 - 2 * (xx + zz), 2 * (yz - wx)],
            [2 * (xz - wy), 2 * (yz + wx), 1 - 2 * (xx + yy)],
        ],
        dtype=np.float64,
    )


def _matrix_to_quat(mat: np.ndarray) -> np.ndarray:
    m = np.asarray(mat, dtype=np.float64)
    trace = float(m[0, 0] + m[1, 1] + m[2, 2])
    if trace > 0.0:
        s = 0.5 / np.sqrt(trace + 1.0)
        w = 0.25 / s
        x = (m[2, 1] - m[1, 2]) * s
        y = (m[0, 2] - m[2, 0]) * s
        z = (m[1, 0] - m[0, 1]) * s
    elif m[0, 0] > m[1, 1] and m[0, 0] > m[2, 2]:
        s = 2.0 * np.sqrt(1.0 + m[0, 0] - m[1, 1] - m[2, 2])
        w = (m[2, 1] - m[1, 2]) / s
        x = 0.25 * s
        y = (m[0, 1] + m[1, 0]) / s
        z = (m[0, 2] + m[2, 0]) / s
    elif m[1, 1] > m[2, 2]:
        s = 2.0 * np.sqrt(1.0 + m[1, 1] - m[0, 0] - m[2, 2])
        w = (m[0, 2] - m[2, 0]) / s
        x = (m[0, 1] + m[1, 0]) / s
        y = 0.25 * s
        z = (m[1, 2] + m[2, 1]) / s
    else:
        s = 2.0 * np.sqrt(1.0 + m[2, 2] - m[0, 0] - m[1, 1])
        w = (m[1, 0] - m[0, 1]) / s
        x = (m[0, 2] + m[2, 0]) / s
        y = (m[1, 2] + m[2, 1]) / s
        z = 0.25 * s
    quat = np.array([x, y, z, w], dtype=np.float64)
    return quat / max(np.linalg.norm(quat), 1e-12)


def _pose_matrix(position: list[float] | np.ndarray, quaternion: list[float] | np.ndarray) -> np.ndarray:
    pose = np.eye(4, dtype=np.float64)
    pose[:3, :3] = _quat_to_matrix(quaternion)
    pose[:3, 3] = np.asarray(position, dtype=np.float64)
    return pose


def _decompose_pose_matrix(pose: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    return np.asarray(pose[:3, 3], dtype=np.float64), _matrix_to_quat(pose[:3, :3])


def _change_basis_pose(pose: np.ndarray, basis: np.ndarray = _UNITY_TO_OPENCV_4X4) -> np.ndarray:
    return basis @ pose @ basis


def _change_basis_points(points: np.ndarray, basis: np.ndarray = _UNITY_TO_OPENCV) -> np.ndarray:
    pts = np.asarray(points, dtype=np.float64).reshape((-1, 3))
    return (basis @ pts.T).T


def _average_quaternions(quaternions: list[np.ndarray]) -> np.ndarray:
    if not quaternions:
        return np.array([0.0, 0.0, 0.0, 1.0], dtype=np.float64)

    ref = quaternions[0]
    accum = np.zeros(4, dtype=np.float64)
    for quat in quaternions:
        current = np.asarray(quat, dtype=np.float64)
        if np.dot(current, ref) < 0.0:
            current = -current
        accum += current
    norm = np.linalg.norm(accum)
    if norm <= 0.0:
        return ref / max(np.linalg.norm(ref), 1e-12)
    return accum / norm


def _load_session(dataset_dir: Path) -> dict[str, Any]:
    session_path = dataset_dir / "session.json"
    if not session_path.exists():
        raise SystemExit(f"session.json not found: {session_path}")
    return json.loads(session_path.read_text(encoding="utf-8"))


def _dataset_root(output_root: str | Path, name: str) -> Path:
    root = Path(output_root) / name
    if not root.exists():
        raise SystemExit(f"Dataset not found: {root}")
    return root


def _default_session_dir(dataset_root: Path) -> Path:
    if (dataset_root / "aligned_frames.parquet").exists():
        return dataset_root
    session_dirs = _iter_session_dirs(dataset_root)
    if len(session_dirs) != 1:
        names = ", ".join(path.name for path in session_dirs)
        raise SystemExit(
            f"Dataset {dataset_root} contains multiple sessions ({names}); "
            "the AprilTag detection segment is ambiguous."
        )
    return session_dirs[0]


def _iter_session_dirs(dataset_root: Path) -> list[Path]:
    direct_aligned = dataset_root / "aligned_frames.parquet"
    if direct_aligned.exists():
        return [dataset_root]
    session_dirs = [
        child
        for child in sorted(dataset_root.iterdir())
        if child.is_dir() and child.name != "__MACOSX" and (child / "aligned_frames.parquet").exists()
    ]
    if not session_dirs:
        raise SystemExit(f"No sessions with aligned_frames.parquet found under {dataset_root}")
    return session_dirs


def _rename_schema_for_tag_space(schema: pa.Schema) -> pa.Schema:
    fields = []
    for field in schema:
        fields.append(field.with_name(TAG_FIELD_RENAMES.get(field.name, field.name)))
    return pa.schema(fields, metadata=schema.metadata)


def _load_segment(dataset_dir: Path, segment_index: int) -> dict[str, Any]:
    segments_path = dataset_dir / "segments.json"
    if not segments_path.exists():
        raise SystemExit(f"segments.json not found in {dataset_dir}")

    segments_data = json.loads(segments_path.read_text(encoding="utf-8"))
    for segment in segments_data.get("segments", []):
        if int(segment.get("segment_index", -1)) == segment_index:
            return segment

    available = ", ".join(str(segment.get("segment_index")) for segment in segments_data.get("segments", []))
    raise SystemExit(f"Segment {segment_index} not found in {segments_path}. Available segments: {available}")


def _load_camera_matrix(session: dict[str, Any], frame_width: int, frame_height: int) -> np.ndarray:
    calibration = session.get("camera_calibration") or {}
    focal = calibration.get("focal_length")
    principal = calibration.get("principal_point")

    if focal is None or principal is None:
        defaults = session.get("projection_defaults") or {}
        if "fx" in defaults and "fy" in defaults:
            return np.array(
                [
                    [float(defaults["fx"]), 0.0, float(defaults.get("cx", frame_width * 0.5))],
                    [0.0, float(defaults["fy"]), float(defaults.get("cy", frame_height * 0.5))],
                    [0.0, 0.0, 1.0],
                ],
                dtype=np.float64,
            )
        raise SystemExit("Missing camera intrinsics in session.json.")

    sensor_resolution = np.asarray(
        calibration.get("sensor_resolution") or calibration.get("current_resolution") or [frame_width, frame_height],
        dtype=np.float64,
    )
    current_resolution = np.asarray(
        calibration.get("current_resolution") or [frame_width, frame_height],
        dtype=np.float64,
    )
    scale = current_resolution / sensor_resolution
    scale /= max(float(scale[0]), float(scale[1]), 1e-12)
    crop_xy = sensor_resolution * (1.0 - scale) * 0.5
    crop_wh = sensor_resolution * scale

    fx = frame_width / crop_wh[0] * float(focal[0])
    fy = frame_height / crop_wh[1] * float(focal[1])
    cx = frame_width / crop_wh[0] * (float(principal[0]) - crop_xy[0])
    cy = frame_height - frame_height / crop_wh[1] * (float(principal[1]) - crop_xy[1])
    return np.array([[fx, 0.0, cx], [0.0, fy, cy], [0.0, 0.0, 1.0]], dtype=np.float64)


def _load_dist_coeffs(session: dict[str, Any]) -> np.ndarray:
    calibration = session.get("camera_calibration") or {}
    coeffs = calibration.get("distortion_coefficients") or calibration.get("dist_coeffs") or session.get(
        "distortion_coefficients"
    )
    if coeffs is None:
        return np.zeros((5, 1), dtype=np.float64)
    arr = np.asarray(coeffs, dtype=np.float64).reshape(-1, 1)
    if arr.size not in (4, 5, 8, 12):
        return np.zeros((5, 1), dtype=np.float64)
    return arr


def _create_detector(cv2, dictionary_name: str):
    if not hasattr(cv2.aruco, dictionary_name):
        raise SystemExit(f"OpenCV aruco dictionary not found: {dictionary_name}")
    dictionary = cv2.aruco.getPredefinedDictionary(getattr(cv2.aruco, dictionary_name))
    if hasattr(cv2.aruco, "DetectorParameters"):
        params = cv2.aruco.DetectorParameters()
    else:  # pragma: no cover
        params = cv2.aruco.DetectorParameters_create()
    if hasattr(cv2.aruco, "CORNER_REFINE_APRILTAG") and hasattr(params, "cornerRefinementMethod"):
        params.cornerRefinementMethod = cv2.aruco.CORNER_REFINE_APRILTAG
    if hasattr(cv2.aruco, "ArucoDetector"):
        return cv2.aruco.ArucoDetector(dictionary, params)
    return (dictionary, params)


def _detect_markers(cv2, detector, frame):
    if hasattr(detector, "detectMarkers"):
        return detector.detectMarkers(frame)
    dictionary, params = detector
    return cv2.aruco.detectMarkers(frame, dictionary, parameters=params)


def _solve_marker_pose(cv2, object_points: np.ndarray, image_points: np.ndarray, camera_matrix: np.ndarray, dist_coeffs: np.ndarray):
    flags = getattr(cv2, "SOLVEPNP_IPPE_SQUARE", cv2.SOLVEPNP_ITERATIVE)
    success, rvec, tvec = cv2.solvePnP(object_points, image_points, camera_matrix, dist_coeffs, flags=flags)
    if not success:
        success, rvec, tvec = cv2.solvePnP(object_points, image_points, camera_matrix, dist_coeffs)
    if not success:
        return None
    return rvec, tvec


def _reprojection_error_px(cv2, object_points: np.ndarray, image_points: np.ndarray, rvec: np.ndarray, tvec: np.ndarray, camera_matrix: np.ndarray, dist_coeffs: np.ndarray) -> float:
    projected, _ = cv2.projectPoints(object_points, rvec, tvec, camera_matrix, dist_coeffs)
    projected = projected.reshape((-1, 2))
    image_points = image_points.reshape((-1, 2))
    residual = projected - image_points
    return float(np.sqrt(np.mean(np.sum(residual * residual, axis=1))))


def _build_frame_pose_from_row(row: dict[str, Any]) -> np.ndarray | None:
    position = row.get("camera_position_world")
    quaternion = row.get("camera_quaternion_world")
    if position is None or quaternion is None:
        return None
    return _pose_matrix(position, quaternion)


def _transform_pose_to_tag_space(tag_to_world: np.ndarray, pose_world: np.ndarray) -> np.ndarray:
    return np.linalg.inv(tag_to_world) @ pose_world


def _set_pose_fields(row: dict[str, Any], position_key: str, quaternion_key: str, pose: np.ndarray | None) -> None:
    if pose is None:
        row[position_key] = None
        row[quaternion_key] = None
        return
    position, quaternion = _decompose_pose_matrix(pose)
    row[position_key] = position.tolist()
    row[quaternion_key] = quaternion.tolist()


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="align_quest_dataset_apriltag",
        description="Detect AprilTags and rewrite Quest dataset poses into tag space.",
    )
    parser.add_argument("--name", required=True, help="Dataset name under ./data.")
    parser.add_argument(
        "--segment",
        type=int,
        default=1,
        help="segment_index from segments.json used for AprilTag detection. Defaults to 1.",
    )
    parser.add_argument("--output-root", default=DEFAULT_OUTPUT_ROOT, help="Dataset root.")
    parser.add_argument(
        "--marker-length-m",
        type=float,
        required=True,
        help="Physical side length of each AprilTag in meters.",
    )
    parser.add_argument(
        "--dictionary",
        default=DEFAULT_DICTIONARY,
        help="OpenCV aruco dictionary name, for example DICT_APRILTAG_36h11.",
    )
    parser.add_argument(
        "--reference-tag-id",
        type=int,
        default=None,
        help="Optional reference tag id. If omitted, the most frequently detected tag is used.",
    )
    parser.add_argument(
        "--detections-output",
        default=DEFAULT_DETECTIONS_OUTPUT,
        help="Parquet file for per-frame AprilTag detections.",
    )
    parser.add_argument(
        "--aligned-output",
        default=DEFAULT_ALIGNED_OUTPUT,
        help="Parquet file for pose rows rewritten into tag space.",
    )
    return parser


def _detect_reference_transform(
    cv2,
    args: argparse.Namespace,
    dataset_dir: Path,
) -> tuple[dict[str, Any], list[dict[str, Any]], np.ndarray, np.ndarray, dict[str, Any]]:
    session = _load_session(dataset_dir)
    aligned_table = pq.read_table(dataset_dir / "aligned_frames.parquet")
    aligned_rows = aligned_table.to_pylist()
    if not aligned_rows:
        raise SystemExit(f"No aligned rows found in dataset: {dataset_dir}")

    segment = _load_segment(dataset_dir, args.segment)
    start_frame_index = int(segment["start_frame_index"])
    end_frame_index = int(segment["end_frame_index"])
    if start_frame_index < 0 or end_frame_index < start_frame_index:
        raise SystemExit(f"Invalid frame range in segment {args.segment}: {start_frame_index}..{end_frame_index}")
    end_frame_index = min(end_frame_index, len(aligned_rows) - 1)

    video_path = dataset_dir / session.get("video_path", "camera.mp4")
    capture = cv2.VideoCapture(str(video_path))
    if not capture.isOpened():
        raise SystemExit(f"Could not open video: {video_path}")
    capture.set(cv2.CAP_PROP_POS_FRAMES, start_frame_index)

    camera_width = int(aligned_rows[0].get("camera_width") or aligned_rows[0].get("camera_height") or 0)
    camera_height = int(aligned_rows[0].get("camera_height") or aligned_rows[0].get("camera_width") or 0)
    camera_matrix = _load_camera_matrix(session, camera_width, camera_height)
    dist_coeffs = _load_dist_coeffs(session)
    detector = _create_detector(cv2, args.dictionary)

    object_points = np.array(
        [
            [-args.marker_length_m / 2.0, args.marker_length_m / 2.0, 0.0],
            [args.marker_length_m / 2.0, args.marker_length_m / 2.0, 0.0],
            [args.marker_length_m / 2.0, -args.marker_length_m / 2.0, 0.0],
            [-args.marker_length_m / 2.0, -args.marker_length_m / 2.0, 0.0],
        ],
        dtype=np.float64,
    )

    detection_rows: list[dict[str, Any]] = []
    tag_world_poses_by_id: dict[int, list[np.ndarray]] = defaultdict(list)
    tag_counts: Counter[int] = Counter()

    logging.info(
        "Detecting AprilTags in segment %d frames %d..%d",
        args.segment,
        start_frame_index,
        end_frame_index,
    )

    for frame_index in range(start_frame_index, end_frame_index + 1):
        aligned_row = aligned_rows[frame_index]
        if (frame_index - start_frame_index) > 0 and (frame_index - start_frame_index) % PROGRESS_LOG_INTERVAL == 0:
            logging.info(
                "Detected tags through segment %d frame %d/%d, detections=%d",
                args.segment,
                frame_index,
                end_frame_index,
                len(detection_rows),
            )
        ok, frame = capture.read()
        if not ok:
            break

        camera_pose_world = _build_frame_pose_from_row(aligned_row)
        if camera_pose_world is None:
            continue
        camera_pose_world_c = _change_basis_pose(camera_pose_world)

        corners, ids, _rejected = _detect_markers(cv2, detector, frame)
        if ids is None or len(ids) == 0:
            continue

        for idx, marker_id in enumerate(ids.reshape(-1)):
            marker_id = int(marker_id)
            image_points = np.asarray(corners[idx], dtype=np.float64).reshape((-1, 2))
            pose = _solve_marker_pose(cv2, object_points, image_points, camera_matrix, dist_coeffs)
            if pose is None:
                continue
            rvec, tvec = pose
            rotation_matrix, _ = cv2.Rodrigues(rvec)
            tag_pose_camera = np.eye(4, dtype=np.float64)
            tag_pose_camera[:3, :3] = rotation_matrix
            tag_pose_camera[:3, 3] = tvec.reshape(3)
            tag_pose_world = camera_pose_world_c @ tag_pose_camera

            tag_position_world, tag_quaternion_world = _decompose_pose_matrix(tag_pose_world)
            reprojection_error = _reprojection_error_px(
                cv2,
                object_points,
                image_points,
                rvec,
                tvec,
                camera_matrix,
                dist_coeffs,
            )

            detection_row = {
                "camera_frame_index": frame_index,
                "camera_frame_id": aligned_row.get("camera_frame_id"),
                "tag_id": marker_id,
                "corner_0_x": float(image_points[0, 0]),
                "corner_0_y": float(image_points[0, 1]),
                "corner_1_x": float(image_points[1, 0]),
                "corner_1_y": float(image_points[1, 1]),
                "corner_2_x": float(image_points[2, 0]),
                "corner_2_y": float(image_points[2, 1]),
                "corner_3_x": float(image_points[3, 0]),
                "corner_3_y": float(image_points[3, 1]),
                "rvec_camera": np.asarray(rvec, dtype=np.float64).reshape(-1).tolist(),
                "tvec_camera": np.asarray(tvec, dtype=np.float64).reshape(-1).tolist(),
                "tag_position_world": tag_position_world.tolist(),
                "tag_quaternion_world": tag_quaternion_world.tolist(),
                "reprojection_error_px": reprojection_error,
            }
            detection_rows.append(detection_row)
            tag_world_poses_by_id[marker_id].append(tag_pose_world)
            tag_counts[marker_id] += 1

    capture.release()

    if not detection_rows:
        raise SystemExit("No AprilTags were detected in the dataset.")
    logging.info("Detected %d AprilTag observations across %d tag ids.", len(detection_rows), len(tag_counts))

    if args.reference_tag_id is not None:
        reference_tag_id = args.reference_tag_id
        if reference_tag_id not in tag_counts:
            available = ", ".join(str(tag_id) for tag_id in sorted(tag_counts))
            raise SystemExit(f"Reference tag {reference_tag_id} was not detected. Available tags: {available}")
        reference_source = "manual"
    else:
        reference_tag_id = max(tag_counts.items(), key=lambda item: (item[1], -item[0]))[0]
        reference_source = "auto_most_detected"

    reference_poses = tag_world_poses_by_id.get(reference_tag_id, [])
    if not reference_poses:
        raise SystemExit(f"No world poses available for reference tag {reference_tag_id}.")

    reference_positions = []
    reference_quaternions = []
    for pose in reference_poses:
        position, quaternion = _decompose_pose_matrix(pose)
        reference_positions.append(position)
        reference_quaternions.append(quaternion)
    tag_position_world = np.median(np.stack(reference_positions, axis=0), axis=0)
    tag_quaternion_world = _average_quaternions(reference_quaternions)
    tag_to_world = _pose_matrix(tag_position_world, tag_quaternion_world)
    world_to_tag = np.linalg.inv(tag_to_world)

    transform_metadata = {
        "reference_tag_id": reference_tag_id,
        "reference_tag_source": reference_source,
        "reference_tag_detection_count": len(reference_poses),
        "tag_segment_index": int(args.segment),
        "tag_segment_start_frame_index": start_frame_index,
        "tag_segment_end_frame_index": end_frame_index,
        "detected_tag_ids": sorted(int(tag_id) for tag_id in tag_counts),
        "total_detections": len(detection_rows),
        "tag_to_world_matrix": tag_to_world.tolist(),
        "world_to_tag_matrix": world_to_tag.tolist(),
        "tag_position_world": tag_position_world.tolist(),
        "tag_quaternion_world": tag_quaternion_world.tolist(),
    }
    return session, detection_rows, tag_to_world, world_to_tag, transform_metadata


def _convert_aligned_table_to_tag_space(aligned_table: pa.Table, tag_to_world: np.ndarray) -> pa.Table:
    aligned_rows = aligned_table.to_pylist()
    world_to_tag = np.linalg.inv(tag_to_world)
    aligned_tag_rows: list[dict[str, Any]] = []
    for aligned_row in aligned_rows:
        tag_row = {TAG_FIELD_RENAMES.get(key, key): value for key, value in aligned_row.items()}
        tag_row["camera_position_tag"] = None
        tag_row["camera_quaternion_tag"] = None
        tag_row["head_position_tag"] = None
        tag_row["head_quaternion_tag"] = None
        tag_row["left_wrist_position_tag"] = None
        tag_row["left_wrist_quaternion_tag"] = None
        tag_row["right_wrist_position_tag"] = None
        tag_row["right_wrist_quaternion_tag"] = None
        tag_row["left_landmarks_tag"] = None
        tag_row["right_landmarks_tag"] = None

        camera_pose_world = _build_frame_pose_from_row(aligned_row)
        if camera_pose_world is not None:
            camera_pose_world_c = _change_basis_pose(camera_pose_world)
            camera_pose_tag = _transform_pose_to_tag_space(tag_to_world, camera_pose_world_c)
            _set_pose_fields(tag_row, "camera_position_tag", "camera_quaternion_tag", camera_pose_tag)

        for prefix in ("head", "left_wrist", "right_wrist"):
            position = aligned_row.get(f"{prefix}_position")
            quaternion = aligned_row.get(f"{prefix}_quaternion")
            if position is None or quaternion is None:
                continue
            pose_world = _pose_matrix(position, quaternion)
            pose_world_c = _change_basis_pose(pose_world)
            pose_tag = _transform_pose_to_tag_space(tag_to_world, pose_world_c)
            _set_pose_fields(tag_row, f"{prefix}_position_tag", f"{prefix}_quaternion_tag", pose_tag)

        for side in ("left", "right"):
            world_points = aligned_row.get(f"{side}_landmarks_world")
            if world_points is None:
                continue
            points_world = np.asarray(world_points, dtype=np.float64).reshape((-1, 3))
            points_world_c = _change_basis_points(points_world)
            points_tag = (world_to_tag[:3, :3] @ points_world_c.T).T + world_to_tag[:3, 3]
            tag_row[f"{side}_landmarks_tag"] = points_tag.reshape(-1).tolist()

        aligned_tag_rows.append(tag_row)

    return pa.Table.from_pylist(aligned_tag_rows, schema=_rename_schema_for_tag_space(aligned_table.schema))


def _write_session_outputs(
    session_dir: Path,
    args: argparse.Namespace,
    transform_metadata: dict[str, Any],
    tag_to_world: np.ndarray,
) -> None:
    session = _load_session(session_dir)
    aligned_table = pq.read_table(session_dir / "aligned_frames.parquet")
    aligned_tag_table = _convert_aligned_table_to_tag_space(aligned_table, tag_to_world)
    pq.write_table(aligned_tag_table, session_dir / args.aligned_output)

    converted_fields = sorted(TAG_FIELD_RENAMES.values())
    apriltag_metadata = {
        "enabled": True,
        "dictionary": args.dictionary,
        "marker_length_m": args.marker_length_m,
        "detections_path": args.detections_output,
        "aligned_tag_path": args.aligned_output,
        "aligned_tag_schema": "aligned_frames_columns_with_converted_fields_renamed_to_explicit_tag_suffix",
        "converted_fields": converted_fields,
        "dataset_basis": "unity_lh_x_right_y_up_z_forward",
        "opencv_basis": "opencv_camera_x_right_y_down_z_forward",
        "basis_change_matrix": _UNITY_TO_OPENCV.tolist(),
        **transform_metadata,
        "notes": (
            "HTS streams arrive in Unity left-handed coordinates. "
            "This script converts them into an OpenCV-style camera basis with Y flipped "
            "before solving AprilTag pose, estimates the reference tag transform from the selected segment, "
            "then rewrites the dataset into the reference tag frame. "
            "This assumes all converted sessions share the same Quest world origin."
        ),
    }
    session["apriltag"] = apriltag_metadata
    (session_dir / "session.json").write_text(json.dumps(session, indent=2), encoding="utf-8")
    logging.info("Wrote %s and updated %s", session_dir / args.aligned_output, session_dir / "session.json")


def main() -> None:
    args = _build_parser().parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    cv2 = _import_cv2()

    dataset_root = _dataset_root(args.output_root, args.name)
    detection_dir = _default_session_dir(dataset_root)
    session_dirs = _iter_session_dirs(dataset_root)

    logging.info("Detecting AprilTags from %s segment %d", detection_dir, args.segment)
    _, detection_rows, tag_to_world, _world_to_tag, transform_metadata = _detect_reference_transform(
        cv2, args, detection_dir
    )
    detections_table = pa.Table.from_pylist(detection_rows)
    pq.write_table(detections_table, detection_dir / args.detections_output)
    logging.info("Wrote %s", detection_dir / args.detections_output)

    for session_dir in session_dirs:
        _write_session_outputs(session_dir, args, transform_metadata, tag_to_world)


if __name__ == "__main__":
    main()
