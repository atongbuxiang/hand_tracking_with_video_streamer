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
    python ./scripts/align_quest_dataset_apriltag.py --name demo --session session_001 --marker-length-m 0.05
    python ./scripts/align_quest_dataset_apriltag.py --name demo --marker-length-m 0.05 --reference-tag-id 3
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

from hts_dataset_utils import resolve_replay_dir


DEFAULT_OUTPUT_ROOT = "./data"
DEFAULT_DATASET_DENSITY = 0
DEFAULT_DICTIONARY = "DICT_APRILTAG_36h11"
DEFAULT_DETECTIONS_OUTPUT = "apriltag_detections.parquet"
DEFAULT_ALIGNED_OUTPUT = "aligned_frames_tag.parquet"

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


def _set_pose_fields(row: dict[str, Any], prefix: str, pose: np.ndarray | None) -> None:
    if pose is None:
        row[f"{prefix}_position_tag"] = None
        row[f"{prefix}_quaternion_tag"] = None
        return
    position, quaternion = _decompose_pose_matrix(pose)
    row[f"{prefix}_position_tag"] = position.tolist()
    row[f"{prefix}_quaternion_tag"] = quaternion.tolist()


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="align_quest_dataset_apriltag",
        description="Detect AprilTags and rewrite Quest dataset poses into tag space.",
    )
    parser.add_argument("--name", required=True, help="Dataset name under ./data.")
    parser.add_argument(
        "--session",
        default=None,
        help="Optional session name. When omitted, the script auto-detects a single session directory.",
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
    parser.add_argument(
        "--progress-interval",
        type=int,
        default=500,
        help="Log progress every N frames. Use 0 to disable progress logs.",
    )
    return parser


def main() -> None:
    args = _build_parser().parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    cv2 = _import_cv2()

    dataset_dir = resolve_replay_dir(args.output_root, args.name, args.session)
    session = _load_session(dataset_dir)
    aligned_rows = pq.read_table(dataset_dir / "aligned_frames.parquet").to_pylist()
    if not aligned_rows:
        raise SystemExit("No aligned rows found.")

    video_path = dataset_dir / session.get("video_path", "camera.mp4")
    capture = cv2.VideoCapture(str(video_path))
    if not capture.isOpened():
        raise SystemExit(f"Could not open video: {video_path}")

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
    frame_detections: list[list[dict[str, Any]]] = [[] for _ in aligned_rows]

    for frame_index, aligned_row in enumerate(aligned_rows):
        if args.progress_interval > 0 and frame_index > 0 and frame_index % args.progress_interval == 0:
            logging.info(
                "Processed %d/%d frames, detections=%d",
                frame_index,
                len(aligned_rows),
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
            frame_detections[frame_index].append(detection_row)
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

    aligned_tag_rows: list[dict[str, Any]] = []
    for aligned_row in aligned_rows:
        tag_row = dict(aligned_row)
        tag_row["reference_tag_id"] = reference_tag_id
        tag_row["camera_position_tag"] = None
        tag_row["camera_quaternion_tag"] = None
        tag_row["head_position_tag"] = None
        tag_row["head_quaternion_tag"] = None
        tag_row["left_wrist_position_tag"] = None
        tag_row["left_wrist_quaternion_tag"] = None
        tag_row["right_wrist_position_tag"] = None
        tag_row["right_wrist_quaternion_tag"] = None
        tag_row["left_landmarks_world_tag"] = None
        tag_row["right_landmarks_world_tag"] = None

        camera_pose_world = _build_frame_pose_from_row(aligned_row)
        if camera_pose_world is not None:
            camera_pose_world_c = _change_basis_pose(camera_pose_world)
            camera_pose_tag = _transform_pose_to_tag_space(tag_to_world, camera_pose_world_c)
            _set_pose_fields(tag_row, "camera", camera_pose_tag)

        for prefix in ("head", "left_wrist", "right_wrist"):
            position = aligned_row.get(f"{prefix}_position")
            quaternion = aligned_row.get(f"{prefix}_quaternion")
            if position is None or quaternion is None:
                continue
            pose_world = _pose_matrix(position, quaternion)
            pose_world_c = _change_basis_pose(pose_world)
            pose_tag = _transform_pose_to_tag_space(tag_to_world, pose_world_c)
            _set_pose_fields(tag_row, prefix, pose_tag)

        for side in ("left", "right"):
            world_points = aligned_row.get(f"{side}_landmarks_world")
            if world_points is None:
                continue
            points_world = np.asarray(world_points, dtype=np.float64).reshape((-1, 3))
            points_world_c = _change_basis_points(points_world)
            points_tag = (world_to_tag[:3, :3] @ points_world_c.T).T + world_to_tag[:3, 3]
            tag_row[f"{side}_landmarks_world_tag"] = points_tag.reshape(-1).tolist()

        aligned_tag_rows.append(tag_row)

    detections_table = pa.Table.from_pylist(detection_rows)
    aligned_tag_table = pa.Table.from_pylist(aligned_tag_rows)
    pq.write_table(detections_table, dataset_dir / args.detections_output)
    pq.write_table(aligned_tag_table, dataset_dir / args.aligned_output)
    logging.info("Wrote %s and %s", dataset_dir / args.detections_output, dataset_dir / args.aligned_output)

    apriltag_metadata = {
        "enabled": True,
        "dictionary": args.dictionary,
        "marker_length_m": args.marker_length_m,
        "reference_tag_id": reference_tag_id,
        "reference_tag_source": reference_source,
        "reference_tag_detection_count": len(reference_poses),
        "detected_tag_ids": sorted(int(tag_id) for tag_id in tag_counts),
        "total_detections": len(detection_rows),
        "detections_path": args.detections_output,
        "aligned_tag_path": args.aligned_output,
        "dataset_basis": "unity_lh_x_right_y_up_z_forward",
        "opencv_basis": "opencv_camera_x_right_y_down_z_forward",
        "basis_change_matrix": _UNITY_TO_OPENCV.tolist(),
        "tag_to_world_matrix": tag_to_world.tolist(),
        "world_to_tag_matrix": world_to_tag.tolist(),
        "tag_position_world": tag_position_world.tolist(),
        "tag_quaternion_world": tag_quaternion_world.tolist(),
        "notes": (
            "HTS streams arrive in Unity left-handed coordinates. "
            "This script converts them into an OpenCV-style camera basis with Y flipped "
            "before solving AprilTag pose, then rewrites the poses into the reference tag frame."
        ),
    }
    session["apriltag"] = apriltag_metadata
    (dataset_dir / "session.json").write_text(json.dumps(session, indent=2), encoding="utf-8")
    logging.info("Updated %s", dataset_dir / "session.json")


if __name__ == "__main__":
    main()
