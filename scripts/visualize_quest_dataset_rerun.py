"""Visualize a Quest hand/camera dataset in Rerun.

Usage:
    python ./scripts/visualize_quest_dataset_rerun.py --name demo
    python ./scripts/visualize_quest_dataset_rerun.py --name demo --session session_001
    python ./scripts/visualize_quest_dataset_rerun.py --name demo --save demo.rrd --frame-step 5
"""

from __future__ import annotations

import argparse
import json
import logging
from pathlib import Path
from typing import Any

import cv2
import numpy as np
import pyarrow.parquet as pq
import rerun as rr

from hts_dataset_utils import resolve_replay_dir


DEFAULT_OUTPUT_ROOT = "./data"
DEFAULT_RRD_OUTPUT = "quest_dataset_rerun.rrd"

LEFT_COLOR = [80, 170, 255]
RIGHT_COLOR = [255, 160, 60]
HEAD_COLOR = [230, 230, 230]
CAMERA_COLOR = [255, 230, 80]
TAG_COLOR = [80, 255, 140]
WORLD_COLOR = [180, 180, 180]

UNITY_TO_RERUN = np.array(
    [
        [0.0, 0.0, 1.0],
        [-1.0, 0.0, 0.0],
        [0.0, 1.0, 0.0],
    ],
    dtype=np.float64,
)
OPENCV_TO_RERUN = np.array(
    [
        [0.0, 0.0, 1.0],
        [-1.0, 0.0, 0.0],
        [0.0, -1.0, 0.0],
    ],
    dtype=np.float64,
)


FINGER_SEGMENTS = [
    ("wrist", 1),
    (1, 2),
    (2, 3),
    (3, 4),
    ("wrist", 5),
    (5, 6),
    (6, 7),
    (7, 8),
    ("wrist", 9),
    (9, 10),
    (10, 11),
    (11, 12),
    ("wrist", 13),
    (13, 14),
    (14, 15),
    (15, 16),
    ("wrist", 17),
    (17, 18),
    (18, 19),
    (19, 20),
]


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _load_aligned_rows(dataset_dir: Path, session: dict[str, Any], use_tag_space: bool) -> list[dict[str, Any]]:
    if use_tag_space:
        aligned_path = dataset_dir / session.get("apriltag", {}).get("aligned_tag_path", "aligned_frames_tag.parquet")
        if aligned_path.exists():
            return pq.read_table(aligned_path).to_pylist()
        logging.warning("Tag-space parquet not found: %s. Falling back to aligned_frames.parquet.", aligned_path)
    return pq.read_table(dataset_dir / "aligned_frames.parquet").to_pylist()


def _quat_to_matrix(quat: list[float] | np.ndarray) -> np.ndarray:
    q = np.asarray(quat, dtype=np.float64)
    if q.shape != (4,):
        return np.eye(3, dtype=np.float64)
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


def _matrix_to_quat(matrix: np.ndarray) -> np.ndarray:
    m = np.asarray(matrix, dtype=np.float64)
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
    quat /= max(np.linalg.norm(quat), 1e-12)
    return quat


def _basis_for_rows(use_tag_space: bool) -> np.ndarray:
    return OPENCV_TO_RERUN if use_tag_space else UNITY_TO_RERUN


def _transform_position_to_rerun(position: list[float] | np.ndarray, basis: np.ndarray) -> list[float]:
    pos = np.asarray(position, dtype=np.float64).reshape(3)
    return (basis @ pos).tolist()


def _transform_quaternion_to_rerun(quaternion: list[float] | np.ndarray, basis: np.ndarray) -> rr.Quaternion:
    rot = _quat_to_matrix(quaternion)
    rerun_rot = basis @ rot @ basis.T
    return rr.Quaternion(xyzw=_matrix_to_quat(rerun_rot).tolist())


def _transform_points_to_rerun(points: np.ndarray, basis: np.ndarray) -> np.ndarray:
    pts = np.asarray(points, dtype=np.float64).reshape((-1, 3))
    return (basis @ pts.T).T


def _as_position(row: dict[str, Any], key: str) -> list[float] | None:
    value = row.get(key)
    if value is None:
        return None
    return [float(value[0]), float(value[1]), float(value[2])]


def _as_quaternion(row: dict[str, Any], key: str) -> rr.Quaternion | None:
    value = row.get(key)
    if value is None:
        return None
    return rr.Quaternion(xyzw=[float(value[0]), float(value[1]), float(value[2]), float(value[3])])


def _log_pose(
    entity_path: str,
    position: list[float] | None,
    quaternion: rr.Quaternion | None,
    color: list[int],
    axis_len: float = 0.08,
) -> None:
    if position is None or quaternion is None:
        return
    rr.log(entity_path, rr.Transform3D(translation=position, rotation=quaternion))

    rot = _quat_to_matrix(quaternion.xyzw)
    origins = np.repeat(np.asarray(position, dtype=np.float64).reshape(1, 3), 3, axis=0)
    vectors = np.stack([rot[:, 0], rot[:, 1], rot[:, 2]], axis=0) * axis_len
    colors = [[255, 60, 60], [60, 255, 60], [60, 120, 255]]
    rr.log(f"{entity_path}/axes", rr.Arrows3D(origins=origins, vectors=vectors, colors=colors, radii=0.005))
    rr.log(entity_path, rr.Points3D([position], colors=[color], radii=0.012))


def _points_from_flat(flat: list[float] | None) -> np.ndarray | None:
    if flat is None:
        return None
    points = np.asarray(flat, dtype=np.float64)
    if points.size < 3:
        return None
    return points.reshape((-1, 3))


def _log_hand(side: str, row: dict[str, Any], use_tag_space: bool, color: list[int]) -> None:
    basis = _basis_for_rows(use_tag_space)
    suffix = "_tag" if use_tag_space else ""
    wrist_position = _as_position(row, f"{side}_wrist_position{suffix}")
    wrist_quaternion = _as_quaternion(row, f"{side}_wrist_quaternion{suffix}")
    if wrist_position is not None:
        wrist_position = _transform_position_to_rerun(wrist_position, basis)
    if wrist_quaternion is not None:
        wrist_quaternion = _transform_quaternion_to_rerun(wrist_quaternion.xyzw, basis)
    _log_pose(f"world/{side}_wrist", wrist_position, wrist_quaternion, color, axis_len=0.045)

    landmarks_key = f"{side}_landmarks_world_tag" if use_tag_space else f"{side}_landmarks_world"
    landmarks = _points_from_flat(row.get(landmarks_key))
    if landmarks is None:
        return
    landmarks = _transform_points_to_rerun(landmarks, basis)

    rr.log(f"world/{side}_hand/points", rr.Points3D(landmarks, colors=[color], radii=0.007))
    if wrist_position is None:
        return

    wrist = np.asarray(wrist_position, dtype=np.float64)
    strips = []
    for start, end in FINGER_SEGMENTS:
        start_point = wrist if start == "wrist" else landmarks[int(start)]
        end_point = landmarks[int(end)]
        strips.append([start_point.tolist(), end_point.tolist()])
    rr.log(f"world/{side}_hand/bones", rr.LineStrips3D(strips, colors=[color], radii=0.003))


def _camera_matrix_from_session(session: dict[str, Any], frame_width: int, frame_height: int) -> np.ndarray | None:
    calibration = session.get("camera_calibration") or {}
    focal = calibration.get("focal_length")
    principal = calibration.get("principal_point")
    if focal is None or principal is None:
        defaults = session.get("projection_defaults") or {}
        if "fx" not in defaults or "fy" not in defaults:
            return None
        return np.array(
            [
                [float(defaults["fx"]), 0.0, float(defaults.get("cx", frame_width * 0.5))],
                [0.0, float(defaults["fy"]), float(defaults.get("cy", frame_height * 0.5))],
                [0.0, 0.0, 1.0],
            ],
            dtype=np.float64,
        )

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


def _init_rerun(args: argparse.Namespace, dataset_dir: Path) -> None:
    blueprint = None
    try:
        import rerun.blueprint as rrb

        blueprint = rrb.Blueprint(
            rrb.Horizontal(
                rrb.Spatial3DView(origin="world", name="3D World"),
                rrb.Vertical(
                    rrb.Spatial2DView(origin="video", name="Camera Video"),
                    rrb.TextDocumentView(origin="info", name="Dataset Info"),
                    row_shares=[3, 1],
                ),
                column_shares=[4, 2],
            ),
            collapse_panels=True,
        )
    except Exception as exc:
        logging.warning("Could not build Rerun blueprint: %s", exc)

    rr.init("quest_dataset_rerun", spawn=args.spawn and args.save is None, default_blueprint=blueprint)
    if args.save is not None:
        rr.save(args.save, default_blueprint=blueprint)
    elif not args.spawn:
        output = dataset_dir / DEFAULT_RRD_OUTPUT
        rr.save(output, default_blueprint=blueprint)
        logging.info("No --spawn/--save provided; writing %s", output)


def _log_static_scene(session: dict[str, Any], use_tag_space: bool) -> None:
    rr.log("world", rr.ViewCoordinates.RIGHT_HAND_Z_UP)
    rr.log(
        "world/origin",
        rr.Arrows3D(
            origins=[[0.0, 0.0, 0.0]] * 3,
            vectors=[[0.2, 0.0, 0.0], [0.0, 0.2, 0.0], [0.0, 0.0, 0.2]],
            colors=[[255, 60, 60], [60, 255, 60], [60, 120, 255]],
            labels=["x", "y", "z"],
            show_labels=True,
            radii=0.006,
        ),
    )

    apriltag = session.get("apriltag") or {}
    if apriltag and not use_tag_space:
        tag_to_world = np.asarray(apriltag.get("tag_to_world_matrix"), dtype=np.float64)
        if tag_to_world.shape == (4, 4):
            position = _transform_position_to_rerun(tag_to_world[:3, 3], OPENCV_TO_RERUN)
            quaternion = _transform_quaternion_to_rerun(_matrix_to_quat(tag_to_world[:3, :3]), OPENCV_TO_RERUN)
            _log_pose("world/tag_frame", position, quaternion, TAG_COLOR, axis_len=0.15)
    elif use_tag_space:
        _log_pose("world/tag_frame", [0.0, 0.0, 0.0], rr.Quaternion(xyzw=[0.0, 0.0, 0.0, 1.0]), TAG_COLOR, axis_len=0.15)

    rr.log(
        "info/summary",
        rr.TextDocument(
            f"# Quest dataset\n\n"
            f"- dataset: `{session.get('dataset_name', '')}`\n"
            f"- coordinate view: `{'tag' if use_tag_space else 'world'}`\n"
            f"- source basis: `{'opencv x-right y-down z-forward' if use_tag_space else 'unity x-right y-up z-forward'}`\n"
            f"- rerun basis: `right-handed x-forward y-left z-up`\n"
            f"- video: `{session.get('video_path', 'camera.mp4')}`\n"
            f"- apriltag detections: `{apriltag.get('total_detections', 0)}`\n",
            media_type=rr.MediaType.MARKDOWN,
        ),
    )


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="visualize_quest_dataset_rerun",
        description="Visualize Quest dataset poses, hands, tags, and video in Rerun.",
    )
    parser.add_argument("--name", required=True, help="Dataset name under ./data.")
    parser.add_argument("--session", default=None, help="Optional session name under the dataset.")
    parser.add_argument("--output-root", default=DEFAULT_OUTPUT_ROOT, help="Dataset root.")
    parser.add_argument("--frame-step", type=int, default=5, help="Log every Nth frame.")
    parser.add_argument("--max-frames", type=int, default=None, help="Optional maximum number of logged frames.")
    parser.add_argument("--save", type=Path, default=None, help="Write a .rrd file instead of spawning the viewer.")
    parser.add_argument("--spawn", action="store_true", help="Open the Rerun viewer.")
    parser.add_argument(
        "--world-space",
        action="store_true",
        help="Visualize original world-space poses instead of tag-space poses.",
    )
    return parser


def main() -> None:
    args = _build_parser().parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    dataset_dir = resolve_replay_dir(args.output_root, args.name, args.session)
    session = _load_json(dataset_dir / "session.json")
    use_tag_space = not args.world_space and bool(session.get("apriltag"))
    rows = _load_aligned_rows(dataset_dir, session, use_tag_space)
    if not rows:
        raise SystemExit("No aligned rows found.")

    video_path = dataset_dir / session.get("video_path", "camera.mp4")
    cap = cv2.VideoCapture(str(video_path))
    if not cap.isOpened():
        raise SystemExit(f"Could not open video: {video_path}")

    _init_rerun(args, dataset_dir)
    _log_static_scene(session, use_tag_space)

    frame_width = int(rows[0].get("camera_width") or cap.get(cv2.CAP_PROP_FRAME_WIDTH) or 0)
    frame_height = int(rows[0].get("camera_height") or cap.get(cv2.CAP_PROP_FRAME_HEIGHT) or 0)
    camera_matrix = _camera_matrix_from_session(session, frame_width, frame_height)
    if camera_matrix is not None:
        rr.log(
            "world/camera/image",
            rr.Pinhole(
                image_from_camera=camera_matrix,
                resolution=[frame_width, frame_height],
                camera_xyz=rr.ViewCoordinates.RDF,
            ),
            static=True,
        )

    logged = 0
    frame_step = max(int(args.frame_step), 1)
    try:
        for index, row in enumerate(rows):
            ok, frame_bgr = cap.read()
            if not ok:
                break
            if index % frame_step != 0:
                continue
            if args.max_frames is not None and logged >= args.max_frames:
                break

            timestamp_ns = row.get("camera_timestamp_ns")
            rr.set_time("frame", sequence=int(index))
            if timestamp_ns is not None:
                rr.set_time("quest_time", timestamp=int(timestamp_ns) / 1e9)

            frame_rgb = cv2.cvtColor(frame_bgr, cv2.COLOR_BGR2RGB)
            rr.log("video/image", rr.Image(frame_rgb))

            suffix = "_tag" if use_tag_space else "_world"
            camera_position = _as_position(row, f"camera_position{suffix}")
            camera_quaternion = _as_quaternion(row, f"camera_quaternion{suffix}")
            head_position = _as_position(row, f"head_position{'_tag' if use_tag_space else ''}")
            head_quaternion = _as_quaternion(row, f"head_quaternion{'_tag' if use_tag_space else ''}")
            basis = _basis_for_rows(use_tag_space)

            if camera_position is not None:
                camera_position = _transform_position_to_rerun(camera_position, basis)
            if camera_quaternion is not None:
                camera_quaternion = _transform_quaternion_to_rerun(camera_quaternion.xyzw, basis)
            if head_position is not None:
                head_position = _transform_position_to_rerun(head_position, basis)
            if head_quaternion is not None:
                head_quaternion = _transform_quaternion_to_rerun(head_quaternion.xyzw, basis)

            _log_pose("world/camera", camera_position, camera_quaternion, CAMERA_COLOR, axis_len=0.10)
            _log_pose("world/head", head_position, head_quaternion, HEAD_COLOR, axis_len=0.08)
            _log_hand("left", row, use_tag_space, LEFT_COLOR)
            _log_hand("right", row, use_tag_space, RIGHT_COLOR)

            logged += 1
            if logged % 200 == 0:
                logging.info("Logged %d frames through dataset frame %d", logged, index)
    finally:
        cap.release()

    logging.info("Logged %d frames to Rerun.", logged)


if __name__ == "__main__":
    main()
