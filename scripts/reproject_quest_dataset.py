"""Replay a recorded dataset and project hand data back into the camera image.

Usage:
    python ./scripts/reproject_quest_dataset.py --name demo
    python ./scripts/reproject_quest_dataset.py --name demo --session session_001
    python ./scripts/reproject_quest_dataset.py --name demo --segment 3
    python ./scripts/reproject_quest_dataset.py --name demo --output-root ./data --fps 15
"""

from __future__ import annotations

import argparse
import json
import logging
import time
from pathlib import Path

import imageio.v2 as imageio
import numpy as np
import pyarrow.parquet as pq
from PIL import Image, ImageDraw

from hts_dataset_utils import (
    camera_pose_from_head,
    default_camera_offset,
    finger_segment_indices,
    project_world_to_image_with_calibration,
    resolve_replay_dir,
)


DEFAULT_OUTPUT_ROOT = "./data"
DEFAULT_DATASET_FPS = 15
DEFAULT_CAMERA_EYE = "left"
DEFAULT_CAMERA_OFFSET = default_camera_offset(DEFAULT_CAMERA_EYE)
DEFAULT_CAMERA_ROTATION_OFFSET = np.array([0.0, 0.0, 0.0, 1.0], dtype=np.float64)


def _draw_hand_overlay(
    image: Image.Image,
    wrist_position: list[float] | None,
    landmarks_world: list[float] | None,
    camera_position: np.ndarray,
    camera_quaternion: np.ndarray,
    calibration: dict,
    point_color: tuple[int, int, int],
    line_color: tuple[int, int, int],
) -> None:
    if wrist_position is None or landmarks_world is None:
        return

    wrist = np.asarray(wrist_position, dtype=np.float64)
    landmarks = np.asarray(landmarks_world, dtype=np.float64)
    if landmarks.size < 3:
        return
    landmarks = landmarks.reshape((-1, 3))

    projected_landmarks, landmark_valid = project_world_to_image_with_calibration(
        landmarks,
        camera_position=camera_position,
        camera_quaternion=camera_quaternion,
        calibration=calibration,
        image_width=image.width,
        image_height=image.height,
    )
    projected_wrist, wrist_valid = project_world_to_image_with_calibration(
        wrist.reshape(1, 3),
        camera_position=camera_position,
        camera_quaternion=camera_quaternion,
        calibration=calibration,
        image_width=image.width,
        image_height=image.height,
    )

    draw = ImageDraw.Draw(image)
    wrist_xy = tuple(projected_wrist[0].tolist())
    if wrist_valid[0]:
        draw.ellipse(
            (wrist_xy[0] - 4, wrist_xy[1] - 4, wrist_xy[0] + 4, wrist_xy[1] + 4),
            fill=line_color,
        )

    for start, end in finger_segment_indices(len(landmarks)):
        if start == "wrist":
            start_xy = wrist_xy
            start_valid = wrist_valid[0]
        else:
            start_xy = tuple(projected_landmarks[start].tolist())
            start_valid = landmark_valid[start]
        end_xy = tuple(projected_landmarks[end].tolist())
        end_valid = landmark_valid[end]
        if start_valid and end_valid:
            draw.line((start_xy[0], start_xy[1], end_xy[0], end_xy[1]), fill=line_color, width=2)

    for index, is_valid in enumerate(landmark_valid):
        if not is_valid:
            continue
        x, y = projected_landmarks[index]
        draw.ellipse((x - 3, y - 3, x + 3, y + 3), fill=point_color)


def _resolve_intrinsics(
    session: dict,
    frame_width: int,
    frame_height: int,
) -> dict:
    calibration = session.get("camera_calibration")
    if calibration is not None:
        calibration = dict(calibration)
        calibration.setdefault("current_resolution", [frame_width, frame_height])
        return calibration

    defaults = session.get("projection_defaults") or {}
    if "fx" not in defaults or "fy" not in defaults:
        raise SystemExit("Missing camera calibration in session.json.")

    return {
        "current_resolution": [frame_width, frame_height],
        "sensor_resolution": [frame_width, frame_height],
        "focal_length": [float(defaults["fx"]), float(defaults["fy"])],
        "principal_point": [
            float(defaults.get("cx", frame_width * 0.5)),
            float(defaults.get("cy", frame_height * 0.5)),
        ],
        "lens_offset_position": session.get("camera_offset_local_m") or DEFAULT_CAMERA_OFFSET.tolist(),
        "lens_offset_rotation": session.get("camera_rotation_offset_quaternion") or DEFAULT_CAMERA_ROTATION_OFFSET.tolist(),
    }


def _load_session(dataset_dir: Path) -> dict:
    session_path = dataset_dir / "session.json"
    if not session_path.exists():
        return {}
    return json.loads(session_path.read_text(encoding="utf-8"))


def _load_segment(dataset_dir: Path, segment_index: int) -> dict:
    segments_path = dataset_dir / "segments.json"
    if not segments_path.exists():
        raise SystemExit(f"segments.json not found in {dataset_dir}")

    segments_data = json.loads(segments_path.read_text(encoding="utf-8"))
    for segment in segments_data.get("segments", []):
        if int(segment.get("segment_index", -1)) == segment_index:
            return segment

    available = ", ".join(str(segment.get("segment_index")) for segment in segments_data.get("segments", []))
    raise SystemExit(f"Segment {segment_index} not found in {segments_path}. Available segments: {available}")


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="reproject_quest_dataset",
        description="Replay a recorded Quest dataset and project hands back into the image.",
    )
    parser.add_argument("--name", required=True, help="Dataset name under ./data.")
    parser.add_argument(
        "--session",
        default=None,
        help="Optional session name. When omitted, the script auto-detects a single session directory.",
    )
    parser.add_argument(
        "--segment",
        type=int,
        default=None,
        help="Optional segment_index from segments.json to replay.",
    )
    parser.add_argument("--output-root", default=DEFAULT_OUTPUT_ROOT, help="Dataset root.")
    parser.add_argument("--fps", type=float, default=DEFAULT_DATASET_FPS, help="Dataset playback fps.")
    return parser


def main() -> None:
    args = _build_parser().parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    dataset_dir = resolve_replay_dir(args.output_root, args.name, args.session)

    session = _load_session(dataset_dir)
    aligned_rows = pq.read_table(dataset_dir / "aligned_frames.parquet").to_pylist()
    video_path = dataset_dir / session.get("video_path", "camera.mp4")
    reader = imageio.get_reader(video_path)
    segment = _load_segment(dataset_dir, args.segment) if args.segment is not None else None
    start_frame_index = 0
    end_frame_index = len(aligned_rows) - 1
    if segment is not None:
        start_frame_index = int(segment["start_frame_index"])
        end_frame_index = int(segment["end_frame_index"])
        if start_frame_index < 0 or end_frame_index < start_frame_index:
            raise SystemExit(f"Invalid frame range in segment {args.segment}: {start_frame_index}..{end_frame_index}")
        end_frame_index = min(end_frame_index, len(aligned_rows) - 1)
        logging.info(
            "Replaying segment %d frames %d..%d",
            args.segment,
            start_frame_index,
            end_frame_index,
        )

    import matplotlib.pyplot as plt

    plt.ion()
    display, axis = plt.subplots()
    axis.axis("off")
    image_artist = None
    pause_seconds = 1.0 / float(args.fps)

    try:
        for index, frame in enumerate(reader):
            if index >= len(aligned_rows):
                break
            if index < start_frame_index:
                continue
            if index > end_frame_index:
                break

            row = aligned_rows[index]
            pil_image = Image.fromarray(frame).convert("RGB")
            calibration = _resolve_intrinsics(session, pil_image.width, pil_image.height)

            camera_position = row.get("camera_position_world")
            camera_quaternion = row.get("camera_quaternion_world")
            if camera_position is None or camera_quaternion is None:
                head_position = row.get("head_position")
                head_quaternion = row.get("head_quaternion")
                if head_position is None or head_quaternion is None:
                    camera_position = None
                    camera_quaternion = None
                else:
                    lens_offset_position = calibration.get("lens_offset_position") or DEFAULT_CAMERA_OFFSET.tolist()
                    lens_offset_rotation = calibration.get("lens_offset_rotation") or DEFAULT_CAMERA_ROTATION_OFFSET.tolist()
                    camera_position, camera_quaternion = camera_pose_from_head(
                        head_position,
                        head_quaternion,
                        lens_offset_position,
                        lens_offset_rotation,
                    )

            if camera_position is not None and camera_quaternion is not None:
                camera_position = np.asarray(camera_position, dtype=np.float64)
                camera_quaternion = np.asarray(camera_quaternion, dtype=np.float64)
                _draw_hand_overlay(
                    pil_image,
                    row.get("left_wrist_position"),
                    row.get("left_landmarks_world"),
                    camera_position,
                    camera_quaternion,
                    calibration,
                    point_color=(90, 170, 255),
                    line_color=(40, 100, 220),
                )
                _draw_hand_overlay(
                    pil_image,
                    row.get("right_wrist_position"),
                    row.get("right_landmarks_world"),
                    camera_position,
                    camera_quaternion,
                    calibration,
                    point_color=(255, 165, 70),
                    line_color=(220, 90, 20),
                )

            if image_artist is None:
                image_artist = axis.imshow(pil_image)
            else:
                image_artist.set_data(pil_image)
            title = f"Quest Dataset Replay - frame {index}"
            if segment is not None:
                title = f"Quest Dataset Replay - segment {args.segment} frame {index}"
            axis.set_title(title)
            display.canvas.draw_idle()
            display.canvas.flush_events()
            plt.pause(pause_seconds)
    finally:
        reader.close()


if __name__ == "__main__":
    main()
