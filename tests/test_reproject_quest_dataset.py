from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))

from reproject_quest_dataset import _find_start_frame_index, _parse_start_time_seconds


def test_parse_start_time_minutes_seconds() -> None:
    assert _parse_start_time_seconds([8, 12]) == 492


def test_find_start_frame_index_from_camera_timestamps() -> None:
    camera_rows = [
        {"camera_frame_index": 0, "camera_timestamp_ns": 1_000_000_000},
        {"camera_frame_index": 1, "camera_timestamp_ns": 1_500_000_000},
        {"camera_frame_index": 2, "camera_timestamp_ns": 2_000_000_000},
    ]

    assert _find_start_frame_index(camera_rows, 1) == 2


def test_find_start_frame_index_respects_lower_bound() -> None:
    camera_rows = [
        {"camera_frame_index": 0, "camera_timestamp_ns": 1_000_000_000},
        {"camera_frame_index": 1, "camera_timestamp_ns": 1_500_000_000},
        {"camera_frame_index": 2, "camera_timestamp_ns": 2_000_000_000},
    ]

    assert _find_start_frame_index(camera_rows, 0, lower_frame_index=1) == 1


def test_find_start_frame_index_rejects_out_of_range_start_time() -> None:
    camera_rows = [
        {"camera_frame_index": 0, "camera_timestamp_ns": 1_000_000_000},
        {"camera_frame_index": 1, "camera_timestamp_ns": 1_500_000_000},
    ]

    with pytest.raises(SystemExit):
        _find_start_frame_index(camera_rows, 1)
