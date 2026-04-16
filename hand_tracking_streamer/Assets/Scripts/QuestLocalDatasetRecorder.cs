using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.IO.Compression;
using System.Text;
using UnityEngine;

[Serializable]
public class QuestLocalSessionInfo
{
    public string dataset_name;
    public long created_at_unix_ns;
    public string video_path;
    public string fps_mode;
    public int fps;
    public string camera_eye;
    public float[] camera_offset_local_m;
    public float[] camera_rotation_offset_quaternion;
    public SerializableVector3 camera_rotation_offset_euler_deg;
    public SerializableProjectionDefaults projection_defaults;
    public string projection_source;
    public string notes;
    public SerializableFingerSegment[] finger_segment_indices;
    public QuestCameraCalibrationMetadata camera_calibration;
    public SerializableStoragePaths storage;
}

[Serializable]
public class SerializableFingerSegment
{
    public string start;
    public int end;
}

[Serializable]
public class SerializableVector3
{
    public float pitch;
    public float yaw;
    public float roll;
}

[Serializable]
public class SerializableProjectionDefaults
{
    public float fx;
    public float fy;
    public float cx;
    public float cy;
}

[Serializable]
public class SerializableStoragePaths
{
    public string dataset_dir;
    public string zip_path;
}

[Serializable]
public class QuestTelemetryRow
{
    public string stream;
    public string kind;
    public int frame_id;
    public long timestamp_ns;
    public string raw_label;
    public float[] values;
}

[Serializable]
public class QuestCameraFrameRow
{
    public int camera_frame_index;
    public int camera_frame_id;
    public long camera_timestamp_ns;
    public long camera_received_at_ns;
    public int width;
    public int height;
}

[Serializable]
public class QuestAlignedFrameRow
{
    public int camera_frame_index;
    public int camera_frame_id;
    public long camera_timestamp_ns;
    public long camera_received_at_ns;
    public int camera_width;
    public int camera_height;

    public int head_frame_id;
    public long head_timestamp_ns;
    public float head_dt_ms;
    public float[] head_position;
    public float[] head_quaternion;

    public int left_wrist_frame_id;
    public long left_wrist_timestamp_ns;
    public float left_wrist_dt_ms;
    public float[] left_wrist_position;
    public float[] left_wrist_quaternion;

    public int right_wrist_frame_id;
    public long right_wrist_timestamp_ns;
    public float right_wrist_dt_ms;
    public float[] right_wrist_position;
    public float[] right_wrist_quaternion;

    public int left_landmarks_frame_id;
    public long left_landmarks_timestamp_ns;
    public float left_landmarks_dt_ms;
    public float[] left_landmarks_local;
    public float[] left_landmarks_world;

    public int right_landmarks_frame_id;
    public long right_landmarks_timestamp_ns;
    public float right_landmarks_dt_ms;
    public float[] right_landmarks_local;
    public float[] right_landmarks_world;

    public float[] camera_position_world;
    public float[] camera_quaternion_world;
    public string camera_pose_source;
    public long camera_pose_timestamp_ns;
}

[Serializable]
public class QuestTelemetrySample
{
    public int frameId;
    public long timestampNs;
    public float[] values;
    public string rawLabel;
}

public class QuestLocalDatasetRecorder : MonoBehaviour
{
    private const string CameraEye = "left";
    private const float DefaultFovXDeg = 90f;
    private const float DefaultFovYDeg = 70f;
    private const float DefaultCameraPitchDeg = 0f;
    private const float DefaultCameraYawDeg = 0f;
    private const float DefaultCameraRollDeg = 0f;
    private const int MaxBufferedSamples = 512;

    private readonly Dictionary<string, TimedSampleBuffer> _buffers = new Dictionary<string, TimedSampleBuffer>();
    private readonly Dictionary<int, QuestCameraFramePoseMetadata> _pendingCameraPoseByFrameId =
        new Dictionary<int, QuestCameraFramePoseMetadata>();
    private readonly object _ioLock = new object();
    private readonly Queue<int> _pendingCameraPoseOrder = new Queue<int>();

    private string _datasetName;
    private string _datasetDirectory;
    private string _zipPath;
    private long _sessionStartedUnixNs;
    private bool _isRecording;
    private int _cameraFrameIndex;
    private StreamWriter _telemetryWriter;
    private StreamWriter _cameraFramesWriter;
    private StreamWriter _alignedFramesWriter;
    private QuestCameraCalibrationMetadata _cameraCalibration;
    private SerializableProjectionDefaults _projectionDefaults;
    private string _projectionSource = "approximate_defaults_in_code";
    private Vector3 _cameraOffset = new Vector3(-0.032f, 0f, 0.015f);
    private Quaternion _cameraRotationOffset = Quaternion.identity;
    private AndroidMp4Recorder _videoRecorder;
    private string _videoFileName = "camera.mp4";
    private int _targetFps = 15;

    public bool IsRecording => _isRecording;
    public string DatasetDirectory => _datasetDirectory;
    public string ZipPath => _zipPath;
    public string VideoOutputPath => string.IsNullOrWhiteSpace(_datasetDirectory) ? null : Path.Combine(_datasetDirectory, _videoFileName);

    private void Awake()
    {
        _buffers["head:pose"] = new TimedSampleBuffer(MaxBufferedSamples);
        _buffers["left:wrist"] = new TimedSampleBuffer(MaxBufferedSamples);
        _buffers["left:landmarks"] = new TimedSampleBuffer(MaxBufferedSamples);
        _buffers["right:wrist"] = new TimedSampleBuffer(MaxBufferedSamples);
        _buffers["right:landmarks"] = new TimedSampleBuffer(MaxBufferedSamples);
    }

    public bool BeginRecording(string datasetName, int targetFps, AndroidMp4Recorder videoRecorder, out string error)
    {
        error = string.Empty;
        if (_isRecording)
        {
            error = "Local dataset recorder is already active.";
            return false;
        }

        if (videoRecorder == null)
        {
            error = "Video recorder is missing.";
            return false;
        }

        _videoRecorder = videoRecorder;
        _datasetName = SanitizeName(datasetName);
        _targetFps = Mathf.Max(1, targetFps);
        _sessionStartedUnixNs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() * 1_000_000L;
        _cameraFrameIndex = 0;
        _cameraCalibration = null;
        _projectionDefaults = null;
        _projectionSource = "approximate_defaults_in_code";
        _pendingCameraPoseByFrameId.Clear();
        _pendingCameraPoseOrder.Clear();
        ClearBuffers();

        string recordingsRoot = Path.Combine(Application.persistentDataPath, "recordings");
        string stamp = DateTime.UtcNow.ToString("yyyyMMdd_HHmmss", CultureInfo.InvariantCulture);
        _datasetDirectory = Path.Combine(recordingsRoot, $"{stamp}_{_datasetName}");
        _zipPath = _datasetDirectory + ".zip";

        try
        {
            Directory.CreateDirectory(_datasetDirectory);
            _telemetryWriter = CreateWriter("telemetry_raw.jsonl");
            _cameraFramesWriter = CreateWriter("camera_frames.jsonl");
            _alignedFramesWriter = CreateWriter("aligned_frames.jsonl");
            _isRecording = true;
            return true;
        }
        catch (Exception ex)
        {
            error = $"Failed to initialize dataset writers: {ex.Message}";
            SafeDisposeWriters();
            _isRecording = false;
            return false;
        }
    }

    public void EndRecording()
    {
        if (!_isRecording)
        {
            return;
        }

        _isRecording = false;
        try
        {
            WriteSessionJson();
            SafeDisposeWriters();
            BuildZipArchive();
        }
        catch (Exception ex)
        {
            Debug.LogError($"[LocalSave] Finalize failed: {ex.Message}");
        }
    }

    public void RecordTelemetry(string stream, string kind, int frameId, long timestampNs, string rawLabel, float[] values)
    {
        if (!_isRecording)
        {
            return;
        }

        QuestTelemetrySample sample = new QuestTelemetrySample
        {
            frameId = frameId,
            timestampNs = timestampNs,
            rawLabel = rawLabel,
            values = values != null ? (float[])values.Clone() : Array.Empty<float>(),
        };

        string key = $"{stream}:{kind}";
        if (_buffers.TryGetValue(key, out TimedSampleBuffer buffer))
        {
            buffer.Append(sample);
        }

        QuestTelemetryRow row = new QuestTelemetryRow
        {
            stream = stream,
            kind = kind,
            frame_id = frameId,
            timestamp_ns = timestampNs,
            raw_label = rawLabel,
            values = sample.values,
        };

        WriteJsonLine(_telemetryWriter, row);
    }

    public void RecordCameraCalibration(QuestCameraCalibrationMetadata calibration)
    {
        if (!_isRecording || calibration == null)
        {
            return;
        }

        _cameraCalibration = calibration;
        if (calibration.focal_length != null &&
            calibration.focal_length.Length >= 2 &&
            calibration.principal_point != null &&
            calibration.principal_point.Length >= 2)
        {
            _projectionDefaults = new SerializableProjectionDefaults
            {
                fx = calibration.focal_length[0],
                fy = calibration.focal_length[1],
                cx = calibration.principal_point[0],
                cy = calibration.principal_point[1],
            };
            _projectionSource = "quest_camera_calibration_metadata";
        }
    }

    public void RecordCameraPose(QuestCameraFramePoseMetadata pose)
    {
        if (!_isRecording || pose == null)
        {
            return;
        }

        _pendingCameraPoseByFrameId[unchecked((int)pose.frame_id)] = pose;
        _pendingCameraPoseOrder.Enqueue(unchecked((int)pose.frame_id));
        while (_pendingCameraPoseOrder.Count > MaxBufferedSamples)
        {
            int oldest = _pendingCameraPoseOrder.Dequeue();
            _pendingCameraPoseByFrameId.Remove(oldest);
        }
    }

    public void RecordCameraFrame(int frameId, long timestampNs, int width, int height, long receivedAtNs)
    {
        if (!_isRecording)
        {
            return;
        }

        QuestCameraFrameRow frameRow = new QuestCameraFrameRow
        {
            camera_frame_index = _cameraFrameIndex,
            camera_frame_id = frameId,
            camera_timestamp_ns = timestampNs,
            camera_received_at_ns = receivedAtNs,
            width = width,
            height = height,
        };
        WriteJsonLine(_cameraFramesWriter, frameRow);

        QuestAlignedFrameRow aligned = BuildAlignedRow(frameId, timestampNs, width, height, receivedAtNs, _cameraFrameIndex);
        WriteJsonLine(_alignedFramesWriter, aligned);
        _cameraFrameIndex++;
    }

    private QuestAlignedFrameRow BuildAlignedRow(int frameId, long timestampNs, int width, int height, long receivedAtNs, int frameIndex)
    {
        QuestAlignedFrameRow row = new QuestAlignedFrameRow
        {
            camera_frame_index = frameIndex,
            camera_frame_id = frameId,
            camera_timestamp_ns = timestampNs,
            camera_received_at_ns = receivedAtNs,
            camera_width = width,
            camera_height = height,
        };

        QuestTelemetrySample head = _buffers["head:pose"].Nearest(timestampNs);
        AppendPose(row, "head", head, timestampNs);

        QuestTelemetrySample leftWrist = _buffers["left:wrist"].Nearest(timestampNs);
        QuestTelemetrySample leftLandmarks = _buffers["left:landmarks"].Nearest(timestampNs);
        AppendPose(row, "left_wrist", leftWrist, timestampNs);
        AppendLandmarks(row, "left", leftWrist, leftLandmarks, timestampNs);

        QuestTelemetrySample rightWrist = _buffers["right:wrist"].Nearest(timestampNs);
        QuestTelemetrySample rightLandmarks = _buffers["right:landmarks"].Nearest(timestampNs);
        AppendPose(row, "right_wrist", rightWrist, timestampNs);
        AppendLandmarks(row, "right", rightWrist, rightLandmarks, timestampNs);

        if (head != null && head.values != null && head.values.Length >= 7)
        {
            float[] headPos = Slice(head.values, 0, 3);
            float[] headRot = Slice(head.values, 3, 4);
            float[] offset = _cameraCalibration != null && _cameraCalibration.lens_offset_position != null &&
                             _cameraCalibration.lens_offset_position.Length >= 3
                ? _cameraCalibration.lens_offset_position
                : new[] { _cameraOffset.x, _cameraOffset.y, _cameraOffset.z };
            float[] rotOffset = _cameraCalibration != null && _cameraCalibration.lens_offset_rotation != null &&
                                _cameraCalibration.lens_offset_rotation.Length >= 4
                ? _cameraCalibration.lens_offset_rotation
                : new[] { _cameraRotationOffset.x, _cameraRotationOffset.y, _cameraRotationOffset.z, _cameraRotationOffset.w };

            Pose cameraPose = DatasetMath.CameraPoseFromHead(headPos, headRot, offset, rotOffset);
            row.camera_position_world = DatasetMath.ToArray(cameraPose.position);
            row.camera_quaternion_world = DatasetMath.ToArray(cameraPose.rotation);
        }

        if (_pendingCameraPoseByFrameId.TryGetValue(frameId, out QuestCameraFramePoseMetadata framePose))
        {
            row.camera_pose_source = "quest_frame_pose";
            row.camera_position_world = framePose.position_world;
            row.camera_quaternion_world = framePose.rotation_world;
            row.camera_pose_timestamp_ns = unchecked((long)framePose.timestamp_ns);
            _pendingCameraPoseByFrameId.Remove(frameId);
        }
        else
        {
            row.camera_pose_source = "head_pose_plus_lens_offset";
            row.camera_pose_timestamp_ns = timestampNs;
        }

        return row;
    }

    private void AppendPose(QuestAlignedFrameRow row, string prefix, QuestTelemetrySample sample, long referenceTimestampNs)
    {
        int frameId = sample != null ? sample.frameId : 0;
        long timestampNs = sample != null ? sample.timestampNs : 0L;
        float dtMs = sample != null ? (sample.timestampNs - referenceTimestampNs) / 1_000_000f : 0f;
        float[] position = sample != null && sample.values != null && sample.values.Length >= 3 ? Slice(sample.values, 0, 3) : null;
        float[] quaternion = sample != null && sample.values != null && sample.values.Length >= 7 ? Slice(sample.values, 3, 4) : null;

        switch (prefix)
        {
            case "head":
                row.head_frame_id = frameId;
                row.head_timestamp_ns = timestampNs;
                row.head_dt_ms = dtMs;
                row.head_position = position;
                row.head_quaternion = quaternion;
                break;
            case "left_wrist":
                row.left_wrist_frame_id = frameId;
                row.left_wrist_timestamp_ns = timestampNs;
                row.left_wrist_dt_ms = dtMs;
                row.left_wrist_position = position;
                row.left_wrist_quaternion = quaternion;
                break;
            case "right_wrist":
                row.right_wrist_frame_id = frameId;
                row.right_wrist_timestamp_ns = timestampNs;
                row.right_wrist_dt_ms = dtMs;
                row.right_wrist_position = position;
                row.right_wrist_quaternion = quaternion;
                break;
        }
    }

    private void AppendLandmarks(
        QuestAlignedFrameRow row,
        string side,
        QuestTelemetrySample wrist,
        QuestTelemetrySample landmarks,
        long referenceTimestampNs)
    {
        int frameId = landmarks != null ? landmarks.frameId : 0;
        long timestampNs = landmarks != null ? landmarks.timestampNs : 0L;
        float dtMs = landmarks != null ? (landmarks.timestampNs - referenceTimestampNs) / 1_000_000f : 0f;
        float[] local = landmarks != null ? landmarks.values : null;
        float[] world = null;

        if (landmarks != null && landmarks.values != null && wrist != null && wrist.values != null && wrist.values.Length >= 7)
        {
            Vector3[] worldPoints = DatasetMath.LandmarksLocalToWorld(
                Slice(wrist.values, 0, 3),
                Slice(wrist.values, 3, 4),
                landmarks.values);
            world = DatasetMath.Flatten(worldPoints);
        }

        if (side == "left")
        {
            row.left_landmarks_frame_id = frameId;
            row.left_landmarks_timestamp_ns = timestampNs;
            row.left_landmarks_dt_ms = dtMs;
            row.left_landmarks_local = local;
            row.left_landmarks_world = world;
        }
        else
        {
            row.right_landmarks_frame_id = frameId;
            row.right_landmarks_timestamp_ns = timestampNs;
            row.right_landmarks_dt_ms = dtMs;
            row.right_landmarks_local = local;
            row.right_landmarks_world = world;
        }
    }

    private StreamWriter CreateWriter(string fileName)
    {
        string path = Path.Combine(_datasetDirectory, fileName);
        return new StreamWriter(path, false, new UTF8Encoding(false));
    }

    private void WriteJsonLine<T>(StreamWriter writer, T value)
    {
        if (writer == null || value == null)
        {
            return;
        }

        lock (_ioLock)
        {
            writer.WriteLine(JsonUtility.ToJson(value));
            writer.Flush();
        }
    }

    private void WriteSessionJson()
    {
        if (string.IsNullOrWhiteSpace(_datasetDirectory))
        {
            return;
        }

        if (_projectionDefaults == null)
        {
            _projectionDefaults = BuildFallbackProjectionDefaults();
        }

        QuestLocalSessionInfo session = new QuestLocalSessionInfo
        {
            dataset_name = _datasetName,
            created_at_unix_ns = _sessionStartedUnixNs,
            video_path = _videoFileName,
            fps_mode = "fixed",
            fps = _targetFps,
            camera_eye = CameraEye,
            camera_offset_local_m = new[] { _cameraOffset.x, _cameraOffset.y, _cameraOffset.z },
            camera_rotation_offset_quaternion = new[]
            {
                _cameraRotationOffset.x,
                _cameraRotationOffset.y,
                _cameraRotationOffset.z,
                _cameraRotationOffset.w,
            },
            camera_rotation_offset_euler_deg = new SerializableVector3
            {
                pitch = DefaultCameraPitchDeg,
                yaw = DefaultCameraYawDeg,
                roll = DefaultCameraRollDeg,
            },
            projection_defaults = _projectionDefaults,
            projection_source = _projectionSource,
            notes = "Camera video is stored as camera.mp4. Quest local save writes jsonl telemetry plus an aligned frame index.",
            finger_segment_indices = BuildFingerSegmentIndices(),
            camera_calibration = _cameraCalibration,
            storage = new SerializableStoragePaths
            {
                dataset_dir = _datasetDirectory,
                zip_path = _zipPath,
            },
        };

        string sessionJson = JsonUtility.ToJson(session, true);
        File.WriteAllText(Path.Combine(_datasetDirectory, "session.json"), sessionJson, new UTF8Encoding(false));
    }

    private void BuildZipArchive()
    {
        if (string.IsNullOrWhiteSpace(_datasetDirectory) || !Directory.Exists(_datasetDirectory))
        {
            return;
        }

        if (File.Exists(_zipPath))
        {
            File.Delete(_zipPath);
        }

        ZipFile.CreateFromDirectory(_datasetDirectory, _zipPath, CompressionLevel.Optimal, false);
    }

    private SerializableProjectionDefaults BuildFallbackProjectionDefaults()
    {
        int width = 640;
        int height = 480;
        if (_cameraCalibration != null && _cameraCalibration.current_resolution != null && _cameraCalibration.current_resolution.Length >= 2)
        {
            width = Mathf.Max(1, _cameraCalibration.current_resolution[0]);
            height = Mathf.Max(1, _cameraCalibration.current_resolution[1]);
        }

        float cx = width * 0.5f;
        float cy = height * 0.5f;
        float fx = cx / Mathf.Tan(DefaultFovXDeg * Mathf.Deg2Rad * 0.5f);
        float fy = cy / Mathf.Tan(DefaultFovYDeg * Mathf.Deg2Rad * 0.5f);
        return new SerializableProjectionDefaults
        {
            fx = fx,
            fy = fy,
            cx = cx,
            cy = cy,
        };
    }

    private SerializableFingerSegment[] BuildFingerSegmentIndices()
    {
        return new[]
        {
            Segment("wrist", 1), Segment("1", 2), Segment("2", 3), Segment("3", 4),
            Segment("wrist", 5), Segment("5", 6), Segment("6", 7), Segment("7", 8),
            Segment("wrist", 9), Segment("9", 10), Segment("10", 11), Segment("11", 12),
            Segment("wrist", 13), Segment("13", 14), Segment("14", 15), Segment("15", 16),
            Segment("wrist", 17), Segment("17", 18), Segment("18", 19), Segment("19", 20),
        };
    }

    private static SerializableFingerSegment Segment(string start, int end)
    {
        return new SerializableFingerSegment
        {
            start = start,
            end = end,
        };
    }

    private void ClearBuffers()
    {
        foreach (TimedSampleBuffer buffer in _buffers.Values)
        {
            buffer.Clear();
        }
    }

    private void SafeDisposeWriters()
    {
        _telemetryWriter?.Dispose();
        _cameraFramesWriter?.Dispose();
        _alignedFramesWriter?.Dispose();
        _telemetryWriter = null;
        _cameraFramesWriter = null;
        _alignedFramesWriter = null;
    }

    private static string SanitizeName(string value)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            return "local_save";
        }

        StringBuilder sb = new StringBuilder(value.Length);
        foreach (char c in value)
        {
            sb.Append(char.IsLetterOrDigit(c) || c == '-' || c == '_' ? c : '_');
        }
        return sb.ToString();
    }

    private static float[] Slice(float[] source, int start, int count)
    {
        if (source == null || start < 0 || count <= 0 || source.Length < start + count)
        {
            return null;
        }

        float[] values = new float[count];
        Array.Copy(source, start, values, 0, count);
        return values;
    }

    private void OnDestroy()
    {
        EndRecording();
    }

    private sealed class TimedSampleBuffer
    {
        private readonly int _maxLength;
        private readonly List<QuestTelemetrySample> _samples;

        public TimedSampleBuffer(int maxLength)
        {
            _maxLength = Mathf.Max(1, maxLength);
            _samples = new List<QuestTelemetrySample>(_maxLength);
        }

        public void Append(QuestTelemetrySample sample)
        {
            if (sample == null)
            {
                return;
            }

            _samples.Add(sample);
            if (_samples.Count > _maxLength)
            {
                _samples.RemoveAt(0);
            }
        }

        public QuestTelemetrySample Nearest(long timestampNs)
        {
            QuestTelemetrySample nearest = null;
            long bestDistance = long.MaxValue;
            for (int i = 0; i < _samples.Count; i++)
            {
                QuestTelemetrySample sample = _samples[i];
                long distance = Math.Abs(sample.timestampNs - timestampNs);
                if (distance < bestDistance)
                {
                    bestDistance = distance;
                    nearest = sample;
                }
            }
            return nearest;
        }

        public void Clear()
        {
            _samples.Clear();
        }
    }

    private static class DatasetMath
    {
        public static Vector3[] LandmarksLocalToWorld(float[] wristPosition, float[] wristQuaternion, float[] landmarksLocal)
        {
            if (wristPosition == null || wristQuaternion == null || landmarksLocal == null || landmarksLocal.Length < 3)
            {
                return Array.Empty<Vector3>();
            }

            Quaternion wristRot = Normalize(new Quaternion(wristQuaternion[0], wristQuaternion[1], wristQuaternion[2], wristQuaternion[3]));
            Vector3 wristPos = new Vector3(wristPosition[0], wristPosition[1], wristPosition[2]);
            int count = landmarksLocal.Length / 3;
            Vector3[] result = new Vector3[count];
            for (int i = 0; i < count; i++)
            {
                Vector3 local = new Vector3(
                    landmarksLocal[i * 3 + 0],
                    landmarksLocal[i * 3 + 1],
                    landmarksLocal[i * 3 + 2]);
                result[i] = wristPos + wristRot * local;
            }
            return result;
        }

        public static Pose CameraPoseFromHead(float[] headPosition, float[] headQuaternion, float[] cameraOffsetLocal, float[] cameraRotationOffset)
        {
            Vector3 headPos = new Vector3(headPosition[0], headPosition[1], headPosition[2]);
            Quaternion headRot = Normalize(new Quaternion(headQuaternion[0], headQuaternion[1], headQuaternion[2], headQuaternion[3]));
            Vector3 offset = new Vector3(cameraOffsetLocal[0], cameraOffsetLocal[1], cameraOffsetLocal[2]);
            Quaternion rotOffset = cameraRotationOffset != null && cameraRotationOffset.Length >= 4
                ? Normalize(new Quaternion(cameraRotationOffset[0], cameraRotationOffset[1], cameraRotationOffset[2], cameraRotationOffset[3]))
                : Quaternion.identity;
            Vector3 cameraPos = headPos + headRot * offset;
            Quaternion cameraRot = Normalize(headRot * rotOffset);
            return new Pose(cameraPos, cameraRot);
        }

        public static float[] ToArray(Vector3 vector)
        {
            return new[] { vector.x, vector.y, vector.z };
        }

        public static float[] ToArray(Quaternion quaternion)
        {
            return new[] { quaternion.x, quaternion.y, quaternion.z, quaternion.w };
        }

        public static float[] Flatten(Vector3[] points)
        {
            if (points == null || points.Length == 0)
            {
                return null;
            }

            float[] values = new float[points.Length * 3];
            for (int i = 0; i < points.Length; i++)
            {
                values[i * 3 + 0] = points[i].x;
                values[i * 3 + 1] = points[i].y;
                values[i * 3 + 2] = points[i].z;
            }
            return values;
        }

        private static Quaternion Normalize(Quaternion quaternion)
        {
            float magnitude = Mathf.Sqrt(
                quaternion.x * quaternion.x +
                quaternion.y * quaternion.y +
                quaternion.z * quaternion.z +
                quaternion.w * quaternion.w);
            if (magnitude <= 1e-6f)
            {
                return Quaternion.identity;
            }
            return new Quaternion(
                quaternion.x / magnitude,
                quaternion.y / magnitude,
                quaternion.z / magnitude,
                quaternion.w / magnitude);
        }
    }
}
