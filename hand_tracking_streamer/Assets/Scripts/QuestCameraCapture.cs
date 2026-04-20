using System;
using System.Linq;
using System.Reflection;
using Unity.Collections;
using UnityEngine;

[Serializable]
public class QuestCameraCalibrationMetadata
{
    public string packet_type = "camera_calibration";
    public string source = "Meta.XR.PassthroughCameraAccess.Intrinsics";
    public string camera_eye;
    public ulong timestamp_ns;
    public int[] current_resolution;
    public int[] sensor_resolution;
    public float[] focal_length;
    public float[] principal_point;
    public float[] lens_offset_position;
    public float[] lens_offset_rotation;
}

[Serializable]
public class QuestCameraFramePoseMetadata
{
    public string packet_type = "camera_pose";
    public string source = "Meta.XR.PassthroughCameraAccess.GetCameraPose";
    public string camera_eye;
    public uint frame_id;
    public ulong timestamp_ns;
    public float[] position_world;
    public float[] rotation_world;
}

public class QuestCameraCapture : MonoBehaviour
{
    public enum CameraEye
    {
        Left,
        Right,
    }

    [Header("Configuration")]
    [SerializeField] private bool autoInitializeOnEnable = true;
    [SerializeField] private CameraEye cameraEye = CameraEye.Left;
    [SerializeField] private Vector2Int requestedResolution = new Vector2Int(640, 480);

    [Header("Preview")]
    [SerializeField] private Renderer previewRenderer;

    [Header("Logging")]
    [SerializeField] private bool logToHUD = true;
    [SerializeField] private string hudLogSource = "Left";

    private static readonly string[] CameraAccessTypeNames =
    {
        "Meta.XR.PassthroughCameraAccess",
        "Meta.XR.MRUtilityKit.PassthroughCameraAccess",
    };

    private Component _cameraAccess;
    private Type _cameraAccessType;
    private MethodInfo _getTextureMethod;
    private MethodInfo _getCameraPoseMethod;
    private PropertyInfo _isPlayingProperty;
    private PropertyInfo _timestampProperty;
    private PropertyInfo _currentResolutionProperty;
    private PropertyInfo _intrinsicsProperty;
    private string _lastStatus = "idle";
    private bool _hasLoggedMissingDependency;
    private Texture2D _cpuReadbackTexture;
    private RenderTexture _cpuReadbackTarget;
    private byte[] _cpuReadbackRgbBytes;
    private uint _encodedFrameId;

    public event Action<Texture, ulong> OnFrameAvailable;

    public bool IsReady => _cameraAccess != null && GetIsPlaying();
    public Texture LatestTexture { get; private set; }
    public ulong LatestSampleTimestampNs { get; private set; }
    public DateTime? LatestCameraTimestampUtc { get; private set; }
    public string LastStatus => _lastStatus;

    public void SetRequestedResolution(Vector2Int resolution)
    {
        requestedResolution = new Vector2Int(
            Mathf.Max(1, resolution.x),
            Mathf.Max(1, resolution.y)
        );

        if (_cameraAccess != null)
        {
            ApplyRequestedSettings();
        }
    }

    public Vector2Int CurrentResolution
    {
        get
        {
            if (_cameraAccess == null || _currentResolutionProperty == null)
            {
                return Vector2Int.zero;
            }

            object value = _currentResolutionProperty.GetValue(_cameraAccess);
            return value is Vector2Int resolution ? resolution : Vector2Int.zero;
        }
    }

    private void OnEnable()
    {
        if (autoInitializeOnEnable)
        {
            EnsureInitialized();
        }
    }

    private void Update()
    {
        if (_cameraAccess == null)
        {
            return;
        }

        if (!GetIsPlaying())
        {
            _lastStatus = "waiting_for_frames";
            return;
        }

        Texture texture = GetLatestTexture();
        if (texture == null)
        {
            _lastStatus = "texture_unavailable";
            return;
        }

        LatestTexture = texture;
        LatestSampleTimestampNs = QuestStreamClock.GetMonotonicTimestampNs();
        LatestCameraTimestampUtc = TryGetCameraTimestampUtc();
        _lastStatus = "streaming";

        if (previewRenderer != null && previewRenderer.material != null)
        {
            previewRenderer.material.mainTexture = texture;
        }

        OnFrameAvailable?.Invoke(texture, LatestSampleTimestampNs);
    }

    public bool EnsureInitialized()
    {
        if (_cameraAccess != null)
        {
            return true;
        }

        _cameraAccessType = ResolveCameraAccessType();
        if (_cameraAccessType == null)
        {
            _lastStatus = "missing_mruk_passthrough_camera";
            if (!_hasLoggedMissingDependency)
            {
                LogHUD(
                    "Camera uplink requires MRUK PassthroughCameraAccess. " +
                    "Install Meta MR Utility Kit v81+ and grant HEADSET_CAMERA permission."
                );
                _hasLoggedMissingDependency = true;
            }
            return false;
        }

        _cameraAccess = GetComponent(_cameraAccessType);
        if (_cameraAccess == null)
        {
            _cameraAccess = gameObject.AddComponent(_cameraAccessType);
        }

        CacheReflectionMembers();
        ApplyRequestedSettings();
        SetBehaviourEnabled(true);

        _lastStatus = "initialized";
        LogHUD($"Camera capture initialized ({cameraEye}, {requestedResolution.x}x{requestedResolution.y}).");
        return true;
    }

    public bool TryEncodeJpegFrame(
        int jpegQuality,
        out byte[] jpegBytes,
        out uint frameId,
        out ulong timestampNs,
        out int width,
        out int height,
        out string error
    )
    {
        jpegBytes = null;
        frameId = 0;
        timestampNs = 0;
        width = 0;
        height = 0;
        error = string.Empty;

        if (_cameraAccess == null)
        {
            error = "Camera access component is not initialized.";
            return false;
        }

        if (!GetIsPlaying())
        {
            error = "Camera is not yet playing.";
            return false;
        }

        Texture sourceTexture = GetLatestTexture();
        if (sourceTexture == null)
        {
            error = "Camera texture is unavailable.";
            return false;
        }

        width = sourceTexture.width;
        height = sourceTexture.height;
        if (width <= 0 || height <= 0)
        {
            error = $"Camera texture has invalid dimensions {width}x{height}.";
            return false;
        }

        try
        {
            EnsureCpuReadbackTargets(width, height);

            Graphics.Blit(sourceTexture, _cpuReadbackTarget);

            RenderTexture previous = RenderTexture.active;
            RenderTexture.active = _cpuReadbackTarget;
            _cpuReadbackTexture.ReadPixels(new Rect(0, 0, width, height), 0, 0, false);
            RenderTexture.active = previous;

            jpegBytes = _cpuReadbackTexture.EncodeToJPG(Mathf.Clamp(jpegQuality, 1, 100));
            if (jpegBytes == null || jpegBytes.Length == 0)
            {
                error = "JPEG encoding returned an empty payload.";
                return false;
            }

            frameId = QuestStreamClock.NextFrameId(ref _encodedFrameId);
            timestampNs = QuestStreamClock.GetMonotonicTimestampNs();
            return true;
        }
        catch (Exception ex)
        {
            error = $"JPEG frame capture failed: {ex.Message}";
            return false;
        }
    }

    public bool TryReadRgbaFrame(
        out Color32[] pixels,
        out uint frameId,
        out ulong timestampNs,
        out int width,
        out int height,
        out string error
    )
    {
        pixels = null;
        frameId = 0;
        timestampNs = 0;
        width = 0;
        height = 0;
        error = string.Empty;

        if (_cameraAccess == null)
        {
            error = "Camera access component is not initialized.";
            return false;
        }

        if (!GetIsPlaying())
        {
            error = "Camera is not yet playing.";
            return false;
        }

        Texture sourceTexture = GetLatestTexture();
        if (sourceTexture == null)
        {
            error = "Camera texture is unavailable.";
            return false;
        }

        width = sourceTexture.width;
        height = sourceTexture.height;
        if (width <= 0 || height <= 0)
        {
            error = $"Camera texture has invalid dimensions {width}x{height}.";
            return false;
        }

        try
        {
            EnsureCpuReadbackTargets(width, height);

            Graphics.Blit(sourceTexture, _cpuReadbackTarget);

            RenderTexture previous = RenderTexture.active;
            RenderTexture.active = _cpuReadbackTarget;
            _cpuReadbackTexture.ReadPixels(new Rect(0, 0, width, height), 0, 0, false);
            RenderTexture.active = previous;

            pixels = _cpuReadbackTexture.GetPixels32();
            if (pixels == null || pixels.Length == 0)
            {
                error = "RGBA readback returned an empty buffer.";
                return false;
            }

            frameId = QuestStreamClock.NextFrameId(ref _encodedFrameId);
            timestampNs = QuestStreamClock.GetMonotonicTimestampNs();
            return true;
        }
        catch (Exception ex)
        {
            error = $"RGBA frame capture failed: {ex.Message}";
            return false;
        }
    }

    public bool TryReadRgbFrame(
        out byte[] rgbBytes,
        out uint frameId,
        out ulong timestampNs,
        out int width,
        out int height,
        out string error
    )
    {
        rgbBytes = null;
        frameId = 0;
        timestampNs = 0;
        width = 0;
        height = 0;
        error = string.Empty;

        if (_cameraAccess == null)
        {
            error = "Camera access component is not initialized.";
            return false;
        }

        if (!GetIsPlaying())
        {
            error = "Camera is not yet playing.";
            return false;
        }

        Texture sourceTexture = GetLatestTexture();
        if (sourceTexture == null)
        {
            error = "Camera texture is unavailable.";
            return false;
        }

        width = sourceTexture.width;
        height = sourceTexture.height;
        if (width <= 0 || height <= 0)
        {
            error = $"Camera texture has invalid dimensions {width}x{height}.";
            return false;
        }

        try
        {
            EnsureCpuReadbackTargets(width, height);
            EnsureCpuReadbackRgbBuffer(width * height * 3);

            Graphics.Blit(sourceTexture, _cpuReadbackTarget);

            RenderTexture previous = RenderTexture.active;
            RenderTexture.active = _cpuReadbackTarget;
            _cpuReadbackTexture.ReadPixels(new Rect(0, 0, width, height), 0, 0, false);
            RenderTexture.active = previous;

            NativeArray<byte> raw = _cpuReadbackTexture.GetRawTextureData<byte>();
            if (!raw.IsCreated || raw.Length == 0)
            {
                error = "RGB readback returned an empty buffer.";
                return false;
            }

            raw.CopyTo(_cpuReadbackRgbBytes);
            rgbBytes = _cpuReadbackRgbBytes;

            frameId = QuestStreamClock.NextFrameId(ref _encodedFrameId);
            timestampNs = QuestStreamClock.GetMonotonicTimestampNs();
            return true;
        }
        catch (Exception ex)
        {
            error = $"RGB frame capture failed: {ex.Message}";
            return false;
        }
    }

    public bool TryBuildCalibrationMetadataJson(out string json, out string error)
    {
        json = null;
        error = string.Empty;

        if (_cameraAccess == null)
        {
            error = "Camera access component is not initialized.";
            return false;
        }

        if (!GetIsPlaying())
        {
            error = "Camera is not yet playing.";
            return false;
        }

        if (_intrinsicsProperty == null)
        {
            error = "Passthrough camera intrinsics are not exposed by this MRUK/PCA version.";
            return false;
        }

        try
        {
            object intrinsics = _intrinsicsProperty.GetValue(_cameraAccess);
            if (intrinsics == null)
            {
                error = "Passthrough camera intrinsics are unavailable.";
                return false;
            }

            Type intrinsicsType = intrinsics.GetType();
            Vector2 focalLength = ReadStructField<Vector2>(intrinsics, intrinsicsType, "FocalLength");
            Vector2 principalPoint = ReadStructField<Vector2>(intrinsics, intrinsicsType, "PrincipalPoint");
            Vector2Int sensorResolution = ReadStructField<Vector2Int>(intrinsics, intrinsicsType, "SensorResolution");
            Pose lensOffset = ReadStructField<Pose>(intrinsics, intrinsicsType, "LensOffset");
            Vector2Int currentResolution = CurrentResolution;

            QuestCameraCalibrationMetadata metadata = new QuestCameraCalibrationMetadata
            {
                camera_eye = cameraEye == CameraEye.Left ? "left" : "right",
                timestamp_ns = QuestStreamClock.GetMonotonicTimestampNs(),
                current_resolution = new[] { currentResolution.x, currentResolution.y },
                sensor_resolution = new[] { sensorResolution.x, sensorResolution.y },
                focal_length = new[] { focalLength.x, focalLength.y },
                principal_point = new[] { principalPoint.x, principalPoint.y },
                lens_offset_position = new[]
                {
                    lensOffset.position.x,
                    lensOffset.position.y,
                    lensOffset.position.z,
                },
                lens_offset_rotation = new[]
                {
                    lensOffset.rotation.x,
                    lensOffset.rotation.y,
                    lensOffset.rotation.z,
                    lensOffset.rotation.w,
                },
            };

            json = JsonUtility.ToJson(metadata);
            return true;
        }
        catch (Exception ex)
        {
            error = $"Camera calibration metadata read failed: {ex.Message}";
            return false;
        }
    }

    public bool TryBuildCalibrationMetadata(out QuestCameraCalibrationMetadata metadata, out string error)
    {
        metadata = null;
        error = string.Empty;

        if (_cameraAccess == null)
        {
            error = "Camera access component is not initialized.";
            return false;
        }

        if (!GetIsPlaying())
        {
            error = "Camera is not yet playing.";
            return false;
        }

        if (_intrinsicsProperty == null)
        {
            error = "Passthrough camera intrinsics are not exposed by this MRUK/PCA version.";
            return false;
        }

        try
        {
            object intrinsics = _intrinsicsProperty.GetValue(_cameraAccess);
            if (intrinsics == null)
            {
                error = "Passthrough camera intrinsics are unavailable.";
                return false;
            }

            Type intrinsicsType = intrinsics.GetType();
            Vector2 focalLength = ReadStructField<Vector2>(intrinsics, intrinsicsType, "FocalLength");
            Vector2 principalPoint = ReadStructField<Vector2>(intrinsics, intrinsicsType, "PrincipalPoint");
            Vector2Int sensorResolution = ReadStructField<Vector2Int>(intrinsics, intrinsicsType, "SensorResolution");
            Pose lensOffset = ReadStructField<Pose>(intrinsics, intrinsicsType, "LensOffset");
            Vector2Int currentResolution = CurrentResolution;

            metadata = new QuestCameraCalibrationMetadata
            {
                camera_eye = cameraEye == CameraEye.Left ? "left" : "right",
                timestamp_ns = QuestStreamClock.GetMonotonicTimestampNs(),
                current_resolution = new[] { currentResolution.x, currentResolution.y },
                sensor_resolution = new[] { sensorResolution.x, sensorResolution.y },
                focal_length = new[] { focalLength.x, focalLength.y },
                principal_point = new[] { principalPoint.x, principalPoint.y },
                lens_offset_position = new[]
                {
                    lensOffset.position.x,
                    lensOffset.position.y,
                    lensOffset.position.z,
                },
                lens_offset_rotation = new[]
                {
                    lensOffset.rotation.x,
                    lensOffset.rotation.y,
                    lensOffset.rotation.z,
                    lensOffset.rotation.w,
                },
            };

            return true;
        }
        catch (Exception ex)
        {
            error = $"Camera calibration metadata read failed: {ex.Message}";
            return false;
        }
    }

    public bool TryBuildFramePoseMetadataJson(
        uint frameId,
        ulong timestampNs,
        out string json,
        out string error
    )
    {
        json = null;
        error = string.Empty;

        if (_cameraAccess == null)
        {
            error = "Camera access component is not initialized.";
            return false;
        }

        if (!GetIsPlaying())
        {
            error = "Camera is not yet playing.";
            return false;
        }

        if (_getCameraPoseMethod == null)
        {
            error = "Passthrough camera pose is not exposed by this MRUK/PCA version.";
            return false;
        }

        try
        {
            object value = _getCameraPoseMethod.Invoke(_cameraAccess, null);
            if (!(value is Pose pose))
            {
                error = "Camera pose returned an unexpected value.";
                return false;
            }

            QuestCameraFramePoseMetadata metadata = new QuestCameraFramePoseMetadata
            {
                camera_eye = cameraEye == CameraEye.Left ? "left" : "right",
                frame_id = frameId,
                timestamp_ns = timestampNs,
                position_world = new[]
                {
                    pose.position.x,
                    pose.position.y,
                    pose.position.z,
                },
                rotation_world = new[]
                {
                    pose.rotation.x,
                    pose.rotation.y,
                    pose.rotation.z,
                    pose.rotation.w,
                },
            };

            json = JsonUtility.ToJson(metadata);
            return true;
        }
        catch (Exception ex)
        {
            error = $"Camera pose metadata read failed: {ex.Message}";
            return false;
        }
    }

    public bool TryBuildFramePoseMetadata(
        uint frameId,
        ulong timestampNs,
        out QuestCameraFramePoseMetadata metadata,
        out string error
    )
    {
        metadata = null;
        error = string.Empty;

        if (_cameraAccess == null)
        {
            error = "Camera access component is not initialized.";
            return false;
        }

        if (!GetIsPlaying())
        {
            error = "Camera is not yet playing.";
            return false;
        }

        if (_getCameraPoseMethod == null)
        {
            error = "Passthrough camera pose is not exposed by this MRUK/PCA version.";
            return false;
        }

        try
        {
            object value = _getCameraPoseMethod.Invoke(_cameraAccess, null);
            if (!(value is Pose pose))
            {
                error = "Camera pose returned an unexpected value.";
                return false;
            }

            metadata = new QuestCameraFramePoseMetadata
            {
                camera_eye = cameraEye == CameraEye.Left ? "left" : "right",
                frame_id = frameId,
                timestamp_ns = timestampNs,
                position_world = new[]
                {
                    pose.position.x,
                    pose.position.y,
                    pose.position.z,
                },
                rotation_world = new[]
                {
                    pose.rotation.x,
                    pose.rotation.y,
                    pose.rotation.z,
                    pose.rotation.w,
                },
            };

            return true;
        }
        catch (Exception ex)
        {
            error = $"Camera pose metadata read failed: {ex.Message}";
            return false;
        }
    }

    private Type ResolveCameraAccessType()
    {
        foreach (string typeName in CameraAccessTypeNames)
        {
            Type direct = Type.GetType(typeName);
            if (direct != null)
            {
                return direct;
            }

            Type discovered = AppDomain.CurrentDomain
                .GetAssemblies()
                .Select(assembly => assembly.GetType(typeName, false))
                .FirstOrDefault(type => type != null);

            if (discovered != null)
            {
                return discovered;
            }
        }

        return null;
    }

    private void CacheReflectionMembers()
    {
        _getTextureMethod = _cameraAccessType.GetMethod("GetTexture", Type.EmptyTypes);
        _getCameraPoseMethod = _cameraAccessType.GetMethod("GetCameraPose", Type.EmptyTypes);
        _isPlayingProperty = _cameraAccessType.GetProperty("IsPlaying");
        _timestampProperty = _cameraAccessType.GetProperty("Timestamp");
        _currentResolutionProperty = _cameraAccessType.GetProperty("CurrentResolution");
        _intrinsicsProperty = _cameraAccessType.GetProperty("Intrinsics");
    }

    private void ApplyRequestedSettings()
    {
        PropertyInfo cameraPositionProperty = _cameraAccessType.GetProperty("CameraPosition");
        if (cameraPositionProperty != null)
        {
            Type enumType = cameraPositionProperty.PropertyType;
            string enumName = cameraEye == CameraEye.Left ? "Left" : "Right";
            object enumValue = Enum.Parse(enumType, enumName);
            cameraPositionProperty.SetValue(_cameraAccess, enumValue);
        }

        PropertyInfo requestedResolutionProperty = _cameraAccessType.GetProperty("RequestedResolution");
        if (requestedResolutionProperty != null && requestedResolutionProperty.PropertyType == typeof(Vector2Int))
        {
            requestedResolutionProperty.SetValue(_cameraAccess, requestedResolution);
        }
    }

    private void SetBehaviourEnabled(bool enabled)
    {
        if (_cameraAccess is Behaviour behaviour)
        {
            behaviour.enabled = enabled;
        }
    }

    private bool GetIsPlaying()
    {
        if (_cameraAccess == null || _isPlayingProperty == null)
        {
            return false;
        }

        object value = _isPlayingProperty.GetValue(_cameraAccess);
        return value is bool isPlaying && isPlaying;
    }

    private Texture GetLatestTexture()
    {
        if (_cameraAccess == null || _getTextureMethod == null)
        {
            return null;
        }

        try
        {
            return _getTextureMethod.Invoke(_cameraAccess, null) as Texture;
        }
        catch (Exception ex)
        {
            _lastStatus = "texture_read_failed";
            LogHUD($"Camera texture read failed: {ex.Message}");
            return null;
        }
    }

    private DateTime? TryGetCameraTimestampUtc()
    {
        if (_cameraAccess == null || _timestampProperty == null)
        {
            return null;
        }

        object value = _timestampProperty.GetValue(_cameraAccess);
        if (value is DateTime dateTime)
        {
            return dateTime.Kind == DateTimeKind.Unspecified
                ? DateTime.SpecifyKind(dateTime, DateTimeKind.Utc)
                : dateTime.ToUniversalTime();
        }

        return null;
    }

    private static T ReadStructField<T>(object boxedStruct, Type structType, string fieldName)
    {
        FieldInfo field = structType.GetField(fieldName, BindingFlags.Instance | BindingFlags.Public);
        if (field == null)
        {
            return default;
        }

        object value = field.GetValue(boxedStruct);
        return value is T typed ? typed : default;
    }

    private void LogHUD(string msg)
    {
        if (logToHUD && LogManager.Instance != null)
        {
            LogManager.Instance.Log(hudLogSource, msg);
        }
    }

    private void EnsureCpuReadbackTargets(int width, int height)
    {
        if (_cpuReadbackTarget == null || _cpuReadbackTarget.width != width || _cpuReadbackTarget.height != height)
        {
            if (_cpuReadbackTarget != null)
            {
                _cpuReadbackTarget.Release();
                Destroy(_cpuReadbackTarget);
            }

            _cpuReadbackTarget = new RenderTexture(width, height, 0, RenderTextureFormat.ARGB32)
            {
                useMipMap = false,
                autoGenerateMips = false,
            };
            _cpuReadbackTarget.Create();
        }

        if (_cpuReadbackTexture == null || _cpuReadbackTexture.width != width || _cpuReadbackTexture.height != height)
        {
            if (_cpuReadbackTexture != null)
            {
                Destroy(_cpuReadbackTexture);
            }

            _cpuReadbackTexture = new Texture2D(width, height, TextureFormat.RGB24, false, false);
        }
    }

    private void EnsureCpuReadbackRgbBuffer(int requiredBytes)
    {
        if (_cpuReadbackRgbBytes == null || _cpuReadbackRgbBytes.Length != requiredBytes)
        {
            _cpuReadbackRgbBytes = new byte[requiredBytes];
        }
    }

    private void OnDestroy()
    {
        if (_cpuReadbackTexture != null)
        {
            Destroy(_cpuReadbackTexture);
            _cpuReadbackTexture = null;
        }

        _cpuReadbackRgbBytes = null;

        if (_cpuReadbackTarget != null)
        {
            _cpuReadbackTarget.Release();
            Destroy(_cpuReadbackTarget);
            _cpuReadbackTarget = null;
        }
    }
}
