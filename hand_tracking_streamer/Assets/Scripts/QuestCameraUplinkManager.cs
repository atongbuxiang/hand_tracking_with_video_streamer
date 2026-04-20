using System;
using System.Threading.Tasks;
using UnityEngine;

public class QuestCameraUplinkManager : MonoBehaviour
{
    private const int MaxLocalSaveFps = 15;

    public enum SessionState
    {
        Idle,
        CameraInitializing,
        Connecting,
        Streaming,
        Stopping,
    }

    [SerializeField] private QuestCameraCapture cameraCapture;
    [SerializeField] private VideoStatsOverlay statsOverlay;
    [SerializeField] private string logSource = "Left";
    [SerializeField] private int maxFps = 15;
    [SerializeField] private string datasetNamePrefix = "quest_local_save";

    private SessionState _state = SessionState.Idle;
    private bool _isStopping;
    private float _sendIntervalSeconds = 1f / 15f;
    private float _sendTimer;
    private int _framesSaved;
    private float _fpsWindowStart;
    private QuestLocalDatasetRecorder _datasetRecorder;
    private AndroidMp4Recorder _videoRecorder;

    public SessionState CurrentState => _state;
    public QuestLocalDatasetRecorder DatasetRecorder => _datasetRecorder;

    public async Task<bool> StartVideoSession(
        string signalingHost,
        int signalingPort,
        string preset,
        int bitrateKbps,
        bool showDebugStats
    )
    {
        if (_state != SessionState.Idle)
        {
            return false;
        }

        if (cameraCapture == null)
        {
            FailFatal("Camera capture component missing.");
            return false;
        }

        _datasetRecorder = GetComponent<QuestLocalDatasetRecorder>();
        if (_datasetRecorder == null)
        {
            _datasetRecorder = gameObject.AddComponent<QuestLocalDatasetRecorder>();
        }

        _videoRecorder = new AndroidMp4Recorder();
        _state = SessionState.CameraInitializing;
        _sendTimer = 0f;
        _framesSaved = 0;
        _fpsWindowStart = Time.realtimeSinceStartup;

        statsOverlay?.SetVisible(showDebugStats || (AppManager.Instance != null && AppManager.Instance.ShowDebugInfo));
        statsOverlay?.SetPreset(preset);
        statsOverlay?.SetSignalingState("local_record_init");
        statsOverlay?.SetPeerState("idle");
        statsOverlay?.SetError(string.Empty);

        if (AppManager.Instance != null)
        {
            cameraCapture.SetRequestedResolution(AppManager.Instance.RequestedCameraResolution);
            maxFps = Mathf.Clamp(AppManager.Instance.RequestedCameraFps, 1, MaxLocalSaveFps);
        }

        _sendIntervalSeconds = 1f / Mathf.Max(1, maxFps);

        if (!cameraCapture.EnsureInitialized())
        {
            FailFatal("Quest camera access is not available. Check MRUK/PCA setup.");
            return false;
        }

        Texture sourceTexture = await WaitForCameraTextureAsync(3000);
        if (sourceTexture == null)
        {
            FailFatal("Quest camera texture did not become ready within 3 seconds.");
            return false;
        }

        _state = SessionState.Connecting;
        statsOverlay?.SetSignalingState("preparing_storage");

        string datasetName = $"{datasetNamePrefix}_{DateTime.UtcNow:yyyyMMdd_HHmmss}";
        if (!_datasetRecorder.BeginRecording(datasetName, maxFps, _videoRecorder, out string datasetError))
        {
            FailFatal(datasetError);
            return false;
        }

        string videoPath = _datasetRecorder.VideoOutputPath;
        if (string.IsNullOrWhiteSpace(videoPath))
        {
            FailFatal("Local video path was not created.");
            return false;
        }

        int width = Mathf.Max(1, sourceTexture.width);
        int height = Mathf.Max(1, sourceTexture.height);
        if (!_videoRecorder.StartRecording(videoPath, width, height, maxFps, out string videoError))
        {
            FailFatal(videoError);
            return false;
        }

        if (cameraCapture.TryBuildCalibrationMetadata(out QuestCameraCalibrationMetadata calibration, out string calibrationError))
        {
            _datasetRecorder.RecordCameraCalibration(calibration);
            LogInfo("camera calibration captured for local save");
        }
        else if (!string.IsNullOrWhiteSpace(calibrationError))
        {
            LogInfo($"camera calibration unavailable: {calibrationError}");
        }

        _state = SessionState.Streaming;
        statsOverlay?.SetSignalingState("recording_local");
        statsOverlay?.SetPeerState("streaming");
        UpdateStatsOverlay(0f);
        LogInfo($"local save started path={_datasetRecorder.DatasetDirectory}");
        return true;
    }

    public Task StopVideoSession(string reason)
    {
        if (_isStopping)
        {
            return Task.CompletedTask;
        }

        _isStopping = true;
        _state = SessionState.Stopping;
        statsOverlay?.SetSignalingState("stopping");
        LogInfo($"local save stopping reason={reason}");

        try
        {
            _videoRecorder?.StopRecording();
        }
        finally
        {
            _videoRecorder?.Dispose();
            _videoRecorder = null;
        }

        if (_datasetRecorder != null)
        {
            _datasetRecorder.FinalizeAlignedFrames();
            _datasetRecorder.EndRecording();
            LogInfo($"local save finalized dir={_datasetRecorder.DatasetDirectory}");
        }

        statsOverlay?.SetSignalingState("idle");
        statsOverlay?.SetPeerState("idle");
        _state = SessionState.Idle;
        _isStopping = false;
        return Task.CompletedTask;
    }

    private void Update()
    {
        if (_state != SessionState.Streaming || cameraCapture == null || _datasetRecorder == null || _videoRecorder == null)
        {
            return;
        }

        _sendTimer += Time.deltaTime;
        if (_sendTimer < _sendIntervalSeconds)
        {
            return;
        }
        _sendTimer = 0f;

        if (!cameraCapture.TryReadRgbFrame(
                out byte[] rgbBytes,
                out uint frameId,
                out ulong timestampNs,
                out int width,
                out int height,
                out string error))
        {
            LogDebug($"camera frame skipped: {error}");
            return;
        }

        if (cameraCapture.TryBuildFramePoseMetadata(frameId, timestampNs, out QuestCameraFramePoseMetadata pose, out string poseError))
        {
            _datasetRecorder.RecordCameraPose(pose);
        }
        else if (!string.IsNullOrWhiteSpace(poseError))
        {
            LogDebug($"camera pose unavailable: {poseError}");
        }

        if (!_videoRecorder.AddFrame(rgbBytes, unchecked((long)timestampNs), out string encodeError))
        {
            FailFatal(encodeError);
            return;
        }

        _datasetRecorder.RecordCameraFrame(
            unchecked((int)frameId),
            unchecked((long)timestampNs),
            width,
            height,
            unchecked((long)QuestStreamClock.GetMonotonicTimestampNs()));

        _framesSaved++;
        float elapsed = Mathf.Max(Time.realtimeSinceStartup - _fpsWindowStart, 0.001f);
        float fps = _framesSaved / elapsed;
        UpdateStatsOverlay(fps);
    }

    private async void FailFatal(string reason)
    {
        if (string.IsNullOrWhiteSpace(reason))
        {
            reason = "Unknown local save failure";
        }

        statsOverlay?.SetError(reason);
        LogInfo($"local save fatal: {reason}");

        if (_state != SessionState.Idle)
        {
            await StopVideoSession("fatal_error");
        }

        if (AppManager.Instance != null && AppManager.Instance.isStreaming)
        {
            AppManager.Instance.HandleDisconnection($"Local save failure: {reason}");
        }
    }

    private void LogDebug(string msg)
    {
        if (LogManager.Instance == null)
        {
            return;
        }

        bool shouldLog = AppManager.Instance != null && AppManager.Instance.ShowDebugInfo;
        if (!shouldLog)
        {
            return;
        }

        LogManager.Instance.Log(logSource, $"[CameraDebug] {msg}");
    }

    private void LogInfo(string msg)
    {
        if (LogManager.Instance == null)
        {
            return;
        }

        LogManager.Instance.Log(logSource, $"[LocalSave] {msg}");
    }

    private void UpdateStatsOverlay(float fps)
    {
        float approxBitrate = fps <= 0f
            ? 0f
            : (cameraCapture.CurrentResolution.x * cameraCapture.CurrentResolution.y * fps * 0.08f);
        statsOverlay?.SetStats(fps, approxBitrate, 0, -1f);
    }

    private async Task<Texture> WaitForCameraTextureAsync(int timeoutMs)
    {
        float deadline = Time.realtimeSinceStartup + (timeoutMs / 1000f);

        while (_state == SessionState.CameraInitializing && Time.realtimeSinceStartup < deadline)
        {
            Texture latestTexture = cameraCapture != null ? cameraCapture.LatestTexture : null;
            if (latestTexture != null && latestTexture.width > 0 && latestTexture.height > 0)
            {
                return latestTexture;
            }

            await Task.Yield();
        }

        return cameraCapture != null ? cameraCapture.LatestTexture : null;
    }
}
