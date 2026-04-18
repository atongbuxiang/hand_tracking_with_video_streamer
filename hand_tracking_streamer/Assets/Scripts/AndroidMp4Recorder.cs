using System;
using UnityEngine;

public class AndroidMp4Recorder : IDisposable
{
    private AndroidJavaObject _encoder;
    private bool _started;
    private int _width;
    private int _height;
    private int _fps;

    public bool IsStarted => _started;

    public bool StartRecording(string outputPath, int width, int height, int fps, out string error)
    {
        error = string.Empty;
        if (_started)
        {
            error = "Android MP4 recorder is already running.";
            return false;
        }

#if UNITY_ANDROID && !UNITY_EDITOR
        try
        {
            using (AndroidJavaClass encoderClass = new AndroidJavaClass("com.codex.localsave.QuestMp4Encoder"))
            {
                _encoder = encoderClass.CallStatic<AndroidJavaObject>("create");
            }

            _encoder.Call("start", outputPath, width, height, Mathf.Max(1, fps));
            _width = width;
            _height = height;
            _fps = Mathf.Max(1, fps);
            _started = true;
            return true;
        }
        catch (Exception ex)
        {
            error = $"Android MP4 start failed: {ex.Message}";
            DisposeEncoder();
            return false;
        }
#else
        error = "Android MP4 recorder only runs on Quest/Android builds.";
        return false;
#endif
    }

    public bool AddFrame(Color32[] pixels, long timestampNs, out string error)
    {
        error = string.Empty;
        if (!_started || _encoder == null)
        {
            error = "Android MP4 recorder is not started.";
            return false;
        }

        if (pixels == null || pixels.Length != _width * _height)
        {
            error = $"Unexpected pixel buffer size. Expected {_width * _height}, got {pixels?.Length ?? 0}.";
            return false;
        }

#if UNITY_ANDROID && !UNITY_EDITOR
        try
        {
            byte[] rgba = new byte[pixels.Length * 4];
            for (int i = 0; i < pixels.Length; i++)
            {
                int offset = i * 4;
                Color32 pixel = pixels[i];
                rgba[offset + 0] = pixel.r;
                rgba[offset + 1] = pixel.g;
                rgba[offset + 2] = pixel.b;
                rgba[offset + 3] = pixel.a;
            }

            _encoder.Call("encodeRgbaFrame", rgba, timestampNs);
            return true;
        }
        catch (Exception ex)
        {
            error = $"Android MP4 frame encode failed: {ex.Message}";
            return false;
        }
#else
        error = "Android MP4 recorder only runs on Quest/Android builds.";
        return false;
#endif
    }

    public void StopRecording()
    {
        if (!_started)
        {
            return;
        }

#if UNITY_ANDROID && !UNITY_EDITOR
        try
        {
            _encoder?.Call("stop");
        }
        catch (Exception ex)
        {
            Debug.LogError($"[LocalSave] Android MP4 stop failed: {ex.Message}");
        }
#endif
        DisposeEncoder();
        _started = false;
    }

    private void DisposeEncoder()
    {
        if (_encoder != null)
        {
            try
            {
                _encoder.Dispose();
            }
            catch
            {
                // ignored
            }
            _encoder = null;
        }
    }

    public void Dispose()
    {
        StopRecording();
    }
}
