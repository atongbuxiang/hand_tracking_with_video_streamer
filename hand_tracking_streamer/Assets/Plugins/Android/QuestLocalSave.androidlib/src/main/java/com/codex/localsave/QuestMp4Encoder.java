package com.codex.localsave;

import android.media.MediaCodec;
import android.media.MediaCodecInfo;
import android.media.MediaFormat;
import android.media.MediaMuxer;

import java.io.IOException;
import java.nio.ByteBuffer;

public final class QuestMp4Encoder {
    private static final String MIME_TYPE = "video/avc";
    private static final int COLOR_FORMAT = MediaCodecInfo.CodecCapabilities.COLOR_FormatYUV420SemiPlanar;
    private static final int BITRATE_FACTOR = 4;
    private static final int TIMEOUT_US = 10000;

    private MediaCodec codec;
    private MediaMuxer muxer;
    private MediaCodec.BufferInfo bufferInfo;
    private int trackIndex = -1;
    private boolean muxerStarted = false;
    private int width;
    private int height;

    public static QuestMp4Encoder create() {
        return new QuestMp4Encoder();
    }

    public void start(String outputPath, int width, int height, int fps) throws IOException {
        stop();

        this.width = width;
        this.height = height;
        bufferInfo = new MediaCodec.BufferInfo();

        MediaFormat format = MediaFormat.createVideoFormat(MIME_TYPE, width, height);
        format.setInteger(MediaFormat.KEY_COLOR_FORMAT, COLOR_FORMAT);
        format.setInteger(MediaFormat.KEY_BIT_RATE, Math.max(width * height * fps * BITRATE_FACTOR, 1_000_000));
        format.setInteger(MediaFormat.KEY_FRAME_RATE, Math.max(1, fps));
        format.setInteger(MediaFormat.KEY_I_FRAME_INTERVAL, 1);

        codec = MediaCodec.createEncoderByType(MIME_TYPE);
        codec.configure(format, null, null, MediaCodec.CONFIGURE_FLAG_ENCODE);
        codec.start();

        muxer = new MediaMuxer(outputPath, MediaMuxer.OutputFormat.MUXER_OUTPUT_MPEG_4);
        trackIndex = -1;
        muxerStarted = false;
    }

    public void encodeRgbaFrame(byte[] rgba, long presentationTimeNs) {
        if (codec == null) {
            throw new IllegalStateException("Encoder is not started.");
        }

        byte[] yuv = rgbaToNv12(rgba, width, height);
        int inputIndex = codec.dequeueInputBuffer(TIMEOUT_US);
        if (inputIndex >= 0) {
            ByteBuffer inputBuffer = codec.getInputBuffer(inputIndex);
            if (inputBuffer != null) {
                inputBuffer.clear();
                inputBuffer.put(yuv);
                codec.queueInputBuffer(inputIndex, 0, yuv.length, presentationTimeNs / 1000L, 0);
            }
        }

        drain(false);
    }

    public void stop() {
        if (codec == null && muxer == null) {
            return;
        }

        try {
            if (codec != null) {
                int inputIndex = codec.dequeueInputBuffer(TIMEOUT_US);
                if (inputIndex >= 0) {
                    codec.queueInputBuffer(inputIndex, 0, 0, 0, MediaCodec.BUFFER_FLAG_END_OF_STREAM);
                }
                drain(true);
            }
        } catch (Exception ignored) {
        }

        try {
            if (codec != null) {
                codec.stop();
            }
        } catch (Exception ignored) {
        }

        try {
            if (codec != null) {
                codec.release();
            }
        } catch (Exception ignored) {
        }
        codec = null;

        try {
            if (muxer != null && muxerStarted) {
                muxer.stop();
            }
        } catch (Exception ignored) {
        }

        try {
            if (muxer != null) {
                muxer.release();
            }
        } catch (Exception ignored) {
        }
        muxer = null;
        muxerStarted = false;
        trackIndex = -1;
    }

    private void drain(boolean endOfStream) {
        while (codec != null) {
            int outputIndex = codec.dequeueOutputBuffer(bufferInfo, TIMEOUT_US);
            if (outputIndex == MediaCodec.INFO_TRY_AGAIN_LATER) {
                if (!endOfStream) {
                    break;
                }
            } else if (outputIndex == MediaCodec.INFO_OUTPUT_FORMAT_CHANGED) {
                if (muxerStarted) {
                    throw new IllegalStateException("Output format changed twice.");
                }
                trackIndex = muxer.addTrack(codec.getOutputFormat());
                muxer.start();
                muxerStarted = true;
            } else if (outputIndex >= 0) {
                ByteBuffer outputBuffer = codec.getOutputBuffer(outputIndex);
                if (outputBuffer != null && bufferInfo.size > 0 && muxerStarted) {
                    outputBuffer.position(bufferInfo.offset);
                    outputBuffer.limit(bufferInfo.offset + bufferInfo.size);
                    muxer.writeSampleData(trackIndex, outputBuffer, bufferInfo);
                }
                codec.releaseOutputBuffer(outputIndex, false);

                if ((bufferInfo.flags & MediaCodec.BUFFER_FLAG_END_OF_STREAM) != 0) {
                    break;
                }
            }
        }
    }

    private static byte[] rgbaToNv12(byte[] rgba, int width, int height) {
        int frameSize = width * height;
        byte[] nv12 = new byte[frameSize + frameSize / 2];
        int yIndex = 0;
        int uvIndex = frameSize;

        for (int j = 0; j < height; j++) {
            for (int i = 0; i < width; i++) {
                int rgbaIndex = (j * width + i) * 4;
                int r = rgba[rgbaIndex] & 0xFF;
                int g = rgba[rgbaIndex + 1] & 0xFF;
                int b = rgba[rgbaIndex + 2] & 0xFF;

                int y = clamp(((66 * r + 129 * g + 25 * b + 128) >> 8) + 16);
                int u = clamp(((-38 * r - 74 * g + 112 * b + 128) >> 8) + 128);
                int v = clamp(((112 * r - 94 * g - 18 * b + 128) >> 8) + 128);

                nv12[yIndex++] = (byte) y;
                if ((j % 2 == 0) && (i % 2 == 0)) {
                    nv12[uvIndex++] = (byte) u;
                    nv12[uvIndex++] = (byte) v;
                }
            }
        }

        return nv12;
    }

    private static int clamp(int value) {
        return Math.max(0, Math.min(255, value));
    }
}
