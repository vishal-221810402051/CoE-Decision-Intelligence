package com.coe.mobile.ui.screens.recorder

import android.content.Context
import android.media.MediaRecorder
import android.os.Build
import java.io.File
import java.io.IOException

class MediaRecorderHelper {

    private var mediaRecorder: MediaRecorder? = null
    private var outputFilePath: String? = null

    fun startRecording(context: Context): Result<String> {
        if (mediaRecorder != null) {
            return Result.failure(IllegalStateException("Recording is already in progress."))
        }

        val fileName = "coe_recording_${System.currentTimeMillis()}.m4a"
        val outputFile = File(context.cacheDir, fileName)
        outputFilePath = outputFile.absolutePath

        return try {
            val recorder = createMediaRecorder(context)
            recorder.setAudioSource(MediaRecorder.AudioSource.MIC)
            recorder.setOutputFormat(MediaRecorder.OutputFormat.MPEG_4)
            recorder.setAudioEncoder(MediaRecorder.AudioEncoder.AAC)
            recorder.setAudioEncodingBitRate(128000)
            recorder.setAudioSamplingRate(44100)
            recorder.setOutputFile(outputFile.absolutePath)
            recorder.prepare()
            recorder.start()

            mediaRecorder = recorder
            Result.success(outputFile.absolutePath)
        } catch (securityException: SecurityException) {
            release()
            Result.failure(IllegalStateException("Microphone permission denied.", securityException))
        } catch (ioException: IOException) {
            release()
            Result.failure(IllegalStateException("Failed to prepare recorder.", ioException))
        } catch (runtimeException: RuntimeException) {
            release()
            Result.failure(IllegalStateException("Failed to start recorder.", runtimeException))
        }
    }

    fun stopRecording(): Result<String> {
        val recorder = mediaRecorder
            ?: return Result.failure(IllegalStateException("No active recording to stop."))

        val recordedPath = outputFilePath
        return try {
            recorder.stop()
            if (recordedPath.isNullOrBlank()) {
                Result.failure(IllegalStateException("Recording finished but file path is missing."))
            } else {
                Result.success(recordedPath)
            }
        } catch (runtimeException: RuntimeException) {
            if (!recordedPath.isNullOrBlank()) {
                File(recordedPath).delete()
            }
            Result.failure(IllegalStateException("Failed to stop recorder cleanly.", runtimeException))
        } finally {
            release()
        }
    }

    fun cancelRecording() {
        val activePath = outputFilePath
        release()
        if (!activePath.isNullOrBlank()) {
            File(activePath).delete()
        }
    }

    private fun createMediaRecorder(context: Context): MediaRecorder {
        return if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.S) {
            MediaRecorder(context)
        } else {
            MediaRecorder()
        }
    }

    private fun release() {
        mediaRecorder?.let { recorder ->
            runCatching { recorder.reset() }
            runCatching { recorder.release() }
        }
        mediaRecorder = null
        outputFilePath = null
    }
}
