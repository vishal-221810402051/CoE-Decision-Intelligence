package com.coe.mobile.ui.screens.recorder

import android.content.Context
import androidx.lifecycle.ViewModel
import androidx.lifecycle.viewModelScope
import com.coe.mobile.data.api.RetrofitInstance
import kotlinx.coroutines.Job
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.flow.update
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import okhttp3.MediaType.Companion.toMediaType
import okhttp3.MultipartBody
import okhttp3.RequestBody.Companion.asRequestBody
import okhttp3.RequestBody.Companion.toRequestBody
import org.json.JSONObject
import java.io.File
import java.io.IOException
import java.time.Instant

class RecorderViewModel : ViewModel() {

    private val _uiState = MutableStateFlow(RecorderUiState())
    val uiState: StateFlow<RecorderUiState> = _uiState.asStateFlow()

    private val mediaRecorderHelper = MediaRecorderHelper()
    private var timerJob: Job? = null

    fun startRecording(context: Context): String? {
        if (_uiState.value.uploadStatus == UploadStatus.UPLOADING) {
            return "Upload in progress. Please wait."
        }

        val result = mediaRecorderHelper.startRecording(context)
        return result.fold(
            onSuccess = {
                _uiState.update { state ->
                    state.copy(
                        isRecording = true,
                        elapsedTime = 0,
                        isReadyToSend = false,
                        audioFilePath = null,
                        uploadStatus = UploadStatus.IDLE,
                        meetingId = null,
                        errorMessage = null
                    )
                }
                startTimer()
                null
            },
            onFailure = { error ->
                _uiState.update { state ->
                    state.copy(
                        isRecording = false,
                        isReadyToSend = false,
                        audioFilePath = null
                    )
                }
                error.message ?: "Recorder failed to start."
            }
        )
    }

    fun stopRecording(): String? {
        stopTimer()
        val result = mediaRecorderHelper.stopRecording()
        return result.fold(
            onSuccess = { filePath ->
                _uiState.update { state ->
                    state.copy(
                        isRecording = false,
                        isReadyToSend = true,
                        audioFilePath = filePath,
                        uploadStatus = UploadStatus.READY,
                        meetingId = null,
                        errorMessage = null
                    )
                }
                null
            },
            onFailure = { error ->
                _uiState.update { state ->
                    state.copy(
                        isRecording = false,
                        isReadyToSend = false,
                        audioFilePath = null,
                        uploadStatus = UploadStatus.IDLE
                    )
                }
                error.message ?: "Recorder failed to stop."
            }
        )
    }

    fun uploadAudio() {
        if (_uiState.value.isRecording) {
            setUploadError("Stop recording before upload.")
            return
        }

        val path = _uiState.value.audioFilePath
        if (path.isNullOrBlank()) {
            setUploadError("No recorded audio file found.")
            return
        }

        val file = File(path)
        if (!file.exists() || !file.isFile) {
            setUploadError("Recorded file is missing.")
            return
        }

        _uiState.update { state ->
            state.copy(
                uploadStatus = UploadStatus.UPLOADING,
                errorMessage = null,
                meetingId = null
            )
        }

        viewModelScope.launch {
            try {
                val response = withContext(Dispatchers.IO) {
                    val audioRequestBody = file.asRequestBody("audio/mp4".toMediaType())
                    val audioPart = MultipartBody.Part.createFormData(
                        "audio_file",
                        file.name,
                        audioRequestBody
                    )
                    val recordedAtBody = Instant.now()
                        .toString()
                        .toRequestBody("text/plain".toMediaType())
                    RetrofitInstance.apiService.uploadAudio(
                        audioFile = audioPart,
                        recordedAt = recordedAtBody
                    )
                }

                if (response.isSuccessful) {
                    val meetingId = response.body()?.meetingId
                    if (meetingId.isNullOrBlank()) {
                        setUploadError("Upload succeeded but meeting_id was missing.")
                    } else {
                        _uiState.update { state ->
                            state.copy(
                                uploadStatus = UploadStatus.SUCCESS,
                                meetingId = meetingId,
                                errorMessage = null
                            )
                        }
                    }
                } else {
                    val rawError = response.errorBody()?.string()
                    val message = parseServerErrorMessage(rawError)
                        ?: "Upload failed (${response.code()})."
                    setUploadError(message)
                }
            } catch (_: IOException) {
                setUploadError("Unable to reach laptop. Check network and server URL.")
            } catch (error: Exception) {
                setUploadError(error.message ?: "Upload failed.")
            }
        }
    }

    private fun setUploadError(message: String) {
        _uiState.update { state ->
            state.copy(
                uploadStatus = UploadStatus.ERROR,
                errorMessage = message
            )
        }
    }

    private fun parseServerErrorMessage(rawError: String?): String? {
        if (rawError.isNullOrBlank()) return null
        return runCatching {
            val json = JSONObject(rawError)
            json.optString("message").takeIf { it.isNotBlank() }
        }.getOrNull()
    }

    private fun startTimer() {
        stopTimer()
        timerJob = viewModelScope.launch {
            while (isActive) {
                delay(1000)
                _uiState.update { state ->
                    if (state.isRecording) {
                        state.copy(elapsedTime = state.elapsedTime + 1)
                    } else {
                        state
                    }
                }
            }
        }
    }

    private fun stopTimer() {
        timerJob?.cancel()
        timerJob = null
    }

    override fun onCleared() {
        stopTimer()
        mediaRecorderHelper.cancelRecording()
        super.onCleared()
    }
}
