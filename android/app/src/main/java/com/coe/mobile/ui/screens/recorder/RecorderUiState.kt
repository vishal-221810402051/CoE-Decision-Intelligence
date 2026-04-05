package com.coe.mobile.ui.screens.recorder

data class RecorderUiState(
    val isRecording: Boolean = false,
    val elapsedTime: Int = 0,
    val isReadyToSend: Boolean = false,
    val audioFilePath: String? = null,
    val uploadStatus: UploadStatus = UploadStatus.IDLE,
    val meetingId: String? = null,
    val errorMessage: String? = null
)
