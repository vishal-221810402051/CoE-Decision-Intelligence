package com.coe.mobile.ui.screens.recorder

import android.Manifest
import android.content.pm.PackageManager
import android.widget.Toast
import androidx.activity.compose.rememberLauncherForActivityResult
import androidx.activity.result.contract.ActivityResultContracts
import androidx.compose.animation.AnimatedVisibility
import androidx.compose.animation.fadeIn
import androidx.compose.animation.fadeOut
import androidx.compose.animation.core.RepeatMode
import androidx.compose.animation.core.animateFloat
import androidx.compose.animation.core.infiniteRepeatable
import androidx.compose.animation.core.rememberInfiniteTransition
import androidx.compose.animation.core.tween
import androidx.compose.foundation.background
import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Box
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.fillMaxSize
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.height
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.layout.size
import androidx.compose.foundation.shape.CircleShape
import androidx.compose.material3.Button
import androidx.compose.material3.ButtonDefaults
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.Text
import androidx.compose.runtime.Composable
import androidx.compose.runtime.collectAsState
import androidx.compose.runtime.getValue
import androidx.compose.runtime.LaunchedEffect
import androidx.compose.ui.draw.scale
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.platform.LocalContext
import androidx.compose.ui.text.font.FontFamily
import androidx.compose.ui.text.font.FontWeight
import androidx.compose.ui.unit.dp
import androidx.compose.ui.unit.sp
import androidx.core.content.ContextCompat
import androidx.lifecycle.viewmodel.compose.viewModel
import com.coe.mobile.ui.components.StatusChip
import com.coe.mobile.ui.components.StatusChipVariant

@Composable
fun RecorderScreen(
    modifier: Modifier = Modifier,
    recorderViewModel: RecorderViewModel = viewModel()
) {
    val context = LocalContext.current
    val uiState by recorderViewModel.uiState.collectAsState()
    val isUploading = uiState.uploadStatus == UploadStatus.UPLOADING
    val statusLabel = when {
        isUploading -> "Uploading"
        uiState.isRecording -> "Recording"
        uiState.isReadyToSend -> "Ready"
        else -> "Idle"
    }
    val statusVariant = when {
        isUploading -> StatusChipVariant.Warning
        uiState.isRecording -> StatusChipVariant.Error
        uiState.isReadyToSend -> StatusChipVariant.Success
        else -> StatusChipVariant.Neutral
    }

    LaunchedEffect(uiState.uploadStatus, uiState.errorMessage) {
        when (uiState.uploadStatus) {
            UploadStatus.SUCCESS -> {
                Toast.makeText(context, "Uploaded successfully", Toast.LENGTH_SHORT).show()
            }

            UploadStatus.ERROR -> {
                val message = uiState.errorMessage ?: "Upload failed."
                Toast.makeText(context, message, Toast.LENGTH_SHORT).show()
            }

            else -> Unit
        }
    }

    val permissionLauncher = rememberLauncherForActivityResult(
        contract = ActivityResultContracts.RequestPermission()
    ) { granted ->
        if (granted) {
            val errorMessage = recorderViewModel.startRecording(context)
            if (errorMessage != null) {
                Toast.makeText(context, errorMessage, Toast.LENGTH_SHORT).show()
            }
        } else {
            Toast.makeText(context, "Microphone permission denied.", Toast.LENGTH_SHORT).show()
        }
    }

    val pulseTransition = rememberInfiniteTransition(label = "recordPulse")
    val pulseScale by pulseTransition.animateFloat(
        initialValue = 1f,
        targetValue = 1.07f,
        animationSpec = infiniteRepeatable(
            animation = tween(durationMillis = 900),
            repeatMode = RepeatMode.Reverse
        ),
        label = "recordPulseScale"
    )

    Column(
        modifier = modifier
            .fillMaxSize()
            .padding(horizontal = 24.dp, vertical = 20.dp),
        verticalArrangement = Arrangement.Center,
        horizontalAlignment = Alignment.CenterHorizontally
    ) {
        Text(
            text = "Capture Console",
            style = MaterialTheme.typography.labelSmall,
            color = MaterialTheme.colorScheme.onSurfaceVariant
        )
        Text(
            text = formatElapsedTime(uiState.elapsedTime),
            fontFamily = FontFamily.Monospace,
            fontSize = 54.sp,
            fontWeight = FontWeight.Bold
        )
        Box(
            modifier = Modifier.padding(top = 28.dp, bottom = 20.dp),
            contentAlignment = Alignment.Center
        ) {
            if (uiState.isRecording) {
                Box(
                    modifier = Modifier
                        .size(196.dp)
                        .scale(pulseScale)
                        .background(
                            color = MaterialTheme.colorScheme.error.copy(alpha = 0.15f),
                            shape = CircleShape
                        )
                )
            }

            Button(
                onClick = {
                    if (uiState.isRecording) {
                        val errorMessage = recorderViewModel.stopRecording()
                        if (errorMessage != null) {
                            Toast.makeText(context, errorMessage, Toast.LENGTH_SHORT).show()
                        }
                    } else {
                        if (hasMicrophonePermission(context)) {
                            val errorMessage = recorderViewModel.startRecording(context)
                            if (errorMessage != null) {
                                Toast.makeText(context, errorMessage, Toast.LENGTH_SHORT).show()
                            }
                        } else {
                            permissionLauncher.launch(Manifest.permission.RECORD_AUDIO)
                        }
                    }
                },
                enabled = !isUploading,
                colors = ButtonDefaults.buttonColors(
                    containerColor = if (uiState.isRecording) {
                        MaterialTheme.colorScheme.error
                    } else {
                        MaterialTheme.colorScheme.primary
                    }
                ),
                modifier = Modifier
                    .size(176.dp)
                    .scale(if (uiState.isRecording) 1f else 0.98f)
            ) {
                Text(
                    text = if (uiState.isRecording) "Stop" else "Record",
                    fontSize = 22.sp
                )
            }
        }

        StatusChip(
            label = statusLabel,
            variant = statusVariant
        )

        AnimatedVisibility(
            visible = uiState.isReadyToSend,
            enter = fadeIn(animationSpec = tween(160)),
            exit = fadeOut(animationSpec = tween(120))
        ) {
            Button(
                onClick = { recorderViewModel.uploadAudio() },
                enabled = !isUploading,
                modifier = Modifier
                    .fillMaxWidth()
                    .height(52.dp)
                    .padding(top = 18.dp)
            ) {
                Text(text = "Send Recording")
            }
        }

        AnimatedVisibility(
            visible = isUploading,
            enter = fadeIn(animationSpec = tween(160)),
            exit = fadeOut(animationSpec = tween(120))
        ) {
            Text(
                text = "Uploading to intelligence pipeline...",
                modifier = Modifier.padding(top = 16.dp),
                color = MaterialTheme.colorScheme.onSurfaceVariant
            )
        }
    }
}

private fun formatElapsedTime(totalSeconds: Int): String {
    val minutes = totalSeconds / 60
    val seconds = totalSeconds % 60
    return String.format("%02d:%02d", minutes, seconds)
}

private fun hasMicrophonePermission(context: android.content.Context): Boolean {
    return ContextCompat.checkSelfPermission(
        context,
        Manifest.permission.RECORD_AUDIO
    ) == PackageManager.PERMISSION_GRANTED
}
