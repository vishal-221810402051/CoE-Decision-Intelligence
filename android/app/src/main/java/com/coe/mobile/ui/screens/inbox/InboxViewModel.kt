package com.coe.mobile.ui.screens.inbox

import androidx.lifecycle.ViewModel
import androidx.lifecycle.viewModelScope
import com.coe.mobile.data.api.RetrofitInstance
import com.coe.mobile.data.model.DecisionRequest
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.flow.update
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import org.json.JSONObject
import java.io.IOException

class InboxViewModel : ViewModel() {

    private val _uiState = MutableStateFlow(InboxUiState())
    val uiState: StateFlow<InboxUiState> = _uiState.asStateFlow()

    fun loadPendingInbox() {
        _uiState.update { state ->
            state.copy(
                isLoading = true,
                errorMessage = null
            )
        }

        viewModelScope.launch {
            try {
                val response = withContext(Dispatchers.IO) {
                    RetrofitInstance.apiService.getPendingInbox()
                }

                if (response.isSuccessful) {
                    val items = response.body()?.items.orEmpty()
                    _uiState.update { state ->
                        state.copy(
                            isLoading = false,
                            items = items,
                            errorMessage = null
                        )
                    }
                } else {
                    val message = parseServerErrorMessage(response.errorBody()?.string())
                        ?: "Failed to load inbox (${response.code()})."
                    _uiState.update { state ->
                        state.copy(
                            isLoading = false,
                            errorMessage = message
                        )
                    }
                }
            } catch (_: IOException) {
                _uiState.update { state ->
                    state.copy(
                        isLoading = false,
                        errorMessage = "Unable to reach laptop. Check network and server URL."
                    )
                }
            } catch (error: Exception) {
                _uiState.update { state ->
                    state.copy(
                        isLoading = false,
                        errorMessage = error.message ?: "Failed to load inbox."
                    )
                }
            }
        }
    }

    fun approveItem(meetingId: String, candidateId: String) {
        submitDecision(meetingId, candidateId, "approved")
    }

    fun rejectItem(meetingId: String, candidateId: String) {
        submitDecision(meetingId, candidateId, "rejected")
    }

    fun consumeLastActionMessage() {
        _uiState.update { state ->
            state.copy(lastActionMessage = null)
        }
    }

    private fun submitDecision(
        meetingId: String,
        candidateId: String,
        decision: String
    ) {
        _uiState.update { state ->
            state.copy(
                actionInFlightCandidateId = candidateId,
                errorMessage = null,
                lastActionMessage = null
            )
        }

        viewModelScope.launch {
            try {
                val response = withContext(Dispatchers.IO) {
                    RetrofitInstance.apiService.submitDecision(
                        request = DecisionRequest(
                            meetingId = meetingId,
                            candidateId = candidateId,
                            decision = decision,
                            actor = "android_user"
                        )
                    )
                }

                if (response.isSuccessful) {
                    val actionLabel = if (decision == "approved") "Approved" else "Rejected"
                    val serverMessage = response.body()?.message
                    _uiState.update { state ->
                        state.copy(
                            actionInFlightCandidateId = null,
                            items = state.items.filterNot { item ->
                                item.meetingId == meetingId && item.candidateId == candidateId
                            },
                            lastActionMessage = serverMessage ?: "$actionLabel successfully"
                        )
                    }
                } else {
                    val message = parseServerErrorMessage(response.errorBody()?.string())
                        ?: "Decision failed (${response.code()})."
                    _uiState.update { state ->
                        state.copy(
                            actionInFlightCandidateId = null,
                            errorMessage = message,
                            lastActionMessage = message
                        )
                    }
                }
            } catch (_: IOException) {
                val message = "Unable to reach laptop. Check network and server URL."
                _uiState.update { state ->
                    state.copy(
                        actionInFlightCandidateId = null,
                        errorMessage = message,
                        lastActionMessage = message
                    )
                }
            } catch (error: Exception) {
                val message = error.message ?: "Decision request failed."
                _uiState.update { state ->
                    state.copy(
                        actionInFlightCandidateId = null,
                        errorMessage = message,
                        lastActionMessage = message
                    )
                }
            }
        }
    }

    private fun parseServerErrorMessage(rawError: String?): String? {
        if (rawError.isNullOrBlank()) return null
        return runCatching {
            val json = JSONObject(rawError)
            json.optString("message").takeIf { it.isNotBlank() }
        }.getOrNull()
    }
}
