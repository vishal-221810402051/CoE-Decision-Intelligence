package com.coe.mobile.ui.screens.inbox

import com.coe.mobile.data.model.InboxItem

data class InboxUiState(
    val isLoading: Boolean = false,
    val items: List<InboxItem> = emptyList(),
    val errorMessage: String? = null,
    val actionInFlightCandidateId: String? = null,
    val lastActionMessage: String? = null
)
