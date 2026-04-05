package com.coe.mobile.ui.screens.inbox

import android.widget.Toast
import androidx.compose.animation.AnimatedVisibility
import androidx.compose.animation.fadeIn
import androidx.compose.animation.fadeOut
import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.ExperimentalLayoutApi
import androidx.compose.foundation.layout.FlowRow
import androidx.compose.foundation.layout.PaddingValues
import androidx.compose.foundation.layout.Row
import androidx.compose.foundation.layout.fillMaxSize
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.height
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.lazy.LazyColumn
import androidx.compose.foundation.lazy.items
import androidx.compose.material.icons.Icons
import androidx.compose.material.icons.filled.Inbox
import androidx.compose.material.icons.filled.WarningAmber
import androidx.compose.material3.Button
import androidx.compose.material3.CircularProgressIndicator
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.OutlinedButton
import androidx.compose.material3.Text
import androidx.compose.runtime.Composable
import androidx.compose.runtime.LaunchedEffect
import androidx.compose.runtime.collectAsState
import androidx.compose.runtime.getValue
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.platform.LocalContext
import androidx.compose.ui.text.style.TextOverflow
import androidx.compose.ui.unit.dp
import androidx.lifecycle.viewmodel.compose.viewModel
import com.coe.mobile.data.model.InboxItem
import com.coe.mobile.ui.components.AppCard
import com.coe.mobile.ui.components.EmptyState
import com.coe.mobile.ui.components.SectionHeader
import com.coe.mobile.ui.components.StatusChip
import com.coe.mobile.ui.components.StatusChipVariant

@Composable
fun SuggestionsInboxScreen(
    modifier: Modifier = Modifier,
    inboxViewModel: InboxViewModel = viewModel()
) {
    val context = LocalContext.current
    val uiState by inboxViewModel.uiState.collectAsState()

    LaunchedEffect(Unit) {
        inboxViewModel.loadPendingInbox()
    }

    LaunchedEffect(uiState.lastActionMessage) {
        uiState.lastActionMessage?.let { message ->
            Toast.makeText(context, message, Toast.LENGTH_SHORT).show()
            inboxViewModel.consumeLastActionMessage()
        }
    }

    when {
        uiState.isLoading -> {
            Column(
                modifier = modifier
                    .fillMaxSize()
                    .padding(24.dp),
                horizontalAlignment = Alignment.CenterHorizontally,
                verticalArrangement = Arrangement.Center
            ) {
                CircularProgressIndicator()
                Text(
                    text = "Loading pending suggestions...",
                    modifier = Modifier.padding(top = 12.dp),
                    color = MaterialTheme.colorScheme.onSurfaceVariant
                )
            }
        }

        uiState.items.isEmpty() -> {
            if (!uiState.errorMessage.isNullOrBlank()) {
                EmptyState(
                    icon = Icons.Default.WarningAmber,
                    title = "Unable to load inbox",
                    subtitle = uiState.errorMessage,
                    modifier = modifier.padding(horizontal = 24.dp)
                )
            } else {
                EmptyState(
                    icon = Icons.Default.Inbox,
                    title = "No pending suggestions",
                    subtitle = "New approval candidates will appear here.",
                    modifier = modifier
                )
            }
        }

        else -> {
            LazyColumn(
                modifier = modifier.fillMaxSize(),
                contentPadding = PaddingValues(16.dp),
                verticalArrangement = Arrangement.spacedBy(12.dp)
            ) {
                item {
                    SectionHeader(
                        title = "Pending Approvals",
                        subtitle = "Review and route calendar decisions"
                    )
                }

                if (!uiState.errorMessage.isNullOrBlank()) {
                    item {
                        Text(
                            text = uiState.errorMessage ?: "",
                            color = MaterialTheme.colorScheme.error
                        )
                    }
                }

                items(
                    items = uiState.items,
                    key = { item -> "${item.meetingId}:${item.candidateId}" }
                ) { item ->
                    InboxItemCard(
                        item = item,
                        isActionInFlight = uiState.actionInFlightCandidateId == item.candidateId,
                        onApprove = {
                            inboxViewModel.approveItem(item.meetingId, item.candidateId)
                        },
                        onReject = {
                            inboxViewModel.rejectItem(item.meetingId, item.candidateId)
                        }
                    )
                }
            }
        }
    }
}

@OptIn(ExperimentalLayoutApi::class)
@Composable
private fun InboxItemCard(
    item: InboxItem,
    isActionInFlight: Boolean,
    onApprove: () -> Unit,
    onReject: () -> Unit
) {
    AppCard {
        Column(
            modifier = Modifier.fillMaxWidth(),
            verticalArrangement = Arrangement.spacedBy(8.dp)
        ) {
            Text(
                text = item.title.orEmpty().ifBlank { "Untitled suggestion" },
                style = MaterialTheme.typography.titleMedium
            )

            if (!item.summary.isNullOrBlank()) {
                Text(
                    text = item.summary,
                    style = MaterialTheme.typography.bodyMedium,
                    color = MaterialTheme.colorScheme.onSurfaceVariant,
                    maxLines = 3,
                    overflow = TextOverflow.Ellipsis
                )
            }

            val dateTime = "${item.displayDate.orEmpty()} ${item.displayTime.orEmpty()}".trim()
            if (dateTime.isNotBlank()) {
                Text(
                    text = dateTime,
                    style = MaterialTheme.typography.bodyLarge,
                    color = MaterialTheme.colorScheme.primary
                )
            }

            FlowRow(
                horizontalArrangement = Arrangement.spacedBy(8.dp),
                verticalArrangement = Arrangement.spacedBy(8.dp)
            ) {
                item.type?.takeIf { it.isNotBlank() }?.let {
                    StatusChip(label = "Type: $it", variant = StatusChipVariant.Neutral)
                }
                item.confidence?.takeIf { it.isNotBlank() }?.let {
                    StatusChip(label = "Confidence: $it", variant = StatusChipVariant.Success)
                }
                item.eligibilityStatus?.takeIf { it.isNotBlank() }?.let {
                    val chipVariant = when {
                        it.contains("eligible", ignoreCase = true) -> StatusChipVariant.Success
                        it.contains("block", ignoreCase = true) -> StatusChipVariant.Error
                        else -> StatusChipVariant.Warning
                    }
                    StatusChip(label = "Eligibility: $it", variant = chipVariant)
                }
            }

            if (item.blockers.isNotEmpty()) {
                Text(
                    text = "Blockers",
                    style = MaterialTheme.typography.labelSmall,
                    color = MaterialTheme.colorScheme.onSurfaceVariant
                )
                FlowRow(
                    horizontalArrangement = Arrangement.spacedBy(8.dp),
                    verticalArrangement = Arrangement.spacedBy(8.dp)
                ) {
                    item.blockers.forEach { blocker ->
                        StatusChip(label = "- $blocker", variant = StatusChipVariant.Warning)
                    }
                }
            }

            AnimatedVisibility(
                visible = isActionInFlight,
                enter = fadeIn(),
                exit = fadeOut()
            ) {
                Text(
                    text = "Submitting decision...",
                    style = MaterialTheme.typography.bodySmall
                )
            }

            Row(
                modifier = Modifier.fillMaxWidth(),
                horizontalArrangement = Arrangement.spacedBy(12.dp)
            ) {
                Button(
                    onClick = onApprove,
                    enabled = !isActionInFlight,
                    modifier = Modifier
                        .weight(1f)
                        .height(48.dp)
                ) {
                    Text(text = "Approve")
                }
                OutlinedButton(
                    onClick = onReject,
                    enabled = !isActionInFlight,
                    modifier = Modifier
                        .weight(1f)
                        .height(48.dp)
                ) {
                    Text(text = "Reject")
                }
            }
        }
    }
}

