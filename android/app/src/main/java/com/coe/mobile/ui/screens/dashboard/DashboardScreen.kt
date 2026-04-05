package com.coe.mobile.ui.screens.dashboard

import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.height
import androidx.compose.foundation.layout.Row
import androidx.compose.foundation.layout.fillMaxSize
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.rememberScrollState
import androidx.compose.foundation.verticalScroll
import androidx.compose.material3.Button
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.OutlinedButton
import androidx.compose.material3.Text
import androidx.compose.runtime.Composable
import androidx.compose.ui.Modifier
import androidx.compose.ui.unit.dp
import com.coe.mobile.ui.components.AppCard
import com.coe.mobile.ui.components.SectionHeader

@Composable
fun DashboardScreen(
    modifier: Modifier = Modifier,
    onStartRecording: () -> Unit = {},
    onOpenInbox: () -> Unit = {}
) {
    Column(
        modifier = modifier
            .fillMaxSize()
            .verticalScroll(rememberScrollState())
            .padding(horizontal = 16.dp, vertical = 20.dp),
        verticalArrangement = Arrangement.spacedBy(20.dp)
    ) {
        SectionHeader(
            title = "System Status",
            subtitle = "CoE intelligence pipeline overview"
        )
        AppCard {
            Text(
                text = "Laptop Connected",
                style = MaterialTheme.typography.titleMedium
            )
            Text(
                text = "Last Sync: 2 mins ago",
                style = MaterialTheme.typography.bodyMedium,
                color = MaterialTheme.colorScheme.onSurfaceVariant,
                modifier = Modifier.padding(top = 8.dp)
            )
        }

        SectionHeader(title = "Overview")
        Row(
            modifier = Modifier.fillMaxWidth(),
            horizontalArrangement = Arrangement.spacedBy(12.dp)
        ) {
            AppCard(modifier = Modifier.weight(1f)) {
                Text("Pending Approvals", color = MaterialTheme.colorScheme.onSurfaceVariant)
                Text(
                    text = "4",
                    style = MaterialTheme.typography.titleLarge,
                    modifier = Modifier.padding(top = 8.dp)
                )
            }
            AppCard(modifier = Modifier.weight(1f)) {
                Text("Meetings Today", color = MaterialTheme.colorScheme.onSurfaceVariant)
                Text(
                    text = "7",
                    style = MaterialTheme.typography.titleLarge,
                    modifier = Modifier.padding(top = 8.dp)
                )
            }
        }

        SectionHeader(title = "Quick Actions")
        Row(
            modifier = Modifier.fillMaxWidth(),
            horizontalArrangement = Arrangement.spacedBy(12.dp)
        ) {
            Button(
                onClick = onStartRecording,
                modifier = Modifier
                    .weight(1f)
                    .height(52.dp)
            ) {
                Text("Start Recording")
            }
            OutlinedButton(
                onClick = onOpenInbox,
                modifier = Modifier
                    .weight(1f)
                    .height(52.dp)
            ) {
                Text("Open Inbox")
            }
        }
    }
}
