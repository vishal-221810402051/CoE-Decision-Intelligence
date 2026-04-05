package com.coe.mobile.ui.navigation

import androidx.compose.material.icons.Icons
import androidx.compose.material.icons.filled.Dashboard
import androidx.compose.material.icons.filled.Inbox
import androidx.compose.material.icons.filled.Mic
import androidx.compose.ui.graphics.vector.ImageVector

data class TopLevelDestination(
    val route: String,
    val label: String,
    val icon: ImageVector
)

val topLevelDestinations = listOf(
    TopLevelDestination(
        route = AppRoute.Dashboard.route,
        label = "Dashboard",
        icon = Icons.Default.Dashboard
    ),
    TopLevelDestination(
        route = AppRoute.Recorder.route,
        label = "Recorder",
        icon = Icons.Default.Mic
    ),
    TopLevelDestination(
        route = AppRoute.Inbox.route,
        label = "Inbox",
        icon = Icons.Default.Inbox
    )
)
