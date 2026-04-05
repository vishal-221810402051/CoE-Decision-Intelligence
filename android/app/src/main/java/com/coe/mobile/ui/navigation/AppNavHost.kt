package com.coe.mobile.ui.navigation

import androidx.compose.runtime.Composable
import androidx.compose.ui.Modifier
import androidx.navigation.NavHostController
import androidx.navigation.compose.NavHost
import androidx.navigation.compose.composable
import com.coe.mobile.ui.screens.dashboard.DashboardScreen
import com.coe.mobile.ui.screens.inbox.SuggestionsInboxScreen
import com.coe.mobile.ui.screens.processing.ProcessingStatusScreen
import com.coe.mobile.ui.screens.recorder.RecorderScreen
import com.coe.mobile.ui.screens.suggestiondetail.SuggestionDetailScreen

@Composable
fun AppNavHost(
    navController: NavHostController,
    modifier: Modifier = Modifier
) {
    NavHost(
        navController = navController,
        startDestination = AppRoute.Dashboard.route,
        modifier = modifier
    ) {
        composable(AppRoute.Dashboard.route) {
            DashboardScreen(
                onStartRecording = {
                    navController.navigate(AppRoute.Recorder.route)
                },
                onOpenInbox = {
                    navController.navigate(AppRoute.Inbox.route)
                }
            )
        }
        composable(AppRoute.Recorder.route) {
            RecorderScreen()
        }
        composable(AppRoute.Processing.route) {
            ProcessingStatusScreen()
        }
        composable(AppRoute.Inbox.route) {
            SuggestionsInboxScreen()
        }
        composable(AppRoute.SuggestionDetail.route) {
            SuggestionDetailScreen()
        }
    }
}
