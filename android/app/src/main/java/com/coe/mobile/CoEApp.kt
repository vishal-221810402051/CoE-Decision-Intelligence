package com.coe.mobile

import androidx.compose.foundation.layout.padding
import androidx.compose.material3.Scaffold
import androidx.compose.runtime.Composable
import androidx.compose.runtime.getValue
import androidx.compose.ui.Modifier
import androidx.navigation.NavGraph.Companion.findStartDestination
import androidx.navigation.compose.currentBackStackEntryAsState
import androidx.navigation.compose.rememberNavController
import com.coe.mobile.ui.components.AppBottomNavigation
import com.coe.mobile.ui.components.AppTopBar
import com.coe.mobile.ui.navigation.AppNavHost
import com.coe.mobile.ui.navigation.AppRoute
import com.coe.mobile.ui.theme.CoEMobileTheme
import com.coe.mobile.util.AppConstants

@Composable
fun CoEApp() {
    CoEMobileTheme {
        val navController = rememberNavController()
        val currentBackStackEntry by navController.currentBackStackEntryAsState()
        val currentRoute = currentBackStackEntry?.destination?.route
        val currentTitle = when (currentRoute) {
            AppRoute.Dashboard.route -> "Dashboard"
            AppRoute.Recorder.route -> "Recorder Console"
            AppRoute.Inbox.route -> "Approvals Inbox"
            AppRoute.Processing.route -> "Processing Status"
            AppRoute.SuggestionDetail.route -> "Suggestion Detail"
            else -> AppConstants.APP_TITLE
        }
        Scaffold(
            topBar = {
                AppTopBar(title = currentTitle)
            },
            bottomBar = {
                AppBottomNavigation(
                    currentRoute = currentRoute,
                    onNavigate = { route ->
                        navController.navigate(route) {
                            popUpTo(navController.graph.findStartDestination().id) {
                                saveState = true
                            }
                            launchSingleTop = true
                            restoreState = true
                        }
                    }
                )
            }
        ) { innerPadding ->
            AppNavHost(
                navController = navController,
                modifier = Modifier.padding(innerPadding)
            )
        }
    }
}
