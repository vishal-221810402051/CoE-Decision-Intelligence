package com.coe.mobile.ui.navigation

sealed class AppRoute(val route: String) {
    data object Dashboard : AppRoute("dashboard")
    data object Recorder : AppRoute("recorder")
    data object Processing : AppRoute("processing")
    data object Inbox : AppRoute("inbox")
    data object SuggestionDetail : AppRoute("suggestionDetail")
}
