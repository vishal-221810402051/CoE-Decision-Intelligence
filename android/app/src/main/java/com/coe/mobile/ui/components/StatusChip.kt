package com.coe.mobile.ui.components

import androidx.compose.foundation.background
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.shape.RoundedCornerShape
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.Text
import androidx.compose.runtime.Composable
import androidx.compose.ui.Modifier
import androidx.compose.ui.draw.clip
import androidx.compose.ui.unit.dp
import com.coe.mobile.ui.theme.CoeNeutralChip
import com.coe.mobile.ui.theme.CoeSuccess
import com.coe.mobile.ui.theme.CoeWarning

enum class StatusChipVariant {
    Success,
    Warning,
    Error,
    Neutral
}

@Composable
fun StatusChip(
    label: String,
    variant: StatusChipVariant,
    modifier: Modifier = Modifier
) {
    val (backgroundColor, textColor) = when (variant) {
        StatusChipVariant.Success -> CoeSuccess.copy(alpha = 0.2f) to CoeSuccess
        StatusChipVariant.Warning -> CoeWarning.copy(alpha = 0.2f) to CoeWarning
        StatusChipVariant.Error -> MaterialTheme.colorScheme.error.copy(alpha = 0.2f) to MaterialTheme.colorScheme.error
        StatusChipVariant.Neutral -> CoeNeutralChip to MaterialTheme.colorScheme.onSurfaceVariant
    }

    Text(
        text = label,
        style = MaterialTheme.typography.labelSmall,
        color = textColor,
        modifier = modifier
            .clip(RoundedCornerShape(999.dp))
            .background(backgroundColor)
            .padding(horizontal = 10.dp, vertical = 6.dp)
    )
}
