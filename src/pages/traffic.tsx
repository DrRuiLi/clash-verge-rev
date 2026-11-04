import DataUsageOutlinedIcon from '@mui/icons-material/DataUsageOutlined'
import {
  Box,
  Chip,
  Stack,
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableRow,
  ToggleButton,
  ToggleButtonGroup,
  Typography,
} from '@mui/material'
import { useEffect, useState } from 'react'
import { useTranslation } from 'react-i18next'

import { BaseEmpty, BasePage } from '@/components/base'
import { useVerge } from '@/hooks/use-verge'
import { useVisibility } from '@/hooks/use-visibility'
import type { TrafficPeriodUsage, TrafficStats } from '@/services/bindings'
import { getTrafficStats } from '@/services/cmds'
import parseTraffic from '@/utils/parse-traffic'

type TrafficPeriod = 'today' | 'week' | 'month'

const formatBytes = (value: number) => {
  const [amount, unit] = parseTraffic(value)
  return unit ? `${amount} ${unit}` : amount
}

const periodOf = (stats: TrafficStats, period: TrafficPeriod) => {
  if (period === 'week') return stats.week
  if (period === 'month') return stats.month
  return stats.today
}

const TrafficTotals = ({ usage }: { usage: TrafficPeriodUsage }) => {
  const { t } = useTranslation()
  const rows = [
    {
      label: t('home.trafficAccount.proxy'),
      up: usage.proxy_up,
      down: usage.proxy_down,
    },
    {
      label: t('home.trafficAccount.direct'),
      up: usage.direct_up,
      down: usage.direct_down,
    },
  ]

  return (
    <Stack direction="row" spacing={1.5} useFlexGap sx={{ flexWrap: 'wrap' }}>
      {rows.map((row) => (
        <Box
          key={row.label}
          sx={{
            flex: '1 1 220px',
            borderRadius: 2,
            px: 1.5,
            py: 1.25,
            bgcolor: 'action.hover',
          }}
        >
          <Typography variant="subtitle2">{row.label}</Typography>
          <Typography variant="body2" color="text.secondary">
            {t('home.trafficAccount.upload')} {formatBytes(row.up)}
          </Typography>
          <Typography variant="body2" color="text.secondary">
            {t('home.trafficAccount.download')} {formatBytes(row.down)}
          </Typography>
          <Typography variant="body1" sx={{ fontWeight: 700 }}>
            {formatBytes(row.up + row.down)}
          </Typography>
        </Box>
      ))}
    </Stack>
  )
}

const TrafficPage = () => {
  const { t } = useTranslation()
  const pageVisible = useVisibility()
  const { verge } = useVerge()
  const [period, setPeriod] = useState<TrafficPeriod>('today')
  const [stats, setStats] = useState<TrafficStats | null>(null)

  useEffect(() => {
    if (!pageVisible) return undefined
    let cancelled = false
    const load = () => {
      getTrafficStats()
        .then((next) => {
          if (!cancelled) setStats(next)
        })
        .catch((error: unknown) => {
          console.error('[traffic] failed to load stats', error)
        })
    }
    load()
    const timer = window.setInterval(load, 5000)
    return () => {
      cancelled = true
      window.clearInterval(timer)
    }
  }, [pageVisible])

  const usage = stats ? periodOf(stats, period) : null
  const quotaEnabled = verge?.traffic_quota_enable ?? false
  const quotaLimit = verge?.traffic_quota_bytes ?? 0
  const usedToday =
    (stats?.today.proxy_up ?? 0) + (stats?.today.proxy_down ?? 0)
  const remaining = quotaLimit > usedToday ? quotaLimit - usedToday : 0

  return (
    <BasePage
      title={t('home.trafficAccount.title')}
      header={
        <ToggleButtonGroup
          exclusive
          size="small"
          value={period}
          onChange={(_, value: TrafficPeriod | null) => {
            if (value) setPeriod(value)
          }}
        >
          <ToggleButton value="today">
            {t('home.trafficAccount.ranges.today')}
          </ToggleButton>
          <ToggleButton value="week">
            {t('home.trafficAccount.ranges.week')}
          </ToggleButton>
          <ToggleButton value="month">
            {t('home.trafficAccount.ranges.month')}
          </ToggleButton>
        </ToggleButtonGroup>
      }
    >
      <Stack spacing={1.5} sx={{ p: 1.5 }}>
        <Stack direction="row" spacing={1} sx={{ alignItems: 'center' }}>
          <DataUsageOutlinedIcon fontSize="small" />
          <Typography variant="body2" color="text.secondary">
            {t('home.trafficAccount.caption')}
          </Typography>
        </Stack>
        {quotaEnabled && quotaLimit > 0 && (
          <Stack
            direction="row"
            spacing={1}
            useFlexGap
            sx={{ flexWrap: 'wrap' }}
          >
            <Chip
              color={stats?.quota_tripped ? 'warning' : 'default'}
              label={`${t('home.trafficAccount.quotaRemaining')} ${formatBytes(remaining)}`}
            />
          </Stack>
        )}
        {stats?.quota_tripped && (
          <Typography variant="body2" color="warning.main">
            {t('home.trafficAccount.quotaReached')}
          </Typography>
        )}
        {usage &&
        usage.proxy_up +
          usage.proxy_down +
          usage.direct_up +
          usage.direct_down >
          0 ? (
          <>
            <TrafficTotals usage={usage} />
            {usage.apps.length > 0 ? (
              <Table size="small">
                <TableHead>
                  <TableRow>
                    <TableCell>{t('home.trafficAccount.apps')}</TableCell>
                    <TableCell align="right">
                      {t('home.trafficAccount.proxy')}
                    </TableCell>
                    <TableCell align="right">
                      {t('home.trafficAccount.direct')}
                    </TableCell>
                  </TableRow>
                </TableHead>
                <TableBody>
                  {usage.apps.map((app) => (
                    <TableRow key={app.name}>
                      <TableCell>
                        {app.name === 'unknown'
                          ? t('home.trafficAccount.unknownApp')
                          : app.name === 'other'
                            ? t('home.trafficAccount.otherApps')
                            : app.name}
                      </TableCell>
                      <TableCell align="right">
                        {formatBytes(app.proxy_up + app.proxy_down)}
                      </TableCell>
                      <TableCell align="right">
                        {formatBytes(app.direct_up + app.direct_down)}
                      </TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            ) : null}
          </>
        ) : (
          <BaseEmpty textKey="home.trafficAccount.empty" />
        )}
      </Stack>
    </BasePage>
  )
}

export default TrafficPage
