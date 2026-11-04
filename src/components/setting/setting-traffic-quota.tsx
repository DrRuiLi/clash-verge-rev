import { InputAdornment, TextField } from '@mui/material'
import { useState } from 'react'
import { useTranslation } from 'react-i18next'

import { Switch, TooltipIcon } from '@/components/base'
import { useVerge } from '@/hooks/use-verge'

import { GuardState } from './mods/guard-state'
import { SettingItem, SettingList } from './mods/setting-comp'

const GIB = 1024 ** 3

interface Props {
  onError?: (err: Error) => void
}

const bytesToGb = (bytes: number) => {
  if (!bytes) return ''
  return String(Number((bytes / GIB).toFixed(2)))
}

const SettingTrafficQuota = ({ onError }: Props) => {
  const { t } = useTranslation()
  const { verge, mutateVerge, patchVerge } = useVerge()
  const enabled = verge?.traffic_quota_enable ?? false
  const bytes = verge?.traffic_quota_bytes ?? 0
  const [draft, setDraft] = useState<string | null>(null)

  const onChangeData = (patch: Partial<IVergeConfig>) => {
    mutateVerge({ ...verge, ...patch }, false)
  }

  const commitLimit = async () => {
    if (draft === null) return
    const gb = Number(draft)
    if (!Number.isFinite(gb) || gb <= 0) {
      setDraft(null)
      return
    }
    const next = Math.round(gb * GIB)
    setDraft(null)
    onChangeData({ traffic_quota_bytes: next })
    try {
      await patchVerge({ traffic_quota_bytes: next })
    } catch (error) {
      onChangeData({ traffic_quota_bytes: bytes })
      onError?.(error as Error)
    }
  }

  return (
    <SettingList title={t('settings.sections.trafficQuota.title')}>
      <SettingItem
        label={t('settings.sections.trafficQuota.fields.enable')}
        extra={
          <TooltipIcon
            title={t('settings.sections.trafficQuota.tooltips.enable')}
            sx={{ opacity: '0.7' }}
          />
        }
      >
        <GuardState
          value={enabled}
          valueProps="checked"
          onCatch={onError}
          onFormat={(_event, value) => value}
          onChange={(value) => onChangeData({ traffic_quota_enable: value })}
          onGuard={async (value) => {
            const patch: Partial<IVergeConfig> = {
              traffic_quota_enable: value,
            }
            if (value && !(bytes > 0)) {
              patch.traffic_quota_bytes = 10 * GIB
            }
            onChangeData(patch)
            try {
              await patchVerge(patch)
            } catch (error) {
              onChangeData({
                traffic_quota_enable: enabled,
                traffic_quota_bytes: bytes,
              })
              return Promise.reject(error)
            }
            return Promise.resolve()
          }}
        >
          <Switch edge="end" />
        </GuardState>
      </SettingItem>
      <SettingItem label={t('settings.sections.trafficQuota.fields.limit')}>
        <TextField
          size="small"
          type="number"
          value={draft ?? bytesToGb(bytes)}
          disabled={!enabled}
          onChange={(event) => setDraft(event.target.value)}
          onBlur={() => {
            void commitLimit()
          }}
          onKeyDown={(event) => {
            if (event.key === 'Enter') {
              event.preventDefault()
              void commitLimit()
            }
          }}
          sx={{ width: 140 }}
          slotProps={{
            input: {
              endAdornment: <InputAdornment position="end">GB</InputAdornment>,
            },
            htmlInput: { min: 0.01, step: 0.1 },
          }}
        />
      </SettingItem>
    </SettingList>
  )
}

export default SettingTrafficQuota
