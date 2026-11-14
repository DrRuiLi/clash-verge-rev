import {
  check,
  type CheckOptions,
  type Update,
} from '@tauri-apps/plugin-updater'
import { compareVersions as compareSemver } from 'compare-versions'

import { version as appVersion } from '@root/package.json'

const SEMVER_FULL_REGEX =
  /^\d+(?:\.\d+){1,2}(?:-[0-9A-Za-z.-]+)?(?:\+[0-9A-Za-z.-]+)?$/
const SEMVER_SEARCH_REGEX =
  /v?\d+(?:\.\d+){1,2}(?:-[0-9A-Za-z.-]+)?(?:\+[0-9A-Za-z.-]+)?/i

const normalizeVersion = (input: string | null | undefined): string | null => {
  if (typeof input !== 'string') return null
  const trimmed = input.trim()
  if (!trimmed) return null
  return trimmed.replace(/^v/i, '')
}

const ensureSemver = (input: string | null | undefined): string | null => {
  const normalized = normalizeVersion(input)
  if (!normalized) return null
  return SEMVER_FULL_REGEX.test(normalized) ? normalized : null
}

const extractSemver = (input: string | null | undefined): string | null => {
  if (typeof input !== 'string') return null
  const match = input.match(SEMVER_SEARCH_REGEX)
  if (!match) return null
  return normalizeVersion(match[0])
}

const compareVersions = (a: string | null, b: string | null): number | null => {
  if (!a || !b) return null
  try {
    return compareSemver(a, b)
  } catch {
    return null
  }
}

// `+N` is a fork revision of that release (`2.5.10+1` > `2.5.10`). Other
// build metadata stays older than the stable version, matching the backend.
const numericBuildRevision = (version: string | null): number | null => {
  if (!version) return null
  const plus = version.indexOf('+')
  if (plus === -1) return 0
  const build = version.slice(plus + 1)
  return /^\d+$/.test(build) ? Number(build) : null
}

const sameCoreRemoteIsNewer = (
  local: string | null,
  remote: string | null,
): boolean => {
  const localRevision = numericBuildRevision(local)
  const remoteRevision = numericBuildRevision(remote)
  if (localRevision !== null && remoteRevision !== null) {
    return remoteRevision > localRevision
  }
  return (
    /^\d+\.\d+\.\d+\+[0-9A-Za-z.-]+$/.test(local ?? '') &&
    remote === local?.split('+')[0]
  )
}

const resolveRemoteVersion = (update: Update): string | null => {
  const primary = ensureSemver(update.version)
  if (primary) return primary

  const fallbackPrimary = extractSemver(update.version)
  if (fallbackPrimary) return fallbackPrimary

  const raw = update.rawJson ?? {}
  const rawVersion = ensureSemver(
    typeof raw.version === 'string' ? raw.version : null,
  )
  if (rawVersion) return rawVersion

  const tagVersion = extractSemver(
    typeof raw.tag_name === 'string' ? raw.tag_name : null,
  )
  if (tagVersion) return tagVersion

  const nameVersion = extractSemver(
    typeof raw.name === 'string' ? raw.name : null,
  )
  if (nameVersion) return nameVersion

  return null
}

const localVersionNormalized = normalizeVersion(appVersion)

export const checkUpdateSafe = async (
  options?: CheckOptions,
): Promise<Update | null> => {
  const result = await check(options ?? {})
  if (!result) return null

  const remoteVersion = resolveRemoteVersion(result)
  const comparison = compareVersions(remoteVersion, localVersionNormalized)
  const remoteIsNewer =
    comparison === null
      ? true
      : comparison > 0 ||
        (comparison === 0 &&
          sameCoreRemoteIsNewer(localVersionNormalized, remoteVersion))

  if (!remoteIsNewer) {
    try {
      await result.close()
    } catch (err) {
      console.warn('[updater] failed to close stale update resource', err)
    }
    return null
  }

  return result
}

export type { CheckOptions }
