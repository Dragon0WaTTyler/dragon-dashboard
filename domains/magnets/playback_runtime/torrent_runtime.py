from __future__ import annotations

import json
import shutil
import subprocess
import threading
from itertools import count
from pathlib import Path
from typing import Any, Mapping

from dragon.paths import CACHE_DIR


WEBTORRENT_HELPER_SOURCE = r"""
import { createWriteStream, existsSync } from 'node:fs'
import { mkdir, open as openFile, readFile, stat as statFile } from 'node:fs/promises'
import path from 'node:path'
import readline from 'node:readline'
import WebTorrent from 'webtorrent'

const client = new WebTorrent()
const sessions = new Map()

function ok(requestId, payload = {}) {
  process.stdout.write(JSON.stringify({ ok: true, requestId, ...payload }) + '\n')
}

function fail(requestId, error) {
  process.stdout.write(JSON.stringify({
    ok: false,
    requestId,
    error: error instanceof Error ? error.message : String(error || 'Unknown runtime error')
  }) + '\n')
}

function waitFor(check, timeoutMs, errorMessage) {
  const deadline = Date.now() + timeoutMs
  return new Promise((resolve, reject) => {
    const tick = async () => {
      try {
        if (await Promise.resolve(check())) {
          resolve()
          return
        }
      } catch (error) {
        reject(error)
        return
      }
      if (Date.now() >= deadline) {
        reject(new Error(errorMessage))
        return
      }
      setTimeout(tick, 250)
    }
    tick()
  })
}

function normalizeRelativePath(filePath) {
  const rawValue = String(filePath || '').trim()
  if (!rawValue) {
    return {
      ok: false,
      code: 'selected_file_missing',
      reason: 'Selected file path is missing.',
      relativePath: '',
      expectedPath: '',
    }
  }
  if (path.isAbsolute(rawValue)) {
    return {
      ok: false,
      code: 'unsafe_path',
      reason: 'Selected file path must stay relative to the helper download root.',
      relativePath: '',
      expectedPath: '',
    }
  }
  const normalized = path.posix.normalize(rawValue.replace(/\\/g, '/')).replace(/^\/+/, '')
  if (!normalized || normalized === '.' || normalized === '..' || normalized.startsWith('../')) {
    return {
      ok: false,
      code: 'unsafe_path',
      reason: 'Selected file path escapes the helper download root.',
      relativePath: '',
      expectedPath: '',
    }
  }
  return {
    ok: true,
    code: '',
    reason: '',
    relativePath: normalized,
    expectedPath: '',
  }
}

function resolveExpectedPath(downloadDir, filePath) {
  const normalized = normalizeRelativePath(filePath)
  if (!normalized.ok) {
    return normalized
  }
  const rootPath = path.resolve(String(downloadDir || '.'))
  const expectedPath = path.resolve(rootPath, ...normalized.relativePath.split('/'))
  const relativeToRoot = path.relative(rootPath, expectedPath)
  if (!relativeToRoot || relativeToRoot.startsWith('..') || path.isAbsolute(relativeToRoot)) {
    return {
      ok: false,
      code: 'unsafe_path',
      reason: 'Selected file path resolves outside the helper download root.',
      relativePath: normalized.relativePath,
      expectedPath,
    }
  }
  return {
    ok: true,
    code: '',
    reason: '',
    relativePath: normalized.relativePath,
    expectedPath,
  }
}

function getLocalPath(downloadDir, filePath) {
  const resolved = resolveExpectedPath(downloadDir, filePath)
  return resolved.ok ? resolved.expectedPath : ''
}

async function inspectLocalPath(localPath) {
  if (!localPath) {
    return {
      exists: false,
      size: 0,
      firstByteReadable: false,
    }
  }
  try {
    const fileStat = await statFile(localPath)
    const size = Number(fileStat.size || 0)
    if (size <= 0) {
      return {
        exists: true,
        size,
        firstByteReadable: false,
      }
    }
    const handle = await openFile(localPath, 'r')
    try {
      const buffer = Buffer.alloc(1)
      const result = await handle.read(buffer, 0, 1, 0)
      return {
        exists: true,
        size,
        firstByteReadable: Number(result.bytesRead || 0) > 0,
      }
    } finally {
      await handle.close()
    }
  } catch (_error) {
    return {
      exists: false,
      size: 0,
      firstByteReadable: false,
    }
  }
}

function buildFilePayload(file, index, downloadDir) {
  const resolved = resolveExpectedPath(downloadDir, file.path)
  const relativePath = resolved.ok ? resolved.relativePath : String(file.path || '')
  return {
    index,
    name: file.name,
    path: relativePath,
    relativePath,
    length: Number(file.length || file.size || 0),
    downloaded: Number(file.downloaded || 0),
    progress: Number(file.progress || 0),
    mimeType: String(file.type || ''),
    localPath: resolved.ok ? resolved.expectedPath : '',
  }
}

function stopMaterializer(entry) {
  const materializer = entry.materializer
  if (!materializer) {
    return
  }
  materializer.writerActive = false
  if (materializer.readStream) {
    try {
      materializer.readStream.destroy()
    } catch (_error) {
    }
  }
  if (materializer.writeStream) {
    try {
      materializer.writeStream.destroy()
    } catch (_error) {
    }
  }
}

function nowIso() {
  return new Date().toISOString()
}

function nowMs() {
  return Date.now()
}

function normalizeMessages(messages) {
  return Array.from(new Set((messages || []).map(message => String(message || '').trim()).filter(Boolean)))
}

function detectTrackerIssue(entry) {
  const messages = normalizeMessages([entry.warning, entry.error, ...(entry.trackerMessages || [])])
  const trackerHint = messages.find(message => /tracker|announce|dns|timed out|timeout/i.test(message))
  return trackerHint || ''
}

function classifyMaterializationFailure(entry, materializer, localState) {
  const numPeers = Number(entry.torrent?.numPeers || 0)
  const bytesWritten = Number(materializer?.bytesWritten || 0)
  const firstDataReceived = Boolean(materializer?.firstDataReceived)
  const readStreamStarted = Boolean(materializer?.readStreamStarted)
  const trackerIssue = detectTrackerIssue(entry)
  if (trackerIssue) {
    return { code: 'tracker_unavailable', reason: trackerIssue }
  }
  if (numPeers <= 0) {
    return { code: 'no_peers', reason: 'No peers were connected before materialization timed out.' }
  }
  if (!firstDataReceived && bytesWritten <= 0) {
    return { code: 'peer_connected_but_no_data', reason: 'Peers connected but no data was received before materialization timed out.' }
  }
  if (readStreamStarted && !firstDataReceived) {
    return { code: 'read_stream_waiting_for_pieces', reason: 'The read stream is waiting for torrent pieces to arrive.' }
  }
  if (bytesWritten <= 0 && !localState.exists) {
    return { code: 'no_data_received', reason: 'The selected file never received any bytes before materialization timed out.' }
  }
  return { code: 'materialization_timeout', reason: 'Selected file materialization timeout' }
}

async function ensureMaterializer(entry, file) {
  const resolvedPath = resolveExpectedPath(entry.downloadDir, file.path)
  if (!resolvedPath.ok) {
    entry.materializationTimeout = false
    entry.materializationReason = resolvedPath.reason
    entry.materializer = {
      relativePath: resolvedPath.relativePath || '',
      expectedPath: resolvedPath.expectedPath || '',
      writerActive: false,
      bytesWritten: 0,
      errorCode: resolvedPath.code,
      errorReason: resolvedPath.reason,
      readStream: null,
      writeStream: null,
      readStreamStarted: false,
      readStreamActive: false,
      firstDataReceived: false,
      lastDataAt: '',
      timeSinceLastDataMs: 0,
      materializationTimeoutMs: 0,
    }
    return entry.materializer
  }

  const existing = entry.materializer
  if (existing && existing.relativePath === resolvedPath.relativePath) {
    if (existing.writerActive || existing.bytesWritten > 0 || !existing.errorCode) {
      return existing
    }
  }
  if (existing && existing.relativePath !== resolvedPath.relativePath) {
    stopMaterializer(entry)
  }

  await mkdir(path.dirname(resolvedPath.expectedPath), { recursive: true })
  const readStream = file.createReadStream()
  const writeStream = createWriteStream(resolvedPath.expectedPath, { flags: 'w' })
  const materializer = {
    relativePath: resolvedPath.relativePath,
    expectedPath: resolvedPath.expectedPath,
    writerActive: true,
    bytesWritten: 0,
    errorCode: '',
    errorReason: '',
    readStream,
    writeStream,
    readStreamStarted: true,
    readStreamActive: true,
    firstDataReceived: false,
    lastDataAt: '',
    timeSinceLastDataMs: 0,
    materializationTimeoutMs: 0,
  }
  const failMaterializer = (code, error) => {
    if (materializer.errorCode) {
      return
    }
    materializer.writerActive = false
    materializer.readStreamActive = false
    materializer.errorCode = code
    materializer.errorReason = error instanceof Error ? error.message : String(error || code)
    entry.materializationReason = materializer.errorReason
  }
  const finishMaterializer = () => {
    materializer.writerActive = false
    materializer.readStreamActive = false
  }

  readStream.on('data', chunk => {
    materializer.firstDataReceived = true
    materializer.readStreamStarted = true
    materializer.readStreamActive = true
    materializer.bytesWritten += Number(chunk?.length || 0)
    materializer.lastDataAt = nowIso()
    materializer.timeSinceLastDataMs = 0
  })
  readStream.on('error', error => failMaterializer('read_stream_error', error))
  readStream.on('end', () => finishMaterializer())
  readStream.on('close', () => {
    if (!materializer.errorCode) {
      materializer.readStreamActive = false
    }
  })
  writeStream.on('error', error => failMaterializer('write_stream_error', error))
  writeStream.on('finish', () => finishMaterializer())
  writeStream.on('close', () => {
    if (!materializer.errorCode) {
      finishMaterializer()
    }
  })
  readStream.pipe(writeStream)

  entry.materializer = materializer
  return materializer
}

async function buildMaterializationPayload(entry) {
  const selectedFile = entry.selectedFile
  const resolvedPath = selectedFile ? resolveExpectedPath(entry.downloadDir, selectedFile.path) : null
  const materializer = entry.materializer || null
  if (materializer?.lastDataAt) {
    materializer.timeSinceLastDataMs = Math.max(0, nowMs() - Date.parse(materializer.lastDataAt))
  }
  const selectedFileRelativePath = materializer?.relativePath || (resolvedPath?.relativePath || '')
  const selectedFileExpectedPath = materializer?.expectedPath || (resolvedPath?.expectedPath || '')
  const localState = await inspectLocalPath(selectedFileExpectedPath)
  const materialized = Boolean(localState.exists && localState.size > 0 && localState.firstByteReadable)
  let state = 'idle'
  let code = ''
  let reason = String(entry.materializationReason || '')

  if (selectedFile) {
    if (resolvedPath && !resolvedPath.ok) {
      state = 'materialization_failed'
      code = resolvedPath.code
      reason = resolvedPath.reason
    } else if (materializer?.errorCode) {
      state = 'materialization_failed'
      code = materializer.errorCode
      reason = materializer.errorReason || reason
    } else if (entry.materializationTimeout && !materialized) {
      state = 'materialization_failed'
      const failure = classifyMaterializationFailure(entry, materializer, localState)
      code = failure.code
      reason = failure.reason
    } else if (materialized) {
      state = 'file_ready'
    } else if (Boolean(materializer?.writerActive) || Number(materializer?.bytesWritten || 0) > 0 || (localState.exists && localState.size > 0)) {
      state = 'materializing'
      code = 'waiting_for_bytes'
    } else {
      state = 'metadata_loaded_but_file_missing'
      code = 'selected_file_missing'
    }
  }

  return {
    helperDownloadRoot: String(entry.downloadDir || ''),
    selectedFileRelativePath,
    selectedFileExpectedPath,
    selectedFilePrioritized: Boolean(selectedFile && entry.selectedIndex >= 0),
    localFileExists: Boolean(localState.exists),
    localFileSize: Number(localState.size || 0),
    firstByteReadable: Boolean(localState.firstByteReadable),
    bytesWritten: Number(materializer?.bytesWritten || 0),
    expectedLength: Number(selectedFile?.length || selectedFile?.size || 0),
    writerActive: Boolean(materializer?.writerActive),
    readStreamStarted: Boolean(materializer?.readStreamStarted),
    readStreamActive: Boolean(materializer?.readStreamActive),
    firstDataReceived: Boolean(materializer?.firstDataReceived),
    lastDataAt: String(materializer?.lastDataAt || ''),
    timeSinceLastDataMs: Number(materializer?.timeSinceLastDataMs || 0),
    materializationTimeoutMs: Number(materializer?.materializationTimeoutMs || 0),
    state,
    code,
    reason,
  }
}

function buildWebTorrentPayload(entry) {
  const torrent = entry.torrent || {}
  const selectedFile = entry.selectedFile || null
  const materializer = entry.materializer || null
  const trackerMessages = Array.isArray(entry.trackerMessages) ? entry.trackerMessages : []
  return {
    sourceKind: String(entry.sourceKind || 'magnet'),
    torrentFilePath: String(entry.torrentFilePath || ''),
    torrentFileExists: Boolean(entry.torrentFileExists),
    torrentFileSize: Number(entry.torrentFileSize || 0),
    torrentAddMode: String(entry.torrentAddMode || ''),
    clientAddStarted: Boolean(entry.clientAddStarted),
    metadataEventReceived: Boolean(entry.metadataEventReceived),
    readyEventReceived: Boolean(entry.readyEventReceived),
    helperError: String(entry.helperError || ''),
    numPeers: Number(torrent?.numPeers || 0),
    downloaded: Number(torrent?.downloaded || 0),
    downloadSpeed: Number(torrent?.downloadSpeed || 0),
    progress: Number(torrent?.progress || 0),
    ready: Boolean(torrent?.ready),
    paused: Boolean(torrent?.paused),
    torrentLength: Number(torrent?.length || 0),
    filesCount: Array.isArray(torrent?.files) ? torrent.files.length : 0,
    wiresCount: Array.isArray(torrent?.wires) ? torrent.wires.length : 0,
    selectedFileIndex: Number(entry.selectedIndex ?? -1),
    selectedFileName: String(selectedFile?.name || ''),
    selectedFileLength: Number(selectedFile?.length || selectedFile?.size || 0),
    readStreamStarted: Boolean(materializer?.readStreamStarted),
    readStreamActive: Boolean(materializer?.readStreamActive),
      firstDataReceived: Boolean(materializer?.firstDataReceived),
      bytesWritten: Number(materializer?.bytesWritten || 0),
      lastDataAt: String(materializer?.lastDataAt || ''),
      timeSinceLastDataMs: Number(materializer?.timeSinceLastDataMs || 0),
      materializationTimeoutMs: Number(materializer?.materializationTimeoutMs || 0),
    warningMessages: normalizeMessages([entry.warning]),
    errorMessages: normalizeMessages([entry.error]),
    trackerMessages: normalizeMessages(trackerMessages),
  }
}

async function buildStatusPayload(entry) {
  const torrent = entry.torrent
  const selectedFile = entry.selectedFile
  const materialization = await buildMaterializationPayload(entry)
  const webtorrent = buildWebTorrentPayload(entry)
  return {
    torrentName: String(torrent?.name || ''),
    infoHash: String(torrent?.infoHash || ''),
    progress: Number(torrent?.progress || 0),
    downloadSpeed: Number(torrent?.downloadSpeed || 0),
    numPeers: Number(torrent?.numPeers || 0),
    complete: Boolean(torrent?.done),
    warning: String(entry.warning || ''),
    error: String(entry.error || ''),
    downloadDir: String(entry.downloadDir || ''),
    materialization,
    selectedFile: selectedFile ? buildFilePayload(selectedFile, entry.selectedIndex, entry.downloadDir) : null,
    webtorrent,
  }
}

async function resolveTorrentAddInput(command) {
  const sourceKind = String(command.sourceKind || 'magnet').trim().toLowerCase() || 'magnet'
  if (sourceKind !== 'torrent_file') {
    return {
      sourceKind: 'magnet',
      torrentId: String(command.torrentId || ''),
      torrentFilePath: '',
      torrentFileExists: false,
      torrentFileSize: 0,
      torrentAddMode: 'magnet',
      helperError: '',
    }
  }
  const torrentFilePath = path.resolve(String(command.torrentId || ''))
  const torrentFileExists = existsSync(torrentFilePath)
  if (!torrentFileExists) {
    throw new Error('Torrent file is missing.')
  }
  let fileStat
  try {
    fileStat = await statFile(torrentFilePath)
  } catch (error) {
    throw new Error(`Torrent file could not be read: ${error instanceof Error ? error.message : String(error || 'read failed')}`)
  }
  const torrentFileSize = Number(fileStat.size || 0)
  if (torrentFileSize <= 0) {
    throw new Error('Torrent file is empty.')
  }
  let torrentBuffer
  try {
    torrentBuffer = await readFile(torrentFilePath)
  } catch (error) {
    throw new Error(`Torrent file could not be read: ${error instanceof Error ? error.message : String(error || 'read failed')}`)
  }
  return {
    sourceKind: 'torrent_file',
    torrentId: torrentBuffer,
    torrentFilePath,
    torrentFileExists: true,
    torrentFileSize,
    torrentAddMode: 'buffer',
    helperError: '',
  }
}

async function startSession(command) {
  if (sessions.has(command.sessionId)) {
    return { status: await buildStatusPayload(sessions.get(command.sessionId)) }
  }
  const downloadDir = path.resolve(String(command.downloadDir || '.'))
  await mkdir(downloadDir, { recursive: true })
  const torrentInput = await resolveTorrentAddInput(command)
  let torrent
  try {
    torrent = client.add(torrentInput.torrentId, {
      path: downloadDir,
      strategy: 'sequential',
      deselect: true,
      destroyStoreOnDestroy: false,
    })
  } catch (error) {
    const prefix = torrentInput.sourceKind === 'torrent_file' ? 'Torrent file add failed' : 'Torrent add failed'
    throw new Error(`${prefix}: ${error instanceof Error ? error.message : String(error || 'add failed')}`)
  }
  const entry = {
    torrent,
    downloadDir,
    sourceKind: torrentInput.sourceKind,
    torrentFilePath: torrentInput.torrentFilePath,
    torrentFileExists: torrentInput.torrentFileExists,
    torrentFileSize: torrentInput.torrentFileSize,
    torrentAddMode: torrentInput.torrentAddMode,
    clientAddStarted: true,
    metadataEventReceived: false,
    readyEventReceived: false,
    helperError: String(torrentInput.helperError || ''),
    selectedFile: null,
    selectedIndex: -1,
    materializationTimeout: false,
    materializationReason: '',
    materializer: null,
    trackerMessages: [],
    warning: '',
    error: '',
  }
  sessions.set(command.sessionId, entry)
  torrent.on('metadata', () => {
    entry.metadataEventReceived = true
  })
  torrent.on('ready', () => {
    entry.readyEventReceived = true
  })
  torrent.on('warning', error => {
    entry.warning = error instanceof Error ? error.message : String(error || '')
    entry.trackerMessages = normalizeMessages([...entry.trackerMessages, entry.warning])
  })
  torrent.on('error', error => {
    entry.error = error instanceof Error ? error.message : String(error || '')
    entry.helperError = entry.error
    entry.trackerMessages = normalizeMessages([...entry.trackerMessages, entry.error])
  })
  try {
    await waitFor(
      () => {
        if (torrent.files && torrent.files.length > 0) {
          entry.metadataEventReceived = true
          return true
        }
        return false
      },
      Number(command.metadataTimeoutMs || 20000),
      entry.sourceKind === 'torrent_file' ? 'Torrent file metadata timeout' : 'Torrent metadata timeout'
    )
  } catch (error) {
    entry.helperError = error instanceof Error ? error.message : String(error || '')
    throw error
  }
  if (!torrent.files || torrent.files.length <= 0) {
    entry.helperError = 'Torrent file contains no files.'
    throw new Error('Torrent file contains no files.')
  }
  return {
    torrentName: String(torrent.name || ''),
    infoHash: String(torrent.infoHash || ''),
    files: torrent.files.map((file, index) => buildFilePayload(file, index, downloadDir)),
    status: await buildStatusPayload(entry),
  }
}

async function primeSelectedFile(file, timeoutMs) {
  await new Promise((resolve, reject) => {
    const stream = file.createReadStream({ start: 0, end: 0 })
    let settled = false
    const timer = setTimeout(() => finish(new Error('Selected file primer timeout')), Math.max(Number(timeoutMs || 0), 1))
    const finish = error => {
      if (settled) return
      settled = true
      clearTimeout(timer)
      try {
        stream.destroy()
      } catch (_destroyError) {
      }
      if (error) {
        reject(error)
        return
      }
      resolve()
    }
    stream.once('data', () => finish())
    stream.once('end', () => finish())
    stream.once('error', error => finish(error))
  })
}

async function selectFile(command) {
  const entry = sessions.get(command.sessionId)
  if (!entry) {
    throw new Error('Playback session not found')
  }
  const files = entry.torrent.files || []
  const targetPath = String(command.filePath || '')
  const targetIndex = Number(command.fileIndex ?? -1)
  const file = files.find((item, index) => index === targetIndex || String(item.path || '') === targetPath)
  if (!file) {
    throw new Error('Playable media file not found in torrent')
  }
  files.forEach(candidate => {
    if (candidate === file) {
      candidate.select(10)
    } else {
      candidate.deselect()
    }
  })
  entry.selectedFile = file
  entry.selectedIndex = files.indexOf(file)
  entry.materializationTimeout = false
  entry.materializationReason = ''
  const minReadyBytes = Math.max(Number(command.minReadyBytes || 0), 0)
  const readyTimeoutMs = Number(command.readyTimeoutMs || 45000)
  entry.materializer = entry.materializer || null
  entry.materializer && (entry.materializer.materializationTimeoutMs = readyTimeoutMs)
  try {
    await primeSelectedFile(file, Math.min(readyTimeoutMs, 3000))
  } catch (error) {
    entry.warning = error instanceof Error ? error.message : String(error || '')
    entry.trackerMessages = normalizeMessages([...entry.trackerMessages, entry.warning])
  }
  const materializer = await ensureMaterializer(entry, file)
  materializer.materializationTimeoutMs = readyTimeoutMs
  if (materializer.errorCode === 'unsafe_path') {
    return {
      selectedFile: buildFilePayload(file, entry.selectedIndex, entry.downloadDir),
      status: await buildStatusPayload(entry),
    }
  }
  try {
    await waitFor(
      async () => {
        const localState = await inspectLocalPath(materializer.expectedPath)
        return (
          ((Number(materializer.bytesWritten || 0) >= Math.max(minReadyBytes, 1) || Number(materializer.bytesWritten || 0) > 0)
            && localState.exists
            && localState.size > 0
            && localState.firstByteReadable)
          || Boolean(file.done)
          || Boolean(entry.torrent.done)
          || Boolean(entry.error)
          || Boolean(materializer.errorCode)
        )
      },
      readyTimeoutMs,
      'Selected file materialization timeout'
    )
  } catch (error) {
    entry.materializationTimeout = true
    entry.materializationReason = error instanceof Error ? error.message : String(error || 'Selected file materialization timeout')
  }
  if (entry.error) {
    throw new Error(entry.error)
  }
  return {
    selectedFile: buildFilePayload(file, entry.selectedIndex, entry.downloadDir),
    status: await buildStatusPayload(entry),
  }
}

async function getStatus(command) {
  const entry = sessions.get(command.sessionId)
  if (!entry) {
    throw new Error('Playback session not found')
  }
  return { status: await buildStatusPayload(entry) }
}

async function closeSession(command) {
  const entry = sessions.get(command.sessionId)
  if (!entry) {
    return { closed: true }
  }
  stopMaterializer(entry)
  sessions.delete(command.sessionId)
  await new Promise(resolve => entry.torrent.destroy({ destroyStore: false }, resolve))
  return { closed: true }
}

const handlers = {
  start: startSession,
  select: selectFile,
  status: getStatus,
  close: closeSession,
}

const rl = readline.createInterface({
  input: process.stdin,
  crlfDelay: Infinity,
})

rl.on('line', async line => {
  let command
  try {
    command = JSON.parse(line)
    const handler = handlers[String(command.action || '')]
    if (!handler) {
      throw new Error('Unsupported helper action')
    }
    const payload = await handler(command)
    ok(command.requestId, payload)
  } catch (error) {
    fail(command && command.requestId ? command.requestId : '', error)
  }
})

client.on('error', error => {
  process.stderr.write((error instanceof Error ? error.stack || error.message : String(error || '')) + '\n')
})
"""


class TorrentRuntimeError(RuntimeError):
    pass


class WebTorrentRuntimeClient:
    def __init__(self, *, cache_root: Path | None = None) -> None:
        self.cache_root = Path(cache_root or (CACHE_DIR / "magnets" / "playback_runtime"))
        self.helper_path = self.cache_root / "webtorrent_helper.mjs"
        self.stderr_path = self.cache_root / "webtorrent_helper.stderr.log"
        self._request_ids = count(1)
        self._process: subprocess.Popen[str] | None = None
        self._stderr_handle = None
        self._lock = threading.RLock()

    def start(self, *, session_id: str, torrent_input: str, source_kind: str, download_dir: Path, metadata_timeout_ms: int) -> dict[str, Any]:
        return self._request(
            {
                "action": "start",
                "sessionId": session_id,
                "torrentId": torrent_input,
                "sourceKind": str(source_kind or "magnet"),
                "downloadDir": str(download_dir),
                "metadataTimeoutMs": int(metadata_timeout_ms),
            }
        )

    def select(self, *, session_id: str, file_index: int, file_path: str, min_ready_bytes: int, ready_timeout_ms: int) -> dict[str, Any]:
        return self._request(
            {
                "action": "select",
                "sessionId": session_id,
                "fileIndex": int(file_index),
                "filePath": file_path,
                "minReadyBytes": int(min_ready_bytes),
                "readyTimeoutMs": int(ready_timeout_ms),
            }
        )

    def status(self, *, session_id: str) -> dict[str, Any]:
        return self._request(
            {
                "action": "status",
                "sessionId": session_id,
            }
        )

    def close(self, *, session_id: str) -> dict[str, Any]:
        return self._request(
            {
                "action": "close",
                "sessionId": session_id,
            }
        )

    def helper_pid(self) -> int | None:
        with self._lock:
            if self._process is None:
                return None
            return self._process.pid

    def helper_running(self) -> bool:
        with self._lock:
            return self._process is not None and self._process.poll() is None

    def terminate(self) -> None:
        with self._lock:
            process = self._process
            self._process = None
            if process is None:
                return
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=5)

    def _request(self, payload: Mapping[str, Any]) -> dict[str, Any]:
        with self._lock:
            process = self._ensure_process()
            request_id = f"req-{next(self._request_ids)}"
            envelope = dict(payload)
            envelope["requestId"] = request_id
            assert process.stdin is not None
            assert process.stdout is not None
            process.stdin.write(json.dumps(envelope) + "\n")
            process.stdin.flush()
            while True:
                line = process.stdout.readline()
                if not line:
                    raise TorrentRuntimeError("Torrent helper exited unexpectedly")
                response = json.loads(line)
                if str(response.get("requestId") or "") != request_id:
                    continue
                if not response.get("ok"):
                    raise TorrentRuntimeError(str(response.get("error") or "Torrent runtime failure"))
                response.pop("ok", None)
                response.pop("requestId", None)
                return response

    def _ensure_process(self) -> subprocess.Popen[str]:
        if self._process is not None and self._process.poll() is None:
            return self._process
        node_binary = shutil.which("node")
        if not node_binary:
            raise TorrentRuntimeError("Node.js is required for torrent playback runtime")
        if self._process is not None and self._process.poll() is not None:
            self._process = None
        self.cache_root.mkdir(parents=True, exist_ok=True)
        self.helper_path.write_text(WEBTORRENT_HELPER_SOURCE, encoding="utf-8")
        if self._stderr_handle is None or self._stderr_handle.closed:
            self._stderr_handle = self.stderr_path.open("a", encoding="utf-8")
        self._process = subprocess.Popen(
            [node_binary, str(self.helper_path)],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=self._stderr_handle,
            text=True,
            encoding="utf-8",
            cwd=str(Path(__file__).resolve().parents[3]),
            bufsize=1,
        )
        return self._process
