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
import { mkdir } from 'node:fs/promises'
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
    const tick = () => {
      try {
        if (check()) {
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

function buildFilePayload(file, index, downloadDir) {
  return {
    index,
    name: file.name,
    path: file.path,
    length: Number(file.length || file.size || 0),
    downloaded: Number(file.downloaded || 0),
    progress: Number(file.progress || 0),
    mimeType: String(file.type || ''),
    localPath: path.join(downloadDir, file.path),
  }
}

function buildStatusPayload(entry) {
  const torrent = entry.torrent
  const selectedFile = entry.selectedFile
  return {
    torrentName: String(torrent?.name || ''),
    infoHash: String(torrent?.infoHash || ''),
    progress: Number(torrent?.progress || 0),
    downloadSpeed: Number(torrent?.downloadSpeed || 0),
    numPeers: Number(torrent?.numPeers || 0),
    complete: Boolean(torrent?.done),
    warning: String(entry.warning || ''),
    error: String(entry.error || ''),
    selectedFile: selectedFile ? buildFilePayload(selectedFile, entry.selectedIndex, entry.downloadDir) : null,
  }
}

async function startSession(command) {
  if (sessions.has(command.sessionId)) {
    return { status: buildStatusPayload(sessions.get(command.sessionId)) }
  }
  const downloadDir = path.resolve(String(command.downloadDir || '.'))
  await mkdir(downloadDir, { recursive: true })
  const torrent = client.add(String(command.magnet || ''), {
    path: downloadDir,
    strategy: 'sequential',
    destroyStoreOnDestroy: false,
  })
  const entry = {
    torrent,
    downloadDir,
    selectedFile: null,
    selectedIndex: -1,
    warning: '',
    error: '',
  }
  sessions.set(command.sessionId, entry)
  torrent.on('warning', error => {
    entry.warning = error instanceof Error ? error.message : String(error || '')
  })
  torrent.on('error', error => {
    entry.error = error instanceof Error ? error.message : String(error || '')
  })
  await waitFor(
    () => torrent.files && torrent.files.length > 0,
    Number(command.metadataTimeoutMs || 20000),
    'Torrent metadata timeout'
  )
  return {
    torrentName: String(torrent.name || ''),
    infoHash: String(torrent.infoHash || ''),
    files: torrent.files.map((file, index) => buildFilePayload(file, index, downloadDir)),
    status: buildStatusPayload(entry),
  }
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
      candidate.select(1)
    } else {
      candidate.deselect()
    }
  })
  entry.selectedFile = file
  entry.selectedIndex = files.indexOf(file)
  const minReadyBytes = Math.max(Number(command.minReadyBytes || 0), 0)
  await waitFor(
    () => Number(file.downloaded || 0) >= minReadyBytes || Boolean(file.done) || Boolean(entry.torrent.done) || Boolean(entry.error),
    Number(command.readyTimeoutMs || 45000),
    'Stream initialization failed'
  )
  if (entry.error) {
    throw new Error(entry.error)
  }
  return {
    selectedFile: buildFilePayload(file, entry.selectedIndex, entry.downloadDir),
    status: buildStatusPayload(entry),
  }
}

async function getStatus(command) {
  const entry = sessions.get(command.sessionId)
  if (!entry) {
    throw new Error('Playback session not found')
  }
  return { status: buildStatusPayload(entry) }
}

async function closeSession(command) {
  const entry = sessions.get(command.sessionId)
  if (!entry) {
    return { closed: true }
  }
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

    def start(self, *, session_id: str, magnet: str, download_dir: Path, metadata_timeout_ms: int) -> dict[str, Any]:
        return self._request(
            {
                "action": "start",
                "sessionId": session_id,
                "magnet": magnet,
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
