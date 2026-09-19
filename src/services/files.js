// @ts-nocheck - dynamic Docker/NBT/HTTP-JSON interop; not yet under checkJs (incremental typing).
'use strict';

// Scoped file manager. serverId scopes every operation to that server's data
// directory - which may be a folder under ./data or a Docker volume on another
// host; serverFs hides the difference. serverId = null is the global (admin)
// manager rooted at DATA_DIR itself, which is always panel-local.
//
// Every path is relative to the scope root and resolves through serverFs, so
// nothing can escape it.

const httpError = require('../utils/httpError');
const fsp = require('node:fs/promises');
const config = require('../config');
const db = require('../db');
const serverFs = require('../storage/serverFs');
const { recordEvent } = require('../events');
const indexer = require('../storage/indexer');
const servers = require('./servers');

const MAX_TEXT_BYTES = 8 * 1024 * 1024; // editor cap
const MAX_TEXT_MB_LABEL = '8 MB';
// "find in files" limits - keep a search over a big modpack dir bounded.
const SEARCH_MAX_MATCHES = 300;
const SEARCH_MAX_FILE_BYTES = 2 * 1024 * 1024;
const SEARCH_MAX_FILES_SCANNED = 8000;
const SEARCH_SKIP_DIRS = ['.git', 'node_modules', 'cache', '.cache', 'libraries', 'versions', 'crash-reports'];
// Global scope: panel-internal files at the DATA_DIR root that must never be read,
// written, listed, or downloaded from the UI - the database (password hashes + the
// at-rest secret cipher) and the session secret (that cipher's key + the cookie
// signing key). Any other top-level dotfile is treated the same, defensively.
const PROTECTED_GLOBAL = new Set(['panel.db', 'panel.db-wal', 'panel.db-shm', 'panel.db-journal', '.session-secret']);

/** True for a DATA_DIR-root path that must be hidden/blocked in the global manager. */
function isProtectedGlobal(rel) {
  return PROTECTED_GLOBAL.has(rel) || /^\.[^/\\]+$/.test(rel);
}

/** The filesystem handle for a scope: one server's data, or the whole data root. */
function scopeFor(serverId) {
  return serverId ? serverFs.for(serverId) : serverFs.forLocalRoot(config.dataDir, 'data-root');
}

/** Resolve a scope-relative path to { scope, rel }. Throws 400 on escape. */
function resolvePath(serverId, relPath = '') {
  return { scope: scopeFor(serverId), rel: serverFs.normalizeRel(relPath) };
}

function guardProtected(serverId, rel) {
  if (!serverId && isProtectedGlobal(rel)) {
    // Applies to read/download AND write - the DB holds password hashes and the
    // at-rest secret cipher, and .session-secret is that cipher's key, so neither
    // must ever leave (or change) via the file manager.
    throw httpError(403, 'That panel file is not accessible from the file manager');
  }
}

function parentOf(rel) {
  return rel.includes('/') ? rel.slice(0, rel.lastIndexOf('/')) : '';
}

function baseOf(rel) {
  return rel.includes('/') ? rel.slice(rel.lastIndexOf('/') + 1) : rel;
}

function join(dirRel, name) {
  return dirRel ? `${dirRel}/${name}` : name;
}

/** The key the size indexer files a directory under (always DATA_DIR-relative). */
function indexKey(serverId, rel) {
  return serverId ? `servers/${serverId}${rel ? `/${rel}` : ''}` : rel;
}

/** List a directory: entries {name, dir, size, mtime, mtimeMs, path}, dirs first. */
async function list(serverId, relPath = '') {
  const { scope, rel } = resolvePath(serverId, relPath);
  // Straight to the listing: a stat first would double the round trips of every
  // folder a remote server opens. The stat only runs to explain a failure.
  let listing;
  try {
    listing = await scope.readdir(rel);
  } catch {
    const st = await scope.statOrNull(rel);
    if (!st) throw httpError(404, 'Folder not found');
    if (!st.dir) throw httpError(400, 'Not a folder');
    throw httpError(404, 'Folder not found');
  }

  const entries = [];
  for (const e of listing) {
    // Never surface panel-internal files (DB, session secret) in the global manager.
    const childRel = join(rel, e.name);
    if (!serverId && isProtectedGlobal(childRel)) continue;
    // Folder sizes come from the background indexer instead of a live recursive
    // walk per folder on every listing - opening a folder with a multi-GB world
    // used to stat tens of thousands of files. Deep/not-yet-indexed dirs read 0
    // until the next scan; that's the instant-lookup trade-off.
    const size = e.dir ? indexer.sizeOf(indexKey(serverId, childRel)) : e.size;
    entries.push({
      name: e.name,
      dir: e.dir,
      size,
      mtimeMs: e.mtimeMs,
      mtime: formatWhen(e.mtimeMs),
      path: childRel,
    });
  }
  entries.sort((a, b) => b.dir - a.dir || a.name.localeCompare(b.name, undefined, { sensitivity: 'base' }));
  return { path: rel, entries };
}

/** Read a text file (≤ MAX_TEXT_BYTES; binary rejected by null-byte sniff). */
async function readText(serverId, relPath) {
  const { scope, rel } = resolvePath(serverId, relPath);
  guardProtected(serverId, rel);
  const st = await scope.statOrNull(rel);
  if (!st || st.dir) throw httpError(404, 'File not found');
  if (st.size > MAX_TEXT_BYTES) {
    throw httpError(
      413,
      `File is too large for the editor (${humanBytes(st.size)} - limit is ${MAX_TEXT_MB_LABEL}). Download it instead.`
    );
  }
  const buf = await scope.readFile(rel);
  if (buf.subarray(0, 8192).includes(0)) {
    throw httpError(415, 'This looks like a binary file - download it instead of editing');
  }
  return { content: buf.toString('utf8'), size: st.size };
}

/** Write a text file atomically (tmp + rename). Creates the file when missing. */
async function writeText(serverId, relPath, content, { actor = 'system' } = {}) {
  const { scope, rel } = resolvePath(serverId, relPath);
  guardProtected(serverId, rel);
  if (!rel) throw httpError(400, 'Cannot write the root');
  const bytes = Buffer.byteLength(content, 'utf8');
  if (bytes > MAX_TEXT_BYTES) throw httpError(413, `Content exceeds the ${MAX_TEXT_MB_LABEL} editor limit`);
  assertRoom(serverId, bytes);

  const parent = parentOf(rel);
  const pst = await scope.statOrNull(parent);
  if (!pst || !pst.dir) throw httpError(404, 'Parent folder not found');
  const existing = await scope.statOrNull(rel);
  if (existing && existing.dir) throw httpError(400, 'That path is a folder');

  // server.properties is the one file whose values the itzg image re-asserts
  // from env on every start. Route those writes through the server.properties
  // choke point so the matching env vars are un-set and the edit sticks - a
  // plain write would silently revert on the next restart.
  if (serverId && rel === 'server.properties' && servers.getServer(serverId)) {
    const result = await servers.writeServerProperties(serverId, content, { actor });
    recordEvent({
      serverId,
      actor,
      type: 'file-written',
      summary: `File ${existing ? 'saved' : 'created'}: server.properties (${humanBytes(bytes)}).`,
      details: {
        path: rel,
        sizeBytes: bytes,
        created: !existing,
        rebuildNeeded: result.rebuildNeeded,
        unlocked: result.unlocked,
      },
    });
    return { path: rel, size: bytes, rebuildNeeded: result.rebuildNeeded, unlocked: result.unlocked };
  }

  // Write beside the target and rename over it, so a half-written save can
  // never replace a good config file.
  const tmp = join(parent, `.msm-write-${Date.now()}.tmp`);
  await scope.writeFile(tmp, Buffer.from(content, 'utf8'));
  await scope.rename(tmp, rel);

  recordEvent({
    serverId: serverId || null,
    actor,
    type: 'file-written',
    summary: `File ${existing ? 'saved' : 'created'}: ${rel} (${humanBytes(bytes)}).`,
    details: { path: rel, sizeBytes: bytes, created: !existing },
  });
  return { path: rel, size: bytes };
}

async function mkdir(serverId, relPath, { actor = 'system' } = {}) {
  const { scope, rel } = resolvePath(serverId, relPath);
  if (!rel) throw httpError(400, 'Folder name cannot be empty');
  if (await scope.exists(rel)) throw httpError(409, 'That name already exists');
  await scope.mkdir(rel);
  recordEvent({
    serverId: serverId || null,
    actor,
    type: 'file-mkdir',
    summary: `Folder created: ${rel}.`,
    details: { path: rel },
  });
  return { path: rel };
}

/** Rename in place (newName must not contain path separators). */
async function rename(serverId, relPath, newName, { actor = 'system' } = {}) {
  const { scope, rel } = resolvePath(serverId, relPath);
  guardProtected(serverId, rel);
  if (!rel) throw httpError(400, 'Cannot rename the root');
  const clean = sanitizeName(newName);
  if (!(await scope.exists(rel))) throw httpError(404, 'Not found');
  const target = join(parentOf(rel), clean);
  if (target !== rel && (await scope.exists(target))) throw httpError(409, `"${clean}" already exists here`);
  await scope.rename(rel, target);
  recordEvent({
    serverId: serverId || null,
    actor,
    type: 'file-renamed',
    summary: `Renamed: ${rel} → ${clean}.`,
    details: { from: rel, to: clean },
  });
  return { path: target };
}

/** Move into a destination directory (keeps the base name). */
async function move(serverId, relPath, destRel, { actor = 'system' } = {}) {
  const { scope, rel } = resolvePath(serverId, relPath);
  guardProtected(serverId, rel);
  if (!rel) throw httpError(400, 'Cannot move the root');
  const dest = serverFs.normalizeRel(destRel);
  const dst = await scope.statOrNull(dest);
  if (!(await scope.exists(rel))) throw httpError(404, 'Not found');
  if (!dst || !dst.dir) throw httpError(400, 'Destination folder not found');
  if (dest === rel || dest.startsWith(`${rel}/`)) throw httpError(400, 'Cannot move a folder into itself');

  const name = baseOf(rel);
  const target = join(dest, name);
  if (await scope.exists(target)) throw httpError(409, `"${name}" already exists in the destination`);
  await scope.rename(rel, target);
  recordEvent({
    serverId: serverId || null,
    actor,
    type: 'file-moved',
    summary: `Moved: ${rel} → ${target}.`,
    details: { from: rel, to: target },
  });
  return { path: target };
}

/** Copy into a destination directory (recursive; quota-checked). */
async function copy(serverId, relPath, destRel, { actor = 'system' } = {}) {
  const { scope, rel } = resolvePath(serverId, relPath);
  if (!rel) throw httpError(400, 'Cannot copy the root');
  const dest = serverFs.normalizeRel(destRel);
  const st = await scope.statOrNull(rel);
  const dst = await scope.statOrNull(dest);
  if (!st) throw httpError(404, 'Not found');
  if (!dst || !dst.dir) throw httpError(400, 'Destination folder not found');
  if (dest === rel || dest.startsWith(`${rel}/`)) throw httpError(400, 'Cannot copy a folder into itself');

  const bytes = st.dir ? (await scope.du(rel)).size : st.size;
  assertRoom(serverId, bytes);
  await assertDiskFree(bytes);

  const name = baseOf(rel);
  const target = join(dest, name);
  if (await scope.exists(target)) throw httpError(409, `"${name}" already exists in the destination`);
  await scope.copy(rel, target);
  recordEvent({
    serverId: serverId || null,
    actor,
    type: 'file-copied',
    summary: `Copied: ${rel} → ${target} (${humanBytes(bytes)}).`,
    details: { from: rel, to: target, sizeBytes: bytes },
  });
  indexer.scheduleScan();
  return { path: target, sizeBytes: bytes };
}

/** Delete a file or folder (recursive). Returns freed bytes. */
async function remove(serverId, relPath, { actor = 'system' } = {}) {
  const { scope, rel } = resolvePath(serverId, relPath);
  guardProtected(serverId, rel);
  if (!rel) throw httpError(400, 'Cannot delete the root folder');
  const st = await scope.statOrNull(rel);
  if (!st) throw httpError(404, 'Not found');
  const freedBytes = st.dir ? (await scope.du(rel)).size : st.size;
  await scope.remove(rel);
  recordEvent({
    serverId: serverId || null,
    actor,
    type: 'file-deleted',
    summary: `Deleted: ${rel} (${humanBytes(freedBytes)} freed).`,
    details: { path: rel, freedBytes },
  });
  indexer.scheduleScan();
  return { freedBytes };
}

/** Take an uploaded tmp file into a target directory (used by the upload routes). */
async function acceptUpload(serverId, destRel, tmpAbs, originalName, { actor = 'system' } = {}) {
  const { scope, rel: dest } = resolvePath(serverId, destRel);
  const dst = await scope.statOrNull(dest);
  if (!dst || !dst.dir) throw httpError(400, 'Destination folder not found');
  const filename = sanitizeName(originalName || 'upload.bin');
  const size = (await fsp.stat(tmpAbs)).size;
  assertRoom(serverId, size);

  const rel = join(dest, filename);
  try {
    await scope.uploadFile(tmpAbs, rel);
  } finally {
    // The upload lands in data/tmp first either way, so it is ours to clean up
    // whether the transfer succeeded or not.
    await fsp.rm(tmpAbs, { force: true }).catch(() => {});
  }
  recordEvent({
    serverId: serverId || null,
    actor,
    type: 'file-uploaded',
    summary: `Uploaded: ${rel} (${humanBytes(size)}).`,
    details: { path: rel, sizeBytes: size },
  });
  indexer.scheduleScan();
  return { path: rel, name: filename, size };
}

/** Metadata for a download (files only). */
async function statFile(serverId, relPath) {
  const { scope, rel } = resolvePath(serverId, relPath);
  guardProtected(serverId, rel); // no downloading panel.db either
  const st = await scope.statOrNull(rel);
  if (!st || st.dir) throw httpError(404, 'File not found');
  return { scope, rel, size: st.size, name: baseOf(rel) };
}

/**
 * Open a download: { stream, name, size }. A server whose files live on the
 * Docker host streams them through the daemon, so the route never needs a path
 * on this machine.
 */
async function openDownload(serverId, relPath) {
  const file = await statFile(serverId, relPath);
  return { stream: await file.scope.readStream(file.rel), name: file.name, size: file.size };
}

// ---------------------------------------------------------------------------

function assertRoom(serverId, aboutToAddBytes) {
  if (!serverId) return;
  const server = db.get('SELECT * FROM servers WHERE id = ? AND deleted_at IS NULL', serverId);
  if (server) indexer.assertUnderQuota(server, aboutToAddBytes);
}

async function assertDiskFree(bytes) {
  const { free } = await indexer.diskFree().catch(() => ({ free: Infinity }));
  if (free < bytes * 1.1) throw httpError(507, `Not enough disk space (~${humanBytes(bytes)} needed)`);
}

function sanitizeName(name) {
  const clean = String(name || '')
    .replace(/[\\/:*?"<>|\0]/g, '_')
    .replace(/^\.+$/, '')
    .trim()
    .slice(0, 180);
  if (!clean || clean === '.' || clean === '..') throw httpError(400, 'Invalid name');
  return clean;
}

function formatWhen(ms) {
  if (!ms) return '-';
  const d = new Date(ms);
  const pad = (n) => String(n).padStart(2, '0');
  return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())} ${pad(d.getHours())}:${pad(d.getMinutes())}`;
}

function humanBytes(n) {
  if (n >= 1024 ** 3) return `${(n / 1024 ** 3).toFixed(1)} GB`;
  if (n >= 1024 ** 2) return `${(n / 1024 ** 2).toFixed(1)} MB`;
  return `${Math.max(1, Math.round(n / 1024))} KB`;
}

/**
 * Grep for a plain substring across text files under the scope. Bounded on every
 * axis (files scanned, per-file size, total matches) so a search over a big
 * modpack tree can't run away. Returns { query, matches:[{path,line,col,text}],
 * truncated, filesScanned }.
 */
async function searchFiles(serverId, query, { caseSensitive = false, subdir = '' } = {}) {
  const needle = String(query || '');
  if (needle.length < 2) throw httpError(400, 'Enter at least 2 characters to search for.');
  if (needle.length > 200) throw httpError(400, 'Search text is too long.');
  const { scope, rel: rootRel } = resolvePath(serverId, subdir);
  const rootStat = await scope.statOrNull(rootRel);
  if (!rootStat || !rootStat.dir) throw httpError(404, 'Folder not found');

  const found = await scope.grepFiles(rootRel, needle, {
    caseSensitive,
    skipDirs: SEARCH_SKIP_DIRS,
    maxMatches: SEARCH_MAX_MATCHES,
    maxFileBytes: SEARCH_MAX_FILE_BYTES,
    maxFiles: SEARCH_MAX_FILES_SCANNED,
  });
  // Paths come back relative to the searched folder; the client opens them
  // relative to the manager's root.
  const matches = found.matches
    .filter((m) => serverId || !isProtectedGlobal(join(rootRel, m.path)))
    .map((m) => ({ ...m, path: join(rootRel, m.path) }));
  return { query: needle, matches, truncated: found.truncated, filesScanned: found.filesScanned };
}

module.exports = {
  list,
  readText,
  writeText,
  searchFiles,
  mkdir,
  rename,
  move,
  copy,
  remove,
  acceptUpload,
  statFile,
  openDownload,
  resolvePath,
  assertRoom,
  assertDiskFree,
};
