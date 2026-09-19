// @ts-nocheck - dynamic Docker/NBT/HTTP-JSON interop; not yet under checkJs (incremental typing).
'use strict';

// TOTAL WORLD MANAGEMENT. The world library (./data/library/worlds) plus every
// per-server world operation: import archives (smart root detection), extract
// consistent snapshots from live servers, install/replace/alongside, copy
// between instances, duplicate/rename/reset, downloads.
//
// Archive normalization: every library world zip has the world root at the top
// level (level.dat at the zip root). Bukkit-split dimension dirs travel as
// top-level sibling directories named `<base>_nether` / `<base>_the_end`.

const httpError = require('../utils/httpError');
const fs = require('node:fs');
const fsp = require('node:fs/promises');
const path = require('node:path');
const crypto = require('node:crypto');
const zlib = require('node:zlib');
const archiver = require('archiver');
const { extractZip, MAX_EXTRACT_BYTES, MAX_EXTRACT_ENTRIES } = require('../utils/zip');
const tar = require('tar');
const { nanoid } = require('nanoid');
const db = require('../db');
const { dataPath } = require('../storage/pathGuard');
const serverFs = require('../storage/serverFs');
const { recordEvent } = require('../events');
const { execCapture, inspectStatus } = require('../docker/containers');
const { serializeError } = require('../utils/logSanitize');
const indexer = require('../storage/indexer');
const library = require('./library');
const { withSaveLock } = require('./serverLocks');
const { guardOp } = require('./opLock');
const logger = require('../logger')(path.basename(__filename));

// Run the save-off/flush → copy → save-on dance under the shared per-server save
// lock when the server is running, so it can't overlap a concurrent backup or
// world export and tear the copy. When stopped, just run the copy directly.
async function withPausedSaves(serverId, running, copy, actor = 'system') {
  if (!running) return copy();
  return withSaveLock(serverId, async () => {
    await execCapture(serverId, ['rcon-cli', 'save-off']).catch((err) => {
      logger.warn('Pausing world saves before a world operation failed; the copy may be slightly inconsistent.', {
        serverId,
        err: serializeError(err, { includeStack: false }),
      });
    });
    await execCapture(serverId, ['rcon-cli', 'save-all', 'flush']).catch((err) => {
      logger.warn('Flushing world saves before a world operation failed; the copy may be slightly inconsistent.', {
        serverId,
        err: serializeError(err, { includeStack: false }),
      });
    });
    await sleep(2000); // let region writes settle
    try {
      return await copy();
    } finally {
      // save-on MUST succeed - if it is swallowed here the server would be left
      // with world saves paused and nobody told. Surface it loudly (mirrors the
      // backup path).
      try {
        await execCapture(serverId, ['rcon-cli', 'save-on']);
      } catch (err) {
        logger.error(
          'Re-enabling world saves after a world operation failed: the server may still have saves paused.',
          { serverId, err: serializeError(err, { includeStack: false }) }
        );
        recordEvent({
          serverId,
          actor,
          type: 'world-save-warning',
          summary:
            'World saves were not re-enabled after a world operation - check the server console and run save-on.',
        });
      }
    }
  });
}

const DIM_SUFFIXES = ['_nether', '_the_end'];

const FLAVOR_LABEL = {
  VANILLA: 'Vanilla',
  PAPER: 'Paper',
  PURPUR: 'Purpur',
  PUFFERFISH: 'Pufferfish',
  FOLIA: 'Folia',
  LEAF: 'Leaf',
  SPIGOT: 'Spigot',
  BUKKIT: 'Bukkit',
  FABRIC: 'Fabric',
  FORGE: 'Forge',
  NEOFORGE: 'NeoForge',
  QUILT: 'Quilt',
  AUTO_CURSEFORGE: 'CurseForge pack',
  MODRINTH: 'Modrinth pack',
  FTBA: 'FTB pack',
  CUSTOM: 'Custom jar',
};

// Server-type family - used for compat warnings (Bukkit-family splits worlds
// into three dirs; modded worlds carry loader-specific dimensions/data).
const FAMILY = {
  PAPER: 'bukkit',
  PURPUR: 'bukkit',
  SPIGOT: 'bukkit',
  BUKKIT: 'bukkit',
  FOLIA: 'bukkit',
  LEAF: 'bukkit',
  PUFFERFISH: 'bukkit',
  FABRIC: 'modded',
  FORGE: 'modded',
  NEOFORGE: 'modded',
  QUILT: 'modded',
  AUTO_CURSEFORGE: 'modded',
  MODRINTH: 'modded',
  FTBA: 'modded',
};
const familyOf = (type) => FAMILY[type] || 'vanilla';
const flavorLabel = (type) => FLAVOR_LABEL[type] || type;

// ---------------------------------------------------------------------------
// World root detection

/**
 * Find the world root inside an extracted archive: the shallowest directory
 * containing a level.dat (handles nested single-folder wrappers and level.dat
 * anywhere in the tree). Detects Bukkit-split layouts (sibling <name>_nether /
 * <name>_the_end directories next to the main world).
 * @returns {Promise<{rootAbs:string, split:boolean, dims:string[]}|null>}
 *          dims[0] is always the main root; extras are split dimension dirs.
 */
async function detectWorldRoot(extractedDir) {
  let queue = [path.resolve(extractedDir)];
  let found = null;

  while (queue.length && !found) {
    const next = [];
    const candidates = [];
    for (const dir of queue) {
      let entries;
      try {
        entries = await fsp.readdir(dir, { withFileTypes: true });
      } catch {
        continue;
      }
      if (entries.some((e) => e.isFile() && e.name.toLowerCase() === 'level.dat')) {
        candidates.push(dir); // don't descend into a found world
        continue;
      }
      for (const e of entries) {
        if (e.isDirectory() && !e.isSymbolicLink()) next.push(path.join(dir, e.name));
      }
    }
    if (candidates.length) {
      // Prefer a candidate that isn't itself a Bukkit dimension dir.
      found = candidates.find((c) => !isDimName(path.basename(c))) || candidates[0];
    }
    queue = next;
  }
  if (!found) return null;

  const dims = [found];
  let split = false;
  if (found !== path.resolve(extractedDir)) {
    const base = path.basename(found);
    const parent = path.dirname(found);
    for (const suffix of DIM_SUFFIXES) {
      const sibling = path.join(parent, base + suffix);
      try {
        if ((await fsp.stat(sibling)).isDirectory()) {
          dims.push(sibling);
          split = true;
        }
      } catch {
        /* no such sibling */
      }
    }
  }
  return { rootAbs: found, split, dims };
}

// ---------------------------------------------------------------------------
// Import (upload) into the library

/**
 * Import an uploaded world archive (.zip / .mcworld / .tar / .tar.gz) into the
 * library: extract to tmp, detect the world root, normalize into a fresh zip
 * under library/worlds, hash, and record a library_files row.
 */
async function importArchive(
  uploadPath,
  { name = '', originalName = '', actor = 'system', flavor = null, source = 'upload', onProgress = () => {} } = {}
) {
  const stat = await fsp.stat(uploadPath).catch(() => null);
  if (!stat || !stat.isFile()) throw httpError(400, 'Upload not found - try again');

  // Free-space preflight: extraction + re-zip can need ~3x the archive size.
  const { free } = await indexer.diskFree();
  if (free < stat.size * 3) {
    throw httpError(507, `Not enough disk space to import this world (~${humanBytes(stat.size * 3)} needed)`);
  }

  const tmpDir = dataPath('tmp', `world-import-${nanoid(6)}`);
  const zipTmp = dataPath('tmp', `world-norm-${nanoid(6)}.zip`);
  await fsp.mkdir(tmpDir, { recursive: true });

  try {
    onProgress({ stage: 'extract' });
    await extractArchive(uploadPath, tmpDir, originalName);

    const detected = await detectWorldRoot(tmpDir);
    if (!detected) {
      throw httpError(400, "No level.dat found - this doesn't look like a Minecraft world");
    }

    const mcVersion = readLevelVersion(path.join(detected.rootAbs, 'level.dat'));

    onProgress({ stage: 'pack' });
    await zipWorld(zipTmp, detected.rootAbs, detected.dims.slice(1));

    const worldName =
      (name || '').trim() ||
      path.basename(originalName || '', path.extname(originalName || '')) ||
      path.basename(detected.rootAbs) ||
      'Imported world';

    const row = await addZipToLibrary(zipTmp, {
      name: worldName,
      actor,
      worldSource: source,
      worldFlavor: flavor,
      mcVersion,
      split: detected.split,
    });
    return row;
  } finally {
    await fsp.rm(tmpDir, { recursive: true, force: true }).catch(() => {});
    await fsp.rm(zipTmp, { force: true }).catch(() => {});
    await fsp.rm(uploadPath, { force: true }).catch(() => {});
  }
}

/** Move a finished world zip into library/worlds + insert the DB row (dedup by hash). */
async function addZipToLibrary(zipAbs, { name, actor, worldSource, worldFlavor, mcVersion, split }) {
  const sha256 = await sha256File(zipAbs);
  const existing = db.get("SELECT * FROM library_files WHERE sha256 = ? AND category = 'world'", sha256);
  if (existing) {
    await fsp.rm(zipAbs, { force: true });
    return existing;
  }

  const filename = `${sanitizeFilename(name)}.zip`;
  const relPath = `${library.CATEGORY_DIR.world}/${sha256.slice(0, 8)}-${filename}`;
  await fsp.mkdir(path.dirname(dataPath(relPath)), { recursive: true });
  await moveFile(zipAbs, dataPath(relPath));
  const size = (await fsp.stat(dataPath(relPath))).size;

  const id = `lib_${nanoid(8)}`;
  db.run(
    `INSERT INTO library_files (id, category, name, filename, rel_path, sha256, size_bytes,
       platform, version, mc_versions_json, loaders_json, world_source, world_flavor)
     VALUES (?, 'world', ?, ?, ?, ?, ?, 'upload', ?, ?, '[]', ?, ?)`,
    id,
    name,
    filename,
    relPath,
    sha256,
    size,
    mcVersion || null,
    JSON.stringify(mcVersion ? [mcVersion] : []),
    worldSource || 'upload',
    worldFlavor || null
  );
  recordEvent({
    actor,
    type: 'world-library-added',
    summary: `World added to library: ${name} (${humanBytes(size)}).`,
    details: { id, sha256, sizeBytes: size, split: Boolean(split), mcVersion: mcVersion || null, source: worldSource },
  });
  indexer.scheduleScan();
  return db.get('SELECT * FROM library_files WHERE id = ?', id);
}

// ---------------------------------------------------------------------------
// Extract from a server (consistent snapshot)

/**
 * Snapshot a server's active world (plus Bukkit-split dims) into the library.
 * Works while the server runs - wraps the copy in save-off/save-all/save-on.
 */
async function extractFromServer(serverId, { name = '', actor = 'system' } = {}) {
  const server = mustServer(serverId);
  const handle = serverFs.for(serverId);
  const level = await activeLevelName(server);
  const dims = await serverWorldDims(serverId, level);
  if (!(await handle.exists(`${dims[0]}/level.dat`))) {
    throw httpError(404, `World "${level}" has no level.dat yet - start the server once so it generates the world`);
  }

  const worldBytes = await serverDirsSize(serverId, dims);
  const { free } = await indexer.diskFree();
  if (free < worldBytes * 2.2) {
    throw httpError(507, `Not enough disk space to snapshot this world (~${humanBytes(worldBytes * 2.2)} needed)`);
  }

  const running = await isRunning(serverId);
  const tmpDir = dataPath('tmp', `world-snap-${nanoid(6)}`);
  const zipTmp = dataPath('tmp', `world-snap-${nanoid(6)}.zip`);
  await fsp.mkdir(tmpDir, { recursive: true });

  try {
    // Consistent copy: pause saves, flush, copy to tmp, resume - then zip at leisure.
    await withPausedSaves(
      serverId,
      running,
      async () => {
        // Pulls the files onto this machine when the server lives on another
        // Docker host; a plain copy when they are already here.
        for (const dim of dims) {
          await handle.downloadDir(dim, path.join(tmpDir, dim));
        }
      },
      actor
    );

    const mainCopy = path.join(tmpDir, level);
    const dimCopies = dims.slice(1).map((d) => path.join(tmpDir, d));
    await zipWorld(zipTmp, mainCopy, dimCopies);

    const mcVersion =
      readLevelVersion(path.join(mainCopy, 'level.dat')) ||
      (server.mc_version !== 'LATEST' && server.mc_version !== 'SNAPSHOT' ? server.mc_version : null);

    const row = await addZipToLibrary(zipTmp, {
      name: (name || '').trim() || `${server.display_name} - ${level}`,
      actor,
      worldSource: `extract:${serverId}`,
      worldFlavor: server.type,
      mcVersion,
      split: dims.length > 1,
    });
    recordEvent({
      serverId,
      actor,
      type: 'world-extracted',
      summary: `World "${level}" saved to library as "${row.name}" (${humanBytes(row.size_bytes)}).`,
      details: { libraryId: row.id, level, sizeBytes: row.size_bytes, running },
    });
    return row;
  } finally {
    await fsp.rm(tmpDir, { recursive: true, force: true }).catch(() => {});
    await fsp.rm(zipTmp, { force: true }).catch(() => {});
  }
}

// ---------------------------------------------------------------------------
// Per-server world listing

/**
 * Scan a server dir for worlds (top-level dirs containing level.dat), grouping
 * Bukkit-split dims under their main world and marking the active one.
 *
 * `sizes` picks where the byte counts come from: 'live' measures the world
 * dirs now (the worlds tab, where a size that just changed has to show), and
 * 'index' reads the storage index instead, which is a DB lookup and no walk at
 * all - right for a page that only prints the numbers (the metrics tab).
 * @returns [{name, active, dims:[names], sizeBytes, seed}]
 */
async function listServerWorlds(serverId, { sizes = 'live' } = {}) {
  const cached = cachedWorlds(serverId, sizes);
  if (cached) return cached;
  const server = mustServer(serverId);
  const handle = serverFs.for(serverId);
  // ONE call for the listing, the per-world sizes and which dirs hold a
  // level.dat: separately that is three round trips, and against a remote
  // daemon each one costs more than the work it asks for. server.properties
  // (the active world and its seed) is independent, so it rides along in
  // parallel rather than adding a round trip of its own.
  const [props, entries] = await Promise.all([
    readProps(serverId),
    handle.readdirSized('', { contains: ['level.dat'], withSizes: sizes === 'live' }).catch(() => null),
  ]);
  const level = (server.env && server.env.LEVEL) || props.get('level-name') || 'world';
  if (!entries) return [];
  const dirNames = new Set(entries.filter((e) => e.dir).map((e) => e.name));
  const liveSizes = new Map(entries.map((e) => [e.name, e.sizeBytes]));
  const withLevelDat = entries.filter((e) => e.dir && e.contains.includes('level.dat')).map((e) => e.name);

  // A dir is a split dim (not its own world) when its base world also exists.
  const mains = withLevelDat.filter((n) => {
    const m = dimBase(n);
    return !(m && dirNames.has(m) && withLevelDat.includes(m));
  });

  const indexer = sizes === 'index' ? require('../storage/indexer') : null;
  const sizeOf = (dim) => (indexer ? indexer.sizeOf(`servers/${serverId}/${dim}`) : liveSizes.get(dim) || 0);

  const worlds = [];
  for (const main of mains) {
    const dimNames = [main, ...DIM_SUFFIXES.map((s) => main + s).filter((d) => dirNames.has(d))];
    const sizeBytes = dimNames.reduce((n, dim) => n + sizeOf(dim), 0);
    const active = main === level;
    worlds.push({
      name: main,
      active,
      dims: dimNames,
      sizeBytes,
      seed: active ? props.get('level-seed') || null : null,
    });
  }
  worlds.sort((a, b) => b.active - a.active || a.name.localeCompare(b.name));
  rememberWorlds(serverId, sizes, worlds);
  return worlds;
}

// The listing is what the worlds tab, the metrics tab and the worlds API all
// ask for, and on a remote host it is a directory walk on the Docker host. Hold
// it for a few seconds - long enough that one page render (and a reload right
// behind it) costs one walk, short enough that nothing shows a stale world for
// long. Every world operation in this file drops it outright, so the TTL only
// ever covers changes made outside the panel.
const worldsCache = new Map(); // `${serverId}|${sizes}` -> { at, worlds }
const WORLDS_TTL_MS = 5000;

function cachedWorlds(serverId, sizes) {
  const hit = worldsCache.get(`${serverId}|${sizes}`);
  return hit && Date.now() - hit.at < WORLDS_TTL_MS ? hit.worlds : null;
}

function rememberWorlds(serverId, sizes, worlds) {
  worldsCache.set(`${serverId}|${sizes}`, { at: Date.now(), worlds });
}

/** Drop the cached world listing for a server (after anything that changes it). */
function forgetWorlds(serverId) {
  for (const key of worldsCache.keys()) {
    if (key.startsWith(`${serverId}|`)) worldsCache.delete(key);
  }
}

// ---------------------------------------------------------------------------
// Install from library

/** Compat warnings for installing library world `libraryId` into `serverId`. */
function installWarnings(libraryId, serverId) {
  const lib = mustLibWorld(libraryId);
  const server = mustServer(serverId);
  return compatWarnings({ flavor: lib.world_flavor, version: lib.version }, server);
}

function compatWarnings(world, server) {
  const warnings = [];
  if (world.flavor && familyOf(world.flavor) !== familyOf(server.type)) {
    warnings.push(
      `This world came from a ${flavorLabel(world.flavor)} server but the target runs ${flavorLabel(server.type)} - ` +
        'loader- or plugin-specific data (custom dimensions, plugin files) may not load.'
    );
  }
  const target = server.mc_version;
  if (world.version && target && target !== 'LATEST' && target !== 'SNAPSHOT' && world.version !== target) {
    if (compareVersions(world.version, target) > 0) {
      warnings.push(
        `The world was last played on Minecraft ${world.version} but this server runs ${target} - ` +
          'Minecraft cannot downgrade worlds safely; expect corruption or a refusal to load.'
      );
    } else {
      warnings.push(
        `Version differs: world ${world.version} → server ${target}. The world will be upgraded on first load and cannot be downgraded afterwards.`
      );
    }
  }
  return warnings;
}

/**
 * Install a library world into a server.
 * mode 'replace': requires the server stopped; safety backup, then the active
 *                 world dirs are replaced in place (level-name unchanged).
 * mode 'alongside': extracts under `newName` next to existing worlds - switch
 *                   with activateWorld later. Safe while running.
 */
async function installToServerImpl(libraryId, serverId, { mode = 'replace', newName = '', actor = 'system' } = {}) {
  const lib = mustLibWorld(libraryId);
  const server = mustServer(serverId);
  const warnings = compatWarnings({ flavor: lib.world_flavor, version: lib.version }, server);

  // Disk-growing op: quota + free space first (extracted ≈ up to ~2x the zip).
  indexer.assertUnderQuota(server, lib.size_bytes * 2);
  const { free } = await indexer.diskFree();
  if (free < lib.size_bytes * 2.5) {
    throw httpError(507, `Not enough disk space to install this world (~${humanBytes(lib.size_bytes * 2.5)} needed)`);
  }

  let targetLevel;
  if (mode === 'replace') {
    if (await isRunning(serverId)) {
      throw httpError(
        409,
        'Stop the server before replacing its active world - swapping it while running would corrupt the save'
      );
    }
    targetLevel = await activeLevelName(server);
    const { createBackupUnguarded } = require('./backups');
    // Already inside the 'install' op lock - the guarded form would 409 against it.
    await createBackupUnguarded(serverId, {
      reason: 'manual',
      actor,
      note: `Safety backup before installing world "${lib.name}"`,
    });
  } else {
    targetLevel = sanitizeWorldName(newName || lib.name);
    if (await serverFs.for(serverId).exists(targetLevel)) {
      throw httpError(409, `A world named "${targetLevel}" already exists on this server - pick another name`);
    }
  }

  const tmpDir = dataPath('tmp', `world-install-${nanoid(6)}`);
  await fsp.mkdir(tmpDir, { recursive: true });
  let replacedBytes = 0;
  // Reserved for the same reason as backups.js's createBackup/restoreBackup:
  // stop a second concurrent disk-growing op from passing its own preflight
  // against the same real free bytes this extraction is about to consume.
  const releaseReservation = indexer.reserveDiskSpace(lib.size_bytes * 2);
  try {
    await extractZip(dataPath(lib.rel_path), tmpDir);

    const tops = await fsp.readdir(tmpDir, { withFileTypes: true });
    const dimTops = tops.filter((e) => e.isDirectory() && isDimName(e.name));
    const mainTops = tops.filter((e) => !dimTops.includes(e));

    if (mode === 'replace') {
      const handle = serverFs.for(serverId);
      for (const dim of await serverWorldDims(serverId, targetLevel)) {
        replacedBytes += await serverDirsSize(serverId, [dim]);
        await handle.remove(dim);
      }
    }

    const handle = serverFs.for(serverId);
    await handle.mkdir(targetLevel);
    for (const e of mainTops) {
      if (e.isDirectory()) await handle.uploadDir(path.join(tmpDir, e.name), `${targetLevel}/${e.name}`);
      else await handle.uploadFile(path.join(tmpDir, e.name), `${targetLevel}/${e.name}`);
    }
    for (const e of dimTops) {
      const suffix = e.name.endsWith('_the_end') ? '_the_end' : '_nether';
      await handle.uploadDir(path.join(tmpDir, e.name), targetLevel + suffix);
    }
  } finally {
    releaseReservation();
    await fsp.rm(tmpDir, { recursive: true, force: true }).catch(() => {});
  }

  const sizeBytes = await serverDirsSize(serverId, await serverWorldDims(serverId, targetLevel));
  recordEvent({
    serverId,
    actor,
    type: 'world-installed',
    summary: `World "${lib.name}" installed as "${targetLevel}" (${mode}, ${humanBytes(sizeBytes)}).`,
    details: { libraryId, mode, installedAs: targetLevel, sizeBytes, replacedBytes, warnings },
  });
  logger.info('Installed a world onto a server.', { serverId, actor, installedAs: targetLevel, mode, sizeBytes });
  forgetWorlds(serverId);
  indexer.scheduleScan();
  return { installedAs: targetLevel, mode, warnings, sizeBytes };
}

// Guarded under the shared per-server op lock (keyed on serverId, the 2nd
// arg here) so it can't interleave with a concurrent start/stop/recreate/
// delete/restore/another world op - the 'replace' mode's isRunning() check
// above and its destructive directory work must be atomic with respect to
// those, or a start landing in between corrupts the live world.
const installToServer = guardOp('install', installToServerImpl, (_libraryId, serverId) => serverId);

/** Warnings for a server→server copy (source world flavor/version vs target). */
async function copyWarnings(sourceServerId, targetServerId) {
  const source = mustServer(sourceServerId);
  const target = mustServer(targetServerId);
  const level = await activeLevelName(source);
  const version =
    (await readServerLevelVersion(sourceServerId, level)) ||
    (source.mc_version !== 'LATEST' && source.mc_version !== 'SNAPSHOT' ? source.mc_version : null);
  return compatWarnings({ flavor: source.type, version }, target);
}

/**
 * Copy the active world from one server to another via the library machinery:
 * snapshot source (works while running) → install into target.
 */
async function copyBetweenServers(
  sourceServerId,
  targetServerId,
  { mode = 'replace', newName = '', actor = 'system' } = {}
) {
  const source = mustServer(sourceServerId);
  const target = mustServer(targetServerId);
  if (sourceServerId === targetServerId)
    throw httpError(400, 'Source and target are the same server - use Duplicate instead');

  const row = await extractFromServer(sourceServerId, {
    name: `${source.display_name} → ${target.display_name} (copy)`,
    actor,
  });
  const result = await installToServer(row.id, targetServerId, { mode, newName, actor });
  recordEvent({
    serverId: targetServerId,
    actor,
    type: 'world-copied',
    summary: `World copied from ${source.display_name} (${humanBytes(result.sizeBytes)}, ${mode}).`,
    details: { sourceServerId, libraryId: row.id, ...result },
  });
  forgetWorlds(targetServerId);
  return { library: row, ...result };
}

// ---------------------------------------------------------------------------
// Duplicate / rename / activate / reset / delete

/** Fork a copy of a world within the same server (consistent while running). */
async function duplicateWorld(serverId, worldName, { actor = 'system' } = {}) {
  const server = mustServer(serverId);
  checkWorldName(worldName);
  const handle = serverFs.for(serverId);
  const dims = await serverWorldDims(serverId, worldName);
  if (!(await handle.exists(dims[0]))) throw httpError(404, `No world named "${worldName}" on this server`);

  let copyName = `${worldName}-copy`;
  for (let i = 2; await handle.exists(copyName); i++) copyName = `${worldName}-copy${i}`;

  const sizeBytes = await serverDirsSize(serverId, dims);
  indexer.assertUnderQuota(server, sizeBytes);
  const { free } = await indexer.diskFree();
  if (free < sizeBytes * 1.1)
    throw httpError(507, `Not enough disk space to duplicate (~${humanBytes(sizeBytes)} needed)`);

  const active = worldName === (await activeLevelName(server));
  const running = active && (await isRunning(serverId));
  const releaseReservation = indexer.reserveDiskSpace(sizeBytes);
  try {
    await withPausedSaves(
      serverId,
      running,
      async () => {
        for (const dim of dims) {
          await handle.copy(dim, copyName + dim.slice(worldName.length));
        }
      },
      actor
    );
  } finally {
    releaseReservation();
  }

  recordEvent({
    serverId,
    actor,
    type: 'world-duplicated',
    summary: `World "${worldName}" duplicated as "${copyName}" (${humanBytes(sizeBytes)}).`,
    details: { worldName, copyName, sizeBytes },
  });
  forgetWorlds(serverId);
  indexer.scheduleScan();
  return { name: copyName, sizeBytes };
}

/** Rename a world (server must be stopped); updates level-name/LEVEL when active. */
async function renameWorldImpl(serverId, worldName, newName, { actor = 'system' } = {}) {
  const server = mustServer(serverId);
  checkWorldName(worldName);
  const clean = sanitizeWorldName(newName);
  if (await isRunning(serverId)) throw httpError(409, 'Stop the server before renaming worlds');
  const handle = serverFs.for(serverId);
  const dims = await serverWorldDims(serverId, worldName);
  if (!(await handle.exists(dims[0]))) throw httpError(404, `No world named "${worldName}" on this server`);
  if (await handle.exists(clean)) {
    throw httpError(409, `A world named "${clean}" already exists on this server`);
  }

  for (const dim of dims) {
    await handle.rename(dim, clean + dim.slice(worldName.length));
  }

  const wasActive = worldName === (await activeLevelName(server));
  if (wasActive) await setActiveLevel(server, clean, { actor });

  recordEvent({
    serverId,
    actor,
    type: 'world-renamed',
    summary: `World "${worldName}" renamed to "${clean}"${wasActive ? ' (active world - level-name updated)' : ''}.`,
    details: { from: worldName, to: clean, wasActive },
  });
  forgetWorlds(serverId);
  return { name: clean, wasActive };
}

const renameWorld = guardOp('rename', renameWorldImpl);

/** Make a world the active one (sets level-name / LEVEL). Server must be stopped. */
async function activateWorldImpl(serverId, worldName, { actor = 'system' } = {}) {
  const server = mustServer(serverId);
  checkWorldName(worldName);
  if (await isRunning(serverId)) throw httpError(409, 'Stop the server before switching worlds');
  if (!(await serverFs.for(serverId).exists(`${worldName}/level.dat`))) {
    throw httpError(404, `No world named "${worldName}" on this server`);
  }
  const previous = await activeLevelName(server);
  if (previous === worldName) return { active: worldName, changed: false };
  await setActiveLevel(server, worldName, { actor });
  recordEvent({
    serverId,
    actor,
    type: 'world-activated',
    summary: `Active world switched: "${previous}" → "${worldName}".`,
    details: { from: previous, to: worldName },
  });
  forgetWorlds(serverId);
  return { active: worldName, changed: true };
}

const activateWorld = guardOp('activate', activateWorldImpl);

const LEVEL_TYPES = new Set(['DEFAULT', 'FLAT', 'LARGEBIOMES', 'AMPLIFIED']);

/**
 * Reset (re-roll) the active world: optional auto-backup, delete its dirs, and
 * regenerate on next start with full control over the seed and world type.
 *   seedMode: 'keep'   → reuse the current seed (server.properties → level.dat)
 *             'random' → clear the seed for a fresh random world (default)
 *             'custom' → use the provided seed (numbers or text both work)
 *   levelType: '' keeps the current type, else DEFAULT/FLAT/LARGEBIOMES/AMPLIFIED
 *   backup:   take a safety backup first (default true)
 * Server must be stopped.
 */
async function resetWorldImpl(
  serverId,
  { seedMode = 'random', seed = '', levelType = '', backup = true, actor = 'system' } = {}
) {
  const server = mustServer(serverId);
  if (await isRunning(serverId)) throw httpError(409, 'Stop the server before resetting the world');
  const handle = serverFs.for(serverId);
  const level = await activeLevelName(server);
  const dims = await serverWorldDims(serverId, level);
  if (!(await handle.exists(dims[0]))) throw httpError(404, `World "${level}" does not exist yet - nothing to reset`);

  // Resolve the seed to apply (null → cleared → Minecraft picks a random one).
  let newSeed = null;
  if (seedMode === 'keep') {
    newSeed = (await readProps(serverId)).get('level-seed') || (await readServerLevelSeed(serverId, dims[0])) || null;
  } else if (seedMode === 'custom') {
    newSeed = String(seed || '').trim() || null;
  }
  const applyType = LEVEL_TYPES.has(levelType) ? levelType : '';

  if (backup) {
    const { createBackupUnguarded } = require('./backups');
    // Already inside the 'reset' op lock - the guarded form would 409 against it.
    await createBackupUnguarded(serverId, {
      reason: 'pre-restore',
      actor,
      note: `Safety backup before resetting world "${level}"`,
    });
  }

  const freedBytes = await serverDirsSize(serverId, dims);
  for (const dim of dims) await handle.remove(dim);

  // Persist the seed in server.properties + the SEED env var (itzg applies SEED
  // to level-seed on start); world type rides on the LEVEL_TYPE env the same way.
  const env = { ...server.env };
  if (newSeed) {
    await setProp(serverId, 'level-seed', String(newSeed));
    env.SEED = String(newSeed);
  } else {
    await setProp(serverId, 'level-seed', '');
    delete env.SEED;
  }
  if (applyType) env.LEVEL_TYPE = applyType;
  if (JSON.stringify(env) !== JSON.stringify(server.env)) {
    require('./servers').updateServer(serverId, { env }, { actor });
  }

  const seedNote =
    seedMode === 'keep'
      ? newSeed
        ? `keeping seed ${newSeed}`
        : 'seed could not be read - a new random seed will be used'
      : newSeed
        ? `with seed ${newSeed}`
        : 'with a new random seed';
  recordEvent({
    serverId,
    actor,
    type: 'world-reset',
    summary: `World "${level}" reset ${seedNote}${applyType ? `, type ${applyType}` : ''} (${humanBytes(freedBytes)} cleared).`,
    details: {
      level,
      seedMode,
      seed: newSeed ? String(newSeed) : null,
      levelType: applyType || null,
      backup,
      freedBytes,
    },
  });
  logger.info('Reset a world.', { serverId, actor, level, seedMode, freedBytes });
  forgetWorlds(serverId);
  indexer.scheduleScan();
  return {
    level,
    seedMode,
    keptSeed: seedMode === 'keep' && newSeed ? String(newSeed) : null,
    seed: newSeed ? String(newSeed) : null,
    levelType: applyType || null,
    freedBytes,
  };
}

const resetWorld = guardOp('reset', resetWorldImpl);

/** Delete a non-active world from a server. Returns freed bytes. */
async function deleteServerWorldImpl(serverId, worldName, { actor = 'system' } = {}) {
  const server = mustServer(serverId);
  checkWorldName(worldName);
  if (worldName === (await activeLevelName(server))) {
    throw httpError(409, 'This is the active world - activate another world first, or use Reset to regenerate it');
  }
  const handle = serverFs.for(serverId);
  const dims = await serverWorldDims(serverId, worldName);
  if (!(await handle.exists(dims[0]))) throw httpError(404, `No world named "${worldName}" on this server`);
  const freedBytes = await serverDirsSize(serverId, dims);
  for (const dim of dims) await handle.remove(dim);
  recordEvent({
    serverId,
    actor,
    type: 'world-deleted',
    summary: `World "${worldName}" deleted (${humanBytes(freedBytes)} freed).`,
    details: { worldName, freedBytes },
  });
  logger.info('Deleted a world from a server.', { serverId, actor, worldName, freedBytes });
  forgetWorlds(serverId);
  indexer.scheduleScan();
  return { freedBytes };
}

// Delete can rm -rf a whole world inside the server dir tree, so it must never
// interleave with a backup/restore/install that is zipping or swapping the same
// tree - guard it like every other world mutation.
const deleteServerWorld = guardOp('delete-world', deleteServerWorldImpl);

// ---------------------------------------------------------------------------
// Downloads

/**
 * Zip a server world into ./data/tmp for a one-off download (consistent
 * snapshot while running). Caller must delete absPath when done sending.
 */
async function prepareWorldDownload(serverId, worldName, { actor = 'system' } = {}) {
  const server = mustServer(serverId);
  checkWorldName(worldName);
  const handle = serverFs.for(serverId);
  const dims = await serverWorldDims(serverId, worldName);
  if (!(await handle.exists(dims[0]))) throw httpError(404, `No world named "${worldName}" on this server`);

  const sizeBytes = await serverDirsSize(serverId, dims);
  const { free } = await indexer.diskFree();
  if (free < sizeBytes * 1.2)
    throw httpError(507, `Not enough disk space to stage the download (~${humanBytes(sizeBytes)} needed)`);

  const active = worldName === (await activeLevelName(server));
  const running = active && (await isRunning(serverId));
  const zipAbs = dataPath('tmp', `world-dl-${nanoid(6)}.zip`);
  const stagingDir = handle.remote ? dataPath('tmp', `world-dl-${nanoid(6)}`) : null;
  try {
    await withPausedSaves(
      serverId,
      running,
      async () => {
        if (!stagingDir)
          return zipWorld(
            zipAbs,
            handle.localPath(dims[0]),
            dims.slice(1).map((d) => handle.localPath(d))
          );
        // The files are on the Docker host: pull the snapshot here first, then
        // zip it at leisure, so saves stay paused only for the transfer.
        for (const dim of dims) await handle.downloadDir(dim, path.join(stagingDir, dim));
      },
      actor
    );
    if (stagingDir) {
      await zipWorld(
        zipAbs,
        path.join(stagingDir, dims[0]),
        dims.slice(1).map((d) => path.join(stagingDir, d))
      );
    }
  } finally {
    if (stagingDir) await fsp.rm(stagingDir, { recursive: true, force: true }).catch(() => {});
  }
  const size = (await fsp.stat(zipAbs)).size;
  recordEvent({
    serverId,
    actor,
    type: 'world-downloaded',
    summary: `World "${worldName}" downloaded (${humanBytes(size)}).`,
    details: { worldName, sizeBytes: size },
  });
  return {
    absPath: zipAbs,
    filename: `${sanitizeFilename(server.display_name)}-${sanitizeFilename(worldName)}.zip`,
    size,
  };
}

// ---------------------------------------------------------------------------
// Library listing / delete

/** All library worlds mapped for the UI (friendly source labels, compat info). */
function libraryWorlds() {
  return db.all("SELECT * FROM library_files WHERE category = 'world' ORDER BY created_at DESC").map((row) => {
    let source = 'Imported';
    let sourceKind = 'import';
    if (row.world_source === 'upload') {
      source = 'Uploaded';
      sourceKind = 'upload';
    } else if (row.world_source && row.world_source.startsWith('extract:')) {
      const sid = row.world_source.slice('extract:'.length);
      const server = db.get('SELECT display_name FROM servers WHERE id = ?', sid);
      source = `Extracted from ${server ? server.display_name : sid}`;
      sourceKind = 'extract';
    }
    return {
      id: row.id,
      name: row.name,
      filename: row.filename,
      source,
      sourceKind,
      flavor: row.world_flavor ? flavorLabel(row.world_flavor) : null,
      mcVersion: row.version || null,
      size: row.size_bytes,
      created: (row.created_at || '').slice(0, 16),
      // created_at is SQLite datetime('now') - UTC without a zone marker.
      // Epoch ms lets the frontend format in the viewer's locale/timezone.
      createdMs: (() => {
        const ms = Date.parse(String(row.created_at || '').replace(' ', 'T') + 'Z');
        return Number.isFinite(ms) ? ms : null;
      })(),
      hash: row.sha256.slice(0, 10),
    };
  });
}

/** Delete a library world archive (delegates to the shared library service). */
async function deleteLibraryWorld(id, { actor = 'system' } = {}) {
  mustLibWorld(id);
  return library.deleteLibraryFile(id, { actor });
}

// ---------------------------------------------------------------------------
// server.properties + level helpers

/** Active level name: LEVEL env wins, then server.properties, then 'world'. */
async function activeLevelName(server) {
  if (server.env && server.env.LEVEL) return server.env.LEVEL;
  return (await readProps(server.id)).get('level-name') || 'world';
}

// server.properties is read by half the server page (the active world, the
// seed, PVP, whitelist enforcement), and on a remote host each read is a round
// trip. It changes only when the panel or the server writes it, so hold the
// parse for a couple of seconds - long enough to collapse one page render,
// short enough that nothing reads a stale file.
const propsCache = new Map(); // serverId -> { at, map }
const PROPS_TTL_MS = 2000;

/** Drop the cached server.properties for a server (after a write). */
function forgetProps(serverId) {
  propsCache.delete(serverId);
}

/** Parse server.properties into a Map (empty when missing). */
async function readProps(serverId) {
  const hit = propsCache.get(serverId);
  if (hit && Date.now() - hit.at < PROPS_TTL_MS) return hit.map;
  const map = new Map();
  const text = await serverFs.for(serverId).readText('server.properties');
  for (const line of (text || '').split(/\r?\n/)) {
    if (!line || line.startsWith('#')) continue;
    const eq = line.indexOf('=');
    if (eq > 0) map.set(line.slice(0, eq).trim(), line.slice(eq + 1).trim());
  }
  propsCache.set(serverId, { at: Date.now(), map });
  return map;
}

/** Set one server.properties key atomically (create the file when missing). */
async function setProp(serverId, key, value) {
  const handle = serverFs.for(serverId);
  const text = (await handle.readText('server.properties')) || '';
  const re = new RegExp(`^${key.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}=.*$`, 'm');
  const next = re.test(text)
    ? text.replace(re, `${key}=${value}`)
    : `${text}${text && !text.endsWith('\n') ? '\n' : ''}${key}=${value}\n`;
  // Write beside the file and rename over it, so an interrupted write can never
  // leave the server with half a properties file.
  await handle.writeFile('server.properties.tmp', next);
  await handle.rename('server.properties.tmp', 'server.properties');
  forgetProps(serverId);
}

/** Point the server at a new level: property always, LEVEL env when present. */
async function setActiveLevel(server, levelName, { actor }) {
  await setProp(server.id, 'level-name', levelName);
  if (server.env && server.env.LEVEL !== undefined) {
    require('./servers').updateServer(server.id, { env: { ...server.env, LEVEL: levelName } }, { actor });
  }
  // Keep BlueMap (if enabled) pointed at whichever world is actually active -
  // otherwise a rename/switch after enabling the map silently breaks it again.
  const mapService = require('./map');
  if (mapService.getMapConfig(server.id).enabled) await mapService.writeMapConfigs(server.id);
}

/** Existing dim dirs for a world: [main, main_nether?, main_the_end?] (relative names). */
async function serverWorldDims(serverId, worldName) {
  const handle = serverFs.for(serverId);
  const dims = [worldName];
  for (const suffix of DIM_SUFFIXES) {
    if (await handle.exists(worldName + suffix)) dims.push(worldName + suffix);
  }
  return dims;
}

/** Total bytes of a set of a server's world dirs, wherever those files live. */
async function serverDirsSize(serverId, dims) {
  const handle = serverFs.for(serverId);
  let total = 0;
  for (const dim of dims) total += (await handle.du(dim)).size;
  return total;
}

/**
 * Run one of the level.dat readers against a world on a server. The readers
 * parse raw NBT from a file path, so a server whose files are on the Docker
 * host gets a temp copy of just that one file - level.dat is a few KB.
 */
async function readServerLevel(serverId, worldName, reader) {
  try {
    return await serverFs.for(serverId).withLocalCopy(`${worldName}/level.dat`, (abs) => reader(abs));
  } catch {
    return null; // no world yet / unreadable - every caller treats this as "unknown"
  }
}

const readServerLevelVersion = (serverId, worldName) => readServerLevel(serverId, worldName, readLevelVersion);
const readServerLevelSeed = (serverId, worldName) => readServerLevel(serverId, worldName, readLevelSeed);
const readServerLevelSpawn = (serverId, worldName) => readServerLevel(serverId, worldName, readLevelSpawn);

function isDimName(name) {
  return DIM_SUFFIXES.some((s) => name.endsWith(s) && name.length > s.length);
}

/** 'world_nether' -> 'world', null when not a dim name. */
function dimBase(name) {
  for (const suffix of DIM_SUFFIXES) {
    if (name.endsWith(suffix) && name.length > suffix.length) return name.slice(0, -suffix.length);
  }
  return null;
}

// ---------------------------------------------------------------------------
// level.dat best-effort NBT scans (no NBT dependency - gzip + tag-pattern scan)

/** Read the MC version name ("1.21.5") out of level.dat, or null. */
function readLevelVersion(levelDatAbs) {
  const buf = readLevelBuffer(levelDatAbs);
  if (!buf) return null;
  // NBT string tag: 0x08, name length (2B BE) = 4, "Name", value length (2B BE), value
  const needle = Buffer.from('080004' + Buffer.from('Name').toString('hex'), 'hex');
  let idx = buf.indexOf(needle);
  while (idx !== -1) {
    const lenOff = idx + needle.length;
    if (lenOff + 2 <= buf.length) {
      const len = buf.readUInt16BE(lenOff);
      const value = buf.slice(lenOff + 2, lenOff + 2 + len).toString('utf8');
      if (/^\d+\.\d+/.test(value)) return value;
    }
    idx = buf.indexOf(needle, idx + 1);
  }
  return null;
}

/** Read the world seed out of level.dat (RandomSeed or WorldGenSettings.seed), or null. */
function readLevelSeed(levelDatAbs) {
  const buf = readLevelBuffer(levelDatAbs);
  if (!buf) return null;
  // NBT long tag: 0x04, name length (2B BE), name, 8-byte BE value
  for (const name of ['RandomSeed', 'seed']) {
    const needle = Buffer.concat([Buffer.from([0x04, 0x00, name.length]), Buffer.from(name, 'latin1')]);
    const idx = buf.indexOf(needle);
    if (idx !== -1 && idx + needle.length + 8 <= buf.length) {
      return buf.readBigInt64BE(idx + needle.length).toString();
    }
  }
  return null;
}

/** Read the world spawn (SpawnX/SpawnZ block coords) out of level.dat, or null. */
function readLevelSpawn(levelDatAbs) {
  const buf = readLevelBuffer(levelDatAbs);
  if (!buf) return null;
  // NBT int tag: 0x03, name length (2B BE), name, 4-byte BE value
  const readInt = (name) => {
    const needle = Buffer.concat([Buffer.from([0x03, 0x00, name.length]), Buffer.from(name, 'latin1')]);
    const idx = buf.indexOf(needle);
    if (idx === -1 || idx + needle.length + 4 > buf.length) return null;
    return buf.readInt32BE(idx + needle.length);
  };
  const x = readInt('SpawnX');
  const z = readInt('SpawnZ');
  if (x == null || z == null) return null;
  return { x, z };
}

function readLevelBuffer(levelDatAbs) {
  try {
    const raw = fs.readFileSync(levelDatAbs);
    return raw[0] === 0x1f && raw[1] === 0x8b ? zlib.gunzipSync(raw) : raw;
  } catch {
    return null;
  }
}

// ---------------------------------------------------------------------------
// Archive plumbing

/** Zip a world: root contents at the top level, split dims as sibling dirs. */
function zipWorld(outFile, rootAbs, dimDirs = []) {
  return new Promise((resolve, reject) => {
    let done = false;
    const fail = (err) => {
      if (done) return;
      done = true;
      fsp.rm(outFile, { force: true }).catch(() => {});
      reject(err);
    };
    const output = fs.createWriteStream(outFile);
    const archive = archiver('zip', { zlib: { level: 6 } });
    output.on('close', () => {
      done = true;
      resolve();
    });
    // 'error' on the write stream (ENOSPC/EACCES on the target) would otherwise
    // leave this promise unsettled forever and, with no listener, surface as an
    // uncaught exception. Clean up the partial file either way.
    output.on('error', fail);
    archive.on('error', fail);
    archive.pipe(output);
    archive.directory(rootAbs, false);
    for (const dim of dimDirs) archive.directory(dim, path.basename(dim));
    archive.finalize();
  });
}

/** Route an archive to the right extractor by magic bytes (zip/.mcworld, tar, tar.gz). */
async function extractArchive(file, destDir, originalName = '') {
  const fd = await fsp.open(file, 'r');
  const head = Buffer.alloc(265);
  await fd.read(head, 0, 265, 0);
  await fd.close();

  const isZip = head[0] === 0x50 && head[1] === 0x4b;
  const isGzip = head[0] === 0x1f && head[1] === 0x8b;
  const isTar = head.slice(257, 262).toString('latin1') === 'ustar';

  if (isZip) return extractZip(file, destDir);
  if (isGzip || isTar || /\.tar$/i.test(originalName)) {
    // Size-check BEFORE extracting, as a separate header-only pass (tar.t
    // reads entry headers, not file content - cheap): a `filter` callback
    // that throws is NOT propagated as a rejection by node-tar (verified -
    // it escapes as an uncaught exception instead), which used to leave
    // extraction hung forever with a leaked tmp dir on an oversized archive.
    let tarTotal = 0;
    let tarEntries = 0;
    await tar.t({
      file,
      onentry: (entry) => {
        tarEntries++;
        tarTotal += entry.size || 0;
      },
    });
    if (tarEntries > MAX_EXTRACT_ENTRIES) {
      throw httpError(413, `Archive has too many entries (> ${MAX_EXTRACT_ENTRIES}).`);
    }
    if (tarTotal > MAX_EXTRACT_BYTES) {
      throw httpError(
        413,
        `Archive is too large uncompressed (> ${Math.round(MAX_EXTRACT_BYTES / 1024 ** 3)} GB) - refusing to extract (possible decompression bomb).`
      );
    }
    // node-tar sanitizes absolute paths and skips `..` entries by default;
    // this filter is defense in depth and only returns a boolean, never throws.
    return tar.x({
      file,
      cwd: destDir,
      filter: (p) => !p.split(/[\\/]/).includes('..'),
    });
  }
  throw httpError(400, `That doesn't look like a zip or tar archive${originalName ? ` (${originalName})` : ''}`);
}

// ---------------------------------------------------------------------------
// Small utilities

async function isRunning(serverId) {
  const info = await inspectStatus(serverId).catch(() => ({ exists: false }));
  return info.exists && ['running', 'starting', 'unhealthy'].includes(info.status);
}

function sha256File(abs) {
  return new Promise((resolve, reject) => {
    const hash = crypto.createHash('sha256');
    fs.createReadStream(abs)
      .on('data', (chunk) => hash.update(chunk))
      .on('end', () => resolve(hash.digest('hex')))
      .on('error', reject);
  });
}

/** rename with cross-device fallback (tmp and servers share ./data, but be safe). */
async function moveFile(from, to) {
  try {
    await fsp.rename(from, to);
  } catch (err) {
    if (err.code !== 'EXDEV') throw err;
    await fsp.copyFile(from, to);
    await fsp.rm(from, { force: true });
  }
}

/** World dir names: strip path separators & control chars, keep it friendly. */
function sanitizeWorldName(name) {
  const clean = String(name || '')
    .replace(/[\\/:*?"<>|\0]/g, '_')
    .replace(/^\.+/, '')
    .trim()
    .slice(0, 64);
  if (!clean) throw httpError(400, 'World name cannot be empty');
  return clean;
}

/** Reject world names that could traverse paths (route params are user input). */
function checkWorldName(name) {
  if (!name || /[\\/\0]/.test(name) || name === '.' || name === '..' || name.startsWith('.')) {
    throw httpError(400, 'Invalid world name');
  }
}

function sanitizeFilename(name) {
  return String(name)
    .replace(/[\\/:*?"<>|\0]/g, '_')
    .slice(0, 120);
}

function mustServer(serverId) {
  const server = require('./servers').getServer(serverId);
  if (!server) throw httpError(404, 'Server not found');
  return server;
}

function mustLibWorld(libraryId) {
  const lib = db.get("SELECT * FROM library_files WHERE id = ? AND category = 'world'", libraryId);
  if (!lib) throw httpError(404, 'World not found in the library');
  return lib;
}

/** Compare dotted versions: >0 when a is newer than b. Non-numeric parts compare as strings. */
function compareVersions(a, b) {
  const pa = String(a).split('.');
  const pb = String(b).split('.');
  for (let i = 0; i < Math.max(pa.length, pb.length); i++) {
    const na = Number(pa[i] || 0);
    const nb = Number(pb[i] || 0);
    if (Number.isNaN(na) || Number.isNaN(nb)) {
      const cmp = String(pa[i] || '').localeCompare(String(pb[i] || ''));
      if (cmp !== 0) return cmp;
      continue;
    }
    if (na !== nb) return na - nb;
  }
  return 0;
}

function humanBytes(n) {
  if (n >= 1024 ** 3) return `${(n / 1024 ** 3).toFixed(1)} GB`;
  if (n >= 1024 ** 2) return `${(n / 1024 ** 2).toFixed(1)} MB`;
  return `${Math.max(1, Math.round(n / 1024))} KB`;
}

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms).unref());
}

module.exports = {
  isDimName,
  readLevelSpawn,
  detectWorldRoot,
  importArchive,
  extractFromServer,
  listServerWorlds,
  forgetWorlds,
  installWarnings,
  installToServer,
  copyWarnings,
  copyBetweenServers,
  duplicateWorld,
  renameWorld,
  activateWorld,
  resetWorld,
  deleteServerWorld,
  prepareWorldDownload,
  libraryWorlds,
  deleteLibraryWorld,
  activeLevelName,
  serverWorldDims,
  serverDirsSize,
  forgetProps,
  readProps,
  readServerLevelVersion,
  readServerLevelSeed,
  readServerLevelSpawn,
  compatWarnings,
  readLevelVersion,
};
