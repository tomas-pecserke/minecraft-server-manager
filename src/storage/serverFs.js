// @ts-nocheck - dynamic Docker/NBT/HTTP-JSON interop; not yet under checkJs (incremental typing).
'use strict';

// One way to touch a server's /data, whichever host it lives on.
//
//   SERVER_STORAGE=bind   - data/servers/<id> on the panel's own disk, as
//                           before; every call is a plain fs call.
//   SERVER_STORAGE=volume - a Docker named volume on the daemon's host, reached
//                           through the file sidecar (src/docker/sidecar.js).
//
// Nothing outside this module may build a path into a server's data directory.
// Every path here is RELATIVE to that directory ('mods/foo.jar', '' for the
// root), is checked for containment, and - on a remote host - is re-checked
// inside the sidecar so a symlink planted by a mod can't lead the read out.
//
// Some libraries (NBT readers, zip readers) only take a real file path. Those
// callers use withLocalCopy/withLocalDir, which materialise a temp copy under
// data/tmp and clean it up afterwards.

const fs = require('node:fs');
const fsp = require('node:fs/promises');
const path = require('node:path');
const { Readable, pipeline, promises: streamPromises } = require('node:stream');
const tarStream = require('tar-stream');
const config = require('../config');
const { safeJoin, PathEscapeError } = require('./pathGuard');
const httpError = require('../utils/httpError');

const MOUNT = '/data'; // where the volume is mounted inside the sidecar
const READ_CAP_BYTES = 64 * 1024 * 1024; // ceiling for a whole-file read into memory
const ESCAPE_EXIT = 90; // exit code the in-sidecar containment guard uses
const EXIT_MARKER = '__msm_exit:'; // how a command reports its status, on stderr
const IS_DIR_EXIT = 91; // a read asked for a directory
const TOO_BIG_EXIT = 92; // a read asked for more than READ_CAP_BYTES

/** True when server data lives in a Docker volume rather than on the panel's disk. */
function isRemote() {
  return config.serverStorage === 'volume';
}

/**
 * Normalise a caller-supplied relative path: POSIX separators, no leading
 * slash, no '..', no NUL. Returns '' for the server root.
 */
function normalizeRel(rel) {
  const raw = String(rel == null ? '' : rel);
  if (raw.includes('\0')) throw new PathEscapeError(raw);
  const posix = raw.split('\\').join('/');
  const parts = [];
  for (const part of posix.split('/')) {
    if (part === '' || part === '.') continue;
    if (part === '..') throw new PathEscapeError(raw);
    parts.push(part);
  }
  if (path.isAbsolute(posix) || /^[A-Za-z]:/.test(posix)) throw new PathEscapeError(raw);
  return parts.join('/');
}

/* ------------------------------------------------------------------ *
 * Local backend - the panel's own filesystem, exactly as it always was.
 * ------------------------------------------------------------------ */

function localRoot(serverId) {
  return safeJoin(config.dataDir, 'servers', serverId);
}

/** Resolve a relative path under an absolute local root. */
function localAbs(root, rel) {
  const norm = normalizeRel(rel);
  return norm ? safeJoin(root, norm) : root;
}

function entryFrom(name, st) {
  return {
    name,
    dir: st.isDirectory(),
    symlink: st.isSymbolicLink(),
    size: st.isDirectory() ? 0 : st.size,
    mtimeMs: st.mtimeMs,
  };
}

const localBackend = {
  async stat(root, rel) {
    const st = await fsp.lstat(localAbs(root, rel));
    return {
      dir: st.isDirectory(),
      symlink: st.isSymbolicLink(),
      size: st.isDirectory() ? 0 : st.size,
      mtimeMs: st.mtimeMs,
      mode: st.mode & 0o7777,
    };
  },

  async statMany(root, rels) {
    return Promise.all(
      rels.map((rel) =>
        localBackend.stat(root, rel).catch((err) => {
          if (err.code === 'ENOENT') return null;
          throw err;
        })
      )
    );
  },

  async readdirMany(root, rels) {
    return Promise.all(rels.map((rel) => localBackend.readdir(root, rel).catch(() => [])));
  },

  /** readdirSized for local storage - see the remote backend for the contract. */
  async readdirSized(root, rel, { contains = [], withSizes = true } = {}) {
    const entries = await localBackend.readdir(root, rel);
    return Promise.all(
      entries.map(async (entry) => {
        const childRel = rel ? `${rel}/${entry.name}` : entry.name;
        const measure = withSizes && entry.dir && !entry.symlink;
        const sizeBytes = measure ? (await localBackend.du(root, childRel)).size : entry.dir ? 0 : entry.size;
        const held = [];
        if (entry.dir && !entry.symlink) {
          const found = await localBackend.statMany(
            root,
            contains.map((name) => `${childRel}/${name}`)
          );
          contains.forEach((name, i) => {
            if (found[i]) held.push(name);
          });
        }
        return { ...entry, sizeBytes, contains: held };
      })
    );
  },

  async readdir(root, rel) {
    const abs = localAbs(root, rel);
    const dirents = await fsp.readdir(abs, { withFileTypes: true });
    const out = [];
    for (const d of dirents) {
      const st = await fsp.lstat(path.join(abs, d.name)).catch(() => null);
      if (!st) continue; // vanished between readdir and lstat
      out.push(entryFrom(d.name, st));
    }
    return out;
  },

  async walk(root, rel, { maxDepth = Infinity } = {}) {
    const base = localAbs(root, rel);
    const out = [];
    async function step(dirAbs, dirRel, depth) {
      let dirents;
      try {
        dirents = await fsp.readdir(dirAbs, { withFileTypes: true });
      } catch {
        return; // unreadable subtree - skipped, as the walkers this replaces did
      }
      for (const d of dirents) {
        const childRel = dirRel ? `${dirRel}/${d.name}` : d.name;
        const childAbs = path.join(dirAbs, d.name);
        const st = await fsp.lstat(childAbs).catch(() => null);
        if (!st) continue;
        out.push({ ...entryFrom(d.name, st), path: childRel });
        if (st.isDirectory() && !st.isSymbolicLink() && depth + 1 < maxDepth) await step(childAbs, childRel, depth + 1);
      }
    }
    await step(base, '', 0);
    return out;
  },

  async readFile(root, rel) {
    return fsp.readFile(localAbs(root, rel));
  },

  async readMany(root, rels) {
    return Promise.all(
      rels.map((rel) =>
        fsp.readFile(localAbs(root, rel)).catch((err) => {
          if (err.code === 'ENOENT' || err.code === 'EISDIR') return null;
          throw err;
        })
      )
    );
  },

  async writeFile(root, rel, data) {
    const abs = localAbs(root, rel);
    await fsp.mkdir(path.dirname(abs), { recursive: true });
    await fsp.writeFile(abs, data);
  },

  async mkdir(root, rel) {
    await fsp.mkdir(localAbs(root, rel), { recursive: true });
  },

  async remove(root, rel) {
    await fsp.rm(localAbs(root, rel), { recursive: true, force: true });
  },

  async rename(root, from, to) {
    const dest = localAbs(root, to);
    await fsp.mkdir(path.dirname(dest), { recursive: true });
    await fsp.rename(localAbs(root, from), dest);
  },

  async copy(root, from, to) {
    const dest = localAbs(root, to);
    await fsp.mkdir(path.dirname(dest), { recursive: true });
    await fsp.cp(localAbs(root, from), dest, { recursive: true, force: true });
  },

  async duTree(root, rel) {
    const total = await localBackend.du(root, rel);
    const children = new Map();
    for (const entry of await localBackend.readdir(root, rel).catch(() => [])) {
      if (entry.symlink) continue;
      const childRel = rel ? `${rel}/${entry.name}` : entry.name;
      children.set(entry.name, entry.dir ? (await localBackend.du(root, childRel)).size : entry.size);
    }
    return { ...total, children };
  },

  async du(root, rel) {
    const entries = await localBackend.walk(root, rel);
    let size = 0;
    let files = 0;
    for (const e of entries) {
      if (e.dir || e.symlink) continue;
      size += e.size;
      files += 1;
    }
    return { size, files };
  },

  async readStream(root, rel) {
    return fs.createReadStream(localAbs(root, rel));
  },

  async writeStream(root, rel, source, _opts) {
    const abs = localAbs(root, rel);
    await fsp.mkdir(path.dirname(abs), { recursive: true });
    await streamPromises.pipeline(source, fs.createWriteStream(abs));
  },

  async uploadFile(root, localPath, rel) {
    const abs = localAbs(root, rel);
    await fsp.mkdir(path.dirname(abs), { recursive: true });
    await fsp.copyFile(localPath, abs);
  },

  async uploadDir(root, localDir, rel) {
    const abs = localAbs(root, rel);
    await fsp.mkdir(abs, { recursive: true });
    await fsp.cp(localDir, abs, { recursive: true, force: true });
  },

  async downloadFile(root, rel, localPath) {
    await fsp.mkdir(path.dirname(localPath), { recursive: true });
    await fsp.copyFile(localAbs(root, rel), localPath);
  },

  async downloadDir(root, rel, localDir) {
    await fsp.mkdir(localDir, { recursive: true });
    await fsp.cp(localAbs(root, rel), localDir, { recursive: true, force: true });
  },
};

/* ------------------------------------------------------------------ *
 * Remote backend - a Docker volume, reached through the file sidecar.
 * ------------------------------------------------------------------ */

function sidecar() {
  return require('../docker/sidecar');
}

function mountPath(rel) {
  const norm = normalizeRel(rel);
  return norm ? `${MOUNT}/${norm}` : MOUNT;
}

/**
 * Wrap a shell command so it only ever runs against a path that still resolves
 * inside the mount. A symlink under /data (a mod can plant one) would otherwise
 * let a read or a write follow it out into the sidecar's own filesystem. The
 * check and the command share one exec, so containment costs no round trip.
 * `$p` holds the resolved path; `$raw` the unresolved one, for mkdir/mv targets
 * that don't exist yet.
 */
function guarded(rel, body) {
  const raw = mountPath(rel);
  // The body runs in a subshell so its own `exit` codes reach the marker, and
  // the command reports its status on stderr rather than leaving us to ask
  // the daemon for it: that question is a whole extra round trip per call, and
  // over a network it costs about as much as the command itself.
  return [
    'sh',
    '-c',
    `raw=${shellQuote(raw)}; p=$(readlink -f "$raw" || echo "$raw"); ` +
      `case "$p" in ${MOUNT}|${MOUNT}/*) ;; *) printf '${EXIT_MARKER}%s' ${ESCAPE_EXIT} >&2; exit 0;; esac; ` +
      `( ${body} ); printf '${EXIT_MARKER}%s' "$?" >&2`,
  ];
}

/**
 * Split the status marker off a command's stderr: { stderr, exitCode }, with a
 * null code when the marker never arrived (killed, or output truncated).
 */
function splitExitMarker(stderr) {
  const at = stderr.lastIndexOf(EXIT_MARKER);
  if (at === -1) return { stderr, exitCode: null };
  const code = Number(stderr.slice(at + EXIT_MARKER.length).trim());
  return { stderr: stderr.slice(0, at), exitCode: Number.isFinite(code) ? code : null };
}

function shellQuote(value) {
  return `'${String(value).split("'").join(`'\\''`)}'`;
}

/** Run a guarded command, turning the guard's exit code into a path error. */
async function run(serverId, rel, body, { timeoutMs } = {}) {
  const res = await sidecar().exec(serverId, guarded(rel, body), {
    wantExitCode: false, // the marker carries it - see guarded()
    ...(timeoutMs ? { timeoutMs } : {}),
  });
  const { stderr, exitCode } = splitExitMarker(res.stderr);
  if (exitCode === ESCAPE_EXIT) throw new PathEscapeError(String(rel));
  return { ...res, stderr, exitCode };
}

/** Ownership of the data root, so files the panel writes belong to the server. */
const ownerCache = new Map();
async function ownership(serverId) {
  const cached = ownerCache.get(serverId);
  if (cached) return cached;
  const { stdout } = await sidecar().exec(serverId, ['sh', '-c', `stat -c '%u %g' ${MOUNT}`], {
    wantExitCode: false, // the output itself says whether it worked
  });
  let owner = { uid: 1000, gid: 1000 }; // the itzg image's default user
  const [uid, gid] = stdout.toString('utf8').trim().split(/\s+/).map(Number);
  // A brand-new volume is still root-owned because nothing has run against it
  // yet. Writing files as root there would leave the server - which runs as its
  // own user - unable to touch what the panel installed before its first start,
  // so keep the image's default user until the volume says otherwise.
  if (Number.isInteger(uid) && Number.isInteger(gid) && uid !== 0) owner = { uid, gid };
  ownerCache.set(serverId, owner);
  return owner;
}

/** Parse the NUL-separated records `find -printf` emits for us. */
function parseFindRecords(buffer) {
  const out = [];
  for (const record of buffer.toString('utf8').split('\0')) {
    if (!record) continue;
    const [type, size, mtime, name] = record.split('\t');
    if (name === undefined) continue;
    out.push({
      name,
      dir: type === 'd',
      symlink: type === 'l',
      size: type === 'd' ? 0 : Number(size) || 0,
      mtimeMs: Math.round(Number(mtime) * 1000) || 0,
    });
  }
  return out;
}

const FIND_FORMAT = '%y\\t%s\\t%T@\\t';

// How a multi-part command separates its sections. NUL on both sides, so a
// marker can never collide with a filename - the one thing these carry.
const SECTION_SH = '\\000__msm_section__\\000';
const SECTION_BUF = Buffer.from('\0__msm_section__\0', 'utf8');

/** Split a multi-section reply into `count` buffers (a missing one is empty). */
function splitSections(buffer, count) {
  const out = [];
  let at = 0;
  while (out.length < count - 1) {
    const next = buffer.indexOf(SECTION_BUF, at);
    if (next === -1) break;
    out.push(buffer.subarray(at, next));
    at = next + SECTION_BUF.length;
  }
  out.push(buffer.subarray(at));
  while (out.length < count) out.push(Buffer.alloc(0));
  return out;
}

/** One-entry tar carrying `data` as `name`, owned by the server's user. */
function singleFileTar(name, data, owner) {
  const pack = tarStream.pack();
  pack.entry({ name, mode: 0o644, uid: owner.uid, gid: owner.gid, mtime: new Date() }, data);
  pack.finalize();
  return pack;
}

const remoteBackend = {
  async stat(serverId, rel) {
    // The trailing field of FIND_FORMAT is the mode here rather than a name -
    // one find call answers type, size, mtime and mode together.
    const { stdout, exitCode } = await run(serverId, rel, `find -P "$p" -maxdepth 0 -printf '${FIND_FORMAT}%m\\0'`);
    const [record] = exitCode === 0 ? parseFindRecords(stdout) : [];
    if (!record) throw notFound(rel);
    const { name, ...rest } = record;
    // find -P describes the link itself, not its target; the file manager only
    // needs to know that it IS a link, which is what `symlink` carries.
    return { ...rest, mode: Number.parseInt(name, 8) || 0o644 };
  },

  /**
   * Stat several paths in ONE call. The background scanners (crash reports, the
   * size index) check a handful of paths per server per tick; one round trip
   * for all of them is what keeps that cheap over a network.
   */
  async statMany(serverId, rels) {
    if (!rels.length) return [];
    const paths = rels.map((rel) => shellQuote(mountPath(rel))).join(' ');
    // find -P never follows a symlink, so a planted link is reported as a link
    // rather than followed out of the mount; these paths are panel-built, not
    // user input, so they need no further guard.
    const { stdout } = await sidecar().exec(serverId, [
      'sh',
      '-c',
      `find -P ${paths} -maxdepth 0 -printf '${FIND_FORMAT}%p\\0' 2>/dev/null`,
    ]);
    const byPath = new Map(parseFindRecords(stdout).map((r) => [r.name, r]));
    return rels.map((rel) => {
      const record = byPath.get(mountPath(rel));
      if (!record) return null;
      const { name, ...rest } = record;
      return { ...rest, mode: 0o644 };
    });
  },

  /**
   * List several directories in ONE call - the mods tab alone reads three, and
   * a round trip each is what makes it crawl over a network. A directory that
   * isn't there comes back as an empty list, like readdir(...).catch(() => []).
   */
  async readdirMany(serverId, rels) {
    if (!rels.length) return [];
    const paths = rels.map((rel) => shellQuote(mountPath(rel))).join(' ');
    const { stdout } = await sidecar().exec(
      serverId,
      ['sh', '-c', `find -P ${paths} -mindepth 1 -maxdepth 1 -printf '%H\\t${FIND_FORMAT}%f\\0' 2>/dev/null`],
      { wantExitCode: false }
    );
    const byDir = new Map(rels.map((rel) => [mountPath(rel), []]));
    for (const record of stdout.toString('utf8').split('\0')) {
      if (!record) continue;
      const [dir, type, size, mtime, name] = record.split('\t');
      if (name === undefined || !byDir.has(dir)) continue;
      byDir.get(dir).push({
        name,
        dir: type === 'd',
        symlink: type === 'l',
        size: type === 'd' ? 0 : Number(size) || 0,
        mtimeMs: Math.round(Number(mtime) * 1000) || 0,
      });
    }
    return rels.map((rel) => byDir.get(mountPath(rel)) || []);
  },

  async readdir(serverId, rel) {
    const { stdout, exitCode, stderr } = await run(
      serverId,
      rel,
      `find -P "$p" -mindepth 1 -maxdepth 1 -printf '${FIND_FORMAT}%f\\0'`
    );
    if (exitCode !== 0) throw notFound(rel, stderr);
    return parseFindRecords(stdout);
  },

  /**
   * The listing of `rel`, the recursive size of every entry, and which of
   * `contains` each child directory holds - in ONE call. As three calls
   * (readdir, duTree, statMany) it is three round trips, and the worlds tab
   * wants exactly this trio: the entries, how big each world is, and which
   * of them hold a level.dat. `withSizes: false` drops the size walk - the
   * expensive half - for callers that take their numbers from the size index.
   */
  async readdirSized(serverId, rel, { contains = [], withSizes = true } = {}) {
    const names = contains.map((name) => `-name ${shellQuote(name)}`).join(' -o ');
    const probe = contains.length
      ? `find -P "$p" -mindepth 2 -maxdepth 2 \\( ${names} \\) -printf '%P\\0' 2>/dev/null`
      : ':';
    // The listing alone decides whether the directory is there; du and the
    // probe only add detail, so their statuses never become a not-found.
    const { stdout, exitCode, stderr } = await run(
      serverId,
      rel,
      `find -P "$p" -mindepth 1 -maxdepth 1 -printf '${FIND_FORMAT}%f\\0' || exit 1; ` +
        `printf '${SECTION_SH}'; ${withSizes ? `du -ab --max-depth=1 "$p" 2>/dev/null` : ':'}; ` +
        `printf '${SECTION_SH}'; ${probe}; exit 0`,
      { timeoutMs: 300000 }
    );
    if (exitCode !== 0) throw notFound(rel, stderr);

    const [listing, sizes, probed] = splitSections(stdout, 3);
    const base = mountPath(rel);
    const bySize = new Map();
    for (const line of sizes.toString('utf8').split('\n')) {
      const tab = line.indexOf('\t');
      if (tab === -1) continue;
      const at = line.slice(tab + 1).trim();
      if (at.startsWith(`${base}/`)) bySize.set(at.slice(base.length + 1), Number(line.slice(0, tab)) || 0);
    }
    const held = new Map();
    for (const record of probed.toString('utf8').split('\0')) {
      const slash = record.lastIndexOf('/');
      if (slash <= 0) continue; // '<child>/<name>'; anything else is not a hit
      const child = record.slice(0, slash);
      if (!held.has(child)) held.set(child, []);
      held.get(child).push(record.slice(slash + 1));
    }
    return parseFindRecords(listing).map((entry) => ({
      ...entry,
      sizeBytes: entry.dir ? bySize.get(entry.name) || 0 : entry.size,
      contains: held.get(entry.name) || [],
    }));
  },

  async walk(serverId, rel, { maxDepth = 0 } = {}) {
    const depth = Number.isFinite(maxDepth) && maxDepth > 0 ? `-maxdepth ${maxDepth} ` : '';
    const { stdout, exitCode, stderr } = await run(
      serverId,
      rel,
      `find -P "$p" -mindepth 1 ${depth}-printf '${FIND_FORMAT}%P\\0'`,
      { timeoutMs: 120000 } // a modpack's tree is large; this is not a hot path
    );
    if (exitCode !== 0) throw notFound(rel, stderr);
    return parseFindRecords(stdout).map((e) => ({ ...e, path: e.name, name: e.name.split('/').pop() }));
  },

  /**
   * Read several files in ONE call, each as a Buffer (null when it isn't
   * there). A page that reads a handful of small files - the player tab reads
   * six - would otherwise pay a round trip each, which is seconds over a
   * network. The reply is length-prefixed so file contents stay binary-safe.
   *
   * The paths are panel-built, not user input, so they carry no symlink guard;
   * user-supplied paths go through readFile.
   */
  async readMany(serverId, rels) {
    if (!rels.length) return [];
    const script =
      'for p in "$@"; do if [ -f "$p" ]; then printf "%s %s\n" "$(stat -c %s "$p")" "$p"; cat "$p"; ' +
      'else printf "%s %s\n" -1 "$p"; fi; done';
    const { stdout } = await sidecar().exec(
      serverId,
      ['sh', '-c', script, 'sh', ...rels.map((rel) => mountPath(rel))],
      { timeoutMs: 120000, wantExitCode: false }
    );

    const found = new Map();
    let at = 0;
    while (at < stdout.length) {
      const nl = stdout.indexOf(0x0a, at);
      if (nl === -1) break;
      const header = stdout.subarray(at, nl).toString('utf8');
      const space = header.indexOf(' ');
      if (space === -1) break;
      const size = Number(header.slice(0, space));
      const at2 = nl + 1;
      if (size < 0) {
        found.set(header.slice(space + 1), null); // missing, or not a regular file
        at = at2;
        continue;
      }
      found.set(header.slice(space + 1), stdout.subarray(at2, at2 + size));
      at = at2 + size;
    }
    return rels.map((rel) => {
      const hit = found.get(mountPath(rel));
      return hit === undefined ? null : hit;
    });
  },

  async readFile(serverId, rel) {
    // Type check, size check and the read itself in ONE command: asking for the
    // stat first doubled the round trips of every small config read.
    const { stdout, exitCode, stderr } = await run(
      serverId,
      rel,
      `if [ -d "$p" ]; then exit ${IS_DIR_EXIT}; fi; ` +
        `s=$(stat -c %s "$p") || exit 1; ` +
        `if [ "$s" -gt ${READ_CAP_BYTES} ]; then exit ${TOO_BIG_EXIT}; fi; ` +
        'cat "$p"',
      { timeoutMs: 120000 }
    );
    if (exitCode === IS_DIR_EXIT) {
      const err = new Error(`EISDIR: illegal operation on a directory, read '${normalizeRel(rel)}'`);
      err.code = 'EISDIR';
      throw err;
    }
    if (exitCode === TOO_BIG_EXIT) throw httpError(413, 'That file is too large to open from here.');
    if (exitCode !== 0) throw notFound(rel, stderr);
    return stdout;
  },

  async writeFile(serverId, rel, data) {
    const norm = normalizeRel(rel);
    if (!norm) throw new PathEscapeError(String(rel));
    const parent = norm.includes('/') ? norm.slice(0, norm.lastIndexOf('/')) : '';
    const owner = await ownership(serverId);
    await remoteBackend.mkdir(serverId, parent);
    const buffer = Buffer.isBuffer(data) ? data : Buffer.from(String(data), 'utf8');
    const name = norm.split('/').pop();
    await sidecar().putArchive(serverId, singleFileTar(name, buffer, owner), mountPath(parent));
  },

  async mkdir(serverId, rel) {
    const owner = await ownership(serverId);
    const { exitCode, stderr } = await run(serverId, rel, `mkdir -p "$raw" && chown ${owner.uid}:${owner.gid} "$raw"`);
    if (exitCode !== 0) throw new Error(`Could not create ${normalizeRel(rel)}: ${stderr.trim()}`);
  },

  async remove(serverId, rel) {
    const norm = normalizeRel(rel);
    if (!norm) throw new PathEscapeError(String(rel)); // never rm -rf the data root itself
    const { exitCode, stderr } = await run(serverId, rel, 'rm -rf "$raw"', { timeoutMs: 300000 });
    if (exitCode !== 0) throw new Error(`Could not delete ${norm}: ${stderr.trim()}`);
  },

  async rename(serverId, from, to) {
    const src = mountPath(from);
    const dest = normalizeRel(to);
    if (!dest) throw new PathEscapeError(String(to));
    const parent = dest.includes('/') ? dest.slice(0, dest.lastIndexOf('/')) : '';
    await remoteBackend.mkdir(serverId, parent);
    const { exitCode, stderr } = await run(
      serverId,
      to,
      `mkdir -p "$(dirname "$raw")" && mv -T ${shellQuote(src)} "$raw"`,
      { timeoutMs: 300000 }
    );
    if (exitCode !== 0) throw new Error(`Could not move ${normalizeRel(from)}: ${stderr.trim()}`);
  },

  async copy(serverId, from, to) {
    const src = mountPath(from);
    const { exitCode, stderr } = await run(
      serverId,
      to,
      `mkdir -p "$(dirname "$raw")" && cp -aT ${shellQuote(src)} "$raw"`,
      { timeoutMs: 600000 }
    );
    if (exitCode !== 0) throw new Error(`Could not copy ${normalizeRel(from)}: ${stderr.trim()}`);
  },

  /**
   * Total size plus the size of each immediate child, in one call. The size
   * index refreshes every server on a timer, so this has to be one round trip -
   * a walk would be thousands.
   */
  async duTree(serverId, rel) {
    const { stdout, exitCode } = await run(
      serverId,
      rel,
      'du -ab --max-depth=1 "$p"; echo ---; find -P "$p" -type f | wc -l',
      { timeoutMs: 300000 }
    );
    if (exitCode !== 0) return { size: 0, files: 0, children: new Map() };
    const [sizes, fileCount] = stdout.toString('utf8').split('---');
    const base = mountPath(rel);
    const children = new Map();
    let size = 0;
    for (const line of sizes.trim().split('\n')) {
      const tab = line.indexOf('\t');
      if (tab === -1) continue;
      const bytes = Number(line.slice(0, tab)) || 0;
      const at = line.slice(tab + 1).trim();
      if (at === base)
        size = bytes; // du prints the total last
      else if (at.startsWith(`${base}/`)) children.set(at.slice(base.length + 1), bytes);
    }
    return { size, files: Number(String(fileCount).trim()) || 0, children };
  },

  async du(serverId, rel) {
    const { stdout, exitCode } = await run(serverId, rel, 'du -sb "$p" | cut -f1; find -P "$p" -type f | wc -l', {
      timeoutMs: 300000,
    });
    if (exitCode !== 0) return { size: 0, files: 0 }; // missing dir reads as empty, like the local walker
    const [size, files] = stdout.toString('utf8').trim().split(/\s+/).map(Number);
    return { size: size || 0, files: files || 0 };
  },

  async readStream(serverId, rel) {
    // getArchive is the streaming read: one tar entry, unwrapped on the fly, so
    // a multi-GB world download never buffers in the panel.
    const stream = await sidecar().getArchive(serverId, mountPath(rel));
    const extract = tarStream.extract();
    const out = new Readable({ read() {} });
    extract.on('entry', (header, entryStream, next) => {
      if (header.type !== 'file') {
        entryStream.resume();
        entryStream.on('end', next);
        return;
      }
      entryStream.on('data', (chunk) => out.push(chunk));
      entryStream.on('end', next);
      entryStream.on('error', (err) => out.destroy(err));
    });
    extract.on('finish', () => out.push(null));
    extract.on('error', (err) => out.destroy(err));
    stream.on('error', (err) => out.destroy(err));
    stream.pipe(extract);
    return out;
  },

  /**
   * A tar entry's length goes in its header, so a stream can only be sent
   * straight through when its size is known up front. Without one the content
   * is collected first (bounded by READ_CAP_BYTES) rather than guessed at.
   */
  async writeStream(serverId, rel, source, { size } = {}) {
    const norm = normalizeRel(rel);
    if (!norm) throw new PathEscapeError(String(rel));
    const parent = norm.includes('/') ? norm.slice(0, norm.lastIndexOf('/')) : '';
    await remoteBackend.mkdir(serverId, parent);
    const owner = await ownership(serverId);
    const name = norm.split('/').pop();

    if (!Number.isFinite(size)) {
      const chunks = [];
      let total = 0;
      for await (const chunk of source) {
        total += chunk.length;
        if (total > READ_CAP_BYTES) throw httpError(413, 'That file is too large to send from here.');
        chunks.push(chunk);
      }
      await sidecar().putArchive(serverId, singleFileTar(name, Buffer.concat(chunks), owner), mountPath(parent));
      return;
    }

    const pack = tarStream.pack();
    const entry = pack.entry({ name, size, mode: 0o644, uid: owner.uid, gid: owner.gid, mtime: new Date() });
    const put = sidecar().putArchive(serverId, pack, mountPath(parent));
    await new Promise((resolve, reject) => {
      pipeline(source, entry, (err) => {
        if (err) return reject(err);
        pack.finalize();
        resolve();
      });
    });
    await put;
  },

  async uploadFile(serverId, localPath, rel) {
    const { size } = await fsp.stat(localPath);
    await remoteBackend.writeStream(serverId, rel, fs.createReadStream(localPath), { size });
  },

  async uploadDir(serverId, localDir, rel) {
    await remoteBackend.mkdir(serverId, rel);
    const owner = await ownership(serverId);
    const pack = tarStream.pack();
    const put = sidecar().putArchive(serverId, pack, mountPath(rel));
    await packLocalDir(pack, localDir, '', owner);
    pack.finalize();
    await put;
  },

  async downloadFile(serverId, rel, localPath) {
    await fsp.mkdir(path.dirname(localPath), { recursive: true });
    const source = await remoteBackend.readStream(serverId, rel);
    await streamPromises.pipeline(source, fs.createWriteStream(localPath));
  },

  async downloadDir(serverId, rel, localDir) {
    await fsp.mkdir(localDir, { recursive: true });
    const stream = await sidecar().getArchive(serverId, mountPath(rel));
    await unpackTarTo(stream, localDir, { stripFirstComponent: true });
  },
};

function notFound(rel, stderr = '') {
  const err = new Error(
    `ENOENT: no such file or directory '${normalizeRel(rel)}'${stderr ? ` (${stderr.trim()})` : ''}`
  );
  err.code = 'ENOENT';
  return err;
}

/** Feed a local directory tree into a tar pack, preserving relative layout. */
async function packLocalDir(pack, dir, prefix, owner) {
  const dirents = await fsp.readdir(dir, { withFileTypes: true });
  for (const d of dirents) {
    const abs = path.join(dir, d.name);
    const name = prefix ? `${prefix}/${d.name}` : d.name;
    if (d.isDirectory()) {
      pack.entry({ name, type: 'directory', mode: 0o755, uid: owner.uid, gid: owner.gid });
      await packLocalDir(pack, abs, name, owner);
    } else if (d.isFile()) {
      const st = await fsp.stat(abs);
      const entry = pack.entry({ name, size: st.size, mode: 0o644, uid: owner.uid, gid: owner.gid, mtime: st.mtime });
      await streamPromises.pipeline(fs.createReadStream(abs), entry);
    }
  }
}

/**
 * Unpack a tar stream into a local directory. Docker's getArchive names every
 * entry under the directory it was asked for, so stripFirstComponent drops that
 * wrapper and lands the contents directly in `dest`.
 */
async function unpackTarTo(stream, dest, { stripFirstComponent = false } = {}) {
  const extract = tarStream.extract();
  await new Promise((resolve, reject) => {
    extract.on('entry', (header, entryStream, next) => {
      const relName = stripFirstComponent ? header.name.split('/').slice(1).join('/') : header.name;
      if (!relName) {
        entryStream.resume();
        entryStream.on('end', next);
        return;
      }
      // The tar comes from a directory the server itself can write, so treat
      // its names as hostile: anything climbing out of `dest` is dropped.
      let abs;
      try {
        abs = safeJoin(dest, relName);
      } catch {
        entryStream.resume();
        entryStream.on('end', next);
        return;
      }
      if (header.type === 'directory') {
        fsp
          .mkdir(abs, { recursive: true })
          .then(() => {
            entryStream.resume();
            entryStream.on('end', next);
          })
          .catch(reject);
        return;
      }
      if (header.type !== 'file') {
        entryStream.resume();
        entryStream.on('end', next);
        return;
      }
      fsp
        .mkdir(path.dirname(abs), { recursive: true })
        .then(() => streamPromises.pipeline(entryStream, fs.createWriteStream(abs)))
        .then(() => next())
        .catch(reject);
    });
    extract.on('finish', resolve);
    extract.on('error', reject);
    stream.on('error', reject);
    stream.pipe(extract);
  });
}

/* ------------------------------------------------------------------ *
 * Public handle
 * ------------------------------------------------------------------ */

/** One grep inside the sidecar, trimmed to the same ceilings the local walk uses. */
async function remoteGrep(serverId, rel, needle, { caseSensitive, skipDirs, maxMatches }) {
  const excludes = skipDirs.map((d) => `--exclude-dir=${shellQuote(d)}`).join(' ');
  const flags = `-rnIF${caseSensitive ? '' : 'i'}`;
  const { stdout } = await run(
    serverId,
    rel,
    `grep ${flags} ${excludes} -e ${shellQuote(needle)} -- "$p" 2>/dev/null | head -n ${maxMatches + 1}`,
    { timeoutMs: 120000 }
  );
  const base = mountPath(rel);
  const lines = stdout.toString('utf8').split('\n').filter(Boolean);
  const truncated = lines.length > maxMatches;
  const matches = [];
  const seenFiles = new Set();
  for (const raw of lines.slice(0, maxMatches)) {
    // grep prints <absolute path>:<line>:<text>; the text itself may hold colons.
    const first = raw.indexOf(':');
    const second = raw.indexOf(':', first + 1);
    if (first === -1 || second === -1) continue;
    const abs = raw.slice(0, first);
    const line = Number(raw.slice(first + 1, second));
    const text = raw.slice(second + 1);
    const relPath = abs.startsWith(`${base}/`) ? abs.slice(base.length + 1) : abs.replace(`${MOUNT}/`, '');
    seenFiles.add(relPath);
    const hay = caseSensitive ? text : text.toLowerCase();
    const col = hay.indexOf(caseSensitive ? needle : needle.toLowerCase());
    matches.push({ path: relPath, line, col: col + 1, text: text.length > 300 ? `${text.slice(0, 300)}…` : text });
  }
  return { matches, truncated, filesScanned: seenFiles.size };
}

/** The local walk: bounded by files scanned, per-file size, and total matches. */
async function localGrep(rootAbs, needle, { caseSensitive, skipDirs, maxMatches, maxFileBytes, maxFiles }) {
  const skip = new Set(skipDirs);
  const hay = caseSensitive ? null : needle.toLowerCase();
  const matches = [];
  let filesScanned = 0;
  let truncated = false;

  const step = async (dirAbs, dirRel) => {
    if (truncated) return;
    let dirents;
    try {
      dirents = await fsp.readdir(dirAbs, { withFileTypes: true });
    } catch {
      return;
    }
    for (const e of dirents) {
      if (truncated) return;
      const childRel = dirRel ? `${dirRel}/${e.name}` : e.name;
      const childAbs = path.join(dirAbs, e.name);
      if (e.isDirectory()) {
        if (skip.has(e.name)) continue;
        await step(childAbs, childRel);
        continue;
      }
      if (!e.isFile()) continue;
      if (++filesScanned > maxFiles) {
        truncated = true;
        return;
      }
      let st;
      try {
        st = await fsp.stat(childAbs);
      } catch {
        continue;
      }
      if (st.size === 0 || st.size > maxFileBytes) continue;
      let buf;
      try {
        buf = await fsp.readFile(childAbs);
      } catch {
        continue;
      }
      if (buf.subarray(0, 8192).includes(0)) continue; // binary
      const lines = buf.toString('utf8').split('\n');
      for (let i = 0; i < lines.length; i++) {
        const line = lines[i];
        const idx = caseSensitive ? line.indexOf(needle) : line.toLowerCase().indexOf(hay);
        if (idx === -1) continue;
        matches.push({
          path: childRel,
          line: i + 1,
          col: idx + 1,
          text: line.length > 300 ? `${line.slice(0, 300)}…` : line,
        });
        if (matches.length >= maxMatches) {
          truncated = true;
          return;
        }
        break; // one hit per line is enough for a results list
      }
    }
  };

  await step(rootAbs, '');
  return { matches, truncated, filesScanned };
}

let tmpSeq = 0;
async function makeTmpDir(label) {
  const dir = path.join(config.dataDir, 'tmp', `sfs-${label}-${process.pid}-${Date.now().toString(36)}-${tmpSeq++}`);
  await fsp.mkdir(dir, { recursive: true });
  return dir;
}

/**
 * A handle over one directory tree. `remote` decides which backend answers;
 * `key` is the server id for the remote backend and the absolute local root for
 * the local one. Every path callers pass is relative to that tree.
 */
function makeHandle({ remote, key, serverId, label }) {
  const be = remote ? remoteBackend : localBackend;
  const api = {
    serverId: serverId || null,
    /** Where this tree physically is: 'volume' on the Docker host, or 'bind' here. */
    storage: remote ? 'volume' : 'bind',
    remote,

    /**
     * Absolute path on the panel's disk. ONLY valid for local storage; a remote
     * caller must use the read/write helpers (or withLocalCopy) instead of a
     * path that does not exist on this machine.
     */
    localPath(rel = '') {
      if (remote) {
        throw new Error(
          `Server ${serverId} keeps its files on the Docker host, so they have no path on this machine. ` +
            'Use the serverFs helpers (readFile/writeFile/withLocalCopy) instead.'
        );
      }
      return localAbs(key, rel);
    },

    stat: (rel) => be.stat(key, rel),
    statMany: (rels) => be.statMany(key, rels),
    readdir: (rel = '') => be.readdir(key, rel),
    readdirMany: (rels) => be.readdirMany(key, rels),
    readdirSized: (rel = '', opts) => be.readdirSized(key, rel, opts),
    walk: (rel = '', opts) => be.walk(key, rel, opts),
    readFile: (rel) => be.readFile(key, rel),
    readMany: (rels) => be.readMany(key, rels),
    writeFile: (rel, data) => be.writeFile(key, rel, data),
    mkdir: (rel) => be.mkdir(key, rel),
    remove: (rel) => be.remove(key, rel),
    rename: (from, to) => be.rename(key, from, to),
    copy: (from, to) => be.copy(key, from, to),
    du: (rel = '') => be.du(key, rel),
    duTree: (rel = '') => be.duTree(key, rel),
    readStream: (rel) => be.readStream(key, rel),
    writeStream: (rel, source, opts) => be.writeStream(key, rel, source, opts),
    uploadFile: (localFile, rel) => be.uploadFile(key, localFile, rel),
    uploadDir: (localDir, rel) => be.uploadDir(key, localDir, rel),
    downloadFile: (rel, localFile) => be.downloadFile(key, rel, localFile),
    downloadDir: (rel, localDir) => be.downloadDir(key, rel, localDir),

    /** stat() that answers null instead of throwing when the path isn't there. */
    async statOrNull(rel) {
      try {
        return await api.stat(rel);
      } catch (err) {
        if (err.code === 'ENOENT') return null;
        throw err;
      }
    },

    async exists(rel) {
      return (await api.statOrNull(rel)) !== null;
    },

    /** Read a text file, or null when it isn't there. */
    async readText(rel, encoding = 'utf8') {
      try {
        return (await api.readFile(rel)).toString(encoding);
      } catch (err) {
        if (err.code === 'ENOENT') return null;
        throw err;
      }
    },

    /**
     * readJson for several files at once - one round trip, whatever the
     * backend. Same per-file answer as readJson: null when missing or broken.
     */
    async readJsonMany(rels) {
      const buffers = await api.readMany(rels);
      return buffers.map((buf) => {
        if (!buf) return null;
        try {
          return JSON.parse(buf.toString('utf8'));
        } catch {
          return null;
        }
      });
    },

    /** Read and JSON.parse a file, or null when it's missing or unparseable. */
    async readJson(rel) {
      const text = await api.readText(rel);
      if (text === null) return null;
      try {
        return JSON.parse(text);
      } catch {
        return null;
      }
    },

    /**
     * Run `fn(absolutePath)` against a real file on this machine. Local storage
     * hands over the file itself; a volume pulls a temp copy first and deletes
     * it afterwards. For readers that only take a path (NBT, zip).
     */
    async withLocalCopy(rel, fn) {
      if (!remote) return fn(localAbs(key, rel));
      const dir = await makeTmpDir(label);
      const local = path.join(dir, path.basename(normalizeRel(rel)) || 'file');
      try {
        await api.downloadFile(rel, local);
        return await fn(local);
      } finally {
        await fsp.rm(dir, { recursive: true, force: true }).catch(() => {});
      }
    },

    /** withLocalCopy for a whole directory. Pulls the subtree, then cleans up. */
    async withLocalDir(rel, fn) {
      if (!remote) return fn(localAbs(key, rel));
      const dir = await makeTmpDir(label);
      try {
        await api.downloadDir(rel, dir);
        return await fn(dir);
      } finally {
        await fsp.rm(dir, { recursive: true, force: true }).catch(() => {});
      }
    },

    /**
     * Plain-substring search across the text files under `rel`. A volume runs
     * one grep inside the sidecar - a file-by-file walk would be a round trip
     * per file, and a modpack has tens of thousands. Bounded on matches either
     * way. Returns { matches:[{path,line,col,text}], truncated, filesScanned }.
     */
    async grepFiles(rel, needle, opts = {}) {
      const {
        caseSensitive = false,
        skipDirs = [],
        maxMatches = 300,
        maxFileBytes = 2 * 1024 * 1024,
        maxFiles = 8000,
      } = opts;
      if (remote) return remoteGrep(key, rel, needle, { caseSensitive, skipDirs, maxMatches });
      return localGrep(localAbs(key, rel), needle, { caseSensitive, skipDirs, maxMatches, maxFileBytes, maxFiles });
    },

    /**
     * Stream a whole subtree as a tar. Backup and export turn it into a zip on
     * the panel, so a remote server's backups still land on the panel's disk
     * and never need space on the Docker host.
     */
    async tarStream(rel = '') {
      if (remote) return sidecar().getArchive(key, mountPath(rel));
      const tar = require('tar');
      const base = localAbs(key, rel);
      return tar.c({ cwd: path.dirname(base), portable: true }, [path.basename(base)]);
    },
  };
  return api;
}

/** Handle for a server's /data, wherever that lives. */
function forServer(serverId) {
  return isRemote()
    ? makeHandle({ remote: true, key: serverId, serverId, label: serverId })
    : makeHandle({ remote: false, key: localRoot(serverId), serverId, label: serverId });
}

/**
 * Handle for a directory on the panel's own disk (the admin file manager's
 * DATA_DIR scope, the mod library, blueprints). Always local - only a server's
 * /data ever moves to the Docker host.
 */
function forLocalRoot(absRoot, label = 'local') {
  return makeHandle({ remote: false, key: absRoot, serverId: null, label });
}

module.exports = {
  for: forServer,
  forLocalRoot,
  isRemote,
  normalizeRel,
  unpackTarTo,
};
