// @ts-nocheck - dynamic Docker/NBT/HTTP-JSON interop; not yet under checkJs (incremental typing).
'use strict';

// The file-access sidecar: a tiny root container that mounts a server's data
// volume at /data and does nothing else. It is how the panel reaches server
// files when the daemon doesn't share a filesystem with it (a remote Docker
// host) - the Docker API can copy archives in and out of a container and run
// commands in one, but it cannot touch a volume directly.
//
// One sidecar per server, started on first use and stopped again after
// SIDECAR_IDLE_SECONDS of quiet - stopped, not removed, so the next use starts
// it rather than building it. It is independent of the Minecraft container, so
// files stay reachable while the server is stopped, crashed, or not yet created.

const path = require('node:path');
const { getDocker } = require('./connect');
const { ensureVolume, volumeName } = require('./volumes');
const images = require('./images');
const config = require('../config');
const logger = require('../logger')(path.basename(__filename));

const LABEL = 'msm.id';
const EXEC_TIMEOUT_MS = 30000;

/** serverId -> { container, users, idleTimer } for the sidecars we have up. */
const live = new Map();
/** serverId -> in-flight start, so concurrent callers share one container. */
const starting = new Map();

function sidecarName(serverId) {
  return `msm-fs-${serverId}`;
}

/**
 * Start (or adopt) the sidecar and hand back its container handle. Callers must
 * pair this with release() - prefer withSidecar(), which does that for them.
 */
async function acquire(serverId) {
  const entry = live.get(serverId);
  if (entry) {
    entry.users += 1;
    clearTimeout(entry.idleTimer);
    entry.idleTimer = null;
    return entry.container;
  }
  if (!starting.has(serverId)) {
    starting.set(
      serverId,
      startSidecar(serverId).finally(() => starting.delete(serverId))
    );
  }
  const container = await starting.get(serverId);
  const existing = live.get(serverId);
  if (existing) {
    existing.users += 1;
    clearTimeout(existing.idleTimer);
    existing.idleTimer = null;
  } else {
    live.set(serverId, { container, users: 1, idleTimer: null });
  }
  return container;
}

/** Give the sidecar back. The last release starts the idle countdown. */
function release(serverId) {
  const entry = live.get(serverId);
  if (!entry) return;
  entry.users = Math.max(0, entry.users - 1);
  if (entry.users > 0) return;
  clearTimeout(entry.idleTimer);
  entry.idleTimer = setTimeout(() => {
    // Re-check: a caller may have taken it again between the timer firing and
    // this running, in which case stopping it would break their operation.
    const current = live.get(serverId);
    if (!current || current.users > 0) return;
    live.delete(serverId);
    // Stopped, not removed: a stopped container costs nothing and starting it
    // again is half the work of building a new one, which is what the first
    // click on a server's files after an idle spell would otherwise pay.
    current.container.stop({ t: 1 }).catch((err) => {
      if (err.statusCode === 304 || err.statusCode === 404) return; // already stopped or gone
      logger.debug('Could not stop an idle file sidecar.', { serverId, err: err.message });
    });
  }, config.sidecarIdleMs);
  entry.idleTimer.unref?.();
}

/** True when the sidecar we were holding can no longer serve us: gone, or stopped. */
function isUnusable(err) {
  if (!err) return false;
  if (err.statusCode === 404 || err.statusCode === 409) return true;
  return /no such container|is not running/i.test(err.message || '');
}

/**
 * Run `fn` with the sidecar up, releasing it however `fn` ends.
 *
 * A sidecar we hold a handle to can go away underneath us - another panel
 * process stopped it after its own idle spell, or someone ran `docker stop`
 * or `docker rm`. That must not break every file operation until a restart,
 * so "no such container" and "is not running" both drop the cached handle and
 * bring one back once.
 */
async function withSidecar(serverId, fn) {
  for (let attempt = 0; ; attempt += 1) {
    const container = await acquire(serverId);
    try {
      return await fn(container);
    } catch (err) {
      if (attempt > 0 || !isUnusable(err)) throw err;
      live.delete(serverId);
      logger.debug('The file sidecar was not usable; bringing it back.', { serverId, err: err.message });
    } finally {
      release(serverId);
    }
  }
}

/**
 * Image for the sidecar. The server's own image by default: it is already on
 * that host, and its GNU find/stat/du are what the listing and size code use
 * (BusyBox's cut-down versions don't take the same formats). services/servers
 * requires this module's neighbours, so the require is lazy - see CLAUDE.md on
 * mid-function requires.
 */
function sidecarImage(serverId) {
  if (config.sidecarImage) return config.sidecarImage;
  const servers = require('../services/servers');
  const server = servers.getServer(serverId);
  if (!server) throw new Error(`Cannot start a file sidecar for unknown server ${serverId}.`);
  return servers.resolveImage(server);
}

async function startSidecar(serverId) {
  const docker = getDocker();
  const name = sidecarName(serverId);

  // Look for the existing sidecar FIRST. The common case - it is there, merely
  // stopped after an idle spell - then costs an inspect and a start, instead of
  // also checking the volume and the image on every cold read.
  let container = docker.getContainer(name);
  let info = await container.inspect().catch((err) => {
    if (err.statusCode === 404) return null;
    throw err;
  });

  // A sidecar left behind by a previous panel run is ours to reuse, but only if
  // it still mounts the volume we expect - an image or mount change since then
  // means the old one is wrong, so replace it rather than write to the wrong place.
  if (info && (!mountsVolume(info, serverId) || hasHealthcheck(info))) {
    await container.remove({ force: true }).catch(() => {});
    info = null;
  }
  if (!info) {
    await ensureVolume(serverId);
    const image = sidecarImage(serverId);
    await images.ensureImage(image);
    container = await docker.createContainer({
      name,
      Image: image,
      // BusyBox sleep takes no "infinity", so loop instead. The sidecar must
      // simply stay up; it does all its work through exec and the archive API.
      Entrypoint: ['sh', '-c', 'while :; do sleep 3600; done'],
      Cmd: [],
      User: '0:0', // root reads and writes files of whatever uid the server runs as
      // The server's image carries itzg's mc-health HEALTHCHECK. The sidecar
      // never runs Minecraft, so that probe fails forever and the container
      // sits 'unhealthy'. Disable the inherited check rather than answer it.
      Healthcheck: { Test: ['NONE'] },
      Labels: { [LABEL]: serverId, 'msm.managed': 'true', 'msm.role': 'fs' },
      HostConfig: {
        Binds: [`${volumeName(serverId)}:/data`],
        NetworkMode: 'none', // it only ever touches the volume
        AutoRemove: false,
      },
    });
  }
  await container.start().catch((err) => {
    if (err.statusCode !== 304) throw err; // 304 = already running
  });
  logger.debug('Started a file sidecar.', { serverId, created: !info });
  return container;
}

/** A sidecar from before the healthcheck was disabled - replace it, don't adopt it. */
function hasHealthcheck(info) {
  const test = info.Config && info.Config.Healthcheck && info.Config.Healthcheck.Test;
  return Array.isArray(test) && test.length > 0 && test[0] !== 'NONE';
}

function mountsVolume(info, serverId) {
  const want = volumeName(serverId);
  return (info.Mounts || []).some((m) => m.Name === want && m.Destination === '/data');
}

async function teardown(container) {
  await container.remove({ force: true }).catch((err) => {
    if (err.statusCode !== 404) throw err;
  });
}

/**
 * Stop every sidecar this process started. Called on shutdown - stopped, not
 * removed, so the next run adopts them instead of building them again.
 */
async function stopAll() {
  const entries = [...live.entries()];
  live.clear();
  await Promise.all(
    entries.map(([serverId, entry]) => {
      clearTimeout(entry.idleTimer);
      return entry.container.stop({ t: 1 }).catch((err) => {
        if (err.statusCode === 304 || err.statusCode === 404) return;
        logger.debug('Could not stop a file sidecar during shutdown.', { serverId, err: err.message });
      });
    })
  );
}

/** Remove a server's sidecar outright (used when the server itself goes away). */
async function remove(serverId) {
  const entry = live.get(serverId);
  if (entry) {
    clearTimeout(entry.idleTimer);
    live.delete(serverId);
  }
  const container = getDocker().getContainer(sidecarName(serverId));
  await teardown(container);
}

/**
 * Remove file sidecars left behind by a previous panel process. They hold no
 * state - whatever they were doing died with that process - and a stale one
 * keeps its volume busy, so boot starts from none.
 */
async function pruneOrphans() {
  const containers = await getDocker().listContainers({
    all: true,
    filters: { label: ['msm.role=fs'] },
  });
  // A sidecar for a server this panel still knows about is kept, running or
  // stopped: adopting it is what makes the first file read after a restart
  // cheap. Only one whose server is gone (a delete that died halfway) is
  // removed - nothing will ever adopt that.
  const servers = require('../services/servers');
  const known = new Set(servers.listServers().map((s) => s.id));
  let removed = 0;
  for (const info of containers) {
    const serverId = info.Labels && info.Labels[LABEL];
    if (live.has(serverId) || known.has(serverId)) continue;
    await getDocker()
      .getContainer(info.Id)
      .remove({ force: true })
      .catch(() => {});
    removed += 1;
  }
  if (removed) logger.info('Removed file sidecars whose servers are gone.', { count: removed });
}

/**
 * Run a command in the sidecar and capture both streams plus, unless the caller
 * opts out, the exit code. Never throws on a non-zero exit - the caller decides
 * what that means (a missing file is an error for a read and a success for a
 * delete).
 */
async function exec(serverId, argv, { timeoutMs = EXEC_TIMEOUT_MS, wantExitCode = true } = {}) {
  return withSidecar(serverId, async (container) => {
    const label = argv.join(' ');
    const handle = await container.exec({ Cmd: argv, AttachStdout: true, AttachStderr: true, User: '0:0' });
    const stream = await handle.start({});
    const { stdout, stderr } = await new Promise((resolve, reject) => {
      const outChunks = [];
      const errChunks = [];
      let settled = false;
      const finish = (fn, arg) => {
        if (settled) return;
        settled = true;
        clearTimeout(timer);
        try {
          stream.destroy();
        } catch {
          /* already gone */
        }
        fn(arg);
      };
      const timer = setTimeout(
        () => finish(reject, new Error(`sidecar exec timed out after ${timeoutMs}ms: ${label}`)),
        timeoutMs
      );
      timer.unref?.();
      getDocker().modem.demuxStream(stream, { write: (b) => outChunks.push(b) }, { write: (b) => errChunks.push(b) });
      stream.on('end', () =>
        finish(resolve, { stdout: Buffer.concat(outChunks), stderr: Buffer.concat(errChunks).toString('utf8') })
      );
      stream.on('error', (err) => finish(reject, err));
    });
    // Asking the daemon for the exit code is a THIRD round trip on top of
    // create and start, and on a remote host that is most of what a small file
    // read costs. Callers that can read the status off the command's own output
    // pass wantExitCode: false and skip it (serverFs does - see run()).
    let exitCode = null;
    if (wantExitCode) {
      try {
        const inspected = await handle.inspect();
        if (typeof inspected.ExitCode === 'number') exitCode = inspected.ExitCode;
      } catch {
        /* exit code stays unknown */
      }
    }
    return { stdout, stderr, exitCode };
  });
}

/** Read a path out of the volume as a tar stream. Caller consumes the stream. */
async function getArchive(serverId, containerPath) {
  const container = await acquire(serverId);
  let stream;
  try {
    stream = await container.getArchive({ path: containerPath });
  } catch (err) {
    release(serverId);
    throw err;
  }
  // The sidecar has to stay up until the stream is drained, so the release is
  // tied to the stream's end rather than to this function returning. A stream
  // fires 'end' AND 'close', so the release is guarded: releasing twice for one
  // acquire lets the idle timer stop the container under another caller's feet.
  let released = false;
  const done = () => {
    if (released) return;
    released = true;
    release(serverId);
  };
  stream.once('end', done);
  stream.once('close', done);
  stream.once('error', done);
  return stream;
}

/** Write a tar stream into the volume, rooted at `containerPath`. */
async function putArchive(serverId, tarStream, containerPath) {
  return withSidecar(serverId, (container) => container.putArchive(tarStream, { path: containerPath }));
}

/** Stat one path. Returns Docker's stat object, or null when it isn't there. */
async function infoArchive(serverId, containerPath) {
  return withSidecar(serverId, async (container) => {
    try {
      return await container.infoArchive({ path: containerPath });
    } catch (err) {
      if (err.statusCode === 404) return null;
      throw err;
    }
  });
}

module.exports = {
  sidecarName,
  pruneOrphans,
  acquire,
  release,
  withSidecar,
  exec,
  getArchive,
  putArchive,
  infoArchive,
  remove,
  stopAll,
};
