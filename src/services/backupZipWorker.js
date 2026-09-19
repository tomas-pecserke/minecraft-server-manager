'use strict';

// Runs the actual zip compression for a backup off the main thread. archiver
// streams from disk (or, for a server whose files live on the Docker host, from
// a tar stream the daemon sends), but the zlib deflate + framing still burn
// main-thread CPU on a large modded world; here they burn a worker's instead,
// so the panel stays responsive mid-backup.
//
// Messages back to the parent: {type:'progress', processedBytes} repeatedly,
// then exactly one of {type:'done'} / {type:'error', message}.

const fs = require('node:fs');
const { parentPort, workerData } = require('node:worker_threads');
const archiver = require('archiver');

// sourceDir - a directory on this machine (bind storage).
// sidecarContainer - the file sidecar to stream /data out of (volume storage).
// The parent keeps that sidecar alive for as long as this worker runs.
const { sourceDir, outFile, sidecarContainer } = workerData;

let settled = false;
const finish = (msg) => {
  if (settled) return;
  settled = true;
  parentPort.postMessage(msg);
};

const output = fs.createWriteStream(outFile);
const archive = archiver('zip', { zlib: { level: 6 } });

const fail = (err) => {
  if (settled) return;
  // Drop the half-written archive so a repeatedly failing scheduled backup can't
  // leak fds / orphan partial files.
  try {
    output.destroy();
  } catch {
    /* already gone */
  }
  fs.rm(outFile, { force: true }, () =>
    finish({ type: 'error', message: err && err.message ? err.message : String(err) })
  );
};

output.on('close', () => finish({ type: 'done' }));
output.on('error', fail);
archive.on('error', fail);
archive.on('progress', (d) => {
  if (!settled) parentPort.postMessage({ type: 'progress', processedBytes: d.fs.processedBytes });
});

archive.pipe(output);

if (sidecarContainer) {
  // Volume storage: the files are on the daemon's host. Docker hands them over
  // as one tar stream, which is unpacked entry by entry straight into the zip -
  // nothing is ever staged on this machine's disk.
  const tarStream = require('tar-stream');
  const { getDocker } = require('../docker/connect');

  let processedBytes = 0;
  const extract = tarStream.extract();
  extract.on('entry', (header, entryStream, next) => {
    // Docker names every entry under the directory it was asked for; drop that
    // wrapper so the zip's layout matches a bind-storage backup exactly.
    const name = header.name.split('/').slice(1).join('/');
    if (!name || header.type !== 'file') {
      entryStream.resume();
      entryStream.on('end', next);
      return;
    }
    // archiver is the only consumer of this stream - 'end' fires once it has
    // read the entry through, which is also what paces the tar extraction, so
    // one file is in flight at a time and nothing buffers up.
    archive.append(entryStream, { name, date: header.mtime });
    entryStream.on('end', () => {
      processedBytes += header.size || 0;
      if (!settled) parentPort.postMessage({ type: 'progress', processedBytes });
      next();
    });
    entryStream.on('error', fail);
  });
  extract.on('finish', () => archive.finalize());
  extract.on('error', fail);

  getDocker()
    .getContainer(sidecarContainer)
    .getArchive({ path: '/data' })
    .then((stream) => {
      stream.on('error', fail);
      stream.pipe(extract);
    })
    .catch(fail);
} else {
  archive.directory(sourceDir, false);
  archive.finalize();
}
