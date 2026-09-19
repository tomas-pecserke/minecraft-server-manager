'use strict';

// serverFs is the one way anything reaches a server's files, so its containment
// rules and its local backend are covered here. The volume backend needs a
// Docker daemon and is exercised by the live smoke sweep, not by this suite.

require('./helpers/env');
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const config = require('../src/config');
const serverFs = require('../src/storage/serverFs');

const SID = 'srv_sfs';
const root = path.join(config.dataDir, 'servers', SID);
fs.mkdirSync(root, { recursive: true });
const handle = serverFs.for(SID);

test('normalizeRel strips noise and keeps everything inside the server directory', () => {
  assert.equal(serverFs.normalizeRel(''), '');
  assert.equal(serverFs.normalizeRel('mods/foo.jar'), 'mods/foo.jar');
  assert.equal(serverFs.normalizeRel('./mods//foo.jar'), 'mods/foo.jar');
  assert.equal(serverFs.normalizeRel('config\\bluemap\\core.conf'), 'config/bluemap/core.conf');
  for (const bad of ['../secrets', 'mods/../../etc/passwd', '/etc/passwd', 'C:\\Windows', 'a\0b']) {
    assert.throws(() => serverFs.normalizeRel(bad), /escapes the panel data directory/, bad);
  }
});

test('a local handle reads, writes, lists, moves, copies and deletes', async () => {
  await handle.writeFile('server.properties', 'level-name=world\n');
  assert.equal(await handle.readText('server.properties'), 'level-name=world\n');
  assert.equal(await handle.readText('nope.txt'), null, 'a missing file reads as null, not a throw');

  await handle.writeFile('mods/a.jar', Buffer.from('jar-a'));
  await handle.writeFile('mods/b.jar', Buffer.from('jar-bb'));
  const entries = await handle.readdir('mods');
  assert.deepEqual(
    entries.map((e) => e.name).sort(),
    ['a.jar', 'b.jar'],
    'writeFile creates the parent directory on the way'
  );
  assert.equal(entries.find((e) => e.name === 'b.jar').size, 6);

  await handle.rename('mods/a.jar', 'mods/a.jar.disabled');
  assert.equal(await handle.exists('mods/a.jar'), false);
  assert.equal(await handle.exists('mods/a.jar.disabled'), true);

  await handle.copy('mods', 'mods-backup');
  assert.equal((await handle.readdir('mods-backup')).length, 2);

  await handle.remove('mods-backup');
  assert.equal(await handle.exists('mods-backup'), false);
});

test('stat, statMany and du describe a tree in one shape', async () => {
  await handle.writeFile('world/level.dat', Buffer.alloc(10));
  await handle.writeFile('world/region/r.0.0.mca', Buffer.alloc(100));

  const st = await handle.stat('world');
  assert.equal(st.dir, true);
  assert.equal(st.size, 0, 'a directory reports no size of its own - du is what measures a tree');

  const [level, missing] = await handle.statMany(['world/level.dat', 'world/nothing.dat']);
  assert.equal(level.size, 10);
  assert.equal(missing, null);

  const du = await handle.du('world');
  assert.equal(du.size, 110);
  assert.equal(du.files, 2);

  const walked = await handle.walk('world');
  assert.deepEqual(
    walked.map((e) => e.path).sort(),
    ['level.dat', 'region', 'region/r.0.0.mca'],
    'walk reports paths relative to the directory it was given'
  );
});

test('readdirSized answers a listing, its sizes and its probes in one shape', async () => {
  await handle.writeFile('sized/World/level.dat', Buffer.alloc(10));
  await handle.writeFile('sized/World/region/r.0.0.mca', Buffer.alloc(100));
  await handle.writeFile('sized/mods/a.jar', Buffer.alloc(7));
  await handle.writeFile('sized/notes.txt', Buffer.alloc(3));

  const entries = await handle.readdirSized('sized', { contains: ['level.dat'] });
  const byName = new Map(entries.map((e) => [e.name, e]));
  assert.deepEqual([...byName.keys()].sort(), ['World', 'mods', 'notes.txt']);
  assert.equal(byName.get('World').sizeBytes, 110, 'a directory carries the size of its whole tree');
  assert.equal(byName.get('mods').sizeBytes, 7);
  assert.equal(byName.get('notes.txt').sizeBytes, 3, 'a file carries its own size');
  assert.deepEqual(byName.get('World').contains, ['level.dat'], 'the probe finds the world');
  assert.deepEqual(byName.get('mods').contains, [], 'and says so when a directory has none');

  const bare = await handle.readdirSized('sized', { contains: ['level.dat'], withSizes: false });
  const world = bare.find((e) => e.name === 'World');
  assert.deepEqual(world.contains, ['level.dat'], 'skipping the sizes keeps the probes');
  assert.equal(world.sizeBytes, 0, 'a caller that skips the walk gets no directory sizes');
});

test('withLocalCopy hands a local server its own file, and streams round-trip', async () => {
  await handle.writeFile('logs/latest.log', 'hello\n');
  const seen = await handle.withLocalCopy('logs/latest.log', (abs) => fs.readFileSync(abs, 'utf8'));
  assert.equal(seen, 'hello\n');

  const chunks = [];
  const stream = await handle.readStream('logs/latest.log');
  for await (const chunk of stream) chunks.push(chunk);
  assert.equal(Buffer.concat(chunks).toString('utf8'), 'hello\n');
});

test('grepFiles finds a substring and honours the skip list', async () => {
  await handle.writeFile('config/one.cfg', 'alpha=1\nbeta=2\n');
  await handle.writeFile('cache/two.cfg', 'alpha=3\n');

  const hit = await handle.grepFiles('', 'beta', { skipDirs: ['cache'] });
  assert.equal(hit.matches.length, 1);
  assert.equal(hit.matches[0].path, 'config/one.cfg');
  assert.equal(hit.matches[0].line, 2);

  const skipped = await handle.grepFiles('', 'alpha', { skipDirs: ['cache'] });
  assert.deepEqual(
    skipped.matches.map((m) => m.path),
    ['config/one.cfg'],
    'a skipped directory is never scanned'
  );
});

test('a local-root handle works the same way against any panel directory', async () => {
  const libraryRoot = path.join(config.dataDir, 'library');
  const library = serverFs.forLocalRoot(libraryRoot, 'library');
  await library.writeFile('mods/x.jar', Buffer.from('x'));
  assert.equal(await library.exists('mods/x.jar'), true);
  assert.equal(library.localPath('mods/x.jar'), path.join(libraryRoot, 'mods', 'x.jar'));
  await assert.rejects(() => library.readFile('../panel.db'), /escapes the panel data directory/);
});

test('a local handle exposes real paths; a remote one refuses to invent them', () => {
  assert.equal(handle.storage, 'bind');
  assert.equal(handle.remote, false);
  assert.equal(handle.localPath('mods'), path.join(root, 'mods'));
  assert.equal(serverFs.isRemote(), false, 'SERVER_STORAGE defaults to bind without a remote DOCKER_HOST');
});

test('unpackTarTo lands a Docker-shaped archive and refuses entries that climb out', async () => {
  const tarStream = require('tar-stream');
  const pack = tarStream.pack();
  // Docker's getArchive names every entry under the directory it was asked for,
  // which is the wrapper stripFirstComponent drops.
  pack.entry({ name: 'data/config', type: 'directory', mode: 0o755 });
  pack.entry({ name: 'data/config/app.cfg', size: 4, mode: 0o644 }, 'test');
  pack.entry({ name: 'data/../escape.txt', size: 3, mode: 0o644 }, 'no!');
  pack.finalize();

  const dest = path.join(config.dataDir, 'tmp', 'unpack-test');
  await serverFs.unpackTarTo(pack, dest, { stripFirstComponent: true });

  assert.equal(fs.readFileSync(path.join(dest, 'config', 'app.cfg'), 'utf8'), 'test');
  assert.equal(fs.existsSync(path.join(path.dirname(dest), 'escape.txt')), false, 'a climbing entry is dropped');
});

test('a remote daemon takes its ports from the daemon host, not from this one', async () => {
  // probe() binds a local socket only when the daemon is local; with a remote
  // one it has to answer from the ports that host's containers publish.
  const ports = require('../src/services/ports');
  const config2 = require('../src/config');
  const taken = 24242;
  const original = { remote: config2.dockerRemote };
  const dockerConnect = require('../src/docker/connect');
  const realGetDocker = dockerConnect.getDocker;
  dockerConnect.getDocker = () => ({
    listContainers: async () => [{ Ports: [{ PublicPort: taken, Type: 'tcp' }] }],
  });
  config2.dockerRemote = true;
  try {
    assert.equal(await ports.probe(taken), false, 'a port published on the daemon host is not free');
    assert.equal(await ports.probe(taken + 1), true, 'anything else there is');
  } finally {
    config2.dockerRemote = original.remote;
    dockerConnect.getDocker = realGetDocker;
  }
});
