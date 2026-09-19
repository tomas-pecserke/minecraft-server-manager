'use strict';

// Connect addresses have to name the machine the container actually runs on -
// with a remote Docker host, this machine's interfaces reach nothing.

require('./helpers/env');
const test = require('node:test');
const assert = require('node:assert/strict');
require('../src/db/migrate').migrate();

test('connect addresses name the machine the container runs on', async () => {
  const connect = require('../src/services/connectAddress');
  const cfg = require('../src/config');
  const settings = require('../src/services/settings');
  const before = cfg.dockerHostname;
  try {
    cfg.dockerHostname = null; // local daemon
    const local = connect.connectCandidates(25565);
    assert.ok(local.includes('localhost:25565'), 'a local daemon publishes here, so this machine is a valid address');
    assert.equal(connect.wanIpDescribesServer(), true);

    cfg.dockerHostname = '10.1.2.3'; // daemon on another machine, same network
    const remote = connect.connectCandidates(25565);
    assert.deepEqual(remote, ['10.1.2.3:25565'], 'only the Docker host can be reached');
    assert.ok(!remote.some((a) => a.startsWith('localhost')), 'never this machine');
    assert.equal(connect.wanIpDescribesServer(), true, 'a LAN daemon shares our public address');

    cfg.dockerHostname = 'mc.example.com'; // daemon somewhere else entirely
    assert.equal(connect.wanIpDescribesServer(), false, 'our WAN IP says nothing about that host');

    const publicHost = settings.getPublicHost && settings.getPublicHost();
    if (!publicHost) {
      assert.equal(connect.connectCandidates(25565)[0], 'mc.example.com:25565');
    }
  } finally {
    cfg.dockerHostname = before;
  }
});
