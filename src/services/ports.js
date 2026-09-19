'use strict';

const httpError = require('../utils/httpError');

// Host-port allocation. Scheme (user-approved): game ports first-free from
// 25565, RCON = game + 1000, Bedrock UDP first-free from 19132. A port is
// "taken" if any DB server claims it OR it is already bound where the
// containers actually run - this machine for a local daemon, the daemon host
// (as far as its own containers show) for a remote one.

const net = require('node:net');
const db = require('../db');
const config = require('../config');

// Ports already published by containers on the daemon's host. With a remote
// daemon this is the only view of "taken" we have - binding a socket here says
// nothing about a machine somewhere else, and a collision would otherwise only
// surface as a failed start. Cached for a few seconds because a port search
// asks about many candidates in a row.
let daemonPorts = { at: 0, ports: new Set() };
const DAEMON_PORTS_TTL_MS = 5000;

async function daemonPortsInUse() {
  if (Date.now() - daemonPorts.at < DAEMON_PORTS_TTL_MS) return daemonPorts.ports;
  const ports = new Set();
  try {
    // Running containers only: a stopped one holds nothing, and the panel's own
    // stopped servers are already covered by dbPortsInUse().
    // Lazily required so the docker layer stays swappable (and out of this
    // module's load-time graph).
    for (const c of await require('../docker/connect').getDocker().listContainers()) {
      for (const p of c.Ports || []) if (p.PublicPort) ports.add(p.PublicPort);
    }
  } catch {
    return daemonPorts.ports; // daemon down - keep the last answer rather than claiming everything is free
  }
  daemonPorts = { at: Date.now(), ports };
  return ports;
}

/** OS availability probe. Bounded by a timeout so a wedged bind/close can't
 *  leave the caller (and a server create) hanging forever. */
async function probe(port, host = '0.0.0.0', timeoutMs = 2000) {
  // A remote daemon publishes ports on ITS host; binding one here would prove
  // nothing and would happily hand out a port that is already taken there.
  if (config.dockerRemote) return !(await daemonPortsInUse()).has(port);
  return new Promise((resolve) => {
    const srv = net.createServer();
    srv.unref();
    let settled = false;
    const finish = (ok) => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      try {
        srv.close();
      } catch {
        /* not listening */
      }
      resolve(ok);
    };
    const timer = setTimeout(() => finish(false), timeoutMs);
    timer.unref();
    srv.once('error', () => finish(false)); // EADDRINUSE or any bind failure = not free
    srv.listen({ port, host, exclusive: true }, () => {
      // If the timeout already answered "not free", finish() is a no-op and
      // would leave this late-but-successful listener holding the port.
      if (settled) {
        try {
          srv.close();
        } catch {
          /* ignore */
        }
        return;
      }
      finish(true);
    });
  });
}

function dbPortsInUse() {
  const rows = db.all(
    'SELECT port_game, port_rcon, port_bedrock, extra_ports_json FROM servers WHERE deleted_at IS NULL'
  );
  const used = new Set();
  for (const r of rows) {
    used.add(r.port_game);
    used.add(r.port_rcon);
    if (r.port_bedrock) used.add(r.port_bedrock);
    for (const p of JSON.parse(r.extra_ports_json || '[]')) {
      if (p && p.hostPort) used.add(p.hostPort);
    }
  }
  // BlueMap's web-server port lives in `integrations`, not on the server row -
  // it must be unioned in too, or a fresh port allocation could collide with it.
  for (const row of db.all("SELECT config_json FROM integrations WHERE kind = 'bluemap' AND enabled = 1")) {
    const hostPort = JSON.parse(row.config_json || '{}').hostPort;
    if (hostPort) used.add(hostPort);
  }
  used.add(config.port); // never hand out the panel's own port
  return used;
}

async function isPortFree(port) {
  // undefined/null/NaN/'25565xyz' must NOT pass as free - that silently
  // skipped RCON collision validation for explicit game ports.
  if (!Number.isInteger(port)) return false;
  if (port < 1024 || port > 65535) return false;
  if (dbPortsInUse().has(port)) return false;
  return probe(port);
}

/** Suggest a { game, rcon } pair (and bedrock when requested). */
async function suggestPorts({ withBedrock = false } = {}) {
  const used = dbPortsInUse();
  // RCON = game + offset, so the largest LEGAL game port is 65535 - offset.
  // Probing an rcon > 65535 would fail every candidate and misreport "no free
  // game ports" for a perfectly good range.
  const maxGame = 65535 - config.ports.rconOffset;
  let game = config.ports.gameStart;
  for (;;) {
    const rcon = game + config.ports.rconOffset;
    if (!used.has(game) && !used.has(rcon) && (await probe(game)) && (await probe(rcon))) break;
    game += 1;
    if (game > maxGame)
      throw httpError(409, 'No free game ports are available. Delete a server or widen the port range in your .env.');
  }
  const result = { game, rcon: game + config.ports.rconOffset, bedrock: null };
  if (withBedrock) {
    let b = config.ports.bedrockStart;
    while (used.has(b) || !(await probe(b))) {
      b += 1;
      if (b > 65000)
        throw httpError(
          409,
          'No free Bedrock ports are available. Delete a server or widen the port range in your .env.'
        );
    }
    result.bedrock = b;
  }
  return result;
}

module.exports = { isPortFree, suggestPorts, probe, dbPortsInUse, daemonPortsInUse };
