'use strict';

// The addresses a player types into Minecraft to reach a server.
//
// A server listens on the machine its container runs on, which is not always
// this one: with a remote DOCKER_HOST the ports are published over there, so
// this machine's network interfaces (and certainly `localhost`) name the wrong
// computer. Everything that shows a connect address goes through here.

const os = require('node:os');
const config = require('../config');

/** True for an RFC1918 address - the panel and the daemon share a network. */
function isLan(host) {
  return /^(192\.168\.|10\.|172\.(1[6-9]|2\d|3[01])\.)/.test(host);
}

/** Where the daemon runs, when that isn't this machine. Null for a local daemon. */
function daemonHost() {
  return config.dockerHostname;
}

/** This machine's non-internal IPv4 addresses, LAN-looking ones first. */
function localIPv4s() {
  const ips = [];
  for (const ifaces of Object.values(os.networkInterfaces())) {
    for (const iface of ifaces || []) {
      if (iface.family === 'IPv4' && !iface.internal) ips.push(iface.address);
    }
  }
  return ips.sort((a, b) => Number(isLan(b)) - Number(isLan(a)));
}

/**
 * Connect addresses for a server's game port, best first: the configured public
 * host, then whichever machine actually publishes the port. `loopback` adds
 * localhost, which is worth offering on the server page (the operator may play
 * on this machine) and useless in an invite someone else reads.
 */
function connectCandidates(portGame, { loopback = true } = {}) {
  const out = [];
  const configured = require('./settings').publicAddress(portGame);
  if (configured) out.push(configured);

  const daemon = daemonHost();
  if (daemon) {
    // The containers live over there; this machine's addresses reach nothing.
    out.push(`${daemon}:${portGame}`);
  } else {
    for (const ip of localIPv4s()) out.push(`${ip}:${portGame}`);
    if (loopback) out.push(`localhost:${portGame}`);
  }
  return [...new Set(out)];
}

/**
 * Whether a WAN IP detected from THIS machine's internet connection also
 * describes the server. It does when the container runs here, or on a host on
 * the same private network (same router, so the same public address). It does
 * not when the daemon is somewhere else entirely - that host has its own.
 */
function wanIpDescribesServer() {
  const daemon = daemonHost();
  return !daemon || isLan(daemon);
}

module.exports = { connectCandidates, daemonHost, localIPv4s, isLan, wanIpDescribesServer };
