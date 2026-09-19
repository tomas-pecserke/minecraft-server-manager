// @ts-nocheck - dynamic Docker/NBT/HTTP-JSON interop; not yet under checkJs (incremental typing).
'use strict';

// Named volumes that hold a server's /data when SERVER_STORAGE=volume. The
// daemon owns the storage; the panel never sees these paths on its own disk.

const { getDocker } = require('./connect');

const LABEL = 'msm.id';

/** Volume backing a server's /data. One per server, named after it. */
function volumeName(serverId) {
  return `msm-${serverId}`;
}

/** Create the server's volume if it isn't there yet. Idempotent. */
async function ensureVolume(serverId) {
  const docker = getDocker();
  const name = volumeName(serverId);
  try {
    await docker.getVolume(name).inspect();
    return name;
  } catch (err) {
    if (err.statusCode !== 404) throw err;
  }
  await docker.createVolume({ Name: name, Labels: { [LABEL]: serverId, 'msm.managed': 'true' } });
  return name;
}

/** Remove the server's volume. Missing is success - the goal state is "gone". */
async function removeVolume(serverId) {
  try {
    await getDocker().getVolume(volumeName(serverId)).remove({ force: true });
  } catch (err) {
    if (err.statusCode !== 404) throw err;
  }
}

/** Bytes the volume holds, or null when the daemon can't say (older API, driver without usage data). */
async function volumeSize(serverId) {
  const name = volumeName(serverId);
  const { Volumes } = await getDocker().listVolumes({ filters: { name: [name] } });
  const found = (Volumes || []).find((v) => v.Name === name);
  const size = found && found.UsageData ? found.UsageData.Size : -1;
  return typeof size === 'number' && size >= 0 ? size : null;
}

module.exports = { volumeName, ensureVolume, removeVolume, volumeSize };
