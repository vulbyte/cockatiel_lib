import { test } from 'node:test';
import assert from 'node:assert/strict';
import protobuf from 'protobufjs';
import { normalizeProcessPosition, resolvePin } from '../lib-cockatiel.mjs';

const PROTO = new URL('../cockatiel_protobuf.proto', import.meta.url).pathname;

test('normalizeProcessPosition maps documented strings case-insensitively', () => {
  assert.equal(normalizeProcessPosition('preprocess'), 1);
  assert.equal(normalizeProcessPosition('InProcess'), 2);
  assert.equal(normalizeProcessPosition('POSTPROCESS'), 3);
  assert.equal(normalizeProcessPosition('connection'), 4);
  assert.equal(normalizeProcessPosition('PROCESS_POSITION_PREPROCESS'), 1);
  assert.equal(normalizeProcessPosition('process_position_postprocess'), 3);
  assert.equal(normalizeProcessPosition(3), 3);
  assert.equal(normalizeProcessPosition(0), 0);
  assert.equal(normalizeProcessPosition(null), 0);
  assert.equal(normalizeProcessPosition(undefined), 0);
  assert.equal(normalizeProcessPosition(''), 0);
  assert.throws(() => normalizeProcessPosition('bogus'), /Unknown process position/);
});

test('processPosition string is encoded as its enum value on the wire', async () => {
  const root = await protobuf.load(PROTO);
  const C = root.lookupType('cockatiel_protobuf.ContainerForEngine');
  for (const [name, expected] of [
    ['preprocess', 1],
    ['inprocess', 2],
    ['postprocess', 3],
    ['connection', 4],
  ]) {
    const bytes = C.encode({
      version: 2,
      moduleName: 'x',
      connectionRequest: { pin: 1, processPosition: normalizeProcessPosition(name) },
    }).finish();
    const decoded = C.decode(bytes);
    assert.equal(decoded.connectionRequest.processPosition, expected, name);
  }
});

test('COCKATIEL_PIN env wins over opts.pin', () => {
  assert.equal(resolvePin({ pin: 111 }, { COCKATIEL_PIN: '222' }), 222);
  assert.equal(resolvePin({ pin: '111' }, {}), '111');
  assert.equal(resolvePin({}, {}), 0);
  assert.equal(resolvePin({ pin: 111 }, { COCKATIEL_PIN: '' }), 111);
});
