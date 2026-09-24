import protobuf from 'protobufjs';
import { connectToEngine } from './lib-cockatiel.mjs';

const PROTO = new URL('./cockatiel_protobuf.proto', import.meta.url).pathname;

async function main() {
  const root = await protobuf.load(PROTO);
  const pb = { Container: root.lookupType('cockatiel_protobuf.v1.Container') };
  const opts = { url: 'ws://127.0.0.1:9734', pin: 943072, moduleName: 'cockatiel-audit-viewer', processPosition: 'preprocess' };

  const withTls = process.env.COCKATIEL_TLS_CERT !== undefined;

  try {
    const conn = await connectToEngine(opts, pb);
    const token = conn.authToken;
    if (withTls) {
      console.log(`[WSS ON] CONNECTED; auth token: ${token ? token.slice(0, 16) + '…' : '(empty)'} len=${token.length}`);
      await conn.reconnect();
      console.log(`[WSS ON] RECONNECTED over wss; token: ${conn.authToken.slice(0, 16)}…`);
      await conn.disconnect('verify done');
    } else {
      console.log(`[WSS OFF] UNEXPECTED SUCCESS (plain ws accepted)`);
      await conn.disconnect('verify done');
      process.exit(1);
    }
  } catch (e) {
    if (withTls) {
      console.log(`[WSS ON] FAILED over wss: ${e.message}`);
      process.exit(1);
    } else {
      console.log(`[WSS OFF] Failed as expected (plain ws rejected): ${e.message}`);
    }
  }
  process.exit(0);
}

main();