// Runs a command the way a user would in a terminal and keeps its real output with timestamps, one JSON line per
// chunk: {"t": ms since start, "d": text} and a last {"t", "exit"}. A video replays it in a terminal window with long
// waits shortened, so what a viewer sees is what the command printed. SIGTERM and SIGINT are passed on.
// Usage: node scripts/video/capture.mjs <out.ndjson> <cwd> -- <command> [args...]
import { spawn } from 'node:child_process';
import { appendFileSync, writeFileSync } from 'node:fs';

const [out, cwd, sep, ...cmd] = process.argv.slice(2);
if (!out || !cwd || sep !== '--' || !cmd.length) {
  console.error('usage: capture.mjs <out.ndjson> <cwd> -- <command> [args...]');
  process.exit(2);
}
writeFileSync(out, '');
const t0 = Date.now();
const child = spawn(cmd[0], cmd.slice(1), { cwd, env: process.env });
const keep = d => appendFileSync(out, JSON.stringify({ t: Date.now() - t0, d: d.toString() }) + '\n');
child.stdout.on('data', keep);
child.stderr.on('data', keep);
child.on('exit', code => {
  appendFileSync(out, JSON.stringify({ t: Date.now() - t0, exit: code }) + '\n');
  process.exit(code ?? 0);
});
for (const s of ['SIGTERM', 'SIGINT']) process.on(s, () => child.kill(s));
