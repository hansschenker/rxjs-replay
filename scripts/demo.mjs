import { build } from 'esbuild';
import { createServer } from 'node:http';
import { copyFile, mkdir, readFile } from 'node:fs/promises';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const project = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const output = resolve(project, '.demo');
await mkdir(output, { recursive: true });
await build({
  entryPoints: [resolve(project, 'demo/main.ts')],
  outfile: resolve(output, 'app.js'),
  bundle: true,
  format: 'esm',
  platform: 'browser',
  target: 'es2022',
  sourcemap: true,
  logLevel: 'warning',
});
await copyFile(resolve(project, 'demo/index.html'), resolve(output, 'index.html'));
if (process.argv.includes('--build')) {
  console.log('Built demo in .demo/');
} else {
  const port = Number(process.env.PORT ?? 4173);
  if (!Number.isInteger(port) || port < 1 || port > 65535) throw new Error('PORT must be an integer from 1 to 65535.');
  const routes = new Map([
    ['/', ['index.html', 'text/html; charset=utf-8']],
    ['/index.html', ['index.html', 'text/html; charset=utf-8']],
    ['/app.js', ['app.js', 'text/javascript; charset=utf-8']],
    ['/app.js.map', ['app.js.map', 'application/json; charset=utf-8']],
  ]);
  const server = createServer(async (request, response) => {
    try {
      const path = new URL(request.url ?? '/', 'http://127.0.0.1').pathname;
      const route = routes.get(path);
      if (!route) { response.writeHead(404); response.end('Not found'); return; }
      response.setHeader('Content-Type', route[1]);
      response.setHeader('Cache-Control', 'no-store');
      response.end(await readFile(resolve(output, route[0])));
    } catch {
      response.writeHead(500);
      response.end('Could not read demo asset');
    }
  });
  server.listen(port, '127.0.0.1', () => console.log(`rxjs-replay demo: http://127.0.0.1:${port}`));
}
