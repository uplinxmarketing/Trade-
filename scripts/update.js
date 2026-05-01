#!/usr/bin/env node
'use strict';
// TradeBot AI — self-updater (no git required)
// Downloads the latest code as a ZIP from GitHub and applies it.
// Called by start.bat on every launch and by the in-app Update button.

const https   = require('https');
const http    = require('http');
const fs      = require('fs');
const path    = require('path');
const os      = require('os');
const { execSync } = require('child_process');

// start.bat does `cd /d "%~dp0"` before running this script, so cwd = app root.
// __dirname is the %TEMP% folder when run as a downloaded script — never use it.
const APP_DIR            = process.cwd();
const REMOTE_VERSION_URL = 'https://raw.githubusercontent.com/uplinxmarketing/Trade-/main/public/version.json';
const ZIP_URL            = 'https://github.com/uplinxmarketing/Trade-/archive/refs/heads/main.zip';

// Never overwrite these — they contain user data or are too large to re-copy
const SKIP = new Set(['node_modules', 'logs', '.env', 'dist', '.git', 'scripts']);

function log(msg) {
  process.stdout.write('[update] ' + msg + '\n');
}

// Follow HTTP redirects and return response body as a string
function fetchText(url) {
  return new Promise((resolve, reject) => {
    function get(u) {
      const mod = u.startsWith('https') ? https : http;
      mod.get(u, { headers: { 'User-Agent': 'TradeBot-Updater/1.0' } }, (res) => {
        if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
          return get(res.headers.location);
        }
        if (res.statusCode !== 200) {
          res.resume();
          return reject(new Error('HTTP ' + res.statusCode + ' fetching ' + u));
        }
        let data = '';
        res.on('data', d => { data += d; });
        res.on('end', () => resolve(data));
        res.on('error', reject);
      }).on('error', reject);
    }
    get(url);
  });
}

// Download a URL to a local file path, following redirects
function download(url, destPath) {
  return new Promise((resolve, reject) => {
    function get(u) {
      const mod = u.startsWith('https') ? https : http;
      mod.get(u, { headers: { 'User-Agent': 'TradeBot-Updater/1.0' } }, (res) => {
        if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
          return get(res.headers.location);
        }
        if (res.statusCode !== 200) {
          res.resume();
          return reject(new Error('HTTP ' + res.statusCode + ' downloading ' + u));
        }
        const out = fs.createWriteStream(destPath);
        res.pipe(out);
        out.on('finish', () => out.close(resolve));
        out.on('error', reject);
        res.on('error', reject);
      }).on('error', reject);
    }
    get(url);
  });
}

function ps(cmd) {
  // Run a PowerShell command, swallowing output unless it errors
  execSync('powershell -NoProfile -ExecutionPolicy Bypass -Command "' + cmd + '"',
    { timeout: 120_000, stdio: 'inherit' });
}

function copyEntry(src, destDir) {
  const dst = path.join(destDir, path.basename(src));
  if (process.platform === 'win32') {
    // Copy-Item handles both files and directories with -Recurse
    ps("Copy-Item -Path '" + src + "' -Destination '" + dst + "' -Recurse -Force");
  } else {
    const stat = fs.statSync(src);
    if (stat.isDirectory()) {
      execSync('cp -r "' + src + '" "' + destDir + '"', { timeout: 30_000 });
    } else {
      execSync('cp "' + src + '" "' + dst + '"', { timeout: 30_000 });
    }
  }
}

function cleanupDir(dirPath) {
  try {
    if (!fs.existsSync(dirPath)) return;
    if (process.platform === 'win32') {
      execSync('powershell -NoProfile -ExecutionPolicy Bypass -Command "Remove-Item -Path \'' + dirPath + '\' -Recurse -Force -ErrorAction SilentlyContinue"',
        { timeout: 30_000, stdio: 'ignore' });
    } else {
      execSync('rm -rf "' + dirPath + '"', { timeout: 30_000, stdio: 'ignore' });
    }
  } catch { /* best-effort */ }
}

async function main() {
  log('Checking for updates…');

  // 1. Fetch remote version.json
  let remoteVersion;
  try {
    remoteVersion = JSON.parse(await fetchText(REMOTE_VERSION_URL));
  } catch (e) {
    log('Could not reach GitHub (' + e.message + ') — skipping update.');
    return; // offline or GitHub down — not a fatal error
  }

  // 2. Read local version.json
  const localVersionPath = path.join(APP_DIR, 'public', 'version.json');
  let localVersion = { commit: '', version: '0.0.0' };
  try { localVersion = JSON.parse(fs.readFileSync(localVersionPath, 'utf8')); } catch { /* first run */ }

  if (remoteVersion.commit === localVersion.commit) {
    log('Already up to date (v' + localVersion.version + ').');
    return;
  }

  log('Update available: v' + localVersion.version + ' → v' + remoteVersion.version);
  log('Downloading ZIP from GitHub…');

  const uid       = Date.now();
  const zipPath   = path.join(os.tmpdir(), 'tradebot_update_' + uid + '.zip');
  const extPath   = path.join(os.tmpdir(), 'tradebot_update_ext_' + uid);

  try {
    // 3. Download the full ZIP (GitHub redirects to S3 — download() follows it)
    await download(ZIP_URL, zipPath);
    const zipMB = (fs.statSync(zipPath).size / 1_048_576).toFixed(1);
    log('Downloaded ' + zipMB + ' MB. Extracting…');

    // 4. Extract
    cleanupDir(extPath);
    if (process.platform === 'win32') {
      ps("Expand-Archive -Path '" + zipPath + "' -DestinationPath '" + extPath + "' -Force");
    } else {
      fs.mkdirSync(extPath, { recursive: true });
      execSync('unzip -o "' + zipPath + '" -d "' + extPath + '"', { timeout: 120_000, stdio: 'inherit' });
    }

    // 5. The ZIP contains a single top-level folder (e.g. Trade--main)
    const entries = fs.readdirSync(extPath);
    if (!entries.length) throw new Error('Extracted ZIP is empty — download may be corrupt.');
    const srcDir = path.join(extPath, entries[0]);

    // 6. Copy everything except protected paths
    log('Applying update to ' + APP_DIR + '…');
    for (const entry of fs.readdirSync(srcDir)) {
      if (SKIP.has(entry)) {
        log('  skip: ' + entry);
        continue;
      }
      log('  copy: ' + entry);
      copyEntry(path.join(srcDir, entry), APP_DIR);
    }

    log('Successfully updated to v' + remoteVersion.version + '!');
  } finally {
    // 7. Clean up temp files regardless of success/failure
    try { fs.unlinkSync(zipPath); } catch { /* already gone */ }
    cleanupDir(extPath);
  }
}

main().catch(e => {
  // Log the error but exit 0 so start.bat continues to launch the app
  log('Update failed: ' + e.message);
  process.exit(0);
});
