// CC 本地备份：把云端的数据库和图片全部拉到本地一份。
//
// 跑一次会在 backup/ 下面建一个按日期命名的文件夹，里面有：
//   data/xxx.json   —— 每张表一个文件（聊天记录、问题件、案例、账号、班表……）
//   data/all.json   —— 上面那些合在一起的一个大文件，恢复时用这个
//   报告.txt        —— 这次备份了多少条、多少个文件、有没有失败
// 图片和文件统一放在 backup/files/ 里（所有次共用一份），每次只下新增的。
//
// 用法：双击 backup/备份.bat
// 配置写在 backup/config.txt 里（第一次跑会自动生成模板）。
// 不依赖任何第三方模块——数据库走 Supabase 自带的接口读，Node 自带的 fetch 就够了。
'use strict';

const fs = require('fs');
const path = require('path');

const ROOT = __dirname;
const CONFIG_PATH = path.join(ROOT, 'config.txt');

// ---------- 读配置 ----------
function readConfig() {
  if (!fs.existsSync(CONFIG_PATH)) {
    fs.writeFileSync(CONFIG_PATH, [
      '# CC 备份配置。等号后面填上对应的值，保存后再跑一次。',
      '# 两个值都在 Render 后台 → 你的服务 → Environment 里，点那一行的复制按钮就能拿到。',
      '',
      'SUPABASE_SERVICE_KEY=',
      '# 下面这个用来定位是哪个 Supabase 项目，直接把 Render 里的 DATABASE_URL 整条贴过来就行',
      'DATABASE_URL=',
      '',
      '# 下面这些一般不用改',
      'SUPABASE_BUCKET=cc-files',
      '# 只备份数据库、不下载图片的话，把下面改成 no',
      'DOWNLOAD_FILES=yes',
      '',
    ].join('\r\n'), 'utf8');
    console.log('已经生成配置文件：' + CONFIG_PATH);
    console.log('请把 SUPABASE_SERVICE_KEY 和 DATABASE_URL 填进去，保存后再跑一次。');
    process.exit(0);
  }
  const cfg = {};
  fs.readFileSync(CONFIG_PATH, 'utf8').split(/\r?\n/).forEach((line) => {
    const t = line.trim();
    if (!t || t.startsWith('#')) return;
    const i = t.indexOf('=');
    if (i === -1) return;
    // 复制粘贴经常带上引号、尖括号、空格这些杂质，这里统一去掉
    const value = t.slice(i + 1).trim().replace(/^["'<>`\s]+/, '').replace(/["'`\s]+$/, '');
    cfg[t.slice(0, i).trim()] = value;
  });
  // key 本身是 JWT（eyJ 开头、两个点分三段），前面粘到别的字符就从 eyJ 开始截
  if (cfg.SUPABASE_SERVICE_KEY && !cfg.SUPABASE_SERVICE_KEY.startsWith('eyJ')) {
    const at = cfg.SUPABASE_SERVICE_KEY.indexOf('eyJ');
    if (at > 0) cfg.SUPABASE_SERVICE_KEY = cfg.SUPABASE_SERVICE_KEY.slice(at);
  }
  return cfg;
}

const cfg = readConfig();
const KEY = cfg.SUPABASE_SERVICE_KEY || '';
if (!KEY) {
  console.error('config.txt 里的 SUPABASE_SERVICE_KEY 还是空的，填好再跑。');
  console.error('（Render 后台 → 你的服务 → Environment，点那一行右边的复制按钮）');
  process.exit(1);
}
if (KEY.split('.').length !== 3) {
  console.error('SUPABASE_SERVICE_KEY 看着不像一把完整的 key（应该是 eyJ 开头、中间两个点分成三段）。');
  console.error('去 Render → Environment 用复制按钮重新复制一遍，整条贴进 config.txt。');
  process.exit(1);
}

// Supabase 项目地址：优先配置里写死的，没有就从 DATABASE_URL 里的项目编号推出来
function supabaseUrl() {
  if (cfg.SUPABASE_URL) return cfg.SUPABASE_URL.replace(/\/+$/, '');
  const m = String(cfg.DATABASE_URL || '').match(/\/\/postgres\.([a-z0-9]+):/i);
  return m ? `https://${m[1]}.supabase.co` : '';
}
const SUPABASE_URL = supabaseUrl();
if (!SUPABASE_URL) {
  console.error('认不出是哪个 Supabase 项目。');
  console.error('把 Render 里的 DATABASE_URL 整条贴进 config.txt，或者直接加一行：');
  console.error('SUPABASE_URL=https://你的项目编号.supabase.co');
  process.exit(1);
}
const BUCKET = cfg.SUPABASE_BUCKET || 'cc-files';
const WANT_FILES = String(cfg.DOWNLOAD_FILES || 'yes').toLowerCase() !== 'no';
const AUTH = { apikey: KEY, Authorization: 'Bearer ' + KEY };

// ---------- 要备份的表 ----------
// 有 id 列的按 id 排序取，保证翻页不重不漏；没有 id 的（纯关联表）直接取。
const TABLES = [
  { name: 'users', id: true },
  { name: 'groups', id: true },
  { name: 'group_members', id: false },
  { name: 'group_pins', id: false },
  { name: 'group_reads', id: false },
  { name: 'group_hidden', id: false },
  { name: 'chat_messages', id: true },
  { name: 'problem_item_reports', id: true },
  { name: 'problem_item_options', id: true },
  { name: 'case_library', id: true },
  { name: 'special_requirements', id: true },
  { name: 'inspection_rules_history', id: true },
  { name: 'mall_items', id: true },
  { name: 'mall_arrivals', id: true },
  { name: 'paypay_records', id: true },
  { name: 'staff_shifts', id: true },
  { name: 'staff_members', id: true },
  { name: 'time_records', id: true },
  { name: 'timeclock_names', id: true },
  { name: 'work_items', id: true },
  { name: 'reminders', id: true },
  { name: 'drive_files', id: false },
];

const PAGE = 1000;

async function fetchTable(t) {
  const rows = [];
  for (let offset = 0; ; offset += PAGE) {
    const order = t.id ? '&order=id.asc' : '';
    const url = `${SUPABASE_URL}/rest/v1/${t.name}?select=*&limit=${PAGE}&offset=${offset}${order}`;
    const res = await fetch(url, { headers: AUTH });
    if (!res.ok) {
      const body = await res.text();
      throw new Error(`HTTP ${res.status} ${body.slice(0, 120)}`);
    }
    const batch = await res.json();
    if (!Array.isArray(batch)) throw new Error('返回的不是数组');
    rows.push(...batch);
    if (batch.length < PAGE) break;
  }
  return rows;
}

function stamp() {
  const d = new Date();
  const p = (n) => String(n).padStart(2, '0');
  return `${d.getFullYear()}-${p(d.getMonth() + 1)}-${p(d.getDate())}_${p(d.getHours())}${p(d.getMinutes())}`;
}
function human(bytes) {
  if (bytes < 1024) return bytes + ' B';
  if (bytes < 1024 * 1024) return (bytes / 1024).toFixed(1) + ' KB';
  if (bytes < 1024 * 1024 * 1024) return (bytes / 1024 / 1024).toFixed(1) + ' MB';
  return (bytes / 1024 / 1024 / 1024).toFixed(2) + ' GB';
}

(async () => {
  const outDir = path.join(ROOT, stamp());
  const dataDir = path.join(outDir, 'data');
  // 图片放在共用的 backup/files 里（不按日期分），这样每次只下新增的，不会重复占地方
  const filesDir = path.join(ROOT, 'files');
  fs.mkdirSync(dataDir, { recursive: true });

  const report = [];
  const log = (line) => { console.log(line); report.push(line); };
  log('CC 备份 ' + new Date().toLocaleString('zh-CN'));
  log('项目：' + SUPABASE_URL);
  log('数据保存到：' + outDir);
  log('图片保存到：' + path.join(ROOT, 'files') + '（共用，增量下载）');
  log('');

  // ---------- 1) 数据库 ----------
  const all = {};
  let totalRows = 0;
  let failedTables = 0;
  log('【数据库】');
  for (const t of TABLES) {
    try {
      const rows = await fetchTable(t);
      all[t.name] = rows;
      totalRows += rows.length;
      fs.writeFileSync(path.join(dataDir, t.name + '.json'), JSON.stringify(rows, null, 2), 'utf8');
      log(`  ${t.name} … ${rows.length} 条`);
    } catch (err) {
      all[t.name] = [];
      failedTables++;
      log(`  ${t.name} … 失败（${err.message.split('\n')[0]}）`);
    }
  }
  fs.writeFileSync(path.join(dataDir, 'all.json'), JSON.stringify({ backupAt: Date.now(), tables: all }, null, 2), 'utf8');
  log(`  合计 ${totalRows} 条记录` + (failedTables ? `，${failedTables} 张表失败` : ''));
  log('');

  // ---------- 2) 图片和文件 ----------
  if (!WANT_FILES) {
    log('【文件】按配置跳过（DOWNLOAD_FILES=no）');
  } else {
    fs.mkdirSync(filesDir, { recursive: true });
    // 列出一个文件夹下的所有对象（一次最多 1000 个，翻页拿完）
    async function listFolder(prefix) {
      const out = [];
      for (let offset = 0; ; offset += 1000) {
        const res = await fetch(`${SUPABASE_URL}/storage/v1/object/list/${BUCKET}`, {
          method: 'POST',
          headers: { ...AUTH, 'Content-Type': 'application/json' },
          body: JSON.stringify({ prefix, limit: 1000, offset, sortBy: { column: 'name', order: 'asc' } }),
        });
        if (!res.ok) throw new Error(`列目录 ${prefix} 失败：${res.status}`);
        const batch = await res.json();
        if (!Array.isArray(batch) || batch.length === 0) break;
        batch.forEach((x) => { if (x && x.name && x.id) out.push(prefix + x.name); });
        if (batch.length < 1000) break;
      }
      return out;
    }

    log('【文件】');
    let okCount = 0, skipCount = 0, failCount = 0, bytes = 0;
    for (const folder of ['images/', 'files/', 'drive/']) {
      let names = [];
      try {
        names = await listFolder(folder);
      } catch (err) {
        log(`  ${folder} … 列目录失败：${err.message}`);
        continue;
      }
      fs.mkdirSync(path.join(filesDir, folder), { recursive: true });
      log(`  ${folder} 共 ${names.length} 个`);
      for (const name of names) {
        const dest = path.join(filesDir, name);
        // 已经下过的跳过——这样每次备份只拉新增的，很快
        if (fs.existsSync(dest) && fs.statSync(dest).size > 0) { skipCount++; continue; }
        try {
          const res = await fetch(`${SUPABASE_URL}/storage/v1/object/${BUCKET}/${encodeURI(name)}`, { headers: AUTH });
          if (!res.ok) throw new Error('HTTP ' + res.status);
          const buf = Buffer.from(await res.arrayBuffer());
          fs.writeFileSync(dest, buf);
          bytes += buf.length;
          okCount++;
          if (okCount % 50 === 0) process.stdout.write(`    已下载 ${okCount} 个…\r`);
        } catch (err) {
          failCount++;
          log(`    下载失败 ${name}：${err.message}`);
        }
      }
    }
    log(`  新下载 ${okCount} 个（${human(bytes)}），已有跳过 ${skipCount} 个，失败 ${failCount} 个`);
  }

  // ---------- 3) 旧快照清理：数据库快照只留最近 30 份，图片不动 ----------
  try {
    const KEEP = Number(cfg.KEEP_SNAPSHOTS || 30);
    const snaps = fs.readdirSync(ROOT)
      .filter((n) => /^\d{4}-\d{2}-\d{2}_\d{4}$/.test(n) && fs.statSync(path.join(ROOT, n)).isDirectory())
      .sort();
    if (KEEP > 0 && snaps.length > KEEP) {
      const old = snaps.slice(0, snaps.length - KEEP);
      old.forEach((n) => fs.rmSync(path.join(ROOT, n), { recursive: true, force: true }));
      log('');
      log(`【清理】删掉 ${old.length} 份最旧的数据库快照，保留最近 ${KEEP} 份（图片不受影响）`);
    }
  } catch (err) {
    log('【清理】跳过：' + err.message);
  }

  log('');
  log('完成。');
  fs.writeFileSync(path.join(outDir, '报告.txt'), report.join('\r\n'), 'utf8');
  console.log('\n报告：' + path.join(outDir, '报告.txt'));
})().catch((err) => {
  console.error('\n备份失败：' + err.message);
  process.exit(1);
});
