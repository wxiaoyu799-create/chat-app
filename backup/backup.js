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

// 表的中文名，给 CSV 文件名和查看器用
const TABLE_LABELS = {
  users: '账号', groups: '群组', group_members: '群成员', group_pins: '群置顶',
  group_reads: '群已读位置', group_hidden: '关掉的私聊',
  chat_messages: '聊天记录',
  problem_item_reports: '问题件', problem_item_options: '问题件选项',
  case_library: '案例库', special_requirements: '特殊要求', inspection_rules_history: '检品规则历史',
  mall_items: '商城订货', mall_arrivals: '商城到货', paypay_records: 'PayPay充值',
  staff_shifts: '班表', staff_members: '现场管理名单', time_records: '打卡记录',
  timeclock_names: '打卡人名单', work_items: '工作内容', reminders: '定时提醒',
  drive_files: '云盘文件',
};

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

// 生成一个离线网页：左边选表，右边看内容，能搜。数据以 JSON 直接内嵌。
function buildViewer(all, labels) {
  const payload = JSON.stringify({ tables: all, labels, at: new Date().toLocaleString('zh-CN') })
    .replace(/</g, '\\u003c'); // 防止数据里出现 </script> 把页面截断
  return `<!doctype html>
<html lang="zh"><head><meta charset="utf-8"><title>CC 备份查看</title>
<style>
  * { box-sizing: border-box; }
  body { margin:0; font-family: "Microsoft YaHei", system-ui, sans-serif; background:#f6f7fb; color:#22252b; height:100vh; display:flex; flex-direction:column; }
  header { padding:12px 18px; background:#fff; border-bottom:1px solid #e3e6ee; display:flex; align-items:center; gap:14px; flex-wrap:wrap; }
  header h1 { font-size:16px; margin:0; }
  header .at { font-size:12px; color:#7a808c; }
  header input { flex:1; min-width:200px; max-width:420px; padding:7px 10px; border:1px solid #d7dbe5; border-radius:8px; font-size:13px; outline:none; }
  header input:focus { border-color:#3f6fd6; }
  main { flex:1; display:flex; min-height:0; }
  nav { width:190px; background:#fff; border-right:1px solid #e3e6ee; overflow:auto; padding:8px; }
  nav button { display:flex; justify-content:space-between; gap:8px; width:100%; text-align:left; background:none; border:none; border-radius:7px; padding:7px 10px; font-size:13px; cursor:pointer; font-family:inherit; color:#22252b; }
  nav button:hover { background:#eef1f8; }
  nav button.on { background:#3f6fd6; color:#fff; }
  nav button span { font-size:11px; opacity:.7; }
  section { flex:1; overflow:auto; padding:14px 18px 40px; }
  table { border-collapse:collapse; font-size:12.5px; background:#fff; white-space:nowrap; }
  th, td { border:1px solid #e3e6ee; padding:5px 9px; text-align:left; max-width:420px; overflow:hidden; text-overflow:ellipsis; vertical-align:top; }
  th { background:#f0f2f8; position:sticky; top:0; font-weight:600; }
  tr:nth-child(even) td { background:#fbfcfe; }
  .count { font-size:12px; color:#7a808c; margin-bottom:8px; }
  mark { background:#ffe9a8; }
</style></head><body>
<header>
  <h1>CC 备份查看</h1>
  <span class="at">备份时间：__AT__</span>
  <input id="q" type="search" placeholder="在当前这张表里搜（人名、单号、任意文字）">
</header>
<main><nav id="nav"></nav><section><div class="count" id="count"></div><div id="box"></div></section></main>
<script>
const DATA = __DATA__;
document.querySelector('.at').textContent = '备份时间：' + DATA.at;
const names = Object.keys(DATA.tables).filter(n => DATA.tables[n].length);
let cur = names[0] || '';
const nav = document.getElementById('nav'), box = document.getElementById('box'), q = document.getElementById('q'), countEl = document.getElementById('count');
function label(n){ return (DATA.labels[n] || n); }
function cell(v){
  if (v === null || v === undefined) return '';
  if (typeof v === 'object') return JSON.stringify(v);
  // 13 位数字大概率是时间戳，显示成日期好认
  if (typeof v === 'number' && v > 1500000000000 && v < 4000000000000) return new Date(v).toLocaleString('zh-CN');
  const s = String(v);
  if (/^\\d{4}-\\d{2}-\\d{2}T/.test(s)) { const d = new Date(s); if (!isNaN(d)) return d.toLocaleString('zh-CN'); }
  return s;
}
function esc(s){ return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;'); }
function hi(s, k){ if (!k) return esc(s); return esc(s).replace(new RegExp(k.replace(/[.*+?^\\\${}()|[\\]\\\\]/g,'\\\\$&'),'gi'), m => '<mark>'+m+'</mark>'); }
function renderNav(){
  nav.innerHTML = '';
  names.forEach(n => {
    const b = document.createElement('button');
    b.className = n === cur ? 'on' : '';
    b.innerHTML = label(n) + '<span>' + DATA.tables[n].length + '</span>';
    b.onclick = () => { cur = n; q.value=''; renderNav(); render(); };
    nav.appendChild(b);
  });
}
function render(){
  const rows = DATA.tables[cur] || [];
  const k = q.value.trim().toLowerCase();
  const hit = k ? rows.filter(r => Object.values(r).some(v => String(typeof v === 'object' ? JSON.stringify(v) : v).toLowerCase().includes(k))) : rows;
  countEl.textContent = label(cur) + '：' + (k ? ('搜到 ' + hit.length + ' 条 / 共 ' + rows.length + ' 条') : ('共 ' + rows.length + ' 条')) + (hit.length > 800 ? '（只显示前 800 条，想看全部请用 Excel表格 文件夹里的 csv）' : '');
  if (!hit.length) { box.innerHTML = '<p style="color:#7a808c">没有内容</p>'; return; }
  const cols = Object.keys(rows[0]);
  const show = hit.slice(0, 800);
  box.innerHTML = '<table><thead><tr>' + cols.map(c => '<th>'+esc(c)+'</th>').join('') +
    '</tr></thead><tbody>' + show.map(r => '<tr>' + cols.map(c => '<td title="'+esc(cell(r[c]))+'">' + hi(cell(r[c]), k) + '</td>').join('') + '</tr>').join('') + '</tbody></table>';
}
q.addEventListener('input', render);
renderNav(); render();
</script></body></html>`.replace('__DATA__', payload).replace('__AT__', new Date().toLocaleString('zh-CN'));
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

  // 同一份数据再导一份 Excel 能直接打开的 CSV
  const csvDir = path.join(outDir, 'Excel表格');
  fs.mkdirSync(csvDir, { recursive: true });
  const csvCell = (v) => {
    if (v === null || v === undefined) return '""';
    let t;
    if (typeof v === 'object') t = JSON.stringify(v);
    else t = String(v);
    return '"' + t.replace(/"/g, '""') + '"';
  };
  for (const name of Object.keys(all)) {
    const rows = all[name];
    if (!rows.length) continue;
    const cols = Object.keys(rows[0]);
    const lines = [cols.map(csvCell).join(',')];
    rows.forEach((r) => lines.push(cols.map((c) => csvCell(r[c])).join(',')));
    const label = TABLE_LABELS[name] || name;
    fs.writeFileSync(path.join(csvDir, `${label}(${name}).csv`), '\uFEFF' + lines.join('\r\n'), 'utf8');
  }

  // 再生成一个双击就能看的网页（数据直接嵌在里面，不联网也能开）
  fs.writeFileSync(path.join(outDir, '查看备份.html'), buildViewer(all, TABLE_LABELS), 'utf8');
  log(`  合计 ${totalRows} 条记录` + (failedTables ? `，${failedTables} 张表失败` : ''));
  log('  想直接看：双击这次文件夹里的「查看备份.html」；想用 Excel 打开：进「Excel表格」文件夹');
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
