const express = require('express');
const http = require('http');
const https = require('https');
const WebSocket = require('ws');
const path = require('path');
const fs = require('fs');
const multer = require('multer');
const crypto = require('crypto');
const { Pool } = require('pg');

const app = express();

// 支持可选的HTTPS：如果在项目根目录放了证书文件（certs/cert.pem + certs/key.pem），
// 就用HTTPS启动；没放证书文件就用普通HTTP（Render部署这种云平台不需要放证书，
// Render自己在外层已经套了HTTPS，这里继续用HTTP完全没问题，不影响现有部署）。
// 局域网自建服务器如果要用"拍照搜图"这个功能，摄像头必须要HTTPS才能调用，
// 这时候才需要生成证书放到 certs/ 目录下（用mkcert工具生成，详见部署说明）
const CERT_PATH = path.join(__dirname, 'certs', 'cert.pem');
const KEY_PATH = path.join(__dirname, 'certs', 'key.pem');
const hasLocalCerts = fs.existsSync(CERT_PATH) && fs.existsSync(KEY_PATH);

const server = hasLocalCerts
  ? https.createServer({ cert: fs.readFileSync(CERT_PATH), key: fs.readFileSync(KEY_PATH) }, app)
  : http.createServer(app);

if (hasLocalCerts) {
  console.log('检测到本地证书，以 HTTPS 方式启动（局域网内摄像头等功能可以正常使用）');
}

const wss = new WebSocket.Server({ server });

const PORT = process.env.PORT || 3000;

// ==================== 数据库（只用来持久化置顶公告+它的修改历史，留证用） ====================
// 没配置 DATABASE_URL 环境变量时，dbPool 为 null，整个应用会自动退化成纯内存模式
// （跟接数据库之前的行为完全一样），不会因为没数据库就崩掉。
const DATABASE_URL = process.env.DATABASE_URL || '';
const dbPool = DATABASE_URL
  ? new Pool({ connectionString: DATABASE_URL, ssl: { rejectUnauthorized: false } })
  : null;

if (dbPool) {
  dbPool.on('error', (err) => {
    console.error('[数据库连接池错误]', err.message);
  });
} else {
  console.log('未配置 DATABASE_URL，公告栏/提醒事项等历史记录将只保存在内存中（重启会清空）');
}

// dbPool这个对象只要DATABASE_URL字符串不是空的就会创建成功，但这不代表连接字符串本身是对的——
// pg库的连接是"真正用到的时候才去连"，不是创建Pool对象的时候就连，所以"dbPool存在"≠"真的连上了"。
// 之前踩过坑：填错了连接地址（缺了@符号、host写错），dbPool对象照样创建成功，
// 启动日志误打印"数据库已连接"，但实际上后面每个功能各自尝试查询时都报错退化成了内存模式，
// 这句话就变成了"看起来连上了、其实没连上"的误导性提示。这里改成真正跑一次查询来验证。
let dbConnectionVerified = false;
async function verifyDatabaseConnection() {
  if (!dbPool) return false;
  try {
    await dbPool.query('SELECT 1;');
    dbConnectionVerified = true;
    return true;
  } catch (err) {
    console.error('[数据库连接测试失败，请检查 DATABASE_URL 格式是否正确]', err.message);
    dbConnectionVerified = false;
    return false;
  }
}

app.use(express.static(path.join(__dirname, 'public')));

// ==================== 文件存储：Supabase Storage（配了就用）/ 本机磁盘（没配就跟以前一样） ====================
// Render 这类平台的磁盘是临时的，重新部署就清空；把图片和文件放到 Supabase Storage 里就不怕了。
// 需要三个环境变量（都在 Render 后台设置）：
//   SUPABASE_URL          项目地址，形如 https://xxxx.supabase.co（不填的话会从 DATABASE_URL 里的项目 ref 推出来）
//   SUPABASE_SERVICE_KEY  Project Settings → API 里的 service_role key（只放服务器，别发给任何人）
//   SUPABASE_BUCKET       bucket 名，默认 cc-files（要在 Supabase 后台先建好、设成 Public）
// 三个都没配 → 自动退回本机磁盘，行为跟以前完全一样。
function deriveSupabaseUrl() {
  if (process.env.SUPABASE_URL) return String(process.env.SUPABASE_URL).replace(/\/+$/, '');
  // 连接池的用户名是 postgres.<项目ref>，从这里把 ref 抠出来
  const m = String(DATABASE_URL).match(/\/\/postgres\.([a-z0-9]+):/i);
  return m ? `https://${m[1]}.supabase.co` : '';
}
const SUPABASE_URL = deriveSupabaseUrl();
const SUPABASE_SERVICE_KEY = process.env.SUPABASE_SERVICE_KEY || '';
const SUPABASE_BUCKET = process.env.SUPABASE_BUCKET || 'cc-files';
const STORAGE_ENABLED = !!(SUPABASE_URL && SUPABASE_SERVICE_KEY);
// Supabase 免费版单个文件上限 50MB（付费版可以在后台调高，调了之后把这个环境变量一起改）
const STORAGE_MAX_FILE_SIZE = Number(process.env.SUPABASE_MAX_FILE_MB || 50) * 1024 * 1024;
const STORAGE_PUBLIC_PREFIX = STORAGE_ENABLED ? `${SUPABASE_URL}/storage/v1/object/public/${SUPABASE_BUCKET}/` : '';

function storagePublicUrl(objectPath) {
  return STORAGE_PUBLIC_PREFIX + objectPath;
}
// 把本机临时文件流式传到 Storage，成功后删掉临时文件，返回公开地址
async function uploadToStorage(localPath, objectPath, contentType) {
  const { Readable } = require('stream');
  const stat = fs.statSync(localPath);
  const res = await fetch(`${SUPABASE_URL}/storage/v1/object/${SUPABASE_BUCKET}/${objectPath}`, {
    method: 'POST',
    headers: {
      Authorization: `Bearer ${SUPABASE_SERVICE_KEY}`,
      apikey: SUPABASE_SERVICE_KEY,
      'Content-Type': contentType || 'application/octet-stream',
      'Content-Length': String(stat.size),
      'x-upsert': 'false',
    },
    body: Readable.toWeb(fs.createReadStream(localPath)),
    duplex: 'half',
  });
  fs.unlink(localPath, () => {});
  if (!res.ok) {
    let msg = `Storage 返回 ${res.status}`;
    try { const j = await res.json(); if (j && (j.message || j.error)) msg = j.message || j.error; } catch (e) { /* 忽略 */ }
    throw new Error('上传到云端存储失败：' + msg);
  }
  return storagePublicUrl(objectPath);
}
async function deleteFromStorage(objectPath) {
  try {
    await fetch(`${SUPABASE_URL}/storage/v1/object/${SUPABASE_BUCKET}/${objectPath}`, {
      method: 'DELETE',
      headers: { Authorization: `Bearer ${SUPABASE_SERVICE_KEY}`, apikey: SUPABASE_SERVICE_KEY },
    });
  } catch (err) {
    console.error('[云端存储删除失败]', err.message);
  }
}
// 消息/问题件/案例里带的图片地址只认两种：本机 /uploads/xxx，或者我们自己 bucket 的公开地址
function isOwnUploadUrl(u) {
  if (typeof u !== 'string') return false;
  if (/^\/uploads\/[a-zA-Z0-9_\-.]+$/.test(u)) return true;
  if (!STORAGE_ENABLED || !u.startsWith(STORAGE_PUBLIC_PREFIX)) return false;
  // bucket 里的路径只允许 目录/文件名 这种形状，目录名不带点，堵住 ../ 之类的花样
  const rest = u.slice(STORAGE_PUBLIC_PREFIX.length);
  return /^(?:[a-zA-Z0-9_-]+\/)*[a-zA-Z0-9_-][a-zA-Z0-9_\-.]*$/.test(rest) && !rest.includes('..');
}
if (STORAGE_ENABLED) console.log(`图片/文件将存到 Supabase Storage（bucket: ${SUPABASE_BUCKET}，单文件上限 ${Math.round(STORAGE_MAX_FILE_SIZE / 1024 / 1024)}MB）`);
else console.log('未配置 SUPABASE_SERVICE_KEY，图片/文件存在本机磁盘（部署到 Render 的话重新部署会清空）');

// ==================== 图片上传 ====================
const UPLOADS_DIR = path.join(__dirname, 'uploads');
if (!fs.existsSync(UPLOADS_DIR)) fs.mkdirSync(UPLOADS_DIR, { recursive: true });
app.use('/uploads', express.static(UPLOADS_DIR));

const ALLOWED_IMAGE_MIME = ['image/jpeg', 'image/png', 'image/gif', 'image/webp'];
const upload = multer({
  storage: multer.diskStorage({
    destination: (req, file, cb) => cb(null, UPLOADS_DIR),
    filename: (req, file, cb) => {
      const ext = path.extname(file.originalname).toLowerCase() || '.jpg';
      const randomName = crypto.randomBytes(12).toString('hex');
      cb(null, `${Date.now()}-${randomName}${ext}`);
    },
  }),
  limits: { fileSize: 8 * 1024 * 1024 }, // 单张图片最大 8MB，够用又不会太吃内存/磁盘
  fileFilter: (req, file, cb) => {
    if (!ALLOWED_IMAGE_MIME.includes(file.mimetype)) {
      return cb(new Error('只支持 jpg / png / gif / webp 格式的图片'));
    }
    cb(null, true);
  },
});

app.post('/upload', (req, res) => {
  upload.single('image')(req, res, (err) => {
    if (err) {
      return res.status(400).json({ error: err.message || '上传失败' });
    }
    if (!req.file) {
      return res.status(400).json({ error: '没有收到图片文件' });
    }
    if (!STORAGE_ENABLED) return res.json({ url: `/uploads/${req.file.filename}` });
    uploadToStorage(req.file.path, `images/${req.file.filename}`, req.file.mimetype)
      .then((url) => res.json({ url }))
      .catch((e) => res.status(500).json({ error: e.message }));
  });
});

// 通用文件上传（安装包/文档等），跟图片上传分开一个接口：
// - 不限制文件类型（图片接口特意只放行4种图片格式，这个不加白名单）
// - 上限调到100MB，够放一般的安装包/压缩包，太大的文件还是建议用网盘链接分享
const MAX_FILE_SIZE = 100 * 1024 * 1024;
const uploadFile = multer({
  storage: multer.diskStorage({
    destination: (req, file, cb) => cb(null, UPLOADS_DIR),
    filename: (req, file, cb) => {
      const ext = path.extname(file.originalname) || '';
      const randomName = crypto.randomBytes(12).toString('hex');
      cb(null, `${Date.now()}-${randomName}${ext}`);
    },
  }),
  limits: { fileSize: STORAGE_ENABLED ? Math.min(MAX_FILE_SIZE, STORAGE_MAX_FILE_SIZE) : MAX_FILE_SIZE },
});

app.post('/upload-file', (req, res) => {
  uploadFile.single('file')(req, res, (err) => {
    if (err) {
      if (err.code === 'LIMIT_FILE_SIZE') {
        const cap = STORAGE_ENABLED ? Math.min(MAX_FILE_SIZE, STORAGE_MAX_FILE_SIZE) : MAX_FILE_SIZE;
        return res.status(400).json({ error: `文件太大了，最大支持 ${Math.round(cap / 1024 / 1024)}MB` });
      }
      return res.status(400).json({ error: err.message || '上传失败' });
    }
    if (!req.file) {
      return res.status(400).json({ error: '没有收到文件' });
    }
    // 原始文件名做个长度截断，避免超长文件名把消息体撑得太大；存储用的文件名跟原始名无关，不影响下载时的显示名
    const originalName = String(req.file.originalname || '未命名文件').slice(0, 150);
    if (!STORAGE_ENABLED) {
      return res.json({ url: `/uploads/${req.file.filename}`, name: originalName, size: req.file.size });
    }
    uploadToStorage(req.file.path, `files/${req.file.filename}`, req.file.mimetype)
      .then((url) => res.json({ url, name: originalName, size: req.file.size }))
      .catch((e2) => res.status(500).json({ error: e2.message }));
  });
});

// 问题件提醒导出：把"已解决"和"转处理"这两种终结状态的记录按分类/日期范围导出成CSV表格，
// 用浏览器直接打开这个链接就会触发下载，不用密码保护——导出是查看性质的操作，不是破坏性的
// ==================== 聊天记录关键词搜索 ====================
// 有数据库就搜全部历史（包括早已不在内存里的老消息）；没数据库就只能搜内存里最近这 100 条
const SEARCH_MAX_RESULTS = 60;
app.get('/api/messages/search', async (req, res) => {
  const q = String(req.query.q || '').trim().slice(0, 100);
  if (!q) return res.json({ results: [], total: 0 });
  // 只能搜自己所在的群
  const actor = verifyToken(tokenFromReq(req));
  if (!actor) return res.status(401).json({ error: '登录已失效，请重新登录' });
  const gid = String(req.query.groupId || '');
  if (!isGroupMember(gid, actor.username)) return res.status(403).json({ error: '你不在这个群里' });
  const pick = (m) => ({ id: m.id, username: m.username, text: m.text || '', time: m.time, hasImages: (m.images || []).length > 0, hasFiles: (m.files || []).length > 0 });
  if (!dbPool) {
    const lower = q.toLowerCase();
    const hits = history.filter((m) => m.type === 'message' && !m.deletedAt && (m.groupId || ALL_GROUP_ID) === gid && String(m.text || '').toLowerCase().includes(lower));
    return res.json({ results: hits.slice(-SEARCH_MAX_RESULTS).reverse().map(pick), total: hits.length, scope: 'memory' });
  }
  try {
    // ILIKE 的 % _ 要转义，不然搜 "100%" 这种会变成通配
    const BS = String.fromCharCode(92);
    const pattern = '%' + q.split('').map((ch) => (ch === '%' || ch === '_' || ch === BS ? BS + ch : ch)).join('') + '%';
    const { rows } = await dbPool.query(
      "SELECT * FROM chat_messages WHERE deleted_at IS NULL AND group_id = $3 AND text ILIKE $1 ESCAPE '" + BS + "' ORDER BY id DESC LIMIT $2;",
      [pattern, SEARCH_MAX_RESULTS + 1, gid]
    );
    const more = rows.length > SEARCH_MAX_RESULTS;
    res.json({ results: rows.slice(0, SEARCH_MAX_RESULTS).map((r) => pick(rowToChatMessage(r))), total: rows.length, more, scope: 'db' });
  } catch (err) {
    console.error('[聊天搜索失败]', err.message);
    res.status(500).json({ error: '搜索失败：' + err.message });
  }
});
// 某条老消息的前后各 15 条，给搜索结果点开看上下文用（内存里已经没有的那些）
app.get('/api/messages/context', async (req, res) => {
  const id = Number(req.query.id);
  if (!Number.isFinite(id)) return res.status(400).json({ error: '缺少消息ID' });
  const actor = verifyToken(tokenFromReq(req));
  if (!actor) return res.status(401).json({ error: '登录已失效，请重新登录' });
  const gid = String(req.query.groupId || '');
  if (!isGroupMember(gid, actor.username)) return res.status(403).json({ error: '你不在这个群里' });
  if (!dbPool) {
    const inGroup = history.filter((m) => m.type === 'message' && (m.groupId || ALL_GROUP_ID) === gid);
    const idx = inGroup.findIndex((m) => m.id === id);
    if (idx === -1) return res.json({ messages: [] });
    return res.json({ messages: inGroup.slice(Math.max(0, idx - 15), idx + 16).filter((m) => !m.deletedAt) });
  }
  try {
    const before = await dbPool.query('SELECT * FROM chat_messages WHERE id <= $1 AND group_id = $2 AND deleted_at IS NULL ORDER BY id DESC LIMIT 16;', [id, gid]);
    const after = await dbPool.query('SELECT * FROM chat_messages WHERE id > $1 AND group_id = $2 AND deleted_at IS NULL ORDER BY id ASC LIMIT 15;', [id, gid]);
    res.json({ messages: before.rows.reverse().concat(after.rows).map(rowToChatMessage) });
  } catch (err) {
    res.status(500).json({ error: '读取失败：' + err.message });
  }
});

app.get('/api/problem-item-export', async (req, res) => {
  if (!dbPool) {
    res.status(503).send('数据库未配置，没有历史数据可以导出');
    return;
  }
  const category = String(req.query.category || 'all');
  const startDate = req.query.startDate ? String(req.query.startDate) : '';
  const endDate = req.query.endDate ? String(req.query.endDate) : '';

  // 兼容老数据：这个功能刚上线之前，"转处理"这个状态叫"follow_up"，导出的时候两个名字都当"转处理"处理，
  // 不然老记录会被漏掉
  let query = "SELECT * FROM problem_item_reports WHERE status IN ('resolved', 'transferred', 'follow_up', 'transferred_merchant', 'transferred_task', 'resolved_stocked', 'resolved_reshipped', 'resolved_cancelled')";
  const params = [];
  if (category !== 'all' && PROBLEM_ITEM_CATEGORIES.includes(category)) {
    params.push(category);
    query += ` AND category = $${params.length}`;
  }
  if (startDate) {
    params.push(startDate + ' 00:00:00');
    query += ` AND resolved_at >= $${params.length}`;
  }
  if (endDate) {
    params.push(endDate + ' 23:59:59');
    query += ` AND resolved_at <= $${params.length}`;
  }
  query += ' ORDER BY resolved_at ASC;';

  try {
    const result = await dbPool.query(query, params);
    const escapeCsv = (val) => `"${String(val == null ? '' : val).replace(/"/g, '""')}"`;
    const lines = ['分类,问题类型,检品人员,订单ID/快递单号,备注,图片,提交人,提交时间,状态,跟进状态,处理人,处理时间'];
    result.rows.forEach((row) => {
      const issueTypes = Array.isArray(row.issue_types) ? row.issue_types.join('、') : '';
      const inspectorNames = Array.isArray(row.inspector_names) ? row.inspector_names.join('、') : '';
      const statusLabel = row.status === 'resolved' ? '已入库' // 三个分类里直接点的"已入库"（老名字叫已解决，状态值没改）
        : row.status === 'transferred_merchant' ? '转日志商家'
        : row.status === 'transferred_task' ? '转任务'
        : row.status === 'resolved_stocked' ? '已入库'
        : row.status === 'resolved_reshipped' ? '已补（换）发入库'
        : row.status === 'resolved_cancelled' ? '已取消'
        : '转处理'; // 老数据（transferred / follow_up）
      // 跟进图章：盖了哪几个、分别是谁盖的，一列里写清楚
      const stampObj = (row.follow_stamps && typeof row.follow_stamps === 'object' && !Array.isArray(row.follow_stamps)) ? row.follow_stamps : {};
      const stampLabel = Object.keys(stampObj)
        .map((k) => `${k}(${(stampObj[k] && stampObj[k].by) || ''})`)
        .join('、');
      const submittedTime = new Date(row.submitted_at).toLocaleString('zh-CN');
      const resolvedTime = row.resolved_at ? new Date(row.resolved_at).toLocaleString('zh-CN') : '';
      lines.push([
        escapeCsv(row.category),
        escapeCsv(issueTypes),
        escapeCsv(inspectorNames),
        escapeCsv(row.order_id
          ? `${row.id_kind === 'tracking' ? '快递单号' : row.id_kind === 'rs' ? 'RS单号' : '订单ID'}：${row.order_id}`
          : ''),
        escapeCsv(row.order_note),
        escapeCsv(Array.isArray(row.images) ? row.images.join(' ') : ''),
        escapeCsv(row.submitted_by),
        escapeCsv(submittedTime),
        escapeCsv(statusLabel),
        escapeCsv(stampLabel),
        escapeCsv(row.resolved_by),
        escapeCsv(resolvedTime),
      ].join(','));
    });
    // 开头加UTF-8 BOM，不然用Excel(尤其Windows版)直接打开这个CSV，中文会变成乱码
    const csv = '\uFEFF' + lines.join('\r\n');
    const filenamePart = category === 'all' ? '全部分类' : category;
    const rawFilename = `问题件记录-${filenamePart}.csv`;
    // HTTP响应头不能直接塞中文字符（会被Node拒绝），要用RFC 5987标准的filename*=UTF-8''编码方式，
    // 同时保留一个ASCII安全的兜底文件名给个别不支持这个新语法的老客户端
    res.setHeader('Content-Type', 'text/csv; charset=utf-8');
    res.setHeader('Content-Disposition', `attachment; filename="export.csv"; filename*=UTF-8''${encodeURIComponent(rawFilename)}`);
    res.send(csv);
  } catch (err) {
    console.error('[问题件导出失败]', err.message);
    res.status(500).send('导出失败：' + err.message);
  }
});

// ==================== 云盘（团队共享文件，简单版） ====================
// 文件本体放在 drive/ 目录，元数据（原始文件名/上传人/时间/分类）记在 drive/.index.json 里，
// 跟聊天附件一样存在服务器磁盘上：局域网自建服务器会一直保留；部署到 Render 这类平台时磁盘是临时的，
// 重新部署会清空——大文件、要长期保存的东西还是放正规网盘，这里定位是团队内部随手共享。
const DRIVE_DIR = path.join(__dirname, 'drive');
const DRIVE_INDEX_PATH = path.join(DRIVE_DIR, '.index.json');
// 单个文件 200MB；走 Supabase Storage 时按它的上限来（免费版 50MB）
const DRIVE_MAX_FILE_SIZE = STORAGE_ENABLED ? Math.min(200 * 1024 * 1024, STORAGE_MAX_FILE_SIZE) : 200 * 1024 * 1024;
const DRIVE_MAX_FILES_PER_UPLOAD = 10;
if (!fs.existsSync(DRIVE_DIR)) fs.mkdirSync(DRIVE_DIR, { recursive: true });

let driveFiles = []; // { id, name, size, folder, uploader, time, stored, url }  stored=随机文件名；url=云端地址（存在 Storage 时才有）

// 配了数据库的话，云盘索引也进数据库（.index.json 跟文件一样在临时盘上，重新部署会一起丢）
async function ensureDriveTable() {
  await dbPool.query(`
    CREATE TABLE IF NOT EXISTS drive_files (
      id TEXT PRIMARY KEY,
      name TEXT NOT NULL,
      size BIGINT NOT NULL DEFAULT 0,
      folder TEXT,
      uploader TEXT,
      time BIGINT NOT NULL,
      stored TEXT,
      url TEXT
    );
  `);
}
async function loadDriveIndexFromDB() {
  if (!dbPool) return;
  try {
    await ensureDriveTable();
    const { rows } = await dbPool.query('SELECT * FROM drive_files ORDER BY time ASC;');
    driveFiles = rows
      .map((r) => ({ id: r.id, name: r.name, size: Number(r.size), folder: r.folder || '未分类', uploader: r.uploader || '匿名', time: Number(r.time), stored: r.stored || '', url: r.url || '' }))
      // 本机文件要确认还在；云端的不用查
      .filter((f) => f.url || (f.stored && fs.existsSync(path.join(DRIVE_DIR, f.stored))));
    console.log(`已从数据库加载云盘索引，共 ${driveFiles.length} 个文件`);
  } catch (err) {
    console.error('[云盘索引读取数据库失败]', err.message);
  }
}
async function driveInsertDB(list) {
  if (!dbPool) return;
  for (const f of list) {
    try {
      await dbPool.query(
        'INSERT INTO drive_files (id, name, size, folder, uploader, time, stored, url) VALUES ($1,$2,$3,$4,$5,$6,$7,$8) ON CONFLICT (id) DO NOTHING;',
        [f.id, f.name, f.size, f.folder, f.uploader, f.time, f.stored || '', f.url || '']
      );
    } catch (err) {
      console.error('[云盘索引写入数据库失败]', err.message);
    }
  }
}
async function driveUpdateDB(f) {
  if (!dbPool) return;
  try {
    await dbPool.query('UPDATE drive_files SET name=$1, folder=$2 WHERE id=$3;', [f.name, f.folder, f.id]);
  } catch (err) {
    console.error('[云盘索引更新数据库失败]', err.message);
  }
}
async function driveDeleteDB(id) {
  if (!dbPool) return;
  try {
    await dbPool.query('DELETE FROM drive_files WHERE id=$1;', [id]);
  } catch (err) {
    console.error('[云盘索引删除数据库失败]', err.message);
  }
}

function loadDriveIndex() {
  try {
    if (!fs.existsSync(DRIVE_INDEX_PATH)) return;
    const parsed = JSON.parse(fs.readFileSync(DRIVE_INDEX_PATH, 'utf8'));
    if (!Array.isArray(parsed)) return;
    // 索引里有、磁盘上却没有的（比如被手动删了）直接跳过，避免列表里出现点了下载不了的幽灵文件
    driveFiles = parsed.filter((f) => f && f.id && f.stored && fs.existsSync(path.join(DRIVE_DIR, f.stored)));
    console.log(`已加载云盘索引，共 ${driveFiles.length} 个文件`);
  } catch (err) {
    console.error('[云盘索引读取失败，改用空列表]', err.message);
    driveFiles = [];
  }
}
function saveDriveIndex() {
  if (dbPool) return; // 有数据库就以数据库为准，不再写本机索引
  try {
    fs.writeFileSync(DRIVE_INDEX_PATH, JSON.stringify(driveFiles, null, 2));
  } catch (err) {
    console.error('[云盘索引写入失败]', err.message);
  }
}
if (!dbPool) loadDriveIndex(); // 有数据库的话在 startServer 里从数据库读

function publicDriveList() {
  return driveFiles
    .map(({ id, name, size, folder, uploader, time }) => ({ id, name, size, folder, uploader, time }))
    .sort((a, b) => b.time - a.time);
}
function broadcastDriveUpdate() {
  broadcast({ type: 'drive_update', files: publicDriveList() });
}

// multipart 里的文件名有些浏览器/版本会被按 latin1 解析，中文名变成一串乱码，这里识别出来转回 UTF-8；
// 已经是正常中文或纯英文的原样返回
function fixUploadName(raw) {
  const s = String(raw || '');
  if (!/[\u0080-\u00ff]/.test(s) || /[\u0100-\uffff]/.test(s)) return s;
  const decoded = Buffer.from(s, 'latin1').toString('utf8');
  return decoded.includes('\uFFFD') ? s : decoded;
}

const driveUpload = multer({
  storage: multer.diskStorage({
    destination: (req, file, cb) => cb(null, DRIVE_DIR),
    filename: (req, file, cb) => {
      const ext = (path.extname(file.originalname) || '').slice(0, 16);
      cb(null, `${Date.now()}-${crypto.randomBytes(12).toString('hex')}${ext}`);
    },
  }),
  limits: { fileSize: DRIVE_MAX_FILE_SIZE, files: DRIVE_MAX_FILES_PER_UPLOAD },
});

app.get('/api/drive/list', (req, res) => {
  res.json({ files: publicDriveList(), maxFileSize: DRIVE_MAX_FILE_SIZE, maxFiles: DRIVE_MAX_FILES_PER_UPLOAD });
});

app.post('/api/drive/upload', (req, res) => {
  driveUpload.array('files', DRIVE_MAX_FILES_PER_UPLOAD)(req, res, (err) => {
    if (err) {
      if (err.code === 'LIMIT_FILE_SIZE') {
        return res.status(400).json({ error: `单个文件最大 ${Math.round(DRIVE_MAX_FILE_SIZE / 1024 / 1024)}MB` });
      }
      if (err.code === 'LIMIT_FILE_COUNT' || err.code === 'LIMIT_UNEXPECTED_FILE') {
        return res.status(400).json({ error: `一次最多上传 ${DRIVE_MAX_FILES_PER_UPLOAD} 个文件` });
      }
      return res.status(400).json({ error: err.message || '上传失败' });
    }
    const files = req.files || [];
    if (files.length === 0) return res.status(400).json({ error: '没有收到文件' });
    const uploader = String(req.body.uploader || '').slice(0, 20).trim() || '匿名';
    const folder = String(req.body.folder || '').slice(0, 30).trim() || '未分类';
    const added = files.map((f) => ({
      id: crypto.randomBytes(8).toString('hex'),
      name: fixUploadName(f.originalname).slice(0, 150) || '未命名文件',
      size: f.size,
      folder,
      uploader,
      time: Date.now(),
      stored: f.filename,
      url: '',
      _path: f.path,
      _mime: f.mimetype,
    }));
    (async () => {
      if (STORAGE_ENABLED) {
        for (const f of added) {
          f.url = await uploadToStorage(f._path, `drive/${f.stored}`, f._mime);
        }
      }
      added.forEach((f) => { delete f._path; delete f._mime; });
      driveFiles.push(...added);
      saveDriveIndex();
      await driveInsertDB(added);
      broadcastDriveUpdate();
      res.json({ files: added.map(({ stored, url, ...rest }) => rest) });
    })().catch((e) => {
      added.forEach((f) => { if (f._path) fs.unlink(f._path, () => {}); });
      res.status(500).json({ error: e.message });
    });
  });
});

app.get('/api/drive/download/:id', (req, res) => {
  const f = driveFiles.find((x) => x.id === req.params.id);
  if (!f) return res.status(404).send('文件不存在或已被删除');
  // 云端文件：直接跳到 Storage 的公开地址，带上 download 参数让浏览器按原始文件名保存
  if (f.url) return res.redirect(`${f.url}?download=${encodeURIComponent(f.name)}`);
  // res.download 会自动把中文文件名按 RFC 5987 编码进 Content-Disposition，浏览器保存时显示原始文件名
  res.download(path.join(DRIVE_DIR, f.stored), f.name, (err) => {
    if (err && !res.headersSent) res.status(404).send('文件不存在或已被删除');
  });
});

// 改名/换分类和删除都要编辑密码（跟公告栏/提醒事项共用同一个），避免共享文件被随手删掉
app.post('/api/drive/rename', express.json(), (req, res) => {
  const { id, name, folder, password } = req.body || {};
  const actor = verifyToken(tokenFromReq(req));
  if (!actor || !isEditRole(actor.role)) return res.status(403).json({ error: '你的账号没有这个权限' });
  if (String(password || '') !== PIN_EDIT_PASSWORD) return res.status(403).json({ error: '密码错误' });
  const f = driveFiles.find((x) => x.id === id);
  if (!f) return res.status(404).json({ error: '文件不存在或已被删除' });
  const newName = String(name || '').slice(0, 150).trim();
  if (newName) f.name = newName;
  f.folder = String(folder || '').slice(0, 30).trim() || '未分类';
  saveDriveIndex();
  driveUpdateDB(f);
  broadcastDriveUpdate();
  res.json({ ok: true });
});

app.post('/api/drive/delete', express.json(), (req, res) => {
  const { id, password } = req.body || {};
  const actor = verifyToken(tokenFromReq(req));
  if (!actor || !isEditRole(actor.role)) return res.status(403).json({ error: '你的账号没有这个权限' });
  if (String(password || '') !== PIN_EDIT_PASSWORD) return res.status(403).json({ error: '密码错误，无法删除' });
  const idx = driveFiles.findIndex((x) => x.id === id);
  if (idx === -1) return res.status(404).json({ error: '文件不存在或已被删除' });
  const [removed] = driveFiles.splice(idx, 1);
  saveDriveIndex();
  driveDeleteDB(removed.id);
  if (removed.url) deleteFromStorage(`drive/${removed.stored}`);
  else fs.unlink(path.join(DRIVE_DIR, removed.stored), () => {});
  broadcastDriveUpdate();
  res.json({ ok: true });
});

// 在线用户: ws -> { username, id }
const clients = new Map();
// 最近消息历史——内存里始终保留最近MAX_HISTORY条，用于日常渲染/查找（快，不用每次都查数据库）；
// 如果数据库连上了，这些消息也会异步写入数据库，服务器重启后能从数据库把最近的消息读回来，
// 不会变成空白聊天室。没配置数据库的话，行为跟以前完全一样，纯内存，重启就清空。
const MAX_HISTORY = 600; // 所有群共用一份内存历史，每个群发给客户端时各取最近 100 条
let history = [];
const HISTORY_PER_GROUP = 100;

async function ensureChatMessagesTable() {
  if (!dbPool) return;
  await dbPool.query(`
    CREATE TABLE IF NOT EXISTS chat_messages (
      id BIGINT PRIMARY KEY,
      username TEXT,
      text TEXT,
      images JSONB,
      files JSONB,
      mentions JSONB,
      mentions_all BOOLEAN DEFAULT false,
      quote JSONB,
      reactions JSONB DEFAULT '{}',
      pending JSONB,
      msg_time BIGINT NOT NULL,
      created_at TIMESTAMPTZ DEFAULT now()
    );
  `);
  // 用ALTER TABLE ADD COLUMN IF NOT EXISTS，这样已经在跑的老部署(表已经建过了)也能平滑加上这两个新字段，
  // 不用手动迁移——edited_at记录最后一次编辑的时间(没编辑过就是NULL)，
  // deleted_at记录删除时间(没删就是NULL，删除时同时会清空text/images/files，只留这个时间戳当"墓碑标记")
  await dbPool.query(`ALTER TABLE chat_messages ADD COLUMN IF NOT EXISTS edited_at TIMESTAMPTZ;`);
  await dbPool.query(`ALTER TABLE chat_messages ADD COLUMN IF NOT EXISTS deleted_at TIMESTAMPTZ;`);
  // 群组：每条消息属于一个群，老消息默认都算全员群
  await dbPool.query(`ALTER TABLE chat_messages ADD COLUMN IF NOT EXISTS group_id TEXT NOT NULL DEFAULT 'all';`);
}

function rowToChatMessage(row) {
  return {
    type: 'message',
    id: Number(row.id),
    username: row.username,
    text: row.text,
    images: row.images || [],
    files: row.files || [],
    mentions: row.mentions || [],
    mentionsAll: row.mentions_all,
    quote: row.quote,
    reactions: row.reactions || {},
    pending: row.pending,
    time: Number(row.msg_time),
    editedAt: row.edited_at ? new Date(row.edited_at).getTime() : null,
    deletedAt: row.deleted_at ? new Date(row.deleted_at).getTime() : null,
    groupId: row.group_id || 'all',
  };
}

async function loadChatHistoryFromDB() {
  if (!dbPool) return;
  try {
    await ensureChatMessagesTable();
    // 只读最近MAX_HISTORY条，按时间正序排好，直接当成内存历史用
    const { rows } = await dbPool.query(
      'SELECT * FROM chat_messages ORDER BY id DESC LIMIT $1;',
      [MAX_HISTORY]
    );
    history = rows.reverse().map(rowToChatMessage);
    if (history.length > 0) {
      // 下一条消息的ID接着数据库里最大的那个往后排，避免重启后ID撞车
      nextMessageId = Math.max(...history.map((m) => m.id)) + 1;
    }
    console.log(`已从数据库加载 ${history.length} 条聊天记录`);
  } catch (err) {
    console.error('[加载聊天记录失败，暂时改用空白历史]', err.message);
  }
}

// 写入是"发出去就不等结果"的异步方式——聊天消息发得很频繁，不能让每条消息都等数据库写完才广播给大家，
// 那样会让发消息变得很卡。写失败了就在日志里报个错，不影响当次消息正常收发，
// 只是不写进数据库的话，这一条消息在下次重启后会读不到（历史记录会跳过这条），概率很低但要知道有这个情况
function saveChatMessageToDB(msg) {
  if (!dbPool) return;
  dbPool.query(
    `INSERT INTO chat_messages (id, username, text, images, files, mentions, mentions_all, quote, reactions, pending, msg_time, group_id)
     VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
     ON CONFLICT (id) DO NOTHING;`,
    [
      msg.id, msg.username, msg.text,
      JSON.stringify(msg.images || []), JSON.stringify(msg.files || []),
      JSON.stringify(msg.mentions || []), !!msg.mentionsAll,
      msg.quote ? JSON.stringify(msg.quote) : null,
      JSON.stringify(msg.reactions || {}),
      msg.pending ? JSON.stringify(msg.pending) : null,
      msg.time,
      msg.groupId || 'all',
    ]
  ).catch((err) => console.error('[聊天消息写入数据库失败]', err.message));
}

function updateMessageReactionsInDB(messageId, reactions) {
  if (!dbPool) return;
  dbPool.query('UPDATE chat_messages SET reactions = $1 WHERE id = $2;', [JSON.stringify(reactions), messageId])
    .catch((err) => console.error('[更新消息点赞状态到数据库失败]', err.message));
}

function updateMessagePendingInDB(messageId, pending) {
  if (!dbPool) return;
  dbPool.query('UPDATE chat_messages SET pending = $1 WHERE id = $2;', [pending ? JSON.stringify(pending) : null, messageId])
    .catch((err) => console.error('[更新消息待办状态到数据库失败]', err.message));
}

function updateMessageTextInDB(messageId, newText, editedAt) {
  if (!dbPool) return;
  dbPool.query('UPDATE chat_messages SET text = $1, edited_at = to_timestamp($2 / 1000.0) WHERE id = $3;', [newText, editedAt, messageId])
    .catch((err) => console.error('[更新消息文字到数据库失败]', err.message));
}

function deleteMessageInDB(messageId, deletedAt) {
  if (!dbPool) return;
  // 删除是"软删除"：清空文字/图片/文件内容，但保留这一行记录（发送人、时间、删除时间戳），
  // 这样别人回复引用过这条消息的话，回复关系还能对上，不会变成指向一个凭空消失的东西
  dbPool.query(
    "UPDATE chat_messages SET text = NULL, images = '[]', files = '[]', deleted_at = to_timestamp($1 / 1000.0) WHERE id = $2;",
    [deletedAt, messageId]
  ).catch((err) => console.error('[删除消息到数据库失败]', err.message));
}

function rowToHistoryEntry(row) {
  return {
    id: row.id,
    text: row.text,
    by: row.by_user,
    startTime: new Date(row.start_time).getTime(),
    endTime: row.end_time ? new Date(row.end_time).getTime() : null,
  };
}

// @提及超时未确认的二次提醒：被@的人如果20分钟内完全没有回应过这条消息
// （没点👍、没点❓、没回复过、也没标为待办），就单独给这个人再推一次提醒——
// 这四种行为都算"确认看到了"，任意一种都不需要再提醒，避免真的很忙、
// 暂时没看群的人错过重要消息，也避免已经处理过的人被反复打扰
const MENTION_REMINDER_DELAY = 20 * 60 * 1000; // 20分钟

// 判断某个被@的人有没有以这四种方式之一"确认"过这条消息
function hasAcknowledgedMention(msg, targetUser) {
  const thumbsUp = (msg.reactions && Array.isArray(msg.reactions['👍'])) ? msg.reactions['👍'] : [];
  const question = (msg.reactions && Array.isArray(msg.reactions['❓'])) ? msg.reactions['❓'] : [];
  if (thumbsUp.includes(targetUser) || question.includes(targetUser)) return true;

  // 标为待办：得是这个人自己标的才算数，别人标的不能替他"确认"
  if (msg.pending && msg.pending.by === targetUser) return true;

  // 回复过这条消息：在这条消息之后，这个人发过一条引用回复指向这条消息
  // （引用回复里只存了原消息的用户名+文字内容，没存原消息ID，所以用这两个字段匹配，
  // 极小概率同一个人连发两条一模一样的话才会有歧义，可以接受）
  const repliedByTarget = history.some((m) =>
    m.type === 'message' &&
    m.username === targetUser &&
    m.time > msg.time &&
    m.quote &&
    m.quote.username === msg.username &&
    m.quote.text === msg.text
  );
  if (repliedByTarget) return true;

  return false;
}

function scheduleMentionReminder(msg) {
  if (!Array.isArray(msg.mentions) || msg.mentions.length === 0) return;
  setTimeout(() => {
    // 消息可能已经被挤出内存历史了（超过 MAX_HISTORY 条），这种情况就不追了
    const current = history.find((m) => m.type === 'message' && m.id === msg.id);
    if (!current) return;
    const notAcknowledged = current.mentions.filter((u) => !hasAcknowledgedMention(current, u));
    if (notAcknowledged.length === 0) return;

    for (const [ws, client] of clients.entries()) {
      if (notAcknowledged.includes(client.username) && ws.readyState === WebSocket.OPEN) {
        ws.send(JSON.stringify({
          type: 'mention_reminder',
          messageId: current.id,
          fromUsername: current.username,
          text: current.text,
        }));
      }
    }
  }, MENTION_REMINDER_DELAY);
}

// 置顶公告编辑密码：优先读取环境变量 PIN_EDIT_PASSWORD（部署到Render时在后台设置），
// 本地没配置环境变量时用这个默认值兜底，方便本地测试，正式使用务必在Render上单独设置
const PIN_EDIT_PASSWORD = process.env.PIN_EDIT_PASSWORD || 'changeme123';

// ==================== 账号 / 角色 / 登录 ====================
// 每个人一个账号（用户名就是聊天里显示的名字），由管理员建；密码 scrypt 加盐存，
// 登录后发一个带签名的令牌，浏览器记着，刷新不用重登。改密码/重置密码后旧令牌全部作废。
// 角色决定能改什么：admin/manager 能改所有模块，其余三种只能聊天 + 提问题件，别的只读。
const ROLES = { admin: '管理员', manager: '仓库现场', inspector: '质检员', buyer: '代购', service: '客服' };
const EDIT_ROLES = ['admin', 'manager'];
const TOKEN_TTL_MS = 90 * 24 * 3600 * 1000;
const SESSION_SECRET = process.env.SESSION_SECRET
  || crypto.createHash('sha256').update('cc-session:' + PIN_EDIT_PASSWORD + ':' + DATABASE_URL).digest('hex');
// 第一次启动、用户表还是空的时候，自动建一个管理员账号，密码默认跟编辑密码一样（可用 ADMIN_PASSWORD 单独指定）
const SEED_ADMIN_USERNAME = process.env.ADMIN_USERNAME || '管理员';
const SEED_ADMIN_PASSWORD = process.env.ADMIN_PASSWORD || PIN_EDIT_PASSWORD;

let users = []; // { id, username, role, passwordHash, salt, pwVersion, disabled, mustChangePassword, createdAt }

function hashPassword(password, salt) {
  return crypto.scryptSync(String(password), salt, 64).toString('hex');
}
function newSalt() { return crypto.randomBytes(16).toString('hex'); }
function verifyPassword(user, password) {
  const h = hashPassword(password, user.salt);
  const a1 = Buffer.from(h, 'hex');
  const b1 = Buffer.from(user.passwordHash, 'hex');
  return a1.length === b1.length && crypto.timingSafeEqual(a1, b1);
}
function isEditRole(role) { return EDIT_ROLES.includes(role); }
function publicUser(u) {
  return { id: u.id, username: u.username, role: u.role, roleLabel: ROLES[u.role] || u.role, disabled: !!u.disabled, mustChangePassword: !!u.mustChangePassword, createdAt: u.createdAt };
}
function findUserByName(name) {
  const key = String(name || '').trim().toLowerCase();
  return users.find((u) => u.username.toLowerCase() === key) || null;
}
function findUserById(id) { return users.find((u) => String(u.id) === String(id)) || null; }

// 令牌 = base64(用户ID.密码版本.签发时间) + "." + HMAC。密码版本变了（改密码/重置）令牌就失效
function signToken(user) {
  const payload = Buffer.from(`${user.id}.${user.pwVersion}.${Date.now()}`).toString('base64url');
  const sig = crypto.createHmac('sha256', SESSION_SECRET).update(payload).digest('base64url');
  return `${payload}.${sig}`;
}
function verifyToken(token) {
  if (typeof token !== 'string') return null;
  const dot = token.lastIndexOf('.');
  if (dot === -1) return null;
  const payload = token.slice(0, dot);
  const sig = token.slice(dot + 1);
  const expect = crypto.createHmac('sha256', SESSION_SECRET).update(payload).digest('base64url');
  const a1 = Buffer.from(sig);
  const b1 = Buffer.from(expect);
  if (a1.length !== b1.length || !crypto.timingSafeEqual(a1, b1)) return null;
  const [id, ver, iat] = Buffer.from(payload, 'base64url').toString().split('.');
  const user = findUserById(id);
  if (!user || user.disabled) return null;
  if (String(user.pwVersion) !== String(ver)) return null;
  if (Date.now() - Number(iat) > TOKEN_TTL_MS) return null;
  return user;
}
function tokenFromReq(req) {
  const h = String(req.headers.authorization || '');
  if (h.startsWith('Bearer ')) return h.slice(7);
  if (req.body && req.body.token) return String(req.body.token);
  if (req.query && req.query.token) return String(req.query.token);
  return '';
}

async function ensureUsersTable() {
  await dbPool.query(`
    CREATE TABLE IF NOT EXISTS users (
      id BIGSERIAL PRIMARY KEY,
      username TEXT NOT NULL UNIQUE,
      role TEXT NOT NULL DEFAULT 'inspector',
      password_hash TEXT NOT NULL,
      salt TEXT NOT NULL,
      pw_version INT NOT NULL DEFAULT 1,
      disabled BOOLEAN NOT NULL DEFAULT false,
      must_change_password BOOLEAN NOT NULL DEFAULT true,
      created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
    );
  `);
}
function rowToUser(r) {
  return {
    id: r.id, username: r.username, role: ROLES[r.role] ? r.role : 'inspector',
    passwordHash: r.password_hash, salt: r.salt, pwVersion: Number(r.pw_version),
    disabled: !!r.disabled, mustChangePassword: !!r.must_change_password,
    createdAt: new Date(r.created_at).getTime(),
  };
}
async function loadUsersFromDB() {
  if (dbPool) {
    try {
      await ensureUsersTable();
      const { rows } = await dbPool.query('SELECT * FROM users ORDER BY id ASC;');
      users = rows.map(rowToUser);
      console.log(`已从数据库加载账号，共 ${users.length} 个`);
    } catch (err) {
      console.error('[加载账号失败]', err.message);
    }
  }
  if (users.length === 0) {
    await createUser(SEED_ADMIN_USERNAME, SEED_ADMIN_PASSWORD, 'admin', false);
    console.log(`用户表是空的，已自动建立管理员账号「${SEED_ADMIN_USERNAME}」（密码见 ADMIN_PASSWORD / PIN_EDIT_PASSWORD）`);
  }
}
async function createUser(username, password, role, mustChange) {
  const salt = newSalt();
  const u = {
    id: `mem-u-${Date.now()}-${Math.round(Math.random() * 1e6)}`, username, role,
    passwordHash: hashPassword(password, salt), salt, pwVersion: 1,
    disabled: false, mustChangePassword: !!mustChange, createdAt: Date.now(),
  };
  if (dbPool) {
    try {
      const r = await dbPool.query(
        'INSERT INTO users (username, role, password_hash, salt, must_change_password) VALUES ($1,$2,$3,$4,$5) RETURNING id, created_at;',
        [username, role, u.passwordHash, salt, u.mustChangePassword]
      );
      u.id = r.rows[0].id;
      u.createdAt = new Date(r.rows[0].created_at).getTime();
    } catch (err) {
      console.error('[账号写入数据库失败]', err.message);
      throw new Error('写入数据库失败：' + err.message);
    }
  }
  users.push(u);
  return u;
}
async function setUserPassword(user, password, mustChange) {
  user.salt = newSalt();
  user.passwordHash = hashPassword(password, user.salt);
  user.pwVersion += 1;
  user.mustChangePassword = !!mustChange;
  if (dbPool) {
    try {
      await dbPool.query(
        'UPDATE users SET password_hash=$1, salt=$2, pw_version=$3, must_change_password=$4, updated_at=now() WHERE id=$5;',
        [user.passwordHash, user.salt, user.pwVersion, user.mustChangePassword, user.id]
      );
    } catch (err) {
      console.error('[密码写入数据库失败]', err.message);
    }
  }
}
async function updateUserFields(user, fields) {
  Object.assign(user, fields);
  if (dbPool) {
    try {
      await dbPool.query('UPDATE users SET role=$1, disabled=$2, updated_at=now() WHERE id=$3;', [user.role, user.disabled, user.id]);
    } catch (err) {
      console.error('[账号更新数据库失败]', err.message);
    }
  }
}
async function deleteUser(user) {
  users = users.filter((u) => u !== user);
  if (dbPool) {
    try {
      await dbPool.query('DELETE FROM users WHERE id=$1;', [user.id]);
    } catch (err) {
      console.error('[账号删除数据库失败]', err.message);
    }
  }
}
// 某人被停用/删除/重置密码后，把她在线的连接踢掉，让她重新登录
function kickUserSessions(username, message) {
  Array.from(clients.entries()).forEach(([sock, c]) => {
    if (c.username !== username) return;
    try { sock.send(JSON.stringify({ type: 'kicked', message })); } catch (e) { /* 忽略 */ }
    clients.delete(sock);
    setTimeout(() => { try { sock.close(); } catch (e) { /* 忽略 */ } }, 300);
  });
}

// ---- 登录 / 改密码 ----
app.post('/api/login', express.json(), async (req, res) => {
  const username = String((req.body || {}).username || '').trim();
  const password = String((req.body || {}).password || '');
  const user = findUserByName(username);
  if (!user || !verifyPassword(user, password)) {
    return res.status(401).json({ error: '用户名或密码不对' });
  }
  if (user.disabled) return res.status(403).json({ error: '这个账号已停用，找管理员' });
  res.json({ token: signToken(user), user: publicUser(user) });
});
app.get('/api/session', (req, res) => {
  const user = verifyToken(tokenFromReq(req));
  if (!user) return res.status(401).json({ error: '登录已失效，请重新登录' });
  res.json({ user: publicUser(user) });
});
app.post('/api/change-password', express.json(), async (req, res) => {
  const user = verifyToken(tokenFromReq(req));
  if (!user) return res.status(401).json({ error: '登录已失效，请重新登录' });
  const oldPassword = String(req.body.oldPassword || '');
  const newPassword = String(req.body.newPassword || '');
  if (newPassword.length < 4) return res.status(400).json({ error: '新密码至少 4 位' });
  if (!user.mustChangePassword && !verifyPassword(user, oldPassword)) {
    return res.status(400).json({ error: '原密码不对' });
  }
  await setUserPassword(user, newPassword, false);
  res.json({ token: signToken(user), user: publicUser(user) });
});

// ---- 管理员：账号管理 ----
function requireAdmin(req, res) {
  const user = verifyToken(tokenFromReq(req));
  if (!user) { res.status(401).json({ error: '登录已失效，请重新登录' }); return null; }
  if (user.role !== 'admin') { res.status(403).json({ error: '只有管理员能管账号' }); return null; }
  return user;
}
app.get('/api/admin/users', (req, res) => {
  if (!requireAdmin(req, res)) return;
  res.json({ users: users.map(publicUser), roles: ROLES });
});
app.post('/api/admin/users', express.json(), async (req, res) => {
  if (!requireAdmin(req, res)) return;
  const username = String(req.body.username || '').trim().slice(0, 20);
  const role = ROLES[req.body.role] ? req.body.role : 'inspector';
  const password = String(req.body.password || '');
  if (!username) return res.status(400).json({ error: '请填用户名' });
  if (/[A-Za-z0-9Ａ-Ｚａ-ｚ０-９]/.test(username)) return res.status(400).json({ error: '用户名只能用中文姓名，不能有字母和数字' });
  if (password.length < 4) return res.status(400).json({ error: '初始密码至少 4 位' });
  if (findUserByName(username)) return res.status(400).json({ error: '这个名字已经有账号了' });
  try {
    const u = await createUser(username, password, role, true);
    sendGroupsToEveryone();
    res.json({ user: publicUser(u) });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});
app.post('/api/admin/users/update', express.json(), async (req, res) => {
  const admin = requireAdmin(req, res);
  if (!admin) return;
  const target = findUserById(req.body.id);
  if (!target) return res.status(404).json({ error: '没找到这个账号' });
  const fields = {};
  if (req.body.role !== undefined) {
    if (!ROLES[req.body.role]) return res.status(400).json({ error: '角色不对' });
    fields.role = req.body.role;
  }
  if (req.body.disabled !== undefined) fields.disabled = !!req.body.disabled;
  // 最后一个管理员不能把自己降级/停用，不然没人能管账号了
  const adminsLeft = users.filter((u) => u.role === 'admin' && !u.disabled && u !== target).length;
  if (target.role === 'admin' && adminsLeft === 0 && ((fields.role && fields.role !== 'admin') || fields.disabled)) {
    return res.status(400).json({ error: '这是最后一个管理员，不能降级或停用' });
  }
  await updateUserFields(target, fields);
  if (req.body.newPassword !== undefined) {
    const np = String(req.body.newPassword || '');
    if (np.length < 4) return res.status(400).json({ error: '新密码至少 4 位' });
    await setUserPassword(target, np, true);
    kickUserSessions(target.username, '管理员重置了你的密码，请用新密码重新登录');
  } else if (fields.disabled) {
    kickUserSessions(target.username, '你的账号已被停用');
  } else if (fields.role) {
    kickUserSessions(target.username, '你的账号角色变了，请重新登录');
  }
  res.json({ user: publicUser(target) });
});
app.post('/api/admin/users/delete', express.json(), async (req, res) => {
  const admin = requireAdmin(req, res);
  if (!admin) return;
  const target = findUserById(req.body.id);
  if (!target) return res.status(404).json({ error: '没找到这个账号' });
  if (target === admin) return res.status(400).json({ error: '不能删自己' });
  await deleteUser(target);
  kickUserSessions(target.username, '你的账号已被删除');
  sendGroupsToEveryone();
  res.json({ ok: true });
});

// ==================== 案例库（原来的公告栏换成了这个，编辑密码不变） ====================
// 一条案例 = 平台类别（煤炉/代拍/代购，代购要写清楚是哪个网站）+ 问题 + 图片 + 处理结果 + 改善举措。
// 所有人都能看，新增/修改/删除要编辑密码；配了数据库就落库，没配就纯内存
const CASE_PLATFORMS = ['煤炉', '代拍', '代购'];
const CASE_TEXT_MAX = 2000;
let caseLibrary = []; // [{ id, platform, site, problem, images, result, improvement, by, createdAt, updatedAt }]

async function ensureCaseLibraryTable() {
  await dbPool.query(`
    CREATE TABLE IF NOT EXISTS case_library (
      id BIGSERIAL PRIMARY KEY,
      platform TEXT NOT NULL,
      site TEXT,
      problem TEXT NOT NULL,
      images JSONB,
      result TEXT,
      improvement TEXT,
      by_user TEXT,
      created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
    );
  `);
  // 单号：默认不公开，只存着；勾了"对全员公开"才会随案例一起发给所有人
  await dbPool.query(`ALTER TABLE case_library ADD COLUMN IF NOT EXISTS order_id TEXT;`);
  await dbPool.query(`ALTER TABLE case_library ADD COLUMN IF NOT EXISTS order_public BOOLEAN NOT NULL DEFAULT false;`);
}
function rowToCase(row) {
  return {
    id: row.id,
    platform: row.platform,
    site: row.site || '',
    problem: row.problem || '',
    images: Array.isArray(row.images) ? row.images : [],
    result: row.result || '',
    improvement: row.improvement || '',
    orderId: row.order_id || '',
    orderPublic: !!row.order_public,
    by: row.by_user || '',
    createdAt: new Date(row.created_at).getTime(),
    updatedAt: new Date(row.updated_at).getTime(),
  };
}
async function loadCaseLibraryFromDB() {
  if (!dbPool) return;
  try {
    await ensureCaseLibraryTable();
    const { rows } = await dbPool.query('SELECT * FROM case_library ORDER BY created_at ASC;');
    caseLibrary = rows.map(rowToCase);
    console.log(`已从数据库加载案例库，共 ${caseLibrary.length} 条`);
  } catch (err) {
    console.error('[加载案例库失败，暂时改用内存]', err.message);
  }
}
// 发给客户端的样子：单号只在勾了公开时才带；没公开的只告诉大家"有没有填"，具体号码得凭密码单独查
function publicCaseView(c) {
  const { orderId, ...rest } = c;
  return { ...rest, orderId: c.orderPublic ? (orderId || '') : '', hasOrderId: !!orderId };
}
function caseLibraryForClients() {
  return caseLibrary.map(publicCaseView);
}
// 把前端发来的一条案例整理干净（截长度、过滤非法图片地址、平台只认三种）
function sanitizeCaseInput(data) {
  const platform = CASE_PLATFORMS.includes(data.platform) ? data.platform : '';
  const site = String(data.site || '').trim().slice(0, 60);
  const problem = String(data.problem || '').trim().slice(0, CASE_TEXT_MAX);
  const result = String(data.result || '').trim().slice(0, CASE_TEXT_MAX);
  const improvement = String(data.improvement || '').trim().slice(0, CASE_TEXT_MAX);
  const images = Array.isArray(data.images)
    ? data.images.filter((u) => isOwnUploadUrl(u)).slice(0, 3)
    : [];
  const orderId = String(data.orderId || '').trim().slice(0, 60);
  const orderPublic = data.orderPublic === true;
  return { platform, site, problem, result, improvement, images, orderId, orderPublic };
}
async function addCase(input, byUsername) {
  const now = Date.now();
  let id = `mem-case-${now}`;
  if (dbPool) {
    try {
      const r = await dbPool.query(
        'INSERT INTO case_library (platform, site, problem, images, result, improvement, by_user, order_id, order_public) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9) RETURNING id;',
        [input.platform, input.site, input.problem, JSON.stringify(input.images), input.result, input.improvement, byUsername, input.orderId, input.orderPublic]
      );
      id = r.rows[0].id;
    } catch (err) {
      console.error('[案例库写入数据库失败]', err.message);
    }
  }
  const entry = { id, ...input, by: byUsername, createdAt: now, updatedAt: now };
  caseLibrary.push(entry);
  return entry;
}
async function updateCase(id, input, byUsername) {
  const idx = caseLibrary.findIndex((c) => String(c.id) === String(id));
  if (idx === -1) return null;
  const now = Date.now();
  // 单号没公开时，编辑表单里是看不到原值的；这时前端会留空，留空就当"保持原来的"，别把它清掉
  if (!input.orderId) input.orderId = caseLibrary[idx].orderId || '';
  caseLibrary[idx] = { ...caseLibrary[idx], ...input, updatedAt: now, updatedBy: byUsername };
  if (dbPool) {
    try {
      await dbPool.query(
        'UPDATE case_library SET platform=$1, site=$2, problem=$3, images=$4, result=$5, improvement=$6, order_id=$7, order_public=$8, updated_at=now() WHERE id=$9;',
        [input.platform, input.site, input.problem, JSON.stringify(input.images), input.result, input.improvement, input.orderId, input.orderPublic, id]
      );
    } catch (err) {
      console.error('[案例库更新数据库失败]', err.message);
    }
  }
  return caseLibrary[idx];
}
async function deleteCase(id) {
  const idx = caseLibrary.findIndex((c) => String(c.id) === String(id));
  if (idx === -1) return false;
  caseLibrary.splice(idx, 1);
  if (dbPool) {
    try {
      await dbPool.query('DELETE FROM case_library WHERE id = $1;', [id]);
    } catch (err) {
      console.error('[案例库删除数据库记录失败]', err.message);
    }
  }
  return true;
}

// ==================== 特殊要求（部分用户的特殊检品要求，按入库码查） ====================
// 一条 = 入库码 + 用户名 + 品类 + 平台 + 要求。所有人可看，增删改要编辑密码（跟案例库同一个）
const SPECIAL_REQ_TEXT_MAX = 2000;
let specialRequirements = []; // [{ id, code, username, category, platform, requirement, by, createdAt, updatedAt }]

async function ensureSpecialRequirementsTable() {
  await dbPool.query(`
    CREATE TABLE IF NOT EXISTS special_requirements (
      id BIGSERIAL PRIMARY KEY,
      code TEXT NOT NULL,
      username TEXT NOT NULL,
      category TEXT,
      platform TEXT,
      requirement TEXT NOT NULL,
      by_user TEXT,
      created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
    );
  `);
}
function rowToSpecialReq(row) {
  return {
    id: row.id,
    code: row.code,
    username: row.username,
    category: row.category || '',
    platform: row.platform || '',
    requirement: row.requirement || '',
    by: row.by_user || '',
    createdAt: new Date(row.created_at).getTime(),
    updatedAt: new Date(row.updated_at).getTime(),
  };
}
async function loadSpecialRequirementsFromDB() {
  if (!dbPool) return;
  try {
    await ensureSpecialRequirementsTable();
    const { rows } = await dbPool.query('SELECT * FROM special_requirements ORDER BY code ASC, id ASC;');
    specialRequirements = rows.map(rowToSpecialReq);
    console.log(`已从数据库加载特殊要求，共 ${specialRequirements.length} 条`);
  } catch (err) {
    console.error('[加载特殊要求失败，暂时改用内存]', err.message);
  }
}
function sanitizeSpecialReqInput(data) {
  return {
    code: String(data.code || '').trim().slice(0, 60),
    username: String(data.username || '').trim().slice(0, 60),
    category: String(data.category || '').trim().slice(0, 100),
    platform: String(data.platform || '').trim().slice(0, 60),
    requirement: String(data.requirement || '').trim().slice(0, SPECIAL_REQ_TEXT_MAX),
  };
}
async function addSpecialReq(input, byUsername) {
  const now = Date.now();
  let id = `mem-sr-${now}`;
  if (dbPool) {
    try {
      const r = await dbPool.query(
        'INSERT INTO special_requirements (code, username, category, platform, requirement, by_user) VALUES ($1,$2,$3,$4,$5,$6) RETURNING id;',
        [input.code, input.username, input.category, input.platform, input.requirement, byUsername]
      );
      id = r.rows[0].id;
    } catch (err) {
      console.error('[特殊要求写入数据库失败]', err.message);
    }
  }
  const entry = { id, ...input, by: byUsername, createdAt: now, updatedAt: now };
  specialRequirements.push(entry);
  return entry;
}
async function updateSpecialReq(id, input, byUsername) {
  const idx = specialRequirements.findIndex((r) => String(r.id) === String(id));
  if (idx === -1) return null;
  specialRequirements[idx] = { ...specialRequirements[idx], ...input, updatedAt: Date.now(), updatedBy: byUsername };
  if (dbPool) {
    try {
      await dbPool.query(
        'UPDATE special_requirements SET code=$1, username=$2, category=$3, platform=$4, requirement=$5, updated_at=now() WHERE id=$6;',
        [input.code, input.username, input.category, input.platform, input.requirement, id]
      );
    } catch (err) {
      console.error('[特殊要求更新数据库失败]', err.message);
    }
  }
  return specialRequirements[idx];
}
async function deleteSpecialReq(id) {
  const idx = specialRequirements.findIndex((r) => String(r.id) === String(id));
  if (idx === -1) return false;
  specialRequirements.splice(idx, 1);
  if (dbPool) {
    try {
      await dbPool.query('DELETE FROM special_requirements WHERE id = $1;', [id]);
    } catch (err) {
      console.error('[特殊要求删除数据库记录失败]', err.message);
    }
  }
  return true;
}

// ==================== 商城到货统计 ====================
// 三家供应商各自一份订货明细（Excel 导入），到货时扫 JAN 或手动填数量记一笔"到货"，
// 到齐的自动从"待到货"挪到"已入库"。到货记录能删（仓库现场/管理员），删了数量退回去。
const MALL_SUPPLIERS = [
  { key: 'qilintang', name: '麒麟堂', code: '248074' },
  { key: 'daguo', name: '大国', code: '371194' },
  { key: 'fuhele', name: '福和乐', code: '371194' },
];
let mallItems = [];    // { id, supplier, orderDate, jan, nameCn, nameJp, qty, qtyArrived, status, note, importedBy, createdAt }
let mallArrivals = []; // { id, itemId, qty, at, by }

async function ensureMallTables() {
  await dbPool.query(`
    CREATE TABLE IF NOT EXISTS mall_items (
      id BIGSERIAL PRIMARY KEY,
      supplier TEXT NOT NULL,
      order_date TEXT,
      jan TEXT NOT NULL,
      name_cn TEXT,
      name_jp TEXT,
      qty INT NOT NULL DEFAULT 1,
      qty_arrived INT NOT NULL DEFAULT 0,
      status TEXT NOT NULL DEFAULT 'pending',
      note TEXT,
      imported_by TEXT,
      created_at TIMESTAMPTZ NOT NULL DEFAULT now()
    );
  `);
  await dbPool.query(`
    CREATE TABLE IF NOT EXISTS mall_arrivals (
      id BIGSERIAL PRIMARY KEY,
      item_id BIGINT NOT NULL,
      qty INT NOT NULL,
      at_time BIGINT NOT NULL,
      by_user TEXT
    );
  `);
}
function rowToMallItem(r) {
  return {
    id: String(r.id), supplier: r.supplier, orderDate: r.order_date || '', jan: r.jan, nameCn: r.name_cn || '', nameJp: r.name_jp || '',
    qty: Number(r.qty), qtyArrived: Number(r.qty_arrived), status: r.status, note: r.note || '', importedBy: r.imported_by || '',
    createdAt: new Date(r.created_at).getTime(),
  };
}
async function loadMallFromDB() {
  if (!dbPool) return;
  try {
    await ensureMallTables();
    const a = await dbPool.query('SELECT * FROM mall_items ORDER BY id ASC;');
    mallItems = a.rows.map(rowToMallItem);
    const b = await dbPool.query('SELECT * FROM mall_arrivals ORDER BY id ASC;');
    mallArrivals = b.rows.map((r) => ({ id: String(r.id), itemId: String(r.item_id), qty: Number(r.qty), at: Number(r.at_time), by: r.by_user || '' }));
    console.log(`已从数据库加载商城到货统计，商品 ${mallItems.length} 条 / 到货记录 ${mallArrivals.length} 条`);
  } catch (err) {
    console.error('[加载商城到货统计失败]', err.message);
  }
}
function mallSnapshot() {
  return { type: 'mall_data', suppliers: MALL_SUPPLIERS, items: mallItems, arrivals: mallArrivals };
}
function broadcastMall() { broadcast(mallSnapshot()); }
function mallItemById(id) { return mallItems.find((x) => String(x.id) === String(id)) || null; }
function recomputeMallStatus(item) {
  if (item.status === 'cancelled') return;
  item.status = item.qtyArrived >= item.qty ? 'arrived' : 'pending';
}
async function mallSaveItem(item) {
  if (!dbPool || String(item.id).startsWith('mem-')) return;
  try {
    await dbPool.query('UPDATE mall_items SET qty=$1, qty_arrived=$2, status=$3, note=$4 WHERE id=$5;', [item.qty, item.qtyArrived, item.status, item.note, item.id]);
  } catch (err) { console.error('[商城商品更新失败]', err.message); }
}
async function mallImport(supplier, rows, byUsername) {
  const added = [];
  for (const r of rows) {
    const jan = String(r.jan || '').replace(/\D/g, '');
    if (!/^\d{8,14}$/.test(jan)) continue;
    const qty = Math.max(1, Math.min(9999, Math.round(Number(r.qty) || 1)));
    const status = r.status === 'cancelled' ? 'cancelled' : (r.status === 'arrived' ? 'arrived' : 'pending');
    const item = {
      id: `mem-mi-${Date.now()}-${Math.round(Math.random() * 1e6)}`, supplier,
      orderDate: String(r.orderDate || '').slice(0, 30), jan,
      nameCn: String(r.nameCn || '').slice(0, 200), nameJp: String(r.nameJp || '').slice(0, 200),
      qty, qtyArrived: status === 'arrived' ? qty : 0, status, note: String(r.note || '').slice(0, 200),
      importedBy: byUsername, createdAt: Date.now(),
    };
    if (dbPool) {
      try {
        const ins = await dbPool.query(
          'INSERT INTO mall_items (supplier, order_date, jan, name_cn, name_jp, qty, qty_arrived, status, note, imported_by) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10) RETURNING id;',
          [item.supplier, item.orderDate, item.jan, item.nameCn, item.nameJp, item.qty, item.qtyArrived, item.status, item.note, item.importedBy]
        );
        item.id = String(ins.rows[0].id);
      } catch (err) { console.error('[商城商品写入失败]', err.message); }
    }
    mallItems.push(item);
    // 表格里已经标了"已到库"的，补一条到货记录，方便明细里看得到
    if (status === 'arrived') await mallAddArrival(item, qty, byUsername, true);
    added.push(item);
  }
  return added;
}
async function mallAddArrival(item, qty, byUsername, skipStatus) {
  const rec = { id: `mem-ma-${Date.now()}-${Math.round(Math.random() * 1e6)}`, itemId: String(item.id), qty, at: Date.now(), by: byUsername };
  if (dbPool && !String(item.id).startsWith('mem-')) {
    try {
      const ins = await dbPool.query('INSERT INTO mall_arrivals (item_id, qty, at_time, by_user) VALUES ($1,$2,$3,$4) RETURNING id;', [item.id, qty, rec.at, byUsername]);
      rec.id = String(ins.rows[0].id);
    } catch (err) { console.error('[到货记录写入失败]', err.message); }
  }
  mallArrivals.push(rec);
  if (!skipStatus) {
    item.qtyArrived = Math.min(item.qty, item.qtyArrived + qty);
    recomputeMallStatus(item);
    await mallSaveItem(item);
  }
  return rec;
}
async function mallDeleteArrival(rec) {
  mallArrivals = mallArrivals.filter((x) => x !== rec);
  if (dbPool && !String(rec.id).startsWith('mem-')) {
    try { await dbPool.query('DELETE FROM mall_arrivals WHERE id=$1;', [rec.id]); } catch (err) { console.error('[到货记录删除失败]', err.message); }
  }
  const item = mallItemById(rec.itemId);
  if (item) {
    item.qtyArrived = Math.max(0, item.qtyArrived - rec.qty);
    recomputeMallStatus(item);
    await mallSaveItem(item);
  }
}
async function mallDeleteItem(item) {
  mallItems = mallItems.filter((x) => x !== item);
  mallArrivals = mallArrivals.filter((x) => String(x.itemId) !== String(item.id));
  if (dbPool && !String(item.id).startsWith('mem-')) {
    try {
      await dbPool.query('DELETE FROM mall_arrivals WHERE item_id=$1;', [item.id]);
      await dbPool.query('DELETE FROM mall_items WHERE id=$1;', [item.id]);
    } catch (err) { console.error('[商城商品删除失败]', err.message); }
  }
}

// 检品规则：5个固定分类，每个分类的内容/修改历史机制完全跟公告栏一样（共用同一个编辑密码），
// 只是5个分类共用一张数据库表，用category字段区分，不用建5张一模一样的表
const INSPECTION_RULE_CATEGORIES = ['煤炉', '代拍', '代购', '问题件', '增值服务'];
const INSPECTION_RULE_MAX_LENGTH = 20000; // 规则文本可能很长，放宽到2万字符
const INSPECTION_RULE_HISTORY_MAX = 50;
const DEFAULT_MEILU_RULE_TEXT = `煤炉一旦入库，在商家页面就自动签收，所以检品时一定要注意是否有检品服务、当前卖家已下单订单数、金额。
收件人姓名：森次郎
收件人地址：大阪府大阪市西区本田 4-1-7 3F OOM OOOOOO（订单 ID）

* 02M、05M：mercari 煤炉
* 04M：mercari 商城
* 06M、07M：mercari 代拍

第一步：筛选不需要检品的

* 无检品服务并出现弹窗提示，当前卖家已下单订单数为 1 时，多贵都不检品
* 无检品服务没弹窗提示，当前卖家已下单订单数为 1 且商品金额 5000 日元以下，不检品

第二步：集中处理需要检品的

* 有检品服务
* 04M mercari 商城（确认同捆）
* 当前卖家已下单订单数为 1 以上
* 无弹窗时商品金额 5000 日元以上

增值服务：订单截图、订单留言等平时不需要看，注意【集货用户】需另外操作。遇到多商品或少商品时，确认【コメント】是否有跟商家沟通过赠品或选品问题。
弹窗问题单处理：

* ①手动签收或备注新链接：煤炉一般自动签收，若更换购买链接需社员手动签收。流程：确认原因→勾掉问题单→录视频检品→入库→告诉社员【02/04 的 xxx（订单 ID）需要手动签收】
* ②商家补发：缺货补发→找社员要之前的包裹核对无误后入库；破损补发→尤其确认是否完好，正常检品入库

煤炉到付：

* 04 账号特殊，会出现提示到付但包裹元払的情况，代购后台确认付款情况
* 所有煤炉到付订单已预收顾客 1000 日元
* 检品到煤炉到付包裹时，正常检品不入库，保存检品视频，拿给社员

集货用户（NOID）：

* 仅针对 NOID 的代购平信包裹，正常有物流单号的货物正常入库，不在此范围（noid 包裹单个超 10kg 则不拆包合并，正常入库）
* 兼职人员不用入库，但操作页面出现集货用户订单时，跟 04M mercari 商城一样必须拆开确认
* 注意：包裹内商品并非都是集货用户订单，可能普通顾客与集货顾客同时在该商家下单
   * ①同捆发货：按订单分箱（与普通分箱要求相同，包裹写订单 ID 贴 NOID）
   * ②包含非集货用户商品：非集货用户正常入库，集货用户贴 NOID 贴写订单 ID（正常从有快递单号分出来的订单贴 DAIGOU 贴，集货用户不论有没有单号都贴 NOID）
   * ③有快递单号的集货用户：正常入库，集货入库只操作 NOID 包裹
   * ④全部确认好放指定框中，由社员入库`;

// 每个分类当前内容 + 修改历史，格式跟announcementHistory一样：[{id, text, by, startTime, endTime}]
const inspectionRules = {};
INSPECTION_RULE_CATEGORIES.forEach((cat) => {
  const defaultText = cat === '煤炉' ? DEFAULT_MEILU_RULE_TEXT : '';
  inspectionRules[cat] = {
    text: defaultText,
    history: [{ id: `mem-seed-${cat}`, text: defaultText, by: '系统默认', startTime: Date.now(), endTime: null }],
  };
});

async function ensureInspectionRulesTable() {
  if (!dbPool) return;
  await dbPool.query(`
    CREATE TABLE IF NOT EXISTS inspection_rules_history (
      id BIGSERIAL PRIMARY KEY,
      category TEXT NOT NULL,
      text TEXT NOT NULL,
      by_user TEXT NOT NULL,
      start_time TIMESTAMPTZ NOT NULL DEFAULT now(),
      end_time TIMESTAMPTZ
    );
  `);
}

async function loadInspectionRulesFromDB() {
  if (!dbPool) return; // 没配数据库，继续用内存里的默认值
  try {
    await ensureInspectionRulesTable();
    for (const cat of INSPECTION_RULE_CATEGORIES) {
      const { rows } = await dbPool.query(
        'SELECT * FROM inspection_rules_history WHERE category = $1 ORDER BY start_time ASC;',
        [cat]
      );
      if (rows.length === 0) {
        // 这个分类在数据库里还没记录（第一次接入），把内存里的默认值写进去当第一条
        const defaultText = inspectionRules[cat].text;
        const inserted = await dbPool.query(
          'INSERT INTO inspection_rules_history (category, text, by_user, start_time, end_time) VALUES ($1, $2, $3, now(), NULL) RETURNING *;',
          [cat, defaultText, '系统默认']
        );
        inspectionRules[cat].history = inserted.rows.map(rowToHistoryEntry);
      } else {
        inspectionRules[cat].history = rows.map(rowToHistoryEntry);
      }
      inspectionRules[cat].text = inspectionRules[cat].history[inspectionRules[cat].history.length - 1].text;
    }
    console.log('已从数据库加载检品规则（5个分类）');
  } catch (err) {
    console.error('[加载检品规则失败，暂时改用内存默认值]', err.message);
  }
}

async function recordInspectionRuleChange(category, newText, byUsername) {
  const now = Date.now();
  const rule = inspectionRules[category];
  const last = rule.history[rule.history.length - 1];
  if (last && last.endTime === null) last.endTime = now;
  const newEntry = { id: `mem-${now}`, text: newText, by: byUsername, startTime: now, endTime: null };
  rule.history.push(newEntry);
  if (rule.history.length > INSPECTION_RULE_HISTORY_MAX) rule.history.shift();

  if (!dbPool) return;
  try {
    await dbPool.query(
      'UPDATE inspection_rules_history SET end_time = now() WHERE category = $1 AND end_time IS NULL;',
      [category]
    );
    const inserted = await dbPool.query(
      'INSERT INTO inspection_rules_history (category, text, by_user, start_time, end_time) VALUES ($1, $2, $3, now(), NULL) RETURNING id;',
      [category, newText, byUsername]
    );
    newEntry.id = inserted.rows[0].id;
  } catch (err) {
    console.error('[检品规则历史写入数据库失败]', err.message);
  }
}

async function deleteInspectionRuleHistoryEntry(targetId) {
  if (!dbPool) return;
  try {
    await dbPool.query('DELETE FROM inspection_rules_history WHERE id = $1;', [targetId]);
  } catch (err) {
    console.error('[删除检品规则历史记录失败]', err.message);
  }
}

function getAllInspectionRulesText() {
  const result = {};
  INSPECTION_RULE_CATEGORIES.forEach((cat) => { result[cat] = inspectionRules[cat].text; });
  return result;
}

// ===== 问题件列表：代购/代拍/煤炉三个分类，各自独立计数和记录列表。
// "问题类型"三个分类共用一组选项（数据库里的option_type='issue_type'）。
// "检品人员姓名"直接用当前登录用户名，不再维护选项列表。
// 另外还有两个"去向队列"：转日志商家、转任务——它们不是提交入口，
// 只是把已经转出去的记录按去向汇总起来，方便后续跟进（见 PROBLEM_ITEM_QUEUES）=====
const PROBLEM_ITEM_CATEGORIES = ['代购', '代拍', '煤炉'];
// 队列名 -> 对应的记录状态
const PROBLEM_ITEM_QUEUES = { '日志商家': 'transferred_merchant', '任务': 'transferred_task' };
const DEFAULT_ISSUE_TYPES = ['破损', '脏污', '特典', '少货', '多货', '商品错误', '找不到订单'];
// 队列（日志商家/任务）里的三种结束方式：点完这条记录就从队列里退场，
// 具体是哪种结果存在 status 里，导出的时候分开统计
const PROBLEM_ITEM_RESULTS = {
  stocked: { status: 'resolved_stocked', label: '已入库' },
  reshipped: { status: 'resolved_reshipped', label: '已补（换）发入库' },
  cancelled: { status: 'resolved_cancelled', label: '已取消' },
};
// 队列里的跟进图章：可以同时盖多个（比如先找顾客确认、同时已经建了任务），
// 每个图章记住是谁盖的、什么时候盖的；再点一下就取消
const PROBLEM_ITEM_STAMPS = ['日志顾客确认中', '商家中', '已建任务跟进中'];
// 待处理列表里按"谁转出去的"再分一遍的人名视图：这几个人各占一行，
// 谁点了转日志商家/转任务，那条记录就同时出现在她名下（跟队列视图是同一批数据，只是切法不同）
const PROBLEM_ITEM_HANDLERS = ['王晓雨', '孙韶蔚', '余丽', '钟海燕'];
// 走到头的记录（不管是三个分类里直接"已解决"，还是队列里给了处理结果）都算"已完结"。
// 这些记录不再占着待处理列表，但要在"已完结问题件"表格里实时看得到，
// 所以在内存里另留一份最近的，超出上限就把最老的挤掉（完整历史仍然在数据库里）
const PROBLEM_ITEM_FINISHED_STATUSES = ['resolved', 'resolved_stocked', 'resolved_reshipped', 'resolved_cancelled'];
const PROBLEM_ITEM_FINISHED_LIMIT = 500;
const problemItemFinished = [];
function problemItemStatusLabel(status) {
  if (status === 'resolved') return '已入库';
  if (status === 'resolved_stocked') return '已入库';
  if (status === 'resolved_reshipped') return '已补（换）发入库';
  if (status === 'resolved_cancelled') return '已取消';
  if (status === 'transferred_merchant') return '转日志商家';
  if (status === 'transferred_task') return '转任务';
  return '转处理'; // 老数据（transferred / follow_up）
}
function pushProblemItemFinished(record) {
  problemItemFinished.push(record);
  if (problemItemFinished.length > PROBLEM_ITEM_FINISHED_LIMIT) {
    problemItemFinished.splice(0, problemItemFinished.length - PROBLEM_ITEM_FINISHED_LIMIT);
  }
}

function getIssueTypeOptionKey() {
  return 'issue_type';
}

let problemItemOptions = { issue_type: [], inspector_name: [] };
// 每个分类当前"待处理"（未点已解决/需跟进）的记录列表，已处理的记录不放在内存里，只留在数据库里当历史
let problemItemReports = {};
PROBLEM_ITEM_CATEGORIES.forEach((cat) => { problemItemReports[cat] = []; });
let nextProblemItemOptionId = 1;
let nextProblemItemReportId = 1;

async function ensureProblemItemTables() {
  if (!dbPool) return;
  await dbPool.query(`
    CREATE TABLE IF NOT EXISTS problem_item_options (
      id BIGSERIAL PRIMARY KEY,
      option_type TEXT NOT NULL,
      value TEXT NOT NULL,
      sort_order INT NOT NULL DEFAULT 0,
      created_at TIMESTAMPTZ NOT NULL DEFAULT now()
    );
  `);
  await dbPool.query(`
    CREATE TABLE IF NOT EXISTS problem_item_reports (
      id BIGSERIAL PRIMARY KEY,
      category TEXT NOT NULL,
      issue_types JSONB NOT NULL,
      inspector_names JSONB NOT NULL,
      order_note TEXT,
      submitted_by TEXT NOT NULL,
      submitted_at TIMESTAMPTZ NOT NULL DEFAULT now(),
      status TEXT NOT NULL DEFAULT 'pending',
      resolved_by TEXT,
      resolved_at TIMESTAMPTZ
    );
  `);
  // 后加的三列：订单ID/快递单号、这个号是哪一种、附带的图片地址数组。
  // 用 ADD COLUMN IF NOT EXISTS 就地升级，已经在跑的老库不用手动改表也不会丢数据
  await dbPool.query(`ALTER TABLE problem_item_reports ADD COLUMN IF NOT EXISTS order_id TEXT;`);
  await dbPool.query(`ALTER TABLE problem_item_reports ADD COLUMN IF NOT EXISTS id_kind TEXT;`);
  await dbPool.query(`ALTER TABLE problem_item_reports ADD COLUMN IF NOT EXISTS images JSONB;`);
  await dbPool.query(`ALTER TABLE problem_item_reports ADD COLUMN IF NOT EXISTS follow_stamps JSONB;`);
  // "待跟进暂存"这个状态取消了，老数据里的 shelved 一次性归回待处理，免得永远不显示
  await dbPool.query(`UPDATE problem_item_reports SET status = 'pending' WHERE status = 'shelved';`);
}

function rowToProblemItemOption(row) {
  return { id: row.id, value: row.value };
}
function rowToProblemItemReport(row) {
  return {
    id: row.id,
    category: row.category,
    issueTypes: row.issue_types,
    inspectorNames: row.inspector_names,
    orderNote: row.order_note || '',
    orderId: row.order_id || '',
    idKind: ['tracking', 'rs', 'order'].includes(row.id_kind) ? row.id_kind : 'order',
    images: Array.isArray(row.images) ? row.images : [],
    submittedBy: row.submitted_by,
    submittedAt: new Date(row.submitted_at).getTime(),
    status: row.status,
    followStamps: (row.follow_stamps && typeof row.follow_stamps === 'object' && !Array.isArray(row.follow_stamps)) ? row.follow_stamps : {},
    handledBy: row.resolved_by || '', // 转出去/处理掉这条的人（人名视图按这个分）
  };
}

async function loadProblemItemDataFromDB() {
  // 没配数据库的话，用代码里写死的默认问题类型列表撑着，检品人员姓名列表留空等手动添加
  if (!dbPool) {
    problemItemOptions.issue_type = DEFAULT_ISSUE_TYPES.map((v, i) => ({ id: `mem-issue-${i}`, value: v }));
    problemItemOptions.inspector_name = [];
    return;
  }
  try {
    await ensureProblemItemTables();

    // 通用的选项组加载逻辑：数据库里没有的话，用给定的默认值先写进去，再读出来
    async function loadOptionGroup(optionType, defaults) {
      const rows = await dbPool.query(
        'SELECT * FROM problem_item_options WHERE option_type = $1 ORDER BY sort_order ASC, id ASC;',
        [optionType]
      );
      if (rows.rows.length === 0 && defaults.length > 0) {
        for (let i = 0; i < defaults.length; i++) {
          await dbPool.query(
            'INSERT INTO problem_item_options (option_type, value, sort_order) VALUES ($1, $2, $3);',
            [optionType, defaults[i], i]
          );
        }
        const reloaded = await dbPool.query(
          'SELECT * FROM problem_item_options WHERE option_type = $1 ORDER BY sort_order ASC, id ASC;',
          [optionType]
        );
        return reloaded.rows.map(rowToProblemItemOption);
      }
      return rows.rows.map(rowToProblemItemOption);
    }

    problemItemOptions.issue_type = await loadOptionGroup('issue_type', DEFAULT_ISSUE_TYPES);
    problemItemOptions.inspector_name = await loadOptionGroup('inspector_name', []);

    // 加载"待处理"和"待跟进暂存"这两种状态的记录到内存里——暂存的记录还要继续在列表里显示，
    // 只是不计入侧栏红点。已解决/转处理这两种是终结状态，留在数据库当历史，不占内存也不用同步给客户端
    for (const cat of PROBLEM_ITEM_CATEGORIES) {
      const reportRows = await dbPool.query(
        "SELECT * FROM problem_item_reports WHERE category = $1 AND status IN ('pending', 'transferred_merchant', 'transferred_task') ORDER BY submitted_at ASC;",
        [cat]
      );
      problemItemReports[cat] = reportRows.rows.map(rowToProblemItemReport);
    }

    // 已完结的记录：只读最近的一批进内存，给"已完结问题件"表格用（完整历史还在数据库里，导出走导出）
    const finishedRows = await dbPool.query(
      `SELECT * FROM problem_item_reports WHERE status = ANY($1::text[]) ORDER BY resolved_at DESC NULLS LAST, id DESC LIMIT $2;`,
      [PROBLEM_ITEM_FINISHED_STATUSES, PROBLEM_ITEM_FINISHED_LIMIT]
    );
    problemItemFinished.length = 0;
    finishedRows.rows.reverse().forEach((row) => {
      problemItemFinished.push({
        ...rowToProblemItemReport(row),
        resolvedBy: row.resolved_by || '',
        resolvedAt: row.resolved_at ? new Date(row.resolved_at).getTime() : null,
      });
    });

    console.log('已从数据库加载问题件提醒选项和待处理记录');
  } catch (err) {
    console.error('[加载问题件提醒数据失败，暂时改用内存默认值]', err.message);
    problemItemOptions.issue_type = DEFAULT_ISSUE_TYPES.map((v, i) => ({ id: `mem-issue-${i}`, value: v }));
  }
}

async function addProblemItemReport(category, issueTypes, inspectorNames, orderNote, submittedBy, orderId, idKind, images) {
  const now = Date.now();
  let id = `mem-${now}`;
  if (dbPool) {
    try {
      const result = await dbPool.query(
        'INSERT INTO problem_item_reports (category, issue_types, inspector_names, order_note, submitted_by, order_id, id_kind, images) VALUES ($1, $2, $3, $4, $5, $6, $7, $8) RETURNING id, submitted_at;',
        [category, JSON.stringify(issueTypes), JSON.stringify(inspectorNames), orderNote, submittedBy, orderId, idKind, JSON.stringify(images)]
      );
      id = result.rows[0].id;
    } catch (err) {
      console.error('[问题件列表记录写入数据库失败]', err.message);
    }
  }
  const report = { id, category, issueTypes, inspectorNames, orderNote, orderId, idKind, images, submittedBy, submittedAt: now, status: 'pending', followStamps: {} };
  problemItemReports[category].push(report);
  return report;
}

// 给已经提交的问题件补传照片：电脑上先把单子提了，之后拿手机拍照补上来。
// 只追加、不覆盖，最多留3张。
async function appendProblemItemImages(category, reportId, newImages) {
  const report = (problemItemReports[category] || []).find((r) => String(r.id) === String(reportId));
  if (!report) return null;
  const merged = Array.from(new Set((report.images || []).concat(newImages))).slice(0, 3);
  report.images = merged;
  if (dbPool) {
    try {
      await dbPool.query('UPDATE problem_item_reports SET images = $1 WHERE id = $2;', [JSON.stringify(merged), reportId]);
    } catch (err) {
      console.error('[补传问题件照片写入数据库失败]', err.message);
    }
  }
  return report;
}

// 跟进图章：同一个图章再点一次就取消，不同图章互不影响。
// 存成 { 图章名: { by, at } }，谁盖的直接跟在图章旁边显示
async function toggleProblemItemFollowStamp(category, reportId, stamp, byUsername) {
  const report = (problemItemReports[category] || []).find((r) => String(r.id) === String(reportId));
  if (!report) return null;
  const stamps = (report.followStamps && typeof report.followStamps === 'object') ? { ...report.followStamps } : {};
  if (stamps[stamp]) delete stamps[stamp];
  else stamps[stamp] = { by: byUsername, at: Date.now() };
  report.followStamps = stamps;
  if (dbPool) {
    try {
      await dbPool.query('UPDATE problem_item_reports SET follow_stamps = $1 WHERE id = $2;', [JSON.stringify(stamps), reportId]);
    } catch (err) {
      console.error('[问题件跟进图章写入数据库失败]', err.message);
    }
  }
  return report;
}

async function updateProblemItemReportStatus(category, reportId, status, byUsername) {
  const idx = problemItemReports[category].findIndex((r) => String(r.id) === String(reportId));
  if (idx === -1) return false;

  let finished = null;
  if (status === 'transferred_merchant' || status === 'transferred_task') {
    // 转日志商家 / 转任务：从原分类的待处理列表里"消失"，但记录本身留在内存里，
    // 换到对应的去向队列里继续显示（红点只按 pending 计数，所以转出后不再计入红点）
    problemItemReports[category][idx].status = status;
    problemItemReports[category][idx].handledBy = byUsername;
  } else {
    // 已完结：从待处理/队列里移除，转到"已完结问题件"表格里继续能查能搜
    const gone = problemItemReports[category].splice(idx, 1)[0];
    finished = { ...gone, category, status, resolvedBy: byUsername, resolvedAt: Date.now() };
    pushProblemItemFinished(finished);
  }

  if (dbPool) {
    try {
      await dbPool.query(
        'UPDATE problem_item_reports SET status = $1, resolved_by = $2, resolved_at = now() WHERE id = $3;',
        [status, byUsername, reportId]
      );
    } catch (err) {
      console.error('[问题件提醒状态更新失败]', err.message);
    }
  }
  return finished || true;
}

async function updateProblemItemOptions(optionType, values) {
  const newList = values.map((v, i) => ({ id: `mem-${optionType}-${Date.now()}-${i}`, value: v }));
  problemItemOptions[optionType] = newList;

  if (!dbPool) return;
  try {
    await dbPool.query('DELETE FROM problem_item_options WHERE option_type = $1;', [optionType]);
    for (let i = 0; i < values.length; i++) {
      await dbPool.query(
        'INSERT INTO problem_item_options (option_type, value, sort_order) VALUES ($1, $2, $3);',
        [optionType, values[i], i]
      );
    }
    const reloaded = await dbPool.query(
      'SELECT * FROM problem_item_options WHERE option_type = $1 ORDER BY sort_order ASC, id ASC;',
      [optionType]
    );
    problemItemOptions[optionType] = reloaded.rows.map(rowToProblemItemOption);
  } catch (err) {
    console.error('[问题件提醒选项更新失败]', err.message);
  }
}

function getProblemItemSnapshot() {
  return {
    options: {
      issueTypes: problemItemOptions.issue_type.map((o) => o.value),
      inspectorNames: problemItemOptions.inspector_name.map((o) => o.value),
    },
    reports: problemItemReports,
    finished: problemItemFinished,
    handlers: PROBLEM_ITEM_HANDLERS,
  };
}

// 消息自增ID（用于引用回复）
let nextMessageId = 1;
// 允许的消息表情回应（白名单，避免被塞入任意文本）
const ALLOWED_REACTIONS = ['👍', '❓'];

function broadcast(data, exclude) {
  const msg = JSON.stringify(data);
  wss.clients.forEach((client) => {
    if (client.readyState === WebSocket.OPEN && client !== exclude) {
      client.send(msg);
    }
  });
}

function getOnlineUsers() {
  return Array.from(clients.values()).map((c) => c.username);
}
// 达人广场：所有账号（不含停用的）+ 角色，前端配合在线名单显示谁在线，点谁就能私聊
function getDirectory() {
  return users.filter((u) => !u.disabled).map((u) => ({ username: u.username, role: u.role, roleLabel: ROLES[u.role] || u.role }));
}

// ==================== 群组 ====================
// "全员群"（id = all）不落库，成员永远是全部账号；其他群由管理员建、拉人。
// 每人各自置顶自己的群、各自记"看到哪条了"（算未读用），都存数据库。
const ALL_GROUP_ID = 'all';
const ALL_GROUP_NAME = '全员';
let groups = []; // { id(string), name, members:[username], createdBy, createdAt }
const groupPins = new Map();  // username -> Set(groupId)
const groupReads = new Map(); // username -> Map(groupId -> lastReadId)
const groupHidden = new Map(); // username -> Set(groupId)  关掉的私聊（对方再发消息会自动重新出现）

async function ensureGroupTables() {
  await dbPool.query(`
    CREATE TABLE IF NOT EXISTS groups (
      id BIGSERIAL PRIMARY KEY,
      name TEXT NOT NULL,
      created_by TEXT,
      created_at TIMESTAMPTZ NOT NULL DEFAULT now()
    );
  `);
  await dbPool.query(`ALTER TABLE groups ADD COLUMN IF NOT EXISTS is_dm BOOLEAN NOT NULL DEFAULT false;`);
  await dbPool.query(`
    CREATE TABLE IF NOT EXISTS group_members (
      group_id BIGINT NOT NULL,
      username TEXT NOT NULL,
      PRIMARY KEY (group_id, username)
    );
  `);
  await dbPool.query(`
    CREATE TABLE IF NOT EXISTS group_pins (
      username TEXT NOT NULL,
      group_id TEXT NOT NULL,
      PRIMARY KEY (username, group_id)
    );
  `);
  await dbPool.query(`
    CREATE TABLE IF NOT EXISTS group_hidden (
      username TEXT NOT NULL,
      group_id TEXT NOT NULL,
      PRIMARY KEY (username, group_id)
    );
  `);
  await dbPool.query(`
    CREATE TABLE IF NOT EXISTS group_reads (
      username TEXT NOT NULL,
      group_id TEXT NOT NULL,
      last_read_id BIGINT NOT NULL DEFAULT 0,
      PRIMARY KEY (username, group_id)
    );
  `);
}
async function loadGroupsFromDB() {
  if (!dbPool) return;
  try {
    await ensureGroupTables();
    const g = await dbPool.query('SELECT * FROM groups ORDER BY id ASC;');
    const mem = await dbPool.query('SELECT * FROM group_members;');
    groups = g.rows.map((r) => ({
      id: String(r.id), name: r.name, createdBy: r.created_by || '', createdAt: new Date(r.created_at).getTime(), isDm: !!r.is_dm,
      members: mem.rows.filter((x) => String(x.group_id) === String(r.id)).map((x) => x.username),
    }));
    const pins = await dbPool.query('SELECT * FROM group_pins;');
    pins.rows.forEach((r) => {
      if (!groupPins.has(r.username)) groupPins.set(r.username, new Set());
      groupPins.get(r.username).add(String(r.group_id));
    });
    const hidden = await dbPool.query('SELECT * FROM group_hidden;');
    hidden.rows.forEach((r) => {
      if (!groupHidden.has(r.username)) groupHidden.set(r.username, new Set());
      groupHidden.get(r.username).add(String(r.group_id));
    });
    const reads = await dbPool.query('SELECT * FROM group_reads;');
    reads.rows.forEach((r) => {
      if (!groupReads.has(r.username)) groupReads.set(r.username, new Map());
      groupReads.get(r.username).set(String(r.group_id), Number(r.last_read_id));
    });
    console.log(`已从数据库加载群组，共 ${groups.length} 个`);
  } catch (err) {
    console.error('[加载群组失败]', err.message);
  }
}
function findGroup(id) { return groups.find((g) => g.id === String(id)) || null; }
// 全员群已经取消：所有沟通都在建的群里（含一对一私聊，私聊也是一个只有两个人的群）
function getGroupMembers(groupId) {
  const g = findGroup(groupId);
  return g ? g.members : [];
}
function isGroupMember(groupId, username) {
  const g = findGroup(groupId);
  return !!g && g.members.includes(username);
}
// 谁能建群/改群：管理员和现场管理。改/删只能动自己在里面的群（管理员不限）；私聊不能改
function canManageGroups(role) { return role === 'admin' || role === 'manager'; }
function canEditGroup(g, client) {
  if (!g || g.isDm) return false;
  if (client.role === 'admin') return true;
  return canManageGroups(client.role) && g.members.includes(client.username);
}
function findDm(a, b) {
  return groups.find((g) => g.isDm && g.members.length === 2 && g.members.includes(a) && g.members.includes(b)) || null;
}
function isGroupHidden(username, groupId) {
  const set = groupHidden.get(username);
  return !!set && set.has(String(groupId));
}
async function setGroupHidden(username, groupId, hidden) {
  if (!groupHidden.has(username)) groupHidden.set(username, new Set());
  if (hidden) groupHidden.get(username).add(String(groupId)); else groupHidden.get(username).delete(String(groupId));
  if (dbPool) {
    try {
      if (hidden) await dbPool.query('INSERT INTO group_hidden (username, group_id) VALUES ($1,$2) ON CONFLICT DO NOTHING;', [username, String(groupId)]);
      else await dbPool.query('DELETE FROM group_hidden WHERE username=$1 AND group_id=$2;', [username, String(groupId)]);
    } catch (err) { console.error('[关闭私聊写入失败]', err.message); }
  }
}
function isGroupPinned(username, groupId) {
  const set = groupPins.get(username);
  return !!set && set.has(String(groupId));
}
function getLastRead(username, groupId) {
  const m = groupReads.get(username);
  return m ? (m.get(String(groupId)) || 0) : 0;
}
function countUnread(username, groupId) {
  const last = getLastRead(username, groupId);
  return history.filter((m) => m.type === 'message' && !m.deletedAt && (m.groupId || ALL_GROUP_ID) === groupId && m.id > last && m.username !== username).length;
}
function lastActivity(groupId) {
  for (let i = history.length - 1; i >= 0; i--) {
    const m = history[i];
    if (m.type === 'message' && (m.groupId || ALL_GROUP_ID) === groupId) return m.time;
  }
  return 0;
}
// 某个人看到的群列表：全员群永远在，其他只列她在里面的；管理员额外能看到所有群（管理用）
function groupsForUser(username, role) {
  const view = (id, name, members, isAll) => ({
    id, name, members, isAll,
    memberOfIt: isAll || members.includes(username),
    pinned: isGroupPinned(username, id),
    unread: (isAll || members.includes(username)) ? countUnread(username, id) : 0,
    lastTime: lastActivity(id),
  });
  const out = [];
  groups.forEach((g) => {
    if (g.isDm) {
      // 私聊：只有这两个人看得到，标签上显示对方的名字；自己关掉的不列
      if (!g.members.includes(username)) return;
      if (isGroupHidden(username, g.id)) return;
      const other = g.members.find((m) => m !== username) || username;
      out.push({ ...view(g.id, other, g.members, false), isDm: true });
      return;
    }
    if (role === 'admin' || g.members.includes(username)) out.push({ ...view(g.id, g.name, g.members, false), isDm: false });
  });
  return out;
}
// 某人"默认该停在哪个群"：上次的那个还在就用它，否则置顶的/最近有动静的第一个，一个群都没有就是空
function pickStartGroup(username, role, wanted) {
  if (wanted && isGroupMember(wanted, username)) return wanted;
  const mine = groupsForUser(username, role).filter((g) => g.memberOfIt)
    .sort((a, b) => (a.pinned !== b.pinned) ? (a.pinned ? -1 : 1) : (b.lastTime - a.lastTime));
  return mine.length ? mine[0].id : '';
}
function sendGroupsTo(ws, client) {
  try { ws.send(JSON.stringify({ type: 'groups', groups: groupsForUser(client.username, client.role) })); } catch (e) { /* 忽略 */ }
}
function sendGroupsToEveryone() {
  for (const [sock, c] of clients.entries()) {
    if (sock.readyState === WebSocket.OPEN) sendGroupsTo(sock, c);
  }
}
function broadcastToGroup(groupId, data, exclude) {
  const members = new Set(getGroupMembers(groupId));
  const msg = JSON.stringify(data);
  for (const [sock, c] of clients.entries()) {
    if (sock === exclude || sock.readyState !== WebSocket.OPEN) continue;
    if (members.has(c.username)) sock.send(msg);
  }
}
// 群历史 + 我上次看到哪条（前端画"以下是新消息"的横线）+ 群里每个人看到哪条（画已读回执）
function groupReadsMap(groupId) {
  const out = {};
  getGroupMembers(groupId).forEach((u) => { out[u] = getLastRead(u, groupId); });
  return out;
}
function groupHistoryPayload(groupId, username) {
  if (!groupId) return { type: 'history', groupId: '', messages: [], myLastRead: 0, reads: {} };
  const list = history.filter((m) => m.type === 'message' && (m.groupId || ALL_GROUP_ID) === groupId);
  return {
    type: 'history', groupId, messages: list.slice(-HISTORY_PER_GROUP),
    myLastRead: username ? getLastRead(username, groupId) : 0,
    reads: groupReadsMap(groupId),
  };
}
async function createGroupRecord(name, members, byUsername, isDm) {
  const g = { id: `mem-g-${Date.now()}-${Math.round(Math.random() * 1e6)}`, name, members, createdBy: byUsername, createdAt: Date.now(), isDm: !!isDm };
  if (dbPool) {
    try {
      const r = await dbPool.query('INSERT INTO groups (name, created_by, is_dm) VALUES ($1,$2,$3) RETURNING id;', [name, byUsername, !!isDm]);
      g.id = String(r.rows[0].id);
      for (const u of members) await dbPool.query('INSERT INTO group_members (group_id, username) VALUES ($1,$2) ON CONFLICT DO NOTHING;', [g.id, u]);
    } catch (err) { console.error('[建群写入失败]', err.message); }
  }
  groups.push(g);
  return g;
}
async function setGroupRead(username, groupId, lastId) {
  if (!groupReads.has(username)) groupReads.set(username, new Map());
  const prev = groupReads.get(username).get(String(groupId)) || 0;
  if (lastId <= prev) return;
  groupReads.get(username).set(String(groupId), lastId);
  if (dbPool) {
    try {
      await dbPool.query(
        'INSERT INTO group_reads (username, group_id, last_read_id) VALUES ($1,$2,$3) ON CONFLICT (username, group_id) DO UPDATE SET last_read_id = EXCLUDED.last_read_id;',
        [username, String(groupId), lastId]
      );
    } catch (err) { console.error('[已读位置写入失败]', err.message); }
  }
}
async function setGroupPin(username, groupId, pinned) {
  if (!groupPins.has(username)) groupPins.set(username, new Set());
  if (pinned) groupPins.get(username).add(String(groupId)); else groupPins.get(username).delete(String(groupId));
  if (dbPool) {
    try {
      if (pinned) await dbPool.query('INSERT INTO group_pins (username, group_id) VALUES ($1,$2) ON CONFLICT DO NOTHING;', [username, String(groupId)]);
      else await dbPool.query('DELETE FROM group_pins WHERE username=$1 AND group_id=$2;', [username, String(groupId)]);
    } catch (err) { console.error('[置顶写入失败]', err.message); }
  }
}
function sanitizeMembers(list) {
  const names = new Set(users.map((u) => u.username));
  return Array.from(new Set((Array.isArray(list) ? list : []).map((x) => String(x).trim()).filter((x) => names.has(x))));
}
async function createGroup(name, members, byUsername) {
  return createGroupRecord(name, members, byUsername, false);
}
async function updateGroup(g, name, members) {
  g.name = name;
  g.members = members;
  if (dbPool && !String(g.id).startsWith('mem-')) {
    try {
      await dbPool.query('UPDATE groups SET name=$1 WHERE id=$2;', [name, g.id]);
      await dbPool.query('DELETE FROM group_members WHERE group_id=$1;', [g.id]);
      for (const u of members) await dbPool.query('INSERT INTO group_members (group_id, username) VALUES ($1,$2) ON CONFLICT DO NOTHING;', [g.id, u]);
    } catch (err) { console.error('[改群写入失败]', err.message); }
  }
}
async function deleteGroup(g) {
  groups = groups.filter((x) => x !== g);
  if (dbPool && !String(g.id).startsWith('mem-')) {
    try {
      await dbPool.query('DELETE FROM group_members WHERE group_id=$1;', [g.id]);
      await dbPool.query('DELETE FROM groups WHERE id=$1;', [g.id]);
      await dbPool.query('DELETE FROM group_pins WHERE group_id=$1;', [String(g.id)]);
    } catch (err) { console.error('[删群写入失败]', err.message); }
  }
}

function pushHistory(entry) {
  history.push(entry);
  if (history.length > MAX_HISTORY) history.shift();
}

// 从消息文本中提取被 @ 的用户名（必须是当前在线用户，避免误伤）
function extractMentions(text) {
  const online = getOnlineUsers();
  const mentioned = new Set();

  // 特殊标记：@所有人，命中就等于@了当前所有在线用户
  const allRe = /@所有人(?=\s|[，,。.!！?？]|$)/;
  const isAll = allRe.test(text);
  if (isAll) {
    online.forEach((name) => mentioned.add(name));
  }

  online.forEach((name) => {
    // 按 @用户名 精确匹配（用户名后需跟空白/标点/结尾，避免子串误判）
    const escaped = name.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
    const re = new RegExp(`@${escaped}(?=\\s|[，,。.!！?？]|$)`);
    if (re.test(text)) mentioned.add(name);
  });
  return { mentioned: Array.from(mentioned), isAll };
}

wss.on('connection', (ws) => {
  // 心跳：网线拔了、电脑休眠这类"假死"连接，TCP层可能几分钟都不报错，
  // 服务器会一直以为这人还在线，导致他重连时被自己的旧连接挡在门外（名字被占）。
  // 每30秒ping一次，上一轮没回pong的直接断掉，名字立刻释放。
  ws.isAlive = true;
  ws.on('pong', () => { ws.isAlive = true; });

  ws.on('message', async (raw) => {
    let data;
    try {
      data = JSON.parse(raw);
    } catch (e) {
      return;
    }

    // 只读角色（质检员/代购/客服）只能聊天和提问题件；下面这些改数据的操作一律挡掉，
    // 不管她知不知道编辑密码。错误按各模块自己的错误类型回，前端现成的提示位置能直接显示
    const EDITOR_ONLY_TYPES = {
      case_update: 'case_error', case_add: 'case_error', case_delete: 'case_error', case_reveal_order: 'case_error',
      special_req_update: 'special_req_error', special_req_add: 'special_req_error', special_req_delete: 'special_req_error',
      inspection_rule_update: 'inspection_rule_error', inspection_rule_delete_history: 'inspection_rule_error',
      problem_item_result: 'problem_item_error', problem_item_stamp: 'problem_item_error',
      problem_item_transfer: 'problem_item_error', problem_item_resolve: 'problem_item_error',
      problem_item_options_update: 'problem_item_options_error',
      reminder_update: 'reminder_error', reminder_add: 'reminder_error', reminder_delete: 'reminder_error',
      timeclock_verify_password: 'timeclock_error', timeclock_update_times: 'timeclock_error', timeclock_delete: 'timeclock_error',
      timeclock_name_delete: 'timeclock_error', timeclock_name_add: 'timeclock_error', work_items_update: 'timeclock_error',
      shift_save: 'shift_error', shift_delete: 'shift_error', shift_import: 'shift_error', shift_verify_password: 'shift_error',
      staff_manager_remove: 'shift_error', staff_manager_add: 'shift_error',
      mall_import: 'mall_error', mall_arrive: 'mall_error', mall_arrival_delete: 'mall_error', mall_item_delete: 'mall_error', mall_item_cancel: 'mall_error', mall_item_note: 'mall_error',
    };
    if (EDITOR_ONLY_TYPES[data.type]) {
      const c = clients.get(ws);
      if (!c) return;
      if (!isEditRole(c.role)) {
        ws.send(JSON.stringify({ type: EDITOR_ONLY_TYPES[data.type], message: '你的账号没有这个权限（只读）', category: data.category }));
        return;
      }
    }

    if (data.type === 'join') {
      // 只认登录令牌：名字和角色都从令牌里取，前端说自己叫什么不算数
      const user = verifyToken(data.token);
      if (!user) {
        ws.send(JSON.stringify({ type: 'join_error', code: 'auth', message: '登录已失效，请重新登录' }));
        return;
      }
      const username = user.username;

      // 名字唯一，但改成"后来者把先来的挤掉"：
      // 换台电脑/换个浏览器登录时，旧的那边可能是已经离开的会话（或者忘了关的窗口），
      // 拦住新登录反而更麻烦。所以这里把同名的旧连接踢下线，让新的进来。
      // 大小写不敏感、忽略首尾空格，"张三"和"张三 "算同一个人。
      Array.from(clients.entries()).forEach(([sock, c]) => {
        if (sock === ws) return;
        if (c.username.trim().toLowerCase() !== username.toLowerCase()) return;
        try {
          sock.send(JSON.stringify({
            type: 'kicked',
            message: `你的账号“${username}”在别处登录了，这个窗口已经下线`,
          }));
        } catch (e) { /* 已经断了就算了 */ }
        clients.delete(sock);
        setTimeout(() => { try { sock.close(); } catch (e) { /* 忽略 */ } }, 300);
      });

      clients.set(ws, { username, role: user.role });
      ws.send(JSON.stringify({ type: 'me', user: publicUser(user), roles: ROLES }));

      // 发送历史消息 + 当前在线列表给新用户
      // 先发群列表，再发她上次停留的那个群的历史（不在那个群里了就退回全员群）
      const startGroup = pickStartGroup(username, user.role, String(data.groupId || ''));
      sendGroupsTo(ws, { username, role: user.role });
      ws.send(JSON.stringify(groupHistoryPayload(startGroup, username)));
      ws.send(JSON.stringify({ type: 'online', users: getOnlineUsers(), directory: getDirectory() }));
      ws.send(JSON.stringify({ type: 'case_library', cases: caseLibraryForClients() }));
      ws.send(JSON.stringify({ type: 'special_req_list', items: specialRequirements }));
      ws.send(JSON.stringify(mallSnapshot()));
      ws.send(JSON.stringify({ type: 'reminder_list', reminders }));
      ws.send(JSON.stringify({ type: 'inspection_rules_all', rules: getAllInspectionRulesText() }));
      ws.send(JSON.stringify({ type: 'problem_item_data', ...getProblemItemSnapshot() }));
      ws.send(JSON.stringify(timeclockPayload(getJSTParts(new Date()).dateStr)));
      ws.send(JSON.stringify(shiftPayload()));


      // 不再广播"XX加入了聊天室"这类系统提示——人多的时候刷屏，把正常聊天内容顶上去，
      // 谁在线直接看左侧在线列表就够了
      broadcast({ type: 'online', users: getOnlineUsers(), directory: getDirectory() });
      return;
    }

    if (data.type === 'message') {
      const client = clients.get(ws);
      if (!client) return;
      const text = String(data.text || '').slice(0, 2000);

      // 图片：只接受我们自己 /upload 接口生成的路径，避免被塞入任意外部地址；
      // 最多9张一起发，避免被刷屏/滥用
      const MAX_IMAGES_PER_MESSAGE = 9;
      let images = [];
      if (Array.isArray(data.images)) {
        images = data.images
          .filter((url) => isOwnUploadUrl(url))
          .slice(0, MAX_IMAGES_PER_MESSAGE);
      }

      // 通用文件：同样只认自己 /upload-file 接口生成的路径；每条消息最多5个文件
      const MAX_FILES_PER_MESSAGE = 5;
      let files = [];
      if (Array.isArray(data.files)) {
        files = data.files
          .filter((f) =>
            f && typeof f === 'object' &&
            isOwnUploadUrl(f.url) &&
            typeof f.name === 'string' &&
            typeof f.size === 'number'
          )
          .slice(0, MAX_FILES_PER_MESSAGE)
          .map((f) => ({ url: f.url, name: f.name.slice(0, 150), size: f.size }));
      }

      // 纯文字消息不能是空的；但如果带了图片/文件，文字可以为空（附件本身就是内容）
      if (!text.trim() && images.length === 0 && files.length === 0) return;

      // 引用回复：只保留必要的快照信息（用户名+文本片段），不做原消息查找，
      // 这样即使原消息已经滚出历史记录，引用内容依然完整可显示。
      let quote = null;
      if (data.quote && typeof data.quote === 'object') {
        const quoteUsername = String(data.quote.username || '').slice(0, 20);
        const quoteText = String(data.quote.text || '').slice(0, 300);
        if (quoteUsername && quoteText) {
          quote = { username: quoteUsername, text: quoteText };
        }
      }

      // 发到哪个群：不是这个群的成员就当没发
      const groupId = String(data.groupId || '');
      if (!isGroupMember(groupId, client.username)) return;
      const { mentioned, isAll } = extractMentions(text);
      const members = getGroupMembers(groupId);
      const msg = {
        type: 'message',
        id: nextMessageId++,
        username: client.username,
        text,
        images,
        files,
        // @ 只对群里的人有效，群外的人看不到这条消息，@ 了也没意义
        mentions: mentioned.filter((u) => members.includes(u)),
        mentionsAll: isAll,
        quote,
        reactions: {},
        pending: null, // 待处理标记：null=没标记，{by, at}=有人标了还没处理完
        time: Date.now(),
        groupId,
      };
      pushHistory(msg);
      saveChatMessageToDB(msg);
      // 私聊被对方关掉了的话，来新消息时自动给她重新打开（标签带红点出现）
      const gObj = findGroup(groupId);
      if (gObj && gObj.isDm) {
        for (const m of gObj.members) {
          if (m !== client.username && isGroupHidden(m, groupId)) {
            await setGroupHidden(m, groupId, false);
            for (const [sock, c] of clients.entries()) {
              if (c.username === m && sock.readyState === WebSocket.OPEN) sendGroupsTo(sock, c);
            }
          }
        }
      }
      broadcastToGroup(groupId, msg); // 包括发送者自己（用于统一渲染顺序）
      scheduleMentionReminder(msg);
      return;
    }

    if (data.type === 'reaction') {
      const client = clients.get(ws);
      if (!client) return;
      const messageId = data.messageId;
      const emoji = String(data.emoji || '').slice(0, 8);
      // 只允许这两种表情，避免被塞入任意内容
      if (!ALLOWED_REACTIONS.includes(emoji)) return;
      if (typeof messageId !== 'number') return;
      const msg = history.find((m) => m.type === 'message' && m.id === messageId);
      // 找不到说明这条消息已经被挤出历史记录了（超过 MAX_HISTORY 条），忽略即可
      if (!msg) return;
      if (!msg.reactions || typeof msg.reactions !== 'object') msg.reactions = {};
      if (!Array.isArray(msg.reactions[emoji])) msg.reactions[emoji] = [];
      const list = msg.reactions[emoji];
      const idx = list.indexOf(client.username);
      if (idx === -1) {
        list.push(client.username);
      } else {
        list.splice(idx, 1);
      }
      broadcastToGroup(msg.groupId || ALL_GROUP_ID, { type: 'reaction_update', messageId, emoji, users: list, groupId: msg.groupId || ALL_GROUP_ID });
      updateMessageReactionsInDB(messageId, msg.reactions);
      return;
    }

    if (data.type === 'toggle_pending') {
      const client = clients.get(ws);
      if (!client) return;
      const messageId = data.messageId;
      if (typeof messageId !== 'number') return;
      const msg = history.find((m) => m.type === 'message' && m.id === messageId);
      // 找不到说明这条消息已经被挤出历史记录了（超过 MAX_HISTORY 条），忽略即可
      if (!msg) return;
      // 待处理是个开关：谁都能标、谁都能取消，不需要密码——这是团队协作用的，
      // 跟置顶公告那种"内容管理"性质不一样，越轻量越好用
      msg.pending = msg.pending ? null : { by: client.username, at: Date.now() };
      broadcastToGroup(msg.groupId || ALL_GROUP_ID, { type: 'pending_update', messageId, pending: msg.pending, text: msg.text, username: msg.username, groupId: msg.groupId || ALL_GROUP_ID });
      updateMessagePendingInDB(messageId, msg.pending);
      return;
    }

    if (data.type === 'message_edit') {
      const client = clients.get(ws);
      if (!client) return;
      const messageId = data.messageId;
      if (typeof messageId !== 'number') return;
      const msg = history.find((m) => m.type === 'message' && m.id === messageId);
      if (!msg) return;
      // 关键校验：只能编辑自己发的消息，不能信任客户端隐藏了按钮就够了——
      // 服务端必须自己再查一遍发送人是不是当前这个连接的用户，防止有人绕过前端直接发WS消息改别人的内容
      if (msg.username !== client.username) {
        ws.send(JSON.stringify({ type: 'message_edit_error', messageId, message: '只能编辑自己发的消息' }));
        return;
      }
      if (msg.deletedAt) return; // 已经删除的消息不能编辑
      const newText = String(data.text || '').slice(0, 5000);
      if (!newText.trim()) return; // 编辑成空内容没有意义，直接忽略（要删除请用删除功能）
      const editedAt = Date.now();
      msg.text = newText;
      msg.editedAt = editedAt;
      broadcastToGroup(msg.groupId || ALL_GROUP_ID, { type: 'message_edited', messageId, text: newText, editedAt, groupId: msg.groupId || ALL_GROUP_ID });
      updateMessageTextInDB(messageId, newText, editedAt);
      return;
    }

    if (data.type === 'message_delete') {
      const client = clients.get(ws);
      if (!client) return;
      const messageId = data.messageId;
      if (typeof messageId !== 'number') return;
      const msg = history.find((m) => m.type === 'message' && m.id === messageId);
      if (!msg) return;
      // 同样的关键校验：只能删除自己发的消息
      if (msg.username !== client.username) {
        ws.send(JSON.stringify({ type: 'message_edit_error', messageId, message: '只能删除自己发的消息' }));
        return;
      }
      if (msg.deletedAt) return; // 已经删过了，不用重复处理
      const deletedAt = Date.now();
      msg.text = null;
      msg.images = [];
      msg.files = [];
      msg.deletedAt = deletedAt;
      broadcastToGroup(msg.groupId || ALL_GROUP_ID, { type: 'message_deleted', messageId, deletedAt, groupId: msg.groupId || ALL_GROUP_ID });
      deleteMessageInDB(messageId, deletedAt);
      return;
    }

    // ---- 案例库：看不用密码，增删改都要编辑密码（跟原来公告栏同一个） ----
    if (data.type === 'case_add' || data.type === 'case_update') {
      const client = clients.get(ws);
      if (!client) return;
      if (String(data.password || '') !== PIN_EDIT_PASSWORD) {
        ws.send(JSON.stringify({ type: 'case_error', message: '密码错误，无法保存案例' }));
        return;
      }
      const input = sanitizeCaseInput(data);
      if (!input.platform) {
        ws.send(JSON.stringify({ type: 'case_error', message: '请选择平台类别' }));
        return;
      }
      if (input.platform === '代购' && !input.site) {
        ws.send(JSON.stringify({ type: 'case_error', message: '代购的案例请写明是哪个网站' }));
        return;
      }
      if (!input.problem) {
        ws.send(JSON.stringify({ type: 'case_error', message: '请填写问题' }));
        return;
      }
      if (data.type === 'case_add') {
        await addCase(input, client.username);
      } else {
        const updated = await updateCase(data.id, input, client.username);
        if (!updated) {
          ws.send(JSON.stringify({ type: 'case_error', message: '没找到这条案例，可能已经被删了' }));
          return;
        }
      }
      broadcast({ type: 'case_library', cases: caseLibraryForClients() });
      return;
    }

    // 没公开的单号：凭编辑密码单独查，只回给问的这个人，不广播
    if (data.type === 'case_reveal_order') {
      const client = clients.get(ws);
      if (!client) return;
      if (String(data.password || '') !== PIN_EDIT_PASSWORD) {
        ws.send(JSON.stringify({ type: 'case_error', message: '密码错误，无法查看单号' }));
        return;
      }
      const target = caseLibrary.find((c) => String(c.id) === String(data.id));
      if (!target) {
        ws.send(JSON.stringify({ type: 'case_error', message: '没找到这条案例' }));
        return;
      }
      ws.send(JSON.stringify({ type: 'case_order_revealed', id: target.id, orderId: target.orderId || '' }));
      return;
    }

    // ---- 特殊要求：看不用密码，增删改都要编辑密码 ----
    if (data.type === 'special_req_add' || data.type === 'special_req_update') {
      const client = clients.get(ws);
      if (!client) return;
      if (String(data.password || '') !== PIN_EDIT_PASSWORD) {
        ws.send(JSON.stringify({ type: 'special_req_error', message: '密码错误，无法保存' }));
        return;
      }
      const input = sanitizeSpecialReqInput(data);
      if (!input.code) { ws.send(JSON.stringify({ type: 'special_req_error', message: '请填写入库码' })); return; }
      if (!input.username) { ws.send(JSON.stringify({ type: 'special_req_error', message: '请填写用户名' })); return; }
      if (!input.requirement) { ws.send(JSON.stringify({ type: 'special_req_error', message: '请填写要求' })); return; }
      let saved;
      if (data.type === 'special_req_add') {
        saved = await addSpecialReq(input, client.username);
      } else {
        saved = await updateSpecialReq(data.id, input, client.username);
        if (!saved) {
          ws.send(JSON.stringify({ type: 'special_req_error', message: '没找到这条记录，可能已经被删了' }));
          return;
        }
      }
      broadcast({ type: 'special_req_list', items: specialRequirements, savedId: saved.id });
      return;
    }

    if (data.type === 'special_req_delete') {
      const client = clients.get(ws);
      if (!client) return;
      if (String(data.password || '') !== PIN_EDIT_PASSWORD) {
        ws.send(JSON.stringify({ type: 'special_req_error', message: '密码错误，无法删除' }));
        return;
      }
      const ok = await deleteSpecialReq(data.id);
      if (ok) broadcast({ type: 'special_req_list', items: specialRequirements });
      return;
    }

    // ---- 商城到货统计 ----
    if (data.type === 'mall_import') {
      const client = clients.get(ws);
      if (!client) return;
      const supplier = MALL_SUPPLIERS.find((x) => x.key === data.supplier);
      if (!supplier) { ws.send(JSON.stringify({ type: 'mall_error', message: '请先选一个供应商' })); return; }
      const rows = Array.isArray(data.rows) ? data.rows.slice(0, 2000) : [];
      const added = await mallImport(supplier.key, rows, client.username);
      broadcastMall();
      ws.send(JSON.stringify({ type: 'mall_imported', count: added.length, skipped: rows.length - added.length }));
      return;
    }
    if (data.type === 'mall_arrive') {
      const client = clients.get(ws);
      if (!client) return;
      const qty = Math.max(1, Math.min(9999, Math.round(Number(data.qty) || 1)));
      let item = data.itemId ? mallItemById(data.itemId) : null;
      if (!item && data.jan) {
        // 扫码：在这家供应商还没到齐的商品里找这个 JAN，多条的话取最早导入的
        const jan = String(data.jan).replace(/\D/g, '');
        item = mallItems.find((x) => x.jan === jan && x.status === 'pending' && (!data.supplier || x.supplier === data.supplier)) || null;
        if (!item) {
          const anyOne = mallItems.find((x) => x.jan === jan && (!data.supplier || x.supplier === data.supplier));
          ws.send(JSON.stringify({ type: 'mall_error', message: anyOne ? `JAN ${jan}（${anyOne.nameCn || anyOne.nameJp}）已经到齐了` : `没找到 JAN ${jan} 的订货记录` }));
          return;
        }
      }
      if (!item) { ws.send(JSON.stringify({ type: 'mall_error', message: '没找到这条商品' })); return; }
      if (item.status === 'cancelled') { ws.send(JSON.stringify({ type: 'mall_error', message: '这条已经标成订不到了' })); return; }
      const rec = await mallAddArrival(item, qty, client.username, false);
      broadcastMall();
      ws.send(JSON.stringify({ type: 'mall_arrived', item, arrival: rec }));
      return;
    }
    if (data.type === 'mall_arrival_delete') {
      const client = clients.get(ws);
      if (!client) return;
      const rec = mallArrivals.find((x) => String(x.id) === String(data.id));
      if (!rec) { ws.send(JSON.stringify({ type: 'mall_error', message: '没找到这条到货记录' })); return; }
      await mallDeleteArrival(rec);
      broadcastMall();
      return;
    }
    if (data.type === 'mall_item_note') {
      const client = clients.get(ws);
      if (!client) return;
      const item = mallItemById(data.id);
      if (!item) { ws.send(JSON.stringify({ type: 'mall_error', message: '没找到这条商品' })); return; }
      item.note = String(data.note || '').trim().slice(0, 200);
      await mallSaveItem(item);
      broadcastMall();
      return;
    }
    if (data.type === 'mall_item_delete' || data.type === 'mall_item_cancel') {
      const client = clients.get(ws);
      if (!client) return;
      const item = mallItemById(data.id);
      if (!item) { ws.send(JSON.stringify({ type: 'mall_error', message: '没找到这条商品' })); return; }
      if (data.type === 'mall_item_delete') {
        await mallDeleteItem(item);
      } else {
        item.status = item.status === 'cancelled' ? (item.qtyArrived >= item.qty ? 'arrived' : 'pending') : 'cancelled';
        await mallSaveItem(item);
      }
      broadcastMall();
      return;
    }

    if (data.type === 'case_delete') {
      const client = clients.get(ws);
      if (!client) return;
      if (String(data.password || '') !== PIN_EDIT_PASSWORD) {
        ws.send(JSON.stringify({ type: 'case_error', message: '密码错误，无法删除案例' }));
        return;
      }
      const ok = await deleteCase(data.id);
      if (ok) broadcast({ type: 'case_library', cases: caseLibraryForClients() });
      return;
    }

    if (data.type === 'inspection_rule_update') {
      const client = clients.get(ws);
      if (!client) return;
      const category = String(data.category || '');
      if (!INSPECTION_RULE_CATEGORIES.includes(category)) return;
      const providedPassword = String(data.password || '');
      if (providedPassword !== PIN_EDIT_PASSWORD) {
        ws.send(JSON.stringify({ type: 'inspection_rule_error', category, message: '密码错误，无法修改检品规则' }));
        return;
      }
      const newText = String(data.text || '').slice(0, INSPECTION_RULE_MAX_LENGTH);
      inspectionRules[category].text = newText;
      await recordInspectionRuleChange(category, newText, client.username);
      broadcast({
        type: 'inspection_rule_update',
        category,
        text: newText,
        by: client.username,
        history: inspectionRules[category].history,
      });
      return;
    }

    if (data.type === 'inspection_rule_delete_history') {
      const client = clients.get(ws);
      if (!client) return;
      const category = String(data.category || '');
      if (!INSPECTION_RULE_CATEGORIES.includes(category)) return;
      const providedPassword = String(data.password || '');
      if (providedPassword !== PIN_EDIT_PASSWORD) {
        ws.send(JSON.stringify({ type: 'inspection_rule_error', category, message: '密码错误，无法删除记录' }));
        return;
      }
      const rule = inspectionRules[category];
      const targetId = data.id;
      const idx = rule.history.findIndex((entry) => String(entry.id) === String(targetId));
      if (idx === -1) return;
      if (rule.history[idx].endTime === null) {
        ws.send(JSON.stringify({ type: 'inspection_rule_error', category, message: '不能删除当前生效中的这条记录，请先编辑成新内容后再删' }));
        return;
      }
      rule.history.splice(idx, 1);
      await deleteInspectionRuleHistoryEntry(targetId);
      broadcast({ type: 'inspection_rule_update', category, text: rule.text, history: rule.history });
      return;
    }

    if (data.type === 'problem_item_submit') {
      const client = clients.get(ws);
      if (!client) return;
      const category = String(data.category || '');
      if (!PROBLEM_ITEM_CATEGORIES.includes(category)) return;
      const issueTypes = Array.isArray(data.issueTypes) ? data.issueTypes.filter((v) => typeof v === 'string').slice(0, 20) : [];
      const inspectorNames = Array.isArray(data.inspectorNames) ? data.inspectorNames.filter((v) => typeof v === 'string').slice(0, 20) : [];
      const orderNote = String(data.orderNote || '').slice(0, 500);

      // 订单ID / 快递单号：必填，而且只能是数字。选了"找不到…"这类问题类型时前端会切成快递单号，
      // 这里只按前端传过来的 idKind 记录是哪一种，校验规则两者一样
      // order=订单ID（代拍）/ rs=RS单号（代购、煤炉）/ tracking=快递单号（选了"找不到…"时）
      const idKind = ['tracking', 'rs', 'order'].includes(data.idKind) ? data.idKind : 'order';
      const orderId = String(data.orderId || '').trim().slice(0, 40);
      // RS单号里可能带字母（比如 RS12345678），所以放宽成"字母+数字"；
      // 订单ID和快递单号仍然只能是纯数字
      const idOk = idKind === 'rs' ? /^[A-Za-z0-9]+$/.test(orderId) : /^\d+$/.test(orderId);
      if (!idOk) {
        ws.send(JSON.stringify({
          type: 'problem_item_error',
          message: idKind === 'tracking' ? '请填写快递单号（只能填数字）'
            : idKind === 'rs' ? '请填写RS单号（只能填字母和数字）'
            : '请填写订单ID（只能填数字）',
        }));
        return;
      }

      // 图片：只接受我们自己 /upload 接口生成的路径，最多3张
      const images = Array.isArray(data.images)
        ? data.images.filter((u) => isOwnUploadUrl(u)).slice(0, 3)
        : [];

      // 脏污/破损/多货这几类是需要照片佐证的，但不强制在提交时就传——
      // 检品台的电脑不一定有摄像头，硬卡着会逼人改用手机提交，反而对不上提交人。
      // 改成：先让单子进来，列表里标红"待补照片"，之后用手机点"补传照片"补上。

      // 提交是日常操作，不需要密码——密码只用来保护"编辑下拉选项列表"这种管理性操作
      const report = await addProblemItemReport(category, issueTypes, inspectorNames, orderNote, client.username, orderId, idKind, images);
      broadcast({ type: 'problem_item_report_added', category, report });
      return;
    }

    // 补传照片（针对已经在列表里的记录）
    if (data.type === 'problem_item_add_images') {
      const client = clients.get(ws);
      if (!client) return;
      const category = String(data.category || '');
      if (!PROBLEM_ITEM_CATEGORIES.includes(category)) return;
      const images = Array.isArray(data.images)
        ? data.images.filter((u) => isOwnUploadUrl(u)).slice(0, 3)
        : [];
      if (images.length === 0) {
        ws.send(JSON.stringify({ type: 'problem_item_error', message: '没有可补传的照片' }));
        return;
      }
      const report = await appendProblemItemImages(category, data.reportId, images);
      if (!report) {
        ws.send(JSON.stringify({ type: 'problem_item_error', message: '没找到这条问题件，可能已经被处理掉了' }));
        return;
      }
      broadcast({ type: 'problem_item_report_updated', category, report });
      return;
    }

    // 队列（日志商家/任务）里的处理结果：已入库 / 已补（换）发入库 / 已取消。
    // 点完记录就从队列里消失，结果本身写进数据库，导出时能看到是哪一种
    if (data.type === 'problem_item_result') {
      const client = clients.get(ws);
      if (!client) return;
      const category = String(data.category || '');
      if (!PROBLEM_ITEM_CATEGORIES.includes(category)) return;
      const result = PROBLEM_ITEM_RESULTS[String(data.result || '')];
      if (!result) return;
      const ok = await updateProblemItemReportStatus(category, data.reportId, result.status, client.username);
      if (ok) {
        broadcast({ type: 'problem_item_report_removed', category, reportId: data.reportId });
        if (ok !== true) broadcast({ type: 'problem_item_finished_added', record: ok });
      }
      return;
    }

    // 跟进图章：点亮/取消
    if (data.type === 'problem_item_stamp') {
      const client = clients.get(ws);
      if (!client) return;
      const category = String(data.category || '');
      if (!PROBLEM_ITEM_CATEGORIES.includes(category)) return;
      const stamp = String(data.stamp || '');
      if (!PROBLEM_ITEM_STAMPS.includes(stamp)) return;
      const report = await toggleProblemItemFollowStamp(category, data.reportId, stamp, client.username);
      if (report) broadcast({ type: 'problem_item_report_updated', category, report });
      return;
    }

    if (data.type === 'problem_item_resolve' || data.type === 'problem_item_transfer') {
      const client = clients.get(ws);
      if (!client) return;
      const category = String(data.category || '');
      if (!PROBLEM_ITEM_CATEGORIES.includes(category)) return;
      let status;
      if (data.type === 'problem_item_resolve') status = 'resolved';
      // "转处理"拆成了两种去向：转给日志商家、转成任务。老的 'transferred' 保留，只用来读历史数据
      else if (data.type === 'problem_item_transfer') {
        status = data.target === 'task' ? 'transferred_task'
          : data.target === 'merchant' ? 'transferred_merchant'
          : 'transferred';
      }
      else return;
      const ok = await updateProblemItemReportStatus(category, data.reportId, status, client.username);
      if (ok) {
        if (status === 'transferred_merchant' || status === 'transferred_task') {
          // 转出去的记录没有消失，只是从原分类挪到了"日志商家/任务"队列里，
          // 所以广播状态变更（带上完整记录），让各端把它从原列表移走、加进对应队列
          const moved = (problemItemReports[category] || []).find((r) => String(r.id) === String(data.reportId));
          broadcast({ type: 'problem_item_report_status_changed', category, reportId: data.reportId, status, report: moved });
        } else {
          broadcast({ type: 'problem_item_report_removed', category, reportId: data.reportId });
          if (ok !== true) broadcast({ type: 'problem_item_finished_added', record: ok });
        }
      }
      return;
    }

    if (data.type === 'problem_item_options_update') {
      const client = clients.get(ws);
      if (!client) return;
      const optionType = String(data.optionType || '');
      const providedPassword = String(data.password || '');
      if (providedPassword !== PIN_EDIT_PASSWORD) {
        ws.send(JSON.stringify({ type: 'problem_item_options_error', message: '密码错误，无法修改选项列表' }));
        return;
      }
      const values = Array.isArray(data.values)
        ? data.values.filter((v) => typeof v === 'string' && v.trim()).map((v) => v.trim()).slice(0, 100)
        : [];
      await updateProblemItemOptions(optionType, values);
      broadcast({ type: 'problem_item_options_updated', optionType, values: problemItemOptions[optionType].map((o) => o.value) });
      return;
    }

    if (data.type === 'reminder_add' || data.type === 'reminder_update') {
      const client = clients.get(ws);
      if (!client) return;
      const providedPassword = String(data.password || '');
      if (providedPassword !== PIN_EDIT_PASSWORD) {
        ws.send(JSON.stringify({ type: 'reminder_error', message: '密码错误，无法保存提醒' }));
        return;
      }
      const hour = Number(data.hour);
      const minute = Number(data.minute);
      if (!Number.isInteger(hour) || hour < 0 || hour > 23 || !Number.isInteger(minute) || minute < 0 || minute > 59) {
        ws.send(JSON.stringify({ type: 'reminder_error', message: '时间格式不对' }));
        return;
      }
      // weekdays: 前端传数组(0-6)表示只在这几天提醒；不传/传空数组/传满7天 都当作"每天"处理
      const weekdays = Array.isArray(data.weekdays) &&
        data.weekdays.length > 0 && data.weekdays.length < 7 &&
        data.weekdays.every((d) => Number.isInteger(d) && d >= 0 && d <= 6)
        ? data.weekdays
        : null;
      const text = String(data.text || '').slice(0, 200).trim();
      if (!text) {
        ws.send(JSON.stringify({ type: 'reminder_error', message: '提醒内容不能为空' }));
        return;
      }

      const reminderGroup = findGroup(data.groupId) ? String(data.groupId) : '';
      if (!reminderGroup) {
        ws.send(JSON.stringify({ type: 'reminder_error', message: '请选一个要发到的群' }));
        return;
      }
      if (data.type === 'reminder_add') {
        await addReminder(hour, minute, weekdays, text, reminderGroup);
      } else {
        const ok = await updateReminder(data.id, hour, minute, weekdays, text, reminderGroup);
        if (!ok) {
          ws.send(JSON.stringify({ type: 'reminder_error', message: '没找到这条提醒，可能已经被删除了' }));
          return;
        }
      }
      broadcast({ type: 'reminder_list', reminders });
      return;
    }

    if (data.type === 'reminder_delete') {
      const client = clients.get(ws);
      if (!client) return;
      const providedPassword = String(data.password || '');
      if (providedPassword !== PIN_EDIT_PASSWORD) {
        ws.send(JSON.stringify({ type: 'reminder_error', message: '密码错误，无法删除提醒' }));
        return;
      }
      const ok = await deleteReminder(data.id);
      if (!ok) return;
      broadcast({ type: 'reminder_list', reminders });
      return;
    }

    // ===== 时间管理：签出(开始) / 签入(结束) =====
    // 默认提交人是自己的登录名，直接点按钮就行；
    // 如果要帮别人打卡（data.username 跟自己的登录名不一样），必须带上跟公告栏同一个编辑密码，
    // 这个校验放在服务器做——前端那层"解锁"只是界面上的方便，光改前端绕不过去。
    if (data.type === 'timeclock_punch') {
      const client = clients.get(ws);
      if (!client) return;
      const action = data.action === 'in' ? 'in' : data.action === 'out' ? 'out' : '';
      if (!action) return;

      const target = String(data.username || '').slice(0, 20).trim() || client.username;
      if (target !== client.username && !isEditRole(client.role)) {
        ws.send(JSON.stringify({ type: 'timeclock_error', message: '你的账号没有代别人提交的权限' }));
        return;
      }
      if (target !== client.username && String(data.password || '') !== PIN_EDIT_PASSWORD) {
        ws.send(JSON.stringify({ type: 'timeclock_error', message: '代别人提交需要输入正确的编辑密码' }));
        return;
      }

      const open = findOpenTimeRecord(target);
      const who = target === client.username ? '你' : target;

      if (action === 'out') {
        if (open) {
          ws.send(JSON.stringify({ type: 'timeclock_error', message: `${who}已经签出了，请先签入再重新签出` }));
          return;
        }
        // 签出必须勾选工作内容，不然记录没法回溯这段时间在干什么
        const selectedWorkItems = Array.isArray(data.workItems)
          ? data.workItems.filter((v) => typeof v === 'string' && workItems.includes(v)).slice(0, 20)
          : [];
        if (selectedWorkItems.length === 0) {
          ws.send(JSON.stringify({ type: 'timeclock_error', message: '请勾选工作内容' }));
          return;
        }
        const record = await startTimeRecord(target, selectedWorkItems);
        broadcast(timeclockPayload(record.workDate));
        return;
      }

      if (!open) {
        ws.send(JSON.stringify({ type: 'timeclock_error', message: `${who}还没有签出，先点"签出"开始计时` }));
        return;
      }
      await finishTimeRecord(open);
      broadcast(timeclockPayload(open.workDate));
      return;
    }

    // 查某一天的记录（切换日期/点刷新时用）
    if (data.type === 'timeclock_query') {
      const client = clients.get(ws);
      if (!client) return;
      const date = String(data.date || '');
      if (!/^\d{4}-\d{2}-\d{2}$/.test(date)) return;
      ws.send(JSON.stringify(timeclockPayload(date)));
      return;
    }

    // 前端"解锁代他人提交"时先校验一次密码，校验过了界面才把下拉框和名单管理放开
    if (data.type === 'timeclock_verify_password') {
      const client = clients.get(ws);
      if (!client) return;
      if (String(data.password || '') !== PIN_EDIT_PASSWORD) {
        ws.send(JSON.stringify({ type: 'timeclock_error', message: '密码错误' }));
        return;
      }
      ws.send(JSON.stringify({ type: 'timeclock_password_ok' }));
      return;
    }

    // 改某一条打卡记录的时间点（打卡忘了、点早了点晚了时用），密码同公告栏
    if (data.type === 'timeclock_update_times') {
      const client = clients.get(ws);
      if (!client) return;
      if (String(data.password || '') !== PIN_EDIT_PASSWORD) {
        ws.send(JSON.stringify({ type: 'timeclock_error', message: '密码错误，无法修改时间' }));
        return;
      }
      const record = timeRecords.find((r) => String(r.id) === String(data.id));
      if (!record) {
        ws.send(JSON.stringify({ type: 'timeclock_error', message: '没找到这条记录，可能已经被删掉了' }));
        return;
      }
      const startAt = Number(data.startAt);
      const endAt = data.endAt === null || data.endAt === undefined ? null : Number(data.endAt);
      if (!Number.isFinite(startAt) || (endAt !== null && !Number.isFinite(endAt))) {
        ws.send(JSON.stringify({ type: 'timeclock_error', message: '时间格式不对' }));
        return;
      }
      if (endAt !== null && endAt <= startAt) {
        ws.send(JSON.stringify({ type: 'timeclock_error', message: '签入时间要晚于签出时间' }));
        return;
      }
      record.startAt = startAt;
      record.endAt = endAt;
      record.durationMs = endAt === null ? null : Math.max(0, endAt - startAt);
      if (dbPool) {
        try {
          await dbPool.query(
            'UPDATE time_records SET start_at=$1, end_at=$2, duration_ms=$3 WHERE id=$4;',
            [record.startAt, record.endAt, record.durationMs, record.id]
          );
        } catch (err) {
          console.error('[修改打卡时间写入数据库失败]', err.message);
        }
      }
      broadcast(timeclockPayload(record.workDate));
      return;
    }

    // 删除某一条打卡记录：密码跟公告栏的编辑密码一致
    if (data.type === 'timeclock_delete') {
      const client = clients.get(ws);
      if (!client) return;
      if (String(data.password || '') !== PIN_EDIT_PASSWORD) {
        ws.send(JSON.stringify({ type: 'timeclock_error', message: '密码错误，无法删除记录' }));
        return;
      }
      const removed = await deleteTimeRecord(data.id);
      if (!removed) {
        ws.send(JSON.stringify({ type: 'timeclock_error', message: '没找到这条记录，可能已经被别人删掉了' }));
        return;
      }
      broadcast(timeclockPayload(removed.workDate));
      return;
    }

    // 名单增删：同样要密码，跟"代他人提交"是同一道门槛
    if (data.type === 'timeclock_name_add' || data.type === 'timeclock_name_delete') {
      const client = clients.get(ws);
      if (!client) return;
      if (String(data.password || '') !== PIN_EDIT_PASSWORD) {
        ws.send(JSON.stringify({ type: 'timeclock_error', message: '密码错误，无法修改名单' }));
        return;
      }
      const name = String(data.name || '').slice(0, 20).trim();
      if (!name) {
        ws.send(JSON.stringify({ type: 'timeclock_error', message: '名字不能为空' }));
        return;
      }
      if (data.type === 'timeclock_name_add') {
        const added = await addTimeclockName(name);
        if (!added) {
          ws.send(JSON.stringify({ type: 'timeclock_error', message: `"${name}"已经在名单里了` }));
          return;
        }
      } else {
        await deleteTimeclockName(name);
      }
      broadcast(timeclockPayload(getJSTParts(new Date()).dateStr));
      return;
    }

    // ===== 人员管理（班表）=====
    if (data.type === 'shift_query') {
      const client = clients.get(ws);
      if (!client) return;
      ws.send(JSON.stringify(shiftPayload()));
      return;
    }

    if (data.type === 'shift_save') {
      const client = clients.get(ws);
      if (!client) return;
      if (String(data.password || '') !== PIN_EDIT_PASSWORD) {
        ws.send(JSON.stringify({ type: 'shift_error', message: '密码错误，无法修改班表' }));
        return;
      }
      const workDate = String(data.workDate || '');
      const personName = String(data.personName || '').trim().slice(0, 20);
      const startMin = Number(data.startMin);
      const endMin = Number(data.endMin);
      if (!/^\d{4}-\d{2}-\d{2}$/.test(workDate) || !personName) {
        ws.send(JSON.stringify({ type: 'shift_error', message: '姓名和日期不能为空' }));
        return;
      }
      if (!Number.isInteger(startMin) || !Number.isInteger(endMin) ||
          startMin < 0 || endMin > 24 * 60 || endMin <= startMin) {
        ws.send(JSON.stringify({ type: 'shift_error', message: '时间不对，下班时间要晚于上班时间' }));
        return;
      }
      if (data.id) {
        const updated = await updateShiftEntry(data.id, personName, startMin, endMin);
        if (!updated) {
          ws.send(JSON.stringify({ type: 'shift_error', message: '没找到这条排班，可能已经被别人删了' }));
          return;
        }
      } else {
        await addShiftEntry(workDate, personName, startMin, endMin);
      }
      broadcast(shiftPayload());
      return;
    }

    if (data.type === 'shift_delete') {
      const client = clients.get(ws);
      if (!client) return;
      if (String(data.password || '') !== PIN_EDIT_PASSWORD) {
        ws.send(JSON.stringify({ type: 'shift_error', message: '密码错误，无法删除排班' }));
        return;
      }
      const ok = await deleteShiftEntry(data.id);
      if (!ok) return;
      broadcast(shiftPayload());
      return;
    }

    // 导入：前端把Excel/CSV解析成一行行 { workDate, personName, startMin, endMin } 再发过来，
    // 服务器只做校验和落库，不在服务器上解析表格文件
    if (data.type === 'shift_import') {
      const client = clients.get(ws);
      if (!client) return;
      if (String(data.password || '') !== PIN_EDIT_PASSWORD) {
        ws.send(JSON.stringify({ type: 'shift_error', message: '密码错误，无法导入班表' }));
        return;
      }
      const rows = Array.isArray(data.rows) ? data.rows : [];
      const cleaned = rows
        .map((r) => ({
          workDate: String(r.workDate || ''),
          personName: String(r.personName || '').trim().slice(0, 20),
          startMin: Number(r.startMin),
          endMin: Number(r.endMin),
        }))
        .filter((r) =>
          /^\d{4}-\d{2}-\d{2}$/.test(r.workDate) && r.personName &&
          Number.isInteger(r.startMin) && Number.isInteger(r.endMin) &&
          r.startMin >= 0 && r.endMin <= 24 * 60 && r.endMin > r.startMin)
        .slice(0, 2000);
      if (cleaned.length === 0) {
        ws.send(JSON.stringify({ type: 'shift_error', message: '没解析出有效的排班行，检查一下表格格式' }));
        return;
      }
      const dates = await importShiftEntries(cleaned);
      broadcast(shiftPayload());
      ws.send(JSON.stringify({ type: 'shift_import_ok', count: cleaned.length, dates }));
      return;
    }

    // 工作内容选项的增删改：密码同公告栏
    if (data.type === 'work_items_update') {
      const client = clients.get(ws);
      if (!client) return;
      if (String(data.password || '') !== PIN_EDIT_PASSWORD) {
        ws.send(JSON.stringify({ type: 'timeclock_error', message: '密码错误，无法修改工作内容' }));
        return;
      }
      const list = Array.isArray(data.items)
        ? Array.from(new Set(data.items
            .map((v) => String(v || '').trim().slice(0, 20))
            .filter(Boolean)))
          .slice(0, 40)
        : [];
      if (list.length === 0) {
        ws.send(JSON.stringify({ type: 'timeclock_error', message: '至少要保留一项工作内容' }));
        return;
      }
      await replaceWorkItems(list);
      broadcast(timeclockPayload(getJSTParts(new Date()).dateStr));
      ws.send(JSON.stringify({ type: 'work_items_ok' }));
      return;
    }

    // 人员管理的编辑权限：跟公告栏用同一个密码，前端验一次记在内存里，之后每个操作都带着发过来
    if (data.type === 'shift_verify_password') {
      const client = clients.get(ws);
      if (!client) return;
      if (String(data.password || '') !== PIN_EDIT_PASSWORD) {
        ws.send(JSON.stringify({ type: 'shift_error', message: '密码错误' }));
        return;
      }
      ws.send(JSON.stringify({ type: 'shift_password_ok' }));
      return;
    }

    // 现场管理人员名单：勾上 = 加入，取消勾选 = 移出（都要编辑密码）
    if (data.type === 'staff_manager_add' || data.type === 'staff_manager_remove') {
      const client = clients.get(ws);
      if (!client) return;
      if (String(data.password || '') !== PIN_EDIT_PASSWORD) {
        ws.send(JSON.stringify({ type: 'shift_error', message: '密码错误，无法修改现场管理人员' }));
        return;
      }
      const name = String(data.name || '').trim().slice(0, 20);
      if (!name) {
        ws.send(JSON.stringify({ type: 'shift_error', message: '名字不能为空' }));
        return;
      }
      if (data.type === 'staff_manager_add') {
        await addStaffManager(name);
      } else {
        await removeStaffManager(name);
      }
      // 现场管理人员一变，问题件面板上的"本周负责人"跟着变——它就是这份名单的镜像，
      // shiftPayload 里已经带了 managers，前端收到就会一起刷新，不用另外再广播一次
      broadcast(shiftPayload());
      return;
    }

    // ---- 群组 ----
    if (data.type === 'group_switch') {
      const client = clients.get(ws);
      if (!client) return;
      const gid = String(data.groupId || '');
      if (!isGroupMember(gid, client.username)) {
        ws.send(JSON.stringify({ type: 'group_error', message: '你不在这个群里' }));
        ws.send(JSON.stringify(groupHistoryPayload(pickStartGroup(client.username, client.role, ''), client.username)));
        return;
      }
      ws.send(JSON.stringify(groupHistoryPayload(gid, client.username)));
      return;
    }
    if (data.type === 'group_read') {
      const client = clients.get(ws);
      if (!client) return;
      const gid = String(data.groupId || '');
      if (!gid) return;
      const lastId = Number(data.lastId);
      if (Number.isFinite(lastId) && lastId > 0 && lastId > getLastRead(client.username, gid) && isGroupMember(gid, client.username)) {
        await setGroupRead(client.username, gid, lastId);
        broadcastToGroup(gid, { type: 'group_read_update', groupId: gid, username: client.username, lastId }, ws);
      }
      return;
    }
    if (data.type === 'group_pin') {
      const client = clients.get(ws);
      if (!client) return;
      const gid = String(data.groupId || '');
      if (!findGroup(gid)) return;
      await setGroupPin(client.username, gid, !!data.pinned);
      sendGroupsTo(ws, client);
      return;
    }
    // 一对一私聊：点达人广场里的人 -> 找到已有的私聊群，没有就建一个（两个人都会收到新的群列表）
    if (data.type === 'group_dm_open') {
      const client = clients.get(ws);
      if (!client) return;
      const other = String(data.username || '').trim();
      const target = findUserByName(other);
      if (!target || target.username === client.username) {
        ws.send(JSON.stringify({ type: 'group_error', message: '没找到这个人' }));
        return;
      }
      let dm = findDm(client.username, target.username);
      if (dm && isGroupHidden(client.username, dm.id)) await setGroupHidden(client.username, dm.id, false);
      if (!dm) dm = await createGroupRecord(`${client.username}·${target.username}`, [client.username, target.username], client.username, true);
      for (const [sock, c] of clients.entries()) {
        if (sock.readyState === WebSocket.OPEN && (c.username === client.username || c.username === target.username)) sendGroupsTo(sock, c);
      }
      ws.send(JSON.stringify({ type: 'group_saved', id: dm.id, switchTo: true }));
      return;
    }

    // 关掉一个私聊标签：只对自己隐藏，记录都在；对方再发消息会自动回来
    if (data.type === 'group_dm_close') {
      const client = clients.get(ws);
      if (!client) return;
      const g = findGroup(data.groupId);
      if (!g || !g.isDm || !g.members.includes(client.username)) return;
      await setGroupHidden(client.username, g.id, true);
      sendGroupsTo(ws, client);
      ws.send(JSON.stringify(groupHistoryPayload(pickStartGroup(client.username, client.role, ''), client.username)));
      return;
    }

    if (data.type === 'group_create' || data.type === 'group_update' || data.type === 'group_delete') {
      const client = clients.get(ws);
      if (!client) return;
      if (!canManageGroups(client.role)) {
        ws.send(JSON.stringify({ type: 'group_error', message: '只有管理员和仓库现场能建群、改群' }));
        return;
      }
      if (data.type === 'group_delete') {
        const g = findGroup(data.id);
        if (!g) { ws.send(JSON.stringify({ type: 'group_error', message: '没找到这个群' })); return; }
        if (!canEditGroup(g, client)) { ws.send(JSON.stringify({ type: 'group_error', message: '只能删自己在里面的群' })); return; }
        await deleteGroup(g);
        sendGroupsToEveryone();
        ws.send(JSON.stringify({ type: 'group_saved', id: null }));
        return;
      }
      const name = String(data.name || '').trim().slice(0, 30);
      if (!name) { ws.send(JSON.stringify({ type: 'group_error', message: '请填群名' })); return; }
      const members = sanitizeMembers(data.members);
      if (members.length === 0) { ws.send(JSON.stringify({ type: 'group_error', message: '至少拉一个人进群' })); return; }
      let saved;
      if (data.type === 'group_create') {
        if (client.role !== 'admin' && !members.includes(client.username)) members.push(client.username);
        saved = await createGroup(name, members, client.username);
      } else {
        saved = findGroup(data.id);
        if (!saved) { ws.send(JSON.stringify({ type: 'group_error', message: '没找到这个群' })); return; }
        if (!canEditGroup(saved, client)) { ws.send(JSON.stringify({ type: 'group_error', message: '只能改自己在里面的群' })); return; }
        await updateGroup(saved, name, members);
      }
      sendGroupsToEveryone();
      ws.send(JSON.stringify({ type: 'group_saved', id: saved.id }));
      return;
    }

    if (data.type === 'typing') {
      const client = clients.get(ws);
      if (!client) return;
      const typingGroup = String(data.groupId || '');
      if (isGroupMember(typingGroup, client.username)) broadcastToGroup(typingGroup, { type: 'typing', username: client.username, groupId: typingGroup }, ws);
      return;
    }
  });

  ws.on('close', () => {
    const client = clients.get(ws);
    if (client) {
      clients.delete(ws);
      // 同样不再广播"XX离开了聊天室"
      broadcast({ type: 'online', users: getOnlineUsers(), directory: getDirectory() });
    }
  });
});

// ==================== 定时提醒（按日本时间，可在聊天室里自己编辑，不用改代码） ====================
// 用 Intl.DateTimeFormat 指定 timeZone: 'Asia/Tokyo' 来读取"日本时间"的时分/星期，
// 这样不管 Render 服务器自己配置的是什么时区，读出来的都是准确的日本时间，不用自己算时差。
// 日本不实行夏令时，所以这里也不用额外处理夏令时切换的问题。
const DEFAULT_REMINDERS = [
  { hour: 11, minute: 30, weekdays: null, text: '请关注12点前能否完全前一天的煤炉检品。' },
  { hour: 17, minute: 0, weekdays: [1, 2, 3, 4, 5, 6], text: '帮忙收下各类垃圾' },
];
const WEEKDAY_MAP = { Sun: 0, Mon: 1, Tue: 2, Wed: 3, Thu: 4, Fri: 5, Sat: 6 };
const reminderLastFiredDate = {}; // reminderId -> 'YYYY-MM-DD'（日本时间），防止同一天重复提醒
let reminders = []; // { id, hour, minute, weekdays(数组或null=每天), text }

function getJSTParts(date) {
  const fmt = new Intl.DateTimeFormat('en-US', {
    timeZone: 'Asia/Tokyo',
    year: 'numeric', month: '2-digit', day: '2-digit',
    hour: '2-digit', minute: '2-digit', hour12: false,
    weekday: 'short',
  });
  const map = {};
  fmt.formatToParts(date).forEach((p) => { if (p.type !== 'literal') map[p.type] = p.value; });
  // 极少数情况下 hour12:false 在午夜会给出"24"而不是"00"，这里做个兜底
  const hour = map.hour === '24' ? 0 : Number(map.hour);
  return {
    dateStr: `${map.year}-${map.month}-${map.day}`,
    hour,
    minute: Number(map.minute),
    weekdayNum: WEEKDAY_MAP[map.weekday],
  };
}

function rowToReminder(row) {
  return {
    id: row.id,
    hour: row.hour,
    minute: row.minute,
    weekdays: row.weekdays ? row.weekdays.split(',').map(Number) : null,
    text: row.text,
    groupId: row.group_id || 'all',
  };
}

async function ensureRemindersTable() {
  if (!dbPool) return;
  await dbPool.query(`
    CREATE TABLE IF NOT EXISTS reminders (
      id BIGSERIAL PRIMARY KEY,
      hour INT NOT NULL,
      minute INT NOT NULL,
      weekdays TEXT,
      text TEXT NOT NULL,
      created_at TIMESTAMPTZ NOT NULL DEFAULT now()
    );
  `);
  await dbPool.query(`ALTER TABLE reminders ADD COLUMN IF NOT EXISTS group_id TEXT NOT NULL DEFAULT 'all';`);
}

async function loadRemindersFromDB() {
  if (!dbPool) {
    // 没配数据库：退化成内存模式，用默认的两条兜底，保证功能仍然可用（重启会恢复成默认值）
    reminders = DEFAULT_REMINDERS.map((r, i) => ({ id: `mem-default-${i}`, ...r }));
    return;
  }
  try {
    await ensureRemindersTable();
    const { rows } = await dbPool.query('SELECT * FROM reminders ORDER BY id ASC;');
    if (rows.length === 0) {
      for (const d of DEFAULT_REMINDERS) {
        await dbPool.query(
          'INSERT INTO reminders (hour, minute, weekdays, text) VALUES ($1,$2,$3,$4);',
          [d.hour, d.minute, d.weekdays ? d.weekdays.join(',') : null, d.text]
        );
      }
      const reloaded = await dbPool.query('SELECT * FROM reminders ORDER BY id ASC;');
      reminders = reloaded.rows.map(rowToReminder);
    } else {
      reminders = rows.map(rowToReminder);
    }
    console.log(`已从数据库加载 ${reminders.length} 条定时提醒`);
  } catch (err) {
    console.error('[加载定时提醒失败，暂时改用内存默认值]', err.message);
    reminders = DEFAULT_REMINDERS.map((r, i) => ({ id: `mem-default-${i}`, ...r }));
  }
}

async function addReminder(hour, minute, weekdays, text, groupId) {
  const newReminder = { id: `mem-${Date.now()}`, hour, minute, weekdays, text, groupId: groupId || 'all' };
  reminders.push(newReminder);
  if (!dbPool) return newReminder;
  try {
    const inserted = await dbPool.query(
      'INSERT INTO reminders (hour, minute, weekdays, text, group_id) VALUES ($1,$2,$3,$4,$5) RETURNING id;',
      [hour, minute, weekdays ? weekdays.join(',') : null, text, newReminder.groupId]
    );
    newReminder.id = inserted.rows[0].id;
  } catch (err) {
    console.error('[新增定时提醒写入数据库失败]', err.message);
  }
  return newReminder;
}

async function updateReminder(id, hour, minute, weekdays, text, groupId) {
  const idx = reminders.findIndex((r) => String(r.id) === String(id));
  if (idx === -1) return false;
  reminders[idx] = { ...reminders[idx], hour, minute, weekdays, text, groupId: groupId || 'all' };
  if (!dbPool) return true;
  try {
    await dbPool.query(
      'UPDATE reminders SET hour=$1, minute=$2, weekdays=$3, text=$4, group_id=$6 WHERE id=$5;',
      [hour, minute, weekdays ? weekdays.join(',') : null, text, id, groupId || 'all']
    );
  } catch (err) {
    console.error('[更新定时提醒写入数据库失败]', err.message);
  }
  return true;
}

async function deleteReminder(id) {
  const idx = reminders.findIndex((r) => String(r.id) === String(id));
  if (idx === -1) return false;
  reminders.splice(idx, 1);
  if (!dbPool) return true;
  try {
    await dbPool.query('DELETE FROM reminders WHERE id=$1;', [id]);
  } catch (err) {
    console.error('[删除定时提醒失败]', err.message);
  }
  return true;
}

// 提醒触发时，用跟"@所有人"完全一样的方式广播——让所有在线的人都弹全屏提示框+收到系统通知，
// 不是安安静静发一条系统消息就完事，避免被刷屏的聊天记录淹没错过
function fireReminderBroadcast(text, groupId) {
  if (!groupId || !findGroup(groupId)) { console.log(`[定时提醒] 群不存在，跳过: ${text}`); return; }
  const gid = String(groupId);
  const members = getGroupMembers(gid);
  const onlineUsernames = getOnlineUsers().filter((u) => members.includes(u));
  const msg = {
    type: 'message',
    id: nextMessageId++,
    username: '⏰ 定时提醒',
    text,
    images: [],
    files: [],
    mentions: onlineUsernames,
    mentionsAll: true,
    quote: null,
    reactions: {},
    pending: null,
    time: Date.now(),
    groupId: gid,
  };
  pushHistory(msg);
  saveChatMessageToDB(msg);
  broadcastToGroup(gid, msg);
}

function checkReminders() {
  const { dateStr, hour, minute, weekdayNum } = getJSTParts(new Date());
  reminders.forEach((r) => {
    if (r.hour !== hour || r.minute !== minute) return;
    if (r.weekdays && !r.weekdays.includes(weekdayNum)) return;
    const key = String(r.id);
    if (reminderLastFiredDate[key] === dateStr) return; // 今天已经发过了，不重复发
    reminderLastFiredDate[key] = dateStr;
    fireReminderBroadcast(r.text, r.groupId);
    console.log(`[定时提醒] 已发送: ${r.text}`);
  });
}

// ==================== 晚班上班提醒（结合班表自动发）====================
// 规则：当天班表里，上班时间落在 9:30~15:00 之间的（也就是不是一早就来的那些人），
// 在他上班前10分钟自动发一条提醒，@ 上指定的接收人：
//   "张三将10点上班，请注意工作内容安排。"
// 接收人在人员管理表格里点名字旁边的铃铛指定；一个都没指定时，发给所有在线的人。
const SHIFT_ALERT_MIN_START = 9 * 60 + 30;  // 9:30
const SHIFT_ALERT_MAX_START = 15 * 60;      // 15:00
const SHIFT_ALERT_LEAD_MINUTES = 10;
const shiftAlertFired = {}; // `日期-排班id-上班分钟` -> true，防止重复发；把上班时间也放进key里，改了时间会重新提醒

function formatShiftStartText(startMin) {
  const h = Math.floor(startMin / 60);
  const m = startMin % 60;
  return m === 0 ? `${h}点` : `${h}点${String(m).padStart(2, '0')}分`;
}

function fireShiftAlert(entry) {
  sendPrivateToManagers(
    `${entry.personName}将${formatShiftStartText(entry.startMin)}上班，请注意工作内容安排。`,
    '班表提醒'
  );
}

// 每天早上8:55 的汇总提醒：今天几点开工、都有谁，让管理人员上班前先有个数。
// 跟上面那条"晚来的人提前10分钟提醒"是两码事，互不影响。
const MORNING_SUMMARY_MIN = 8 * 60 + 55;   // 8:55
const MORNING_SUMMARY_DEADLINE = 9 * 60;   // 9:00（过了这个点就不补发了）
const MORNING_START_MIN = 9 * 60;          // "9点上班"这一批的界线
const morningSummaryFired = {};            // 日期 -> true，一天只发一次

// 私信给所有在线的现场管理人员，顺便触发他们那边的提示音和电脑右下角通知
function sendPrivateToManagers(text, logLabel) {
  const targets = [];
  clients.forEach((client, sock) => {
    if (staffManagers.includes(client.username) && sock.readyState === WebSocket.OPEN) {
      targets.push({ sock, username: client.username });
    }
  });
  if (targets.length === 0) {
    console.log(`[${logLabel}] ${text} —— 没有在线的现场管理人员，本次没发出去`);
    return;
  }
  targets.forEach(({ sock, username }) => {
    sock.send(JSON.stringify({
      type: 'message',
      id: nextMessageId++,
      username: '📋 班表提醒',
      text: `@${username} ${text}`,
      images: [],
      files: [],
      mentions: [username],
      mentionsAll: false,
      quote: null,
      reactions: {},
      pending: null,
      private: true,
      time: Date.now(),
    }));
  });
  console.log(`[${logLabel}] ${text} —— 已私信 ${targets.map((t) => t.username).join('、')}`);
}

function checkMorningSummary() {
  const { dateStr, hour, minute } = getJSTParts(new Date());
  const nowMin = hour * 60 + minute;
  if (nowMin < MORNING_SUMMARY_MIN || nowMin >= MORNING_SUMMARY_DEADLINE) return;
  if (morningSummaryFired[dateStr]) return;

  // 9点（含9点之前）开工的这一批人
  const early = shiftEntries
    .filter((e) => e.workDate === dateStr && e.startMin <= MORNING_START_MIN)
    .sort((a, b) => (staffNameRank(a.personName) - staffNameRank(b.personName))
      || a.personName.localeCompare(b.personName, 'zh'));

  morningSummaryFired[dateStr] = true;
  if (early.length === 0) {
    console.log(`[早班汇总] ${dateStr} 今天没有9点上班的人，不发提醒`);
    return;
  }

  const onNine = early.filter((e) => e.startMin === MORNING_START_MIN);
  const earlier = early.filter((e) => e.startMin < MORNING_START_MIN);
  const names = (onNine.length ? onNine : early).map((e) => e.personName).join('、');
  let text = `${names} 9点开始上班，共${early.length}人。`;
  // 有比9点还早的（比如8点半到），单独补一句，免得漏掉
  if (onNine.length && earlier.length) {
    text += `另有 ${earlier.map((e) => `${e.personName}（${formatShiftStartText(e.startMin)}）`).join('、')} 更早上班。`;
  }
  sendPrivateToManagers(text, '早班汇总');
}

setInterval(checkMorningSummary, 30 * 1000);

function checkShiftAlerts() {
  const { dateStr, hour, minute } = getJSTParts(new Date());
  const nowMin = hour * 60 + minute;
  shiftEntries.forEach((entry) => {
    if (entry.workDate !== dateStr) return;
    if (entry.startMin < SHIFT_ALERT_MIN_START || entry.startMin > SHIFT_ALERT_MAX_START) return;
    const alertMin = entry.startMin - SHIFT_ALERT_LEAD_MINUTES;
    // 用"到点了但还没到上班时间"这个区间判断，而不是死等某一分钟——
    // 服务器重启、卡顿几分钟都不会把提醒整个漏掉
    if (nowMin < alertMin || nowMin >= entry.startMin) return;
    const key = `${dateStr}-${entry.id}-${entry.startMin}`;
    if (shiftAlertFired[key]) return;
    shiftAlertFired[key] = true;
    fireShiftAlert(entry);
  });
}

setInterval(checkShiftAlerts, 30 * 1000);

setInterval(() => {
  wss.clients.forEach((sock) => {
    if (sock.isAlive === false) {
      sock.terminate(); // 会触发 close，clients 里的记录和在线列表由那边统一清理
      return;
    }
    sock.isAlive = false;
    try { sock.ping(); } catch (e) { /* 已经关掉的连接，忽略 */ }
  });
}, 30 * 1000);

// 每30秒检查一次，足够精确命中每分钟的提醒时间点，又不会太频繁
setInterval(checkReminders, 30 * 1000);

// ==================== 时间管理（签出/签入打卡，按日本时间归档，可按天导出） ====================
// 规则跟前端按钮一一对应：
//   "签出" = 开始计时，记下点击那一刻的时间戳（毫秒），生成一条"进行中"的记录；
//   "签入" = 结束计时，把结束时间写进同一条记录，并自动算出时长（签入时间 - 签出时间）。
// 提交人直接取聊天室的登录昵称（clients里存的username），不用另外填。
// 归档日期用"签出那一刻的日本时间"，这样跨零点的记录也只会算在开始那天，按天导出不会串到第二天。
const MAX_TIME_RECORDS_IN_MEMORY = 5000;
let timeRecords = []; // { id, username, workDate, startAt, endAt, durationMs }

function rowToTimeRecord(row) {
  return {
    id: row.id,
    username: row.username,
    workDate: row.work_date,
    startAt: Number(row.start_at),
    endAt: row.end_at === null || row.end_at === undefined ? null : Number(row.end_at),
    durationMs: row.duration_ms === null || row.duration_ms === undefined ? null : Number(row.duration_ms),
    workItems: Array.isArray(row.work_items) ? row.work_items : [],
  };
}

async function ensureTimeRecordsTable() {
  if (!dbPool) return;
  await dbPool.query(`
    CREATE TABLE IF NOT EXISTS time_records (
      id BIGSERIAL PRIMARY KEY,
      username TEXT NOT NULL,
      work_date TEXT NOT NULL,
      start_at BIGINT NOT NULL,
      end_at BIGINT,
      duration_ms BIGINT,
      created_at TIMESTAMPTZ NOT NULL DEFAULT now()
    );
  `);
  await dbPool.query('CREATE INDEX IF NOT EXISTS idx_time_records_work_date ON time_records (work_date);');
  // 后加的一列：这次签出勾选的工作内容
  await dbPool.query(`ALTER TABLE time_records ADD COLUMN IF NOT EXISTS work_items JSONB;`);
}

async function loadTimeRecordsFromDB() {
  if (!dbPool) {
    // 没配数据库就退化成纯内存（重启清空），跟公告栏/提醒事项的处理方式保持一致
    timeRecords = [];
    return;
  }
  try {
    await ensureTimeRecordsTable();
    const { rows } = await dbPool.query(
      'SELECT * FROM time_records ORDER BY start_at DESC LIMIT $1;',
      [MAX_TIME_RECORDS_IN_MEMORY]
    );
    timeRecords = rows.map(rowToTimeRecord).reverse();
    console.log(`已从数据库加载 ${timeRecords.length} 条时间管理记录`);
  } catch (err) {
    console.error('[加载时间管理记录失败，暂时改用内存模式]', err.message);
    timeRecords = [];
  }
}

// 提交人名单：默认提交人就是自己的登录名，但有时候需要帮没在电脑前的人代打卡，
// 所以额外维护一份共享名单，代他人提交时从下拉里选。名单所有人共用，加了大家都能看到。
let timeclockNames = [];

async function ensureTimeclockNamesTable() {
  if (!dbPool) return;
  await dbPool.query(`
    CREATE TABLE IF NOT EXISTS timeclock_names (
      id BIGSERIAL PRIMARY KEY,
      name TEXT NOT NULL UNIQUE,
      created_at TIMESTAMPTZ NOT NULL DEFAULT now()
    );
  `);
}

async function loadTimeclockNamesFromDB() {
  if (!dbPool) {
    timeclockNames = [];
    return;
  }
  try {
    await ensureTimeclockNamesTable();
    const { rows } = await dbPool.query('SELECT name FROM timeclock_names ORDER BY id ASC;');
    timeclockNames = rows.map((r) => r.name);
    console.log(`已从数据库加载 ${timeclockNames.length} 个时间管理提交人名字`);
  } catch (err) {
    console.error('[加载时间管理名单失败，暂时改用内存模式]', err.message);
    timeclockNames = [];
  }
}

async function addTimeclockName(name) {
  if (timeclockNames.includes(name)) return false;
  timeclockNames.push(name);
  if (dbPool) {
    try {
      await dbPool.query('INSERT INTO timeclock_names (name) VALUES ($1) ON CONFLICT (name) DO NOTHING;', [name]);
    } catch (err) {
      console.error('[新增提交人名字写入数据库失败]', err.message);
    }
  }
  return true;
}

async function deleteTimeclockName(name) {
  const idx = timeclockNames.indexOf(name);
  if (idx === -1) return false;
  timeclockNames.splice(idx, 1);
  if (dbPool) {
    try {
      await dbPool.query('DELETE FROM timeclock_names WHERE name=$1;', [name]);
    } catch (err) {
      console.error('[删除提交人名字失败]', err.message);
    }
  }
  return true;
}

// ==================== 工作内容（签出时必须勾选，选项可在面板里改，密码同公告栏）====================
const DEFAULT_WORK_ITEMS = ['代购检品', '代拍检品', '煤炉检品', '问题件处理', '入库', '出库', '其他'];
let workItems = [];

async function ensureWorkItemsTable() {
  if (!dbPool) return;
  await dbPool.query(`
    CREATE TABLE IF NOT EXISTS work_items (
      id BIGSERIAL PRIMARY KEY,
      value TEXT NOT NULL,
      sort_order INT NOT NULL DEFAULT 0,
      created_at TIMESTAMPTZ NOT NULL DEFAULT now()
    );
  `);
}

async function loadWorkItemsFromDB() {
  if (!dbPool) {
    workItems = DEFAULT_WORK_ITEMS.slice();
    return;
  }
  try {
    await ensureWorkItemsTable();
    const { rows } = await dbPool.query('SELECT value FROM work_items ORDER BY sort_order ASC, id ASC;');
    if (rows.length === 0) {
      for (let i = 0; i < DEFAULT_WORK_ITEMS.length; i++) {
        await dbPool.query('INSERT INTO work_items (value, sort_order) VALUES ($1,$2);', [DEFAULT_WORK_ITEMS[i], i]);
      }
      workItems = DEFAULT_WORK_ITEMS.slice();
    } else {
      workItems = rows.map((r) => r.value);
    }
  } catch (err) {
    console.error('[加载工作内容选项失败，改用默认值]', err.message);
    workItems = DEFAULT_WORK_ITEMS.slice();
  }
}

async function replaceWorkItems(list) {
  workItems = list;
  if (!dbPool) return;
  try {
    await dbPool.query('DELETE FROM work_items;');
    for (let i = 0; i < list.length; i++) {
      await dbPool.query('INSERT INTO work_items (value, sort_order) VALUES ($1,$2);', [list[i], i]);
    }
  } catch (err) {
    console.error('[保存工作内容选项失败]', err.message);
  }
}

// ==================== 人员管理（班表：谁哪天几点到几点上班） ====================
// 只做未来三天（今天/明天/后天）的排班展示，历史班表不在这里翻，所以数据量很小，
// 直接全量放内存 + 落库，改动后广播给所有人，跟公告栏那套一模一样。
// 时间统一用"从0点开始的分钟数"存（比如 9:00 = 540），前端画柱状图和算工时都直接用数字，
// 不用反复解析字符串，也不受时区影响（班表是本地作息，跟日本时间的日期口径一致）。
let shiftEntries = []; // { id, workDate, personName, startMin, endMin }

function rowToShiftEntry(row) {
  return {
    id: row.id,
    workDate: row.work_date,
    personName: row.person_name,
    startMin: Number(row.start_min),
    endMin: Number(row.end_min),
  };
}

async function ensureShiftsTable() {
  if (!dbPool) return;
  await dbPool.query(`
    CREATE TABLE IF NOT EXISTS staff_shifts (
      id BIGSERIAL PRIMARY KEY,
      work_date TEXT NOT NULL,
      person_name TEXT NOT NULL,
      start_min INT NOT NULL,
      end_min INT NOT NULL,
      created_at TIMESTAMPTZ NOT NULL DEFAULT now()
    );
  `);
  await dbPool.query('CREATE INDEX IF NOT EXISTS idx_staff_shifts_date ON staff_shifts (work_date);');
}

async function loadShiftsFromDB() {
  if (!dbPool) {
    shiftEntries = [];
    return;
  }
  try {
    await ensureShiftsTable();
    // 只把"今天往后"的班表读进内存，过期的留在库里当历史，不占内存
    const { dateStr } = getJSTParts(new Date());
    const { rows } = await dbPool.query(
      'SELECT * FROM staff_shifts WHERE work_date >= $1 ORDER BY work_date ASC, start_min ASC;',
      [dateStr]
    );
    shiftEntries = rows.map(rowToShiftEntry);
    console.log(`已从数据库加载 ${shiftEntries.length} 条班表记录`);
  } catch (err) {
    console.error('[加载班表失败，暂时改用内存模式]', err.message);
    shiftEntries = [];
  }
}

// 现场管理人员名单：人员管理面板最上面那个多选下拉里勾出来的人。
// 被勾中 = 现场管理人员 = 收上班提醒（按聊天登录名匹配），同时也是问题件"本周负责人"的候选人。
// 名字是手输/勾选的，所以管理者这种不在班表里的人也能进来；没勾的人不会显示在面板上。
// 第一次启动（名单表还是空的）时用这几位当默认提醒对象，之后在面板里改，改完以这里为准
const DEFAULT_STAFF_MANAGERS = ['李乔', '孙韶蔚', '余丽', '王晓雨', '钟海燕'];
let staffManagers = [];

async function ensureStaffMembersTable() {
  if (!dbPool) return;
  await dbPool.query(`
    CREATE TABLE IF NOT EXISTS staff_members (
      id BIGSERIAL PRIMARY KEY,
      name TEXT NOT NULL UNIQUE,
      alert_on BOOLEAN NOT NULL DEFAULT false,
      created_at TIMESTAMPTZ NOT NULL DEFAULT now()
    );
  `);
}

async function loadStaffManagersFromDB() {
  if (!dbPool) {
    staffManagers = DEFAULT_STAFF_MANAGERS.slice();
    return;
  }
  try {
    await ensureStaffMembersTable();
    const { rows } = await dbPool.query('SELECT name FROM staff_members ORDER BY id ASC;');
    if (rows.length === 0) {
      // 表还是空的：把默认几位写进去，省得上线后还要手动一个个勾
      for (const name of DEFAULT_STAFF_MANAGERS) {
        await dbPool.query('INSERT INTO staff_members (name, alert_on) VALUES ($1, true) ON CONFLICT (name) DO NOTHING;', [name]);
      }
      const reloaded = await dbPool.query('SELECT name FROM staff_members ORDER BY id ASC;');
      staffManagers = reloaded.rows.map((r) => r.name);
      console.log(`已写入 ${staffManagers.length} 位默认现场管理人员`);
    } else {
      staffManagers = rows.map((r) => r.name);
    }
  } catch (err) {
    console.error('[加载现场管理人员名单失败]', err.message);
    staffManagers = DEFAULT_STAFF_MANAGERS.slice();
  }
}

async function addStaffManager(name) {
  if (staffManagers.includes(name)) return false;
  staffManagers.push(name);
  if (dbPool) {
    try {
      await dbPool.query('INSERT INTO staff_members (name, alert_on) VALUES ($1, true) ON CONFLICT (name) DO NOTHING;', [name]);
    } catch (err) {
      console.error('[新增现场管理人员失败]', err.message);
    }
  }
  return true;
}

async function removeStaffManager(name) {
  const idx = staffManagers.indexOf(name);
  if (idx === -1) return false;
  staffManagers.splice(idx, 1);
  if (dbPool) {
    try {
      await dbPool.query('DELETE FROM staff_members WHERE name=$1;', [name]);
    } catch (err) {
      console.error('[移除现场管理人员失败]', err.message);
    }
  }
  return true;
}

// 从今天（日本时间）开始的连续三天
function getShiftWindowDates() {
  const { dateStr } = getJSTParts(new Date());
  const base = new Date(`${dateStr}T00:00:00Z`);
  return [0, 1, 2].map((offset) => {
    const d = new Date(base.getTime() + offset * 86400000);
    return d.toISOString().slice(0, 10);
  });
}

function getShiftsForWindow() {
  const dates = getShiftWindowDates();
  return {
    dates,
    shifts: shiftEntries
      .filter((e) => dates.includes(e.workDate))
      .sort((a, b) => (a.workDate === b.workDate ? a.startMin - b.startMin : a.workDate < b.workDate ? -1 : 1)),
  };
}

function shiftPayload() {
  const { dates, shifts } = getShiftsForWindow();
  return { type: 'shift_update', dates, shifts, managers: staffManagers };
}

async function addShiftEntry(workDate, personName, startMin, endMin) {
  const entry = { id: `mem-${Date.now()}-${Math.round(Math.random() * 1e6)}`, workDate, personName, startMin, endMin };
  shiftEntries.push(entry);
  if (dbPool) {
    try {
      const inserted = await dbPool.query(
        'INSERT INTO staff_shifts (work_date, person_name, start_min, end_min) VALUES ($1,$2,$3,$4) RETURNING id;',
        [workDate, personName, startMin, endMin]
      );
      entry.id = inserted.rows[0].id;
    } catch (err) {
      console.error('[新增班表写入数据库失败]', err.message);
    }
  }
  return entry;
}

async function updateShiftEntry(id, personName, startMin, endMin) {
  const idx = shiftEntries.findIndex((e) => String(e.id) === String(id));
  if (idx === -1) return null;
  shiftEntries[idx] = { ...shiftEntries[idx], personName, startMin, endMin };
  if (dbPool) {
    try {
      await dbPool.query(
        'UPDATE staff_shifts SET person_name=$1, start_min=$2, end_min=$3 WHERE id=$4;',
        [personName, startMin, endMin, id]
      );
    } catch (err) {
      console.error('[修改班表写入数据库失败]', err.message);
    }
  }
  return shiftEntries[idx];
}

async function deleteShiftEntry(id) {
  const idx = shiftEntries.findIndex((e) => String(e.id) === String(id));
  if (idx === -1) return false;
  shiftEntries.splice(idx, 1);
  if (dbPool) {
    try {
      await dbPool.query('DELETE FROM staff_shifts WHERE id=$1;', [id]);
    } catch (err) {
      console.error('[删除班表失败]', err.message);
    }
  }
  return true;
}

// 导入：按"日期"整天覆盖——导入文件里出现了哪几天，就把那几天原有的排班先清掉再写新的，
// 没出现在文件里的日期一律不动，避免一次导入把别的日子也冲掉
async function importShiftEntries(rows) {
  const dates = Array.from(new Set(rows.map((r) => r.workDate)));
  shiftEntries = shiftEntries.filter((e) => !dates.includes(e.workDate));
  if (dbPool) {
    try {
      await dbPool.query('DELETE FROM staff_shifts WHERE work_date = ANY($1::text[]);', [dates]);
    } catch (err) {
      console.error('[导入班表时清理旧数据失败]', err.message);
    }
  }
  for (const r of rows) {
    await addShiftEntry(r.workDate, r.personName, r.startMin, r.endMin);
  }
  return dates;
}

// 删掉一条打卡记录（打错卡、重复打卡时用），跟公告栏/提醒事项一样要编辑密码
async function deleteTimeRecord(id) {
  const idx = timeRecords.findIndex((r) => String(r.id) === String(id));
  if (idx === -1) return null;
  const [removed] = timeRecords.splice(idx, 1);
  if (dbPool) {
    try {
      await dbPool.query('DELETE FROM time_records WHERE id=$1;', [id]);
    } catch (err) {
      console.error('[删除打卡记录失败]', err.message);
    }
  }
  return removed;
}

// 每次给前端下发时间管理数据的统一格式（某一天的记录 + 谁正在计时 + 提交人名单）
function timeclockPayload(dateStr) {
  return {
    type: 'timeclock_update',
    date: dateStr,
    records: getTimeRecordsByDate(dateStr),
    openRecords: getOpenTimeRecords(),
    names: timeclockNames,
    workItems,
  };
}

function getTimeRecordsByDate(dateStr) {
  return timeRecords
    .filter((r) => r.workDate === dateStr)
    .sort((a, b) => a.startAt - b.startAt);
}

// 当前还没签入（进行中）的记录，前端用它来判断每个人现在是"计时中"还是"空闲"
function getOpenTimeRecords() {
  return timeRecords
    .filter((r) => r.endAt === null)
    .map((r) => ({ id: r.id, username: r.username, startAt: r.startAt, workDate: r.workDate, workItems: r.workItems || [] }));
}

function findOpenTimeRecord(username) {
  return timeRecords.find((r) => r.username === username && r.endAt === null) || null;
}

// 签出：开一条新记录
async function startTimeRecord(username, selectedWorkItems) {
  const now = Date.now();
  const { dateStr } = getJSTParts(new Date(now));
  const record = { id: `mem-${now}`, username, workDate: dateStr, startAt: now, endAt: null, durationMs: null, workItems: selectedWorkItems };
  timeRecords.push(record);
  if (timeRecords.length > MAX_TIME_RECORDS_IN_MEMORY) timeRecords.shift();
  if (dbPool) {
    try {
      const inserted = await dbPool.query(
        'INSERT INTO time_records (username, work_date, start_at, work_items) VALUES ($1,$2,$3,$4) RETURNING id;',
        [username, dateStr, now, JSON.stringify(selectedWorkItems)]
      );
      record.id = inserted.rows[0].id;
    } catch (err) {
      console.error('[签出记录写入数据库失败]', err.message);
    }
  }
  return record;
}

// 签入：把进行中的那条记录收尾，顺带算出时长
async function finishTimeRecord(record) {
  const now = Date.now();
  record.endAt = now;
  record.durationMs = Math.max(0, now - record.startAt);
  if (dbPool) {
    try {
      await dbPool.query(
        'UPDATE time_records SET end_at=$1, duration_ms=$2 WHERE id=$3;',
        [record.endAt, record.durationMs, record.id]
      );
    } catch (err) {
      console.error('[签入记录写入数据库失败]', err.message);
    }
  }
  return record;
}

function formatJSTTime(ts) {
  if (!ts) return '';
  return new Intl.DateTimeFormat('zh-CN', {
    timeZone: 'Asia/Tokyo',
    hour: '2-digit', minute: '2-digit', second: '2-digit', hour12: false,
  }).format(new Date(Number(ts)));
}

function formatDurationText(ms) {
  if (ms === null || ms === undefined) return '';
  const totalMinutes = Math.floor(ms / 60000);
  const h = Math.floor(totalMinutes / 60);
  const m = totalMinutes % 60;
  return h > 0 ? `${h}小时${m}分钟` : `${m}分钟`;
}

// 导出时的人名顺序，跟班表上的行顺序一致（前端 index.html 里也有一份同样的常量，
// 人员有变动两边一起改）。不在名单里的人排在后面。
const STAFF_NAME_ORDER = ['张展菖', '朱莉', '蔡凤麟', '乔倩芸', '曾文超', '陈厚桦', '王苏雅', '江昕航', '齐家驹', '曾征', '刘家珲'];
function staffNameRank(name) {
  const idx = STAFF_NAME_ORDER.indexOf(name);
  return idx === -1 ? STAFF_NAME_ORDER.length : idx;
}

// 按天导出CSV：/api/timeclock/export?date=2026-08-26
// 加UTF-8 BOM，Excel直接双击打开不会乱码
app.get('/api/timeclock/export', (req, res) => {
  const date = String(req.query.date || '');
  if (!/^\d{4}-\d{2}-\d{2}$/.test(date)) {
    return res.status(400).send('日期格式不对，应该是 YYYY-MM-DD');
  }
  const rows = getTimeRecordsByDate(date)
    .slice()
    .sort((a, b) => (staffNameRank(a.username) - staffNameRank(b.username))
      || a.username.localeCompare(b.username, 'zh')
      || (a.startAt - b.startAt));
  const esc = (v) => `"${String(v === null || v === undefined ? '' : v).replace(/"/g, '""')}"`;
  const lines = [['日期', '提交人', '签出时间', '签入时间', '时长(小时)', '时长', '工作内容', '状态'].map(esc).join(',')];
  rows.forEach((r) => {
    lines.push([
      r.workDate,
      r.username,
      formatJSTTime(r.startAt),
      r.endAt ? formatJSTTime(r.endAt) : '',
      r.durationMs === null ? '' : (r.durationMs / 3600000).toFixed(2),
      formatDurationText(r.durationMs),
      (r.workItems || []).join('、'),
      r.endAt ? '已完成' : '进行中',
    ].map(esc).join(','));
  });
  const csv = '﻿' + lines.join('\r\n') + '\r\n';
  res.setHeader('Content-Type', 'text/csv; charset=utf-8');
  res.setHeader('Content-Disposition', `attachment; filename="timeclock-${date}.csv"; filename*=UTF-8''${encodeURIComponent(`工时记录-${date}.csv`)}`);
  res.send(csv);
});

async function startServer() {
  await verifyDatabaseConnection();
  await loadChatHistoryFromDB();
  await loadUsersFromDB();
  await loadGroupsFromDB();
  await loadCaseLibraryFromDB();
  await loadSpecialRequirementsFromDB();
  await loadMallFromDB();
  await loadDriveIndexFromDB();
  await loadInspectionRulesFromDB();
  await loadProblemItemDataFromDB();
  await loadRemindersFromDB();
  await loadTimeRecordsFromDB();
  await loadTimeclockNamesFromDB();
  await loadWorkItemsFromDB();
  await loadShiftsFromDB();
  await loadStaffManagersFromDB();

  server.listen(PORT, '0.0.0.0', () => {
    console.log(`聊天服务器已启动`);
    const proto = hasLocalCerts ? 'https' : 'http';
    console.log(`本机访问: ${proto}://localhost:${PORT}`);
    console.log(`局域网访问: ${proto}://<你的局域网IP>:${PORT}`);
    if (!dbPool) {
      console.log('数据库未配置，公告栏/提醒事项等历史记录仅保存在内存中');
    } else if (dbConnectionVerified) {
      console.log('数据库已连接（已通过实际查询验证），公告栏/提醒事项等历史记录会持久化');
    } else {
      console.log('⚠️ 数据库连接测试失败！DATABASE_URL 已配置但连不上，请检查地址格式是否正确（比如有没有漏掉@符号、host是否正确）。当前会退化成内存模式，重启会清空历史记录');
    }
  });
}

startServer();
