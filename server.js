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
    res.json({ url: `/uploads/${req.file.filename}` });
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
  limits: { fileSize: MAX_FILE_SIZE },
});

app.post('/upload-file', (req, res) => {
  uploadFile.single('file')(req, res, (err) => {
    if (err) {
      if (err.code === 'LIMIT_FILE_SIZE') {
        return res.status(400).json({ error: `文件太大了，最大支持 ${Math.round(MAX_FILE_SIZE / 1024 / 1024)}MB` });
      }
      return res.status(400).json({ error: err.message || '上传失败' });
    }
    if (!req.file) {
      return res.status(400).json({ error: '没有收到文件' });
    }
    // 原始文件名做个长度截断，避免超长文件名把消息体撑得太大；存储用的文件名跟原始名无关，不影响下载时的显示名
    const originalName = String(req.file.originalname || '未命名文件').slice(0, 150);
    res.json({
      url: `/uploads/${req.file.filename}`,
      name: originalName,
      size: req.file.size,
    });
  });
});

// 在线用户: ws -> { username, id }
const clients = new Map();
// 最近消息历史——内存里始终保留最近MAX_HISTORY条，用于日常渲染/查找（快，不用每次都查数据库）；
// 如果数据库连上了，这些消息也会异步写入数据库，服务器重启后能从数据库把最近的消息读回来，
// 不会变成空白聊天室。没配置数据库的话，行为跟以前完全一样，纯内存，重启就清空。
const MAX_HISTORY = 100;
let history = [];

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
    history = rows.reverse().map((row) => ({
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
    }));
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
    `INSERT INTO chat_messages (id, username, text, images, files, mentions, mentions_all, quote, reactions, pending, msg_time)
     VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
     ON CONFLICT (id) DO NOTHING;`,
    [
      msg.id, msg.username, msg.text,
      JSON.stringify(msg.images || []), JSON.stringify(msg.files || []),
      JSON.stringify(msg.mentions || []), !!msg.mentionsAll,
      msg.quote ? JSON.stringify(msg.quote) : null,
      JSON.stringify(msg.reactions || {}),
      msg.pending ? JSON.stringify(msg.pending) : null,
      msg.time,
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

// 公告栏（侧栏那个）：跟置顶公告完全独立，各自内容互不关联，共用同一个编辑密码，
// 修改历史的追踪机制跟置顶公告完全一样（同样会走数据库持久化，如果配置了 DATABASE_URL 的话）
let announcementText = '';
const ANNOUNCEMENT_MAX_LENGTH = 600;
const ANNOUNCEMENT_HISTORY_MAX = 50;
let announcementHistory = [{ id: 'mem-seed', text: announcementText, by: '系统默认', startTime: Date.now(), endTime: null }];

async function ensureAnnouncementTable() {
  if (!dbPool) return;
  await dbPool.query(`
    CREATE TABLE IF NOT EXISTS announcement_history (
      id BIGSERIAL PRIMARY KEY,
      text TEXT NOT NULL,
      by_user TEXT NOT NULL,
      start_time TIMESTAMPTZ NOT NULL DEFAULT now(),
      end_time TIMESTAMPTZ
    );
  `);
}

async function loadAnnouncementStateFromDB() {
  if (!dbPool) return; // 没配置数据库，继续用内存里的默认值（空公告）
  try {
    await ensureAnnouncementTable();
    const { rows } = await dbPool.query('SELECT * FROM announcement_history ORDER BY start_time ASC;');
    if (rows.length === 0) {
      // 数据库是空的（第一次接入），把内存里的默认值（空字符串）写进去当第一条记录
      const inserted = await dbPool.query(
        'INSERT INTO announcement_history (text, by_user, start_time, end_time) VALUES ($1, $2, now(), NULL) RETURNING *;',
        ['', '系统默认']
      );
      announcementHistory = inserted.rows.map(rowToHistoryEntry);
    } else {
      announcementHistory = rows.map(rowToHistoryEntry);
    }
    announcementText = announcementHistory[announcementHistory.length - 1].text;
    console.log(`已从数据库加载公告栏历史，共 ${announcementHistory.length} 条记录`);
  } catch (err) {
    console.error('[加载公告栏历史失败，暂时改用内存默认值]', err.message);
  }
}

async function recordAnnouncementChange(newText, byUsername) {
  const now = Date.now();
  const last = announcementHistory[announcementHistory.length - 1];
  if (last && last.endTime === null) last.endTime = now;
  const newEntry = { id: `mem-${now}`, text: newText, by: byUsername, startTime: now, endTime: null };
  announcementHistory.push(newEntry);
  if (announcementHistory.length > ANNOUNCEMENT_HISTORY_MAX) announcementHistory.shift();

  if (!dbPool) return; // 没配数据库，到这里就结束，只有内存记录（id用临时值即可，反正也没法真删数据库）
  try {
    await dbPool.query(
      "UPDATE announcement_history SET end_time = now() WHERE end_time IS NULL;"
    );
    const inserted = await dbPool.query(
      'INSERT INTO announcement_history (text, by_user, start_time, end_time) VALUES ($1, $2, now(), NULL) RETURNING id;',
      [newText, byUsername]
    );
    // 用数据库真实生成的id替换掉临时id，这样后面删除的时候才能对上数据库里的具体那一行
    newEntry.id = inserted.rows[0].id;
  } catch (err) {
    console.error('[公告栏历史写入数据库失败]', err.message);
  }
}

async function deleteAnnouncementHistoryEntry(targetId) {
  if (!dbPool) return; // 内存模式不用管，内存那边已经在调用处删掉了
  try {
    await dbPool.query('DELETE FROM announcement_history WHERE id = $1;', [targetId]);
  } catch (err) {
    console.error('[删除公告栏历史记录失败]', err.message);
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

// ===== 问题件提醒：代购/代拍/煤炉三个分类，各自独立计数和记录列表，
// 但"问题类型"和"检品人员姓名"这两个下拉选项列表是三个分类共用的一套 =====
const PROBLEM_ITEM_CATEGORIES = ['代购', '代拍', '煤炉'];
const DEFAULT_ISSUE_TYPES = ['破损', '脏污', '特典', '少货', '多货', '商品错误', '找不到订单'];
const DEFAULT_INSPECTOR_NAMES = [];

// 共用选项列表：{ issue_type: [{id, value}], inspector_name: [{id, value}] }
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
    submittedBy: row.submitted_by,
    submittedAt: new Date(row.submitted_at).getTime(),
    status: row.status,
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

    const issueRows = await dbPool.query(
      "SELECT * FROM problem_item_options WHERE option_type = 'issue_type' ORDER BY sort_order ASC, id ASC;"
    );
    if (issueRows.rows.length === 0) {
      // 第一次接入数据库，把代码里的默认问题类型写进去
      for (let i = 0; i < DEFAULT_ISSUE_TYPES.length; i++) {
        await dbPool.query(
          'INSERT INTO problem_item_options (option_type, value, sort_order) VALUES ($1, $2, $3);',
          ['issue_type', DEFAULT_ISSUE_TYPES[i], i]
        );
      }
      const reloaded = await dbPool.query(
        "SELECT * FROM problem_item_options WHERE option_type = 'issue_type' ORDER BY sort_order ASC, id ASC;"
      );
      problemItemOptions.issue_type = reloaded.rows.map(rowToProblemItemOption);
    } else {
      problemItemOptions.issue_type = issueRows.rows.map(rowToProblemItemOption);
    }

    const inspectorRows = await dbPool.query(
      "SELECT * FROM problem_item_options WHERE option_type = 'inspector_name' ORDER BY sort_order ASC, id ASC;"
    );
    problemItemOptions.inspector_name = inspectorRows.rows.map(rowToProblemItemOption);

    // 只加载"待处理"状态的记录到内存里，已解决/已跟进的留在数据库当历史，不占内存也不用同步给客户端
    for (const cat of PROBLEM_ITEM_CATEGORIES) {
      const reportRows = await dbPool.query(
        "SELECT * FROM problem_item_reports WHERE category = $1 AND status = 'pending' ORDER BY submitted_at ASC;",
        [cat]
      );
      problemItemReports[cat] = reportRows.rows.map(rowToProblemItemReport);
    }

    console.log('已从数据库加载问题件提醒选项和待处理记录');
  } catch (err) {
    console.error('[加载问题件提醒数据失败，暂时改用内存默认值]', err.message);
    problemItemOptions.issue_type = DEFAULT_ISSUE_TYPES.map((v, i) => ({ id: `mem-issue-${i}`, value: v }));
  }
}

async function addProblemItemReport(category, issueTypes, inspectorNames, orderNote, submittedBy) {
  const now = Date.now();
  let id = `mem-${now}`;
  if (dbPool) {
    try {
      const result = await dbPool.query(
        'INSERT INTO problem_item_reports (category, issue_types, inspector_names, order_note, submitted_by) VALUES ($1, $2, $3, $4, $5) RETURNING id, submitted_at;',
        [category, JSON.stringify(issueTypes), JSON.stringify(inspectorNames), orderNote, submittedBy]
      );
      id = result.rows[0].id;
    } catch (err) {
      console.error('[问题件提醒记录写入数据库失败]', err.message);
    }
  }
  const report = { id, category, issueTypes, inspectorNames, orderNote, submittedBy, submittedAt: now, status: 'pending' };
  problemItemReports[category].push(report);
  return report;
}

async function updateProblemItemReportStatus(category, reportId, status, byUsername) {
  const idx = problemItemReports[category].findIndex((r) => String(r.id) === String(reportId));
  if (idx === -1) return false;
  problemItemReports[category].splice(idx, 1); // 不管是已解决还是需跟进，都从"待处理"内存列表里移除

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
  return true;
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
  ws.on('message', async (raw) => {
    let data;
    try {
      data = JSON.parse(raw);
    } catch (e) {
      return;
    }

    if (data.type === 'join') {
      const username = String(data.username || '匿名用户').slice(0, 20).trim() || '匿名用户';
      clients.set(ws, { username });

      // 发送历史消息 + 当前在线列表给新用户
      ws.send(JSON.stringify({ type: 'history', messages: history }));
      ws.send(JSON.stringify({ type: 'online', users: getOnlineUsers() }));
      ws.send(JSON.stringify({ type: 'announcement', text: announcementText, history: announcementHistory }));
      ws.send(JSON.stringify({ type: 'reminder_list', reminders }));
      ws.send(JSON.stringify({ type: 'inspection_rules_all', rules: getAllInspectionRulesText() }));
      ws.send(JSON.stringify({ type: 'problem_item_data', ...getProblemItemSnapshot() }));

      // 不再广播"XX加入了聊天室"这类系统提示——人多的时候刷屏，把正常聊天内容顶上去，
      // 谁在线直接看左侧在线列表就够了
      broadcast({ type: 'online', users: getOnlineUsers() });
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
          .filter((url) => typeof url === 'string' && /^\/uploads\/[a-zA-Z0-9_\-.]+$/.test(url))
          .slice(0, MAX_IMAGES_PER_MESSAGE);
      }

      // 通用文件：同样只认自己 /upload-file 接口生成的路径；每条消息最多5个文件
      const MAX_FILES_PER_MESSAGE = 5;
      let files = [];
      if (Array.isArray(data.files)) {
        files = data.files
          .filter((f) =>
            f && typeof f === 'object' &&
            typeof f.url === 'string' && /^\/uploads\/[a-zA-Z0-9_\-.]+$/.test(f.url) &&
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

      const { mentioned, isAll } = extractMentions(text);
      const msg = {
        type: 'message',
        id: nextMessageId++,
        username: client.username,
        text,
        images,
        files,
        mentions: mentioned,
        mentionsAll: isAll,
        quote,
        reactions: {},
        pending: null, // 待处理标记：null=没标记，{by, at}=有人标了还没处理完
        time: Date.now(),
      };
      pushHistory(msg);
      saveChatMessageToDB(msg);
      broadcast(msg); // 包括发送者自己（用于统一渲染顺序）
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
      broadcast({ type: 'reaction_update', messageId, emoji, users: list });
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
      broadcast({ type: 'pending_update', messageId, pending: msg.pending, text: msg.text, username: msg.username });
      updateMessagePendingInDB(messageId, msg.pending);
      return;
    }

    if (data.type === 'announcement') {
      const client = clients.get(ws);
      if (!client) return;
      const providedPassword = String(data.password || '');
      if (providedPassword !== PIN_EDIT_PASSWORD) {
        ws.send(JSON.stringify({ type: 'announcement_error', message: '密码错误，无法修改公告栏' }));
        return;
      }
      announcementText = String(data.text || '').slice(0, ANNOUNCEMENT_MAX_LENGTH);
      // 同样改成await，理由同上——确保广播出去的历史记录ID已经是数据库最终确定的值
      await recordAnnouncementChange(announcementText, client.username);
      broadcast({ type: 'announcement', text: announcementText, by: client.username, history: announcementHistory });
      return;
    }

    if (data.type === 'announcement_delete_history') {
      const client = clients.get(ws);
      if (!client) return;
      const providedPassword = String(data.password || '');
      if (providedPassword !== PIN_EDIT_PASSWORD) {
        ws.send(JSON.stringify({ type: 'announcement_error', message: '密码错误，无法删除记录' }));
        return;
      }
      const targetId = data.id;
      const idx = announcementHistory.findIndex((entry) => String(entry.id) === String(targetId));
      if (idx === -1) return; // 找不到就算了，可能已经被删过了
      if (announcementHistory[idx].endTime === null) {
        // 当前正在生效的这一条不能删，删了就跟公告栏当前显示的内容对不上了；
        // 想删的话得先编辑成新内容，让这条"过期"了再删
        ws.send(JSON.stringify({ type: 'announcement_error', message: '不能删除当前生效中的这条记录，请先编辑成新内容后再删' }));
        return;
      }
      announcementHistory.splice(idx, 1);
      await deleteAnnouncementHistoryEntry(targetId); // 内存已经删了，这里等数据库那边也真删完
      broadcast({ type: 'announcement', text: announcementText, history: announcementHistory });
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
      // 提交是日常操作，不需要密码——密码只用来保护"编辑下拉选项列表"这种管理性操作
      const report = await addProblemItemReport(category, issueTypes, inspectorNames, orderNote, client.username);
      broadcast({ type: 'problem_item_report_added', category, report });
      return;
    }

    if (data.type === 'problem_item_resolve' || data.type === 'problem_item_followup') {
      const client = clients.get(ws);
      if (!client) return;
      const category = String(data.category || '');
      if (!PROBLEM_ITEM_CATEGORIES.includes(category)) return;
      const status = data.type === 'problem_item_resolve' ? 'resolved' : 'follow_up';
      const ok = await updateProblemItemReportStatus(category, data.reportId, status, client.username);
      if (ok) {
        broadcast({ type: 'problem_item_report_removed', category, reportId: data.reportId });
      }
      return;
    }

    if (data.type === 'problem_item_options_update') {
      const client = clients.get(ws);
      if (!client) return;
      const optionType = String(data.optionType || '');
      if (optionType !== 'issue_type' && optionType !== 'inspector_name') return;
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

      if (data.type === 'reminder_add') {
        await addReminder(hour, minute, weekdays, text);
      } else {
        const ok = await updateReminder(data.id, hour, minute, weekdays, text);
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

    if (data.type === 'typing') {
      const client = clients.get(ws);
      if (!client) return;
      broadcast({ type: 'typing', username: client.username }, ws);
      return;
    }
  });

  ws.on('close', () => {
    const client = clients.get(ws);
    if (client) {
      clients.delete(ws);
      // 同样不再广播"XX离开了聊天室"
      broadcast({ type: 'online', users: getOnlineUsers() });
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

async function addReminder(hour, minute, weekdays, text) {
  const newReminder = { id: `mem-${Date.now()}`, hour, minute, weekdays, text };
  reminders.push(newReminder);
  if (!dbPool) return newReminder;
  try {
    const inserted = await dbPool.query(
      'INSERT INTO reminders (hour, minute, weekdays, text) VALUES ($1,$2,$3,$4) RETURNING id;',
      [hour, minute, weekdays ? weekdays.join(',') : null, text]
    );
    newReminder.id = inserted.rows[0].id;
  } catch (err) {
    console.error('[新增定时提醒写入数据库失败]', err.message);
  }
  return newReminder;
}

async function updateReminder(id, hour, minute, weekdays, text) {
  const idx = reminders.findIndex((r) => String(r.id) === String(id));
  if (idx === -1) return false;
  reminders[idx] = { ...reminders[idx], hour, minute, weekdays, text };
  if (!dbPool) return true;
  try {
    await dbPool.query(
      'UPDATE reminders SET hour=$1, minute=$2, weekdays=$3, text=$4 WHERE id=$5;',
      [hour, minute, weekdays ? weekdays.join(',') : null, text, id]
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
function fireReminderBroadcast(text) {
  const onlineUsernames = getOnlineUsers();
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
  };
  pushHistory(msg);
  saveChatMessageToDB(msg);
  broadcast(msg);
}

function checkReminders() {
  const { dateStr, hour, minute, weekdayNum } = getJSTParts(new Date());
  reminders.forEach((r) => {
    if (r.hour !== hour || r.minute !== minute) return;
    if (r.weekdays && !r.weekdays.includes(weekdayNum)) return;
    const key = String(r.id);
    if (reminderLastFiredDate[key] === dateStr) return; // 今天已经发过了，不重复发
    reminderLastFiredDate[key] = dateStr;
    fireReminderBroadcast(r.text);
    console.log(`[定时提醒] 已发送: ${r.text}`);
  });
}

// 每30秒检查一次，足够精确命中每分钟的提醒时间点，又不会太频繁
setInterval(checkReminders, 30 * 1000);

async function startServer() {
  await verifyDatabaseConnection();
  await loadChatHistoryFromDB();
  await loadAnnouncementStateFromDB();
  await loadInspectionRulesFromDB();
  await loadProblemItemDataFromDB();
  await loadRemindersFromDB();

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
