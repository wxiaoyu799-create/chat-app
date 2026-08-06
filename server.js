const express = require('express');
const http = require('http');
const WebSocket = require('ws');
const path = require('path');
const fs = require('fs');
const multer = require('multer');
const crypto = require('crypto');
const { Pool } = require('pg');

const app = express();
const server = http.createServer(app);
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
  console.log('未配置 DATABASE_URL，置顶公告历史将只保存在内存中（重启会清空）');
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
// 最近消息历史（内存中，重启后清空——这部分保持原样不接数据库）
const MAX_HISTORY = 100;
let history = [];
// 置顶公告默认内容（数据库里一条记录都没有时，用这个当第一条）
const DEFAULT_PINNED_TEXT = '您好，本订单包裹已到库，xx商品xx（描述下实际情况）【可在代购订单列表该订单详情中查看截图】（如有提供图），现需要您确认：  ①是否可以直接为您入库？ ②请您于中国时间xx月xx日xx点前回复，如未收到您的回复将默认为您入库并在平台完成签收，签收后再出现任何问题卖家将无法再进行对应，所有问题损失需您自行承担，敬请了解。';
// 下面两个是内存里的"当前快照"，用来快速响应/广播给客户端；
// 真正的持久化存储在数据库里（如果配置了 DATABASE_URL 的话），
// 这两个变量在服务器启动时会从数据库加载出最新状态。
let pinnedText = DEFAULT_PINNED_TEXT;
const PINNED_HISTORY_MAX = 50;
let pinnedHistory = [{ text: pinnedText, by: '系统默认', startTime: Date.now(), endTime: null }];

async function ensurePinnedTable() {
  if (!dbPool) return;
  await dbPool.query(`
    CREATE TABLE IF NOT EXISTS pinned_history (
      id BIGSERIAL PRIMARY KEY,
      text TEXT NOT NULL,
      by_user TEXT NOT NULL,
      start_time TIMESTAMPTZ NOT NULL DEFAULT now(),
      end_time TIMESTAMPTZ
    );
  `);
}

async function loadPinnedStateFromDB() {
  if (!dbPool) return; // 没配置数据库，继续用内存里的默认值
  try {
    await ensurePinnedTable();
    const { rows } = await dbPool.query('SELECT * FROM pinned_history ORDER BY start_time ASC;');
    if (rows.length === 0) {
      // 数据库是空的（第一次接入），把内存里的默认值写进去当第一条记录
      const inserted = await dbPool.query(
        'INSERT INTO pinned_history (text, by_user, start_time, end_time) VALUES ($1, $2, now(), NULL) RETURNING *;',
        [DEFAULT_PINNED_TEXT, '系统默认']
      );
      pinnedHistory = inserted.rows.map(rowToHistoryEntry);
    } else {
      pinnedHistory = rows.map(rowToHistoryEntry);
    }
    pinnedText = pinnedHistory[pinnedHistory.length - 1].text;
    console.log(`已从数据库加载置顶公告历史，共 ${pinnedHistory.length} 条记录`);
  } catch (err) {
    console.error('[加载置顶公告历史失败，暂时改用内存默认值]', err.message);
  }
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

async function recordPinnedChange(newText, byUsername) {
  const now = Date.now();
  // 先更新内存里的快照，保证不管数据库有没有配置/有没有写成功，广播出去的内容始终是对的
  const last = pinnedHistory[pinnedHistory.length - 1];
  if (last && last.endTime === null) last.endTime = now;
  const newEntry = { id: `mem-${now}`, text: newText, by: byUsername, startTime: now, endTime: null };
  pinnedHistory.push(newEntry);
  if (pinnedHistory.length > PINNED_HISTORY_MAX) pinnedHistory.shift();

  if (!dbPool) return; // 没配数据库，到这里就结束，只有内存记录
  try {
    await dbPool.query(
      "UPDATE pinned_history SET end_time = now() WHERE end_time IS NULL;"
    );
    const inserted = await dbPool.query(
      'INSERT INTO pinned_history (text, by_user, start_time, end_time) VALUES ($1, $2, now(), NULL) RETURNING id;',
      [newText, byUsername]
    );
    newEntry.id = inserted.rows[0].id;
  } catch (err) {
    // 数据库写入失败也不影响这次修改本身生效（内存已经更新了），只是这条记录暂时没能持久化
    console.error('[置顶公告历史写入数据库失败]', err.message);
  }
}

async function deletePinnedHistoryEntry(targetId) {
  if (!dbPool) return;
  try {
    await dbPool.query('DELETE FROM pinned_history WHERE id = $1;', [targetId]);
  } catch (err) {
    console.error('[删除置顶公告历史记录失败]', err.message);
  }
}
// 置顶公告最大长度（原来是200，模板较长，放宽一些）
const PINNED_MAX_LENGTH = 600;
// 置顶公告编辑密码：优先读取环境变量 PIN_EDIT_PASSWORD（部署到Render时在后台设置），
// 本地没配置环境变量时用这个默认值兜底，方便本地测试，正式使用务必在Render上单独设置
const PIN_EDIT_PASSWORD = process.env.PIN_EDIT_PASSWORD || 'changeme123';

// 公告栏（侧栏那个）：跟置顶公告完全独立，各自内容互不关联，共用同一个编辑密码，
// 修改历史的追踪机制跟置顶公告完全一样（同样会走数据库持久化，如果配置了 DATABASE_URL 的话）
let announcementText = '';
const ANNOUNCEMENT_MAX_LENGTH = 600;
const ANNOUNCEMENT_HISTORY_MAX = 50;
let announcementHistory = [{ text: announcementText, by: '系统默认', startTime: Date.now(), endTime: null }];

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

// 备忘栏：跟置顶公告、公告栏都各自独立，互不关联，同样共用一个编辑密码，
// 逻辑跟公告栏完全对称（编辑+历史记录+删除+数据库持久化），复制这一套过来改个名字
let memoText = '';
const MEMO_MAX_LENGTH = 600;
const MEMO_HISTORY_MAX = 50;
let memoHistory = [{ text: memoText, by: '系统默认', startTime: Date.now(), endTime: null }];

async function ensureMemoTable() {
  if (!dbPool) return;
  await dbPool.query(`
    CREATE TABLE IF NOT EXISTS memo_history (
      id BIGSERIAL PRIMARY KEY,
      text TEXT NOT NULL,
      by_user TEXT NOT NULL,
      start_time TIMESTAMPTZ NOT NULL DEFAULT now(),
      end_time TIMESTAMPTZ
    );
  `);
}

async function loadMemoStateFromDB() {
  if (!dbPool) return;
  try {
    await ensureMemoTable();
    const { rows } = await dbPool.query('SELECT * FROM memo_history ORDER BY start_time ASC;');
    if (rows.length === 0) {
      const inserted = await dbPool.query(
        'INSERT INTO memo_history (text, by_user, start_time, end_time) VALUES ($1, $2, now(), NULL) RETURNING *;',
        ['', '系统默认']
      );
      memoHistory = inserted.rows.map(rowToHistoryEntry);
    } else {
      memoHistory = rows.map(rowToHistoryEntry);
    }
    memoText = memoHistory[memoHistory.length - 1].text;
    console.log(`已从数据库加载备忘栏历史，共 ${memoHistory.length} 条记录`);
  } catch (err) {
    console.error('[加载备忘栏历史失败，暂时改用内存默认值]', err.message);
  }
}

async function recordMemoChange(newText, byUsername) {
  const now = Date.now();
  const last = memoHistory[memoHistory.length - 1];
  if (last && last.endTime === null) last.endTime = now;
  const newEntry = { id: `mem-${now}`, text: newText, by: byUsername, startTime: now, endTime: null };
  memoHistory.push(newEntry);
  if (memoHistory.length > MEMO_HISTORY_MAX) memoHistory.shift();

  if (!dbPool) return;
  try {
    await dbPool.query(
      "UPDATE memo_history SET end_time = now() WHERE end_time IS NULL;"
    );
    const inserted = await dbPool.query(
      'INSERT INTO memo_history (text, by_user, start_time, end_time) VALUES ($1, $2, now(), NULL) RETURNING id;',
      [newText, byUsername]
    );
    newEntry.id = inserted.rows[0].id;
  } catch (err) {
    console.error('[备忘栏历史写入数据库失败]', err.message);
  }
}

async function deleteMemoHistoryEntry(targetId) {
  if (!dbPool) return;
  try {
    await dbPool.query('DELETE FROM memo_history WHERE id = $1;', [targetId]);
  } catch (err) {
    console.error('[删除备忘栏历史记录失败]', err.message);
  }
}

async function deletePinnedHistoryEntry(targetId) {
  if (!dbPool) return;
  try {
    await dbPool.query('DELETE FROM pinned_history WHERE id = $1;', [targetId]);
  } catch (err) {
    console.error('[删除置顶公告历史记录失败]', err.message);
  }
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

      // 发送历史消息 + 当前在线列表 + 置顶公告给新用户
      ws.send(JSON.stringify({ type: 'history', messages: history }));
      ws.send(JSON.stringify({ type: 'online', users: getOnlineUsers() }));
      ws.send(JSON.stringify({ type: 'pinned', text: pinnedText, history: pinnedHistory }));
      ws.send(JSON.stringify({ type: 'announcement', text: announcementText, history: announcementHistory }));
      ws.send(JSON.stringify({ type: 'memo', text: memoText, history: memoHistory }));

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
        time: Date.now(),
      };
      pushHistory(msg);
      broadcast(msg); // 包括发送者自己（用于统一渲染顺序）
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
      return;
    }

    if (data.type === 'pin') {
      const client = clients.get(ws);
      if (!client) return;
      const providedPassword = String(data.password || '');
      if (providedPassword !== PIN_EDIT_PASSWORD) {
        ws.send(JSON.stringify({ type: 'pin_error', message: '密码错误，无法修改置顶公告' }));
        return;
      }
      pinnedText = String(data.text || '').slice(0, PINNED_MAX_LENGTH);
      // 这里改成 await 了：等数据库真正写完、拿到确定的ID之后再广播，
      // 不然客户端可能会收到一个"临时ID"，等数据库写完真实ID后就对不上了
      // （之前这里是fire-and-forget，结果导致公告栏删除功能出现过ID不一致的bug，这里保持一致改成await更安全）
      await recordPinnedChange(pinnedText, client.username);
      const pinMsg = { type: 'pinned', text: pinnedText, by: client.username, history: pinnedHistory };
      broadcast(pinMsg); // 包括操作者自己，保证所有端一致
      if (pinnedText) {
        const sys = {
          type: 'system',
          text: `${client.username} 更新了置顶公告`,
          time: Date.now(),
        };
        pushHistory(sys);
        broadcast(sys, ws);
      }
      return;
    }

    if (data.type === 'pin_delete_history') {
      const client = clients.get(ws);
      if (!client) return;
      const providedPassword = String(data.password || '');
      if (providedPassword !== PIN_EDIT_PASSWORD) {
        ws.send(JSON.stringify({ type: 'pin_error', message: '密码错误，无法删除记录' }));
        return;
      }
      const targetId = data.id;
      const idx = pinnedHistory.findIndex((entry) => String(entry.id) === String(targetId));
      if (idx === -1) return;
      if (pinnedHistory[idx].endTime === null) {
        ws.send(JSON.stringify({ type: 'pin_error', message: '不能删除当前生效中的这条记录，请先编辑成新内容后再删' }));
        return;
      }
      pinnedHistory.splice(idx, 1);
      await deletePinnedHistoryEntry(targetId);
      broadcast({ type: 'pinned', text: pinnedText, history: pinnedHistory });
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

    if (data.type === 'memo') {
      const client = clients.get(ws);
      if (!client) return;
      const providedPassword = String(data.password || '');
      if (providedPassword !== PIN_EDIT_PASSWORD) {
        ws.send(JSON.stringify({ type: 'memo_error', message: '密码错误，无法修改备忘栏' }));
        return;
      }
      memoText = String(data.text || '').slice(0, MEMO_MAX_LENGTH);
      await recordMemoChange(memoText, client.username);
      broadcast({ type: 'memo', text: memoText, by: client.username, history: memoHistory });
      return;
    }

    if (data.type === 'memo_delete_history') {
      const client = clients.get(ws);
      if (!client) return;
      const providedPassword = String(data.password || '');
      if (providedPassword !== PIN_EDIT_PASSWORD) {
        ws.send(JSON.stringify({ type: 'memo_error', message: '密码错误，无法删除记录' }));
        return;
      }
      const targetId = data.id;
      const idx = memoHistory.findIndex((entry) => String(entry.id) === String(targetId));
      if (idx === -1) return;
      if (memoHistory[idx].endTime === null) {
        ws.send(JSON.stringify({ type: 'memo_error', message: '不能删除当前生效中的这条记录，请先编辑成新内容后再删' }));
        return;
      }
      memoHistory.splice(idx, 1);
      await deleteMemoHistoryEntry(targetId);
      broadcast({ type: 'memo', text: memoText, history: memoHistory });
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

async function startServer() {
  await loadPinnedStateFromDB(); // 没配数据库/加载失败都不会卡住启动，函数内部已经兜底处理
  await loadAnnouncementStateFromDB();
  await loadMemoStateFromDB();

  server.listen(PORT, '0.0.0.0', () => {
    console.log(`聊天服务器已启动`);
    console.log(`本机访问: http://localhost:${PORT}`);
    console.log(`局域网访问: http://<你的局域网IP>:${PORT}`);
    console.log(dbPool ? '数据库已连接，置顶公告历史会持久化' : '数据库未配置，置顶公告历史仅保存在内存中');
  });
}

startServer();
