const express = require('express');
const http = require('http');
const WebSocket = require('ws');
const path = require('path');
const fs = require('fs');
const multer = require('multer');
const crypto = require('crypto');

const app = express();
const server = http.createServer(app);
const wss = new WebSocket.Server({ server });

const PORT = process.env.PORT || 3000;

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

// 在线用户: ws -> { username, id }
const clients = new Map();
// 最近消息历史（内存中，重启后清空）
const MAX_HISTORY = 100;
let history = [];
// 置顶公告（内存中，重启后清空，但会用下面这个默认值重新初始化）
const DEFAULT_PINNED_TEXT = '您好，本订单包裹已到库，xx商品xx（描述下实际情况）【可在代购订单列表该订单详情中查看截图】（如有提供图），现需要您确认：  ①是否可以直接为您入库？ ②请您于中国时间xx月xx日xx点前回复，如未收到您的回复将默认为您入库并在平台完成签收，签收后再出现任何问题卖家将无法再进行对应，所有问题损失需您自行承担，敬请了解。';
let pinnedText = DEFAULT_PINNED_TEXT;
// 置顶公告最大长度（原来是200，模板较长，放宽一些）
const PINNED_MAX_LENGTH = 600;
// 置顶公告编辑密码：优先读取环境变量 PIN_EDIT_PASSWORD（部署到Render时在后台设置），
// 本地没配置环境变量时用这个默认值兜底，方便本地测试，正式使用务必在Render上单独设置
const PIN_EDIT_PASSWORD = process.env.PIN_EDIT_PASSWORD || 'changeme123';
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
  ws.on('message', (raw) => {
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
      ws.send(JSON.stringify({ type: 'pinned', text: pinnedText }));

      // 不再广播"XX加入了聊天室"这类系统提示——人多的时候刷屏，把正常聊天内容顶上去，
      // 谁在线直接看左侧在线列表就够了
      broadcast({ type: 'online', users: getOnlineUsers() });
      return;
    }

    if (data.type === 'message') {
      const client = clients.get(ws);
      if (!client) return;
      const text = String(data.text || '').slice(0, 2000);

      // 图片：只接受我们自己 /upload 接口生成的路径，避免被塞入任意外部地址
      let image = null;
      if (typeof data.image === 'string' && /^\/uploads\/[a-zA-Z0-9_\-.]+$/.test(data.image)) {
        image = data.image;
      }

      // 纯文字消息不能是空的；但如果带了图片，文字可以为空（图片本身就是内容）
      if (!text.trim() && !image) return;

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
        image,
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
      const pinMsg = { type: 'pinned', text: pinnedText, by: client.username };
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

server.listen(PORT, '0.0.0.0', () => {
  console.log(`聊天服务器已启动`);
  console.log(`本机访问: http://localhost:${PORT}`);
  console.log(`局域网访问: http://<你的局域网IP>:${PORT}`);
});
