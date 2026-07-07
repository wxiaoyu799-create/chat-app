const express = require('express');
const http = require('http');
const WebSocket = require('ws');
const path = require('path');

const app = express();
const server = http.createServer(app);
const wss = new WebSocket.Server({ server });

const PORT = process.env.PORT || 3000;

app.use(express.static(path.join(__dirname, 'public')));

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
// 消息自增ID（用于引用回复）
let nextMessageId = 1;

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
  online.forEach((name) => {
    // 按 @用户名 精确匹配（用户名后需跟空白/标点/结尾，避免子串误判）
    const escaped = name.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
    const re = new RegExp(`@${escaped}(?=\\s|[，,。.!！?？]|$)`);
    if (re.test(text)) mentioned.add(name);
  });
  return Array.from(mentioned);
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

      const joinMsg = {
        type: 'system',
        text: `${username} 加入了聊天室`,
        time: Date.now(),
      };
      pushHistory(joinMsg);
      broadcast(joinMsg, ws);
      broadcast({ type: 'online', users: getOnlineUsers() });
      return;
    }

    if (data.type === 'message') {
      const client = clients.get(ws);
      if (!client) return;
      const text = String(data.text || '').slice(0, 2000);
      if (!text.trim()) return;

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

      const msg = {
        type: 'message',
        id: nextMessageId++,
        username: client.username,
        text,
        mentions: extractMentions(text),
        quote,
        time: Date.now(),
      };
      pushHistory(msg);
      broadcast(msg); // 包括发送者自己（用于统一渲染顺序）
      return;
    }

    if (data.type === 'pin') {
      const client = clients.get(ws);
      if (!client) return;
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
      const leaveMsg = {
        type: 'system',
        text: `${client.username} 离开了聊天室`,
        time: Date.now(),
      };
      pushHistory(leaveMsg);
      broadcast(leaveMsg);
      broadcast({ type: 'online', users: getOnlineUsers() });
    }
  });
});

server.listen(PORT, '0.0.0.0', () => {
  console.log(`聊天服务器已启动`);
  console.log(`本机访问: http://localhost:${PORT}`);
  console.log(`局域网访问: http://<你的局域网IP>:${PORT}`);
});
