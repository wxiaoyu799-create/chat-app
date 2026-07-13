const { app, BrowserWindow, Menu, ipcMain, nativeImage, shell } = require('electron');
const path = require('path');
const fs = require('fs');

// 部分电脑（尤其是某些集成显卡/远程桌面/虚拟机环境）在硬件加速下会崩溃，
// 关掉硬件加速可以避免这一类"打开就闪退"的问题。
app.disableHardwareAcceleration();

// 捕获主进程里没被处理的异常，打印出来而不是让整个 App 直接崩溃退出，
// 方便定位问题（用 `npm start` 从终端启动时能在终端看到这些日志）。
process.on('uncaughtException', (err) => {
  console.error('[主进程未捕获异常]', err);
});
process.on('unhandledRejection', (reason) => {
  console.error('[主进程未处理的 Promise 拒绝]', reason);
});

const CONFIG_PATH = path.join(app.getPath('userData'), 'chat-config.json');

function readLastUrl() {
  try {
    const raw = fs.readFileSync(CONFIG_PATH, 'utf8');
    const data = JSON.parse(raw);
    return data.lastUrl || '';
  } catch (e) {
    return '';
  }
}

function saveLastUrl(url) {
  try {
    fs.writeFileSync(CONFIG_PATH, JSON.stringify({ lastUrl: url }), 'utf8');
  } catch (e) {
    // ignore write failures (e.g. read-only fs)
  }
}

let mainWindow = null;

// 任务栏/Dock图标上的红点角标（Windows用setOverlayIcon，Mac用dock badge）
const badgeIconPath = path.join(__dirname, 'build', 'badge-overlay.png');
let badgeImage = null;
try {
  badgeImage = nativeImage.createFromPath(badgeIconPath);
  if (badgeImage.isEmpty()) badgeImage = null;
} catch (e) {
  badgeImage = null;
}

function showMentionAlert() {
  if (!mainWindow || mainWindow.isDestroyed()) return;
  // Windows：任务栏图标闪烁（窗口获得焦点后系统会自动停止闪烁） + 角标红点
  if (process.platform === 'win32') {
    mainWindow.flashFrame(true);
    if (badgeImage) mainWindow.setOverlayIcon(badgeImage, '有人@你了');
  }
  // Mac：Dock图标显示红点角标 + 弹跳提醒一次
  if (process.platform === 'darwin' && app.dock) {
    app.dock.setBadge('●');
    app.dock.bounce('informational');
  }
}

function clearMentionAlert() {
  if (!mainWindow || mainWindow.isDestroyed()) return;
  if (process.platform === 'win32') {
    mainWindow.flashFrame(false);
    mainWindow.setOverlayIcon(null, '');
  }
  if (process.platform === 'darwin' && app.dock) {
    app.dock.setBadge('');
  }
}

function normalizeUrl(input) {
  let url = String(input || '').trim();
  if (!url) return '';
  if (!/^https?:\/\//i.test(url)) {
    url = 'http://' + url;
  }
  return url;
}

function goToConnectPage(errorMessage) {
  if (!mainWindow || mainWindow.isDestroyed()) return;
  const connectFile = path.join(__dirname, 'connect.html');
  const query = errorMessage ? `?error=${encodeURIComponent(errorMessage)}` : '';
  mainWindow.loadFile(connectFile, { search: query });
}

function createWindow() {
  mainWindow = new BrowserWindow({
    width: 1000,
    height: 720,
    minWidth: 640,
    minHeight: 480,
    backgroundColor: '#0f1115',
    icon: path.join(__dirname, 'build', 'icon.png'),
    webPreferences: {
      preload: path.join(__dirname, 'preload.js'),
      contextIsolation: true,
      nodeIntegration: false,
    },
  });

  mainWindow.setMenu(buildMenu());

  // 聊天消息里的链接（target="_blank"）默认会在Electron自己独立的浏览器环境里打开新窗口，
  // 那个环境跟系统默认浏览器完全不共享登录状态/Cookie，会导致像雅虎这种需要登录/年龄验证
  // 的网站打不开。这里改成：外部链接一律丢给系统默认浏览器（Edge/Chrome等）打开，
  // 这样用的就是用户平时已经登录好的那个浏览器环境。
  mainWindow.webContents.setWindowOpenHandler(({ url }) => {
    shell.openExternal(url);
    return { action: 'deny' };
  });

  goToConnectPage();

  mainWindow.webContents.on('did-fail-load', (event, errorCode, errorDescription, validatedURL) => {
    // 忽略连接页自身以及子资源(如图标/字体)加载失败，只处理"连接聊天服务器"这个主导航失败的情况
    if (!validatedURL || validatedURL.startsWith('file://')) return;
    console.error('[加载失败]', errorCode, errorDescription, validatedURL);
    // 重要：不能在 did-fail-load 事件处理函数里直接同步调用 loadFile/loadURL，
    // 这会在 Chromium 底层造成重入，导致整个进程以访问冲突(0xC0000005)崩溃。
    // 用 setImmediate 推迟到下一轮事件循环再跳转即可规避。
    setImmediate(() => {
      goToConnectPage(`无法连接到 ${validatedURL}（${errorDescription}）`);
    });
  });

  mainWindow.webContents.on('render-process-gone', (event, details) => {
    console.error('[渲染进程崩溃]', details);
    setImmediate(() => {
      goToConnectPage(`聊天页面崩溃了（${details.reason}），请重试`);
    });
  });

  mainWindow.on('unresponsive', () => {
    console.error('[窗口无响应]');
  });

  mainWindow.on('focus', () => {
    clearMentionAlert();
  });

  mainWindow.on('closed', () => {
    mainWindow = null;
  });
}

function buildMenu() {
  const template = [
    {
      label: '文件',
      submenu: [
        {
          label: '更换服务器地址',
          accelerator: 'CmdOrCtrl+Shift+H',
          click: () => goToConnectPage(),
        },
        { type: 'separator' },
        { role: 'reload', label: '刷新' },
        {
          label: '强制刷新（忽略缓存）',
          accelerator: 'CmdOrCtrl+Shift+R',
          click: () => {
            if (mainWindow && !mainWindow.isDestroyed()) {
              mainWindow.webContents.reloadIgnoringCache();
            }
          },
        },
        { role: 'quit', label: '退出' },
      ],
    },
    {
      label: '视图',
      submenu: [
        { role: 'resetZoom', label: '实际大小' },
        { role: 'zoomIn', label: '放大' },
        { role: 'zoomOut', label: '缩小' },
        { type: 'separator' },
        { role: 'togglefullscreen', label: '全屏' },
        { role: 'toggleDevTools', label: '开发者工具' },
      ],
    },
  ];
  return Menu.buildFromTemplate(template);
}

ipcMain.handle('get-last-url', () => readLastUrl());

ipcMain.on('mention-alert', () => {
  showMentionAlert();
});

ipcMain.on('connect-to-server', (event, rawUrl) => {
  const url = normalizeUrl(rawUrl);
  if (!url || !mainWindow) return;
  saveLastUrl(url);
  mainWindow.loadURL(url).catch(() => {
    goToConnectPage(`无法连接到 ${url}`);
  });
});

app.whenReady().then(() => {
  createWindow();

  app.on('activate', () => {
    if (BrowserWindow.getAllWindows().length === 0) createWindow();
  });
});

app.on('window-all-closed', () => {
  if (process.platform !== 'darwin') app.quit();
});
