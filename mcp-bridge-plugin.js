(function () {
  "use strict";

  /* ============================================================
     MCP Client Core（内嵌轻量实现）
     ============================================================ */
  function uuid() {
    return "xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx".replace(/[xy]/g, function (c) {
      var r = (Math.random() * 16) | 0;
      return (c === "x" ? r : (r & 0x3) | 0x8).toString(16);
    });
  }

  function MCPConnection(config) {
    this.id = config.id || uuid();
    this.name = config.name || "未命名服务器";
    this.url = config.url;
    this.transport = config.transport || "sse";
    this.status = "disconnected"; // disconnected, connecting, connected
  }

  MCPConnection.prototype.connect = function () {
    this.status = "connecting";
    console.log("[MCP Bridge] 正在连接服务器: " + this.name);
    // 此处为模拟连接成功，请替换为你的实际 SSE/WebSocket 连接逻辑
    setTimeout(() => {
      this.status = "connected";
      console.log("[MCP Bridge] 服务器已连接: " + this.name);
      if (state.view === "settings") render(); // 刷新UI状态
    }, 500);
  };

  MCPConnection.prototype.disconnect = function () {
    this.status = "disconnected";
    console.log("[MCP Bridge] 服务器已断开: " + this.name);
    // 此处清理实际的 SSE/WebSocket 连接
  };

  /* ============================================================
     插件全局状态管理
     ============================================================ */
  var state = {
    roche: null,
    container: null,
    view: "chat", // 视图：'chat'(首页), 'settings'(设置), 'editServer'(编辑服务器)
    configs: [],  // 保存的服务器配置
    servers: [],  // 运行中的服务器实例
    styleEl: null,
    editingServer: null // 当前正在编辑的服务器ID
  };

  /* ============================================================
     配置持久化 (修复退出丢失 bug)
     ============================================================ */
  var STORAGE_KEY = "mcp_bridge_configs_v1";

  async function loadConfig(roche) {
    try {
      let data = null;
      // 兼容多种 roche 存储 API 或 fallback 到 localStorage
      if (roche && roche.storage && typeof roche.storage.get === "function") {
        data = await roche.storage.get(STORAGE_KEY);
      } else if (roche && typeof roche.getConfig === "function") {
        let cfg = await roche.getConfig();
        data = cfg ? cfg[STORAGE_KEY] : null;
      } else {
        data = localStorage.getItem(STORAGE_KEY);
      }
      
      if (data) {
        state.configs = typeof data === "string" ? JSON.parse(data) : data;
      } else {
        state.configs = [];
      }
    } catch (e) {
      console.error("[MCP Bridge] 加载配置失败", e);
      state.configs = [];
    }
  }

  async function saveConfig() {
    try {
      let dataStr = JSON.stringify(state.configs);
      if (state.roche && state.roche.storage && typeof state.roche.storage.set === "function") {
        await state.roche.storage.set(STORAGE_KEY, dataStr);
      } else if (state.roche && typeof state.setConfig === "function") {
        let payload = {};
        payload[STORAGE_KEY] = dataStr;
        await state.roche.setConfig(payload);
      } else {
        localStorage.setItem(STORAGE_KEY, dataStr);
      }
      console.log("[MCP Bridge] 配置已保存持久化");
    } catch (e) {
      console.error("[MCP Bridge] 保存配置失败", e);
    }
  }

  /* ============================================================
     服务器生命周期管理
     ============================================================ */
  function connectServer(cfg) {
    var existing = state.servers.find(s => s.id === cfg.id);
    if (existing) {
      existing.disconnect();
      state.servers = state.servers.filter(s => s.id !== cfg.id);
    }
    var server = new MCPConnection(cfg);
    server.connect();
    state.servers.push(server);
  }

  function disconnectServer(id) {
    var existing = state.servers.find(s => s.id === id);
    if (existing) {
      existing.disconnect();
      state.servers = state.servers.filter(s => s.id !== id);
    }
  }

  /* ============================================================
     UI 渲染与路由
     ============================================================ */
  function getStyles() {
    return `
      .mcp-bridge-container { display: flex; flex-direction: column; height: 100%; width: 100%; background: #f7f7f8; font-family: sans-serif; color: #333; }
      .mcp-header { display: flex; align-items: center; justify-content: space-between; padding: 12px 16px; background: #fff; border-bottom: 1px solid #eee; }
      .mcp-header-btn { font-size: 14px; color: #007aff; cursor: pointer; user-select: none; padding: 4px 8px; }
      .mcp-title { font-size: 16px; font-weight: bold; flex: 1; text-align: center; }
      .mcp-content { flex: 1; overflow-y: auto; padding: 16px; }
      .mcp-card { background: #fff; border-radius: 8px; padding: 16px; margin-bottom: 12px; box-shadow: 0 1px 3px rgba(0,0,0,0.05); }
      .mcp-server-item { display: flex; justify-content: space-between; align-items: center; border-bottom: 1px solid #f0f0f0; padding-bottom: 12px; margin-bottom: 12px; }
      .mcp-server-item:last-child { border-bottom: none; padding-bottom: 0; margin-bottom: 0; }
      .mcp-server-info { flex: 1; }
      .mcp-server-name { font-size: 15px; font-weight: bold; margin-bottom: 4px; }
      .mcp-server-url { font-size: 12px; color: #888; }
      .mcp-server-actions { display: flex; gap: 8px; }
      .mcp-btn-edit { background: #eef2ff; color: #007aff; border: none; padding: 6px 12px; border-radius: 4px; cursor: pointer; }
      .mcp-btn-del { background: #fff0f0; color: #ff3b30; border: none; padding: 6px 12px; border-radius: 4px; cursor: pointer; }
      .mcp-input-group { margin-bottom: 16px; }
      .mcp-label { display: block; font-size: 14px; color: #555; margin-bottom: 6px; }
      .mcp-input { width: 100%; box-sizing: border-box; padding: 10px; border: 1px solid #ddd; border-radius: 6px; font-size: 14px; }
      .mcp-empty { text-align: center; color: #999; padding: 20px 0; font-size: 14px; }
    `;
  }

  function exitPlugin() {
    // 修复 Bug 1: 多重退路兼容，确保能够退出返回 roche 本体
    if (state.roche) {
      if (typeof state.roche.closePlugin === "function") state.roche.closePlugin();
      else if (typeof state.roche.close === "function") state.roche.close();
      else if (typeof state.roche.exit === "function") state.roche.exit();
      else if (typeof state.roche.back === "function") state.roche.back();
      else window.history.back();
    } else {
      window.history.back();
    }
  }

  function renderHeader(title, leftText, leftAction, rightText, rightAction) {
    var header = document.createElement("div");
    header.className = "mcp-header";

    var leftBtn = document.createElement("div");
    leftBtn.className = "mcp-header-btn";
    leftBtn.innerText = leftText || "";
    if (leftAction) leftBtn.onclick = leftAction;

    var titleEl = document.createElement("div");
    titleEl.className = "mcp-title";
    titleEl.innerText = title;

    var rightBtn = document.createElement("div");
    rightBtn.className = "mcp-header-btn";
    rightBtn.innerText = rightText || "";
    if (rightAction) rightBtn.onclick = rightAction;

    header.appendChild(leftBtn);
    header.appendChild(titleEl);
    header.appendChild(rightBtn);
    return header;
  }

  function renderChat() {
    state.container.innerHTML = "";
    // 首页：左上角返回退出插件，右上角进入设置
    var header = renderHeader("MCP 工具桥", "返回", exitPlugin, "设置", function() {
      state.view = "settings";
      render();
    });
    
    var content = document.createElement("div");
    content.className = "mcp-content";
    content.innerHTML = "<div class='mcp-empty'>欢迎使用 MCP 工具桥。<br>请点击右上角配置您的 MCP 服务器。</div>";

    state.container.appendChild(header);
    state.container.appendChild(content);
  }

  function renderSettings() {
    state.container.innerHTML = "";
    var header = renderHeader("MCP 服务器管理", "返回", function() {
      state.view = "chat";
      render();
    }, "添加", function() {
      state.editingServer = null;
      state.view = "editServer";
      render();
    });

    var content = document.createElement("div");
    content.className = "mcp-content";

    if (state.configs.length === 0) {
      content.innerHTML = "<div class='mcp-empty'>暂无配置的服务器</div>";
    } else {
      var card = document.createElement("div");
      card.className = "mcp-card";

      state.configs.forEach(function(cfg) {
        var item = document.createElement("div");
        item.className = "mcp-server-item";

        var srvInstance = state.servers.find(s => s.id === cfg.id);
        var statusText = srvInstance && srvInstance.status === "connected" ? "🟢 已连接" : "🔴 未连接";

        item.innerHTML = `
          <div class="mcp-server-info">
            <div class="mcp-server-name">${cfg.name}</div>
            <div class="mcp-server-url">${cfg.url} <span style="font-size:10px; margin-left:6px;">${statusText}</span></div>
          </div>
        `;

        var actions = document.createElement("div");
        actions.className = "mcp-server-actions";

        var editBtn = document.createElement("button");
        editBtn.className = "mcp-btn-edit";
        editBtn.innerText = "编辑";
        editBtn.onclick = function() {
          state.editingServer = cfg.id;
          state.view = "editServer";
          render();
        };

        // 修复 Bug 4: 增加删除按钮
        var delBtn = document.createElement("button");
        delBtn.className = "mcp-btn-del";
        delBtn.innerText = "删除";
        delBtn.onclick = function() {
          if (confirm("确定要删除服务器 [" + cfg.name + "] 吗？")) {
            disconnectServer(cfg.id);
            state.configs = state.configs.filter(c => c.id !== cfg.id);
            saveConfig(); // 持久化删除操作
            render();
          }
        };

        actions.appendChild(editBtn);
        actions.appendChild(delBtn);
        item.appendChild(actions);
        card.appendChild(item);
      });
      content.appendChild(card);
    }

    state.container.appendChild(header);
    state.container.appendChild(content);
  }

  function renderEditServer() {
    state.container.innerHTML = "";
    var isNew = !state.editingServer;
    var currentConfig = isNew ? null : state.configs.find(c => c.id === state.editingServer);

    var content = document.createElement("div");
    content.className = "mcp-content";

    var card = document.createElement("div");
    card.className = "mcp-card";

    card.innerHTML = `
      <div class="mcp-input-group">
        <label class="mcp-label">服务器名称</label>
        <input type="text" class="mcp-input" id="mcp-input-name" placeholder="例如: 本地文件工具" value="${currentConfig ? currentConfig.name : ''}">
      </div>
      <div class="mcp-input-group">
        <label class="mcp-label">SSE URL地址</label>
        <input type="text" class="mcp-input" id="mcp-input-url" placeholder="http://localhost:3000/sse" value="${currentConfig ? currentConfig.url : ''}">
      </div>
    `;
    content.appendChild(card);

    var header = renderHeader(isNew ? "添加服务器" : "编辑服务器", "取消", function() {
      state.view = "settings";
      render();
    }, "保存", function() {
      var newName = document.getElementById("mcp-input-name").value.trim();
      var newUrl = document.getElementById("mcp-input-url").value.trim();

      if (!newName || !newUrl) {
        alert("名称和URL不能为空");
        return;
      }

      var targetId = isNew ? uuid() : state.editingServer;
      var newConfig = {
        id: targetId,
        name: newName,
        url: newUrl,
        enabled: true
      };

      if (isNew) {
        state.configs.push(newConfig);
        connectServer(newConfig);
      } else {
        var idx = state.configs.findIndex(c => c.id === targetId);
        var oldConfig = state.configs[idx];
        state.configs[idx] = newConfig;

        // 修复 Bug 2: 如果仅仅是修改了名称，不要断开连接；只有 URL 变动时才重连。
        if (oldConfig.url !== newConfig.url) {
          disconnectServer(targetId);
          connectServer(newConfig);
        } else {
          // 只更新内存中正在运行的服务器名称展示
          var runningSrv = state.servers.find(s => s.id === targetId);
          if (runningSrv) runningSrv.name = newConfig.name;
        }
      }

      saveConfig(); // 修复 Bug 3: 执行保存数据持久化
      state.view = "settings";
      render();
    });

    state.container.appendChild(header);
    state.container.appendChild(content);
  }

  function render() {
    if (!state.container) return;
    state.container.className = "mcp-bridge-container";
    
    if (state.view === "chat") {
      renderChat();
    } else if (state.view === "settings") {
      renderSettings();
    } else if (state.view === "editServer") {
      renderEditServer();
    }
  }

  /* ============================================================
     插件注册挂载入口
     ============================================================ */
  var RochePlugin = window.RochePlugin || {
    register: function (pluginData) {
      if (typeof window !== "undefined") {
        window.mcpBridgePlugin = pluginData; // 挂载到全局供宿主读取
      }
    }
  };

  RochePlugin.register({
    id: "mcp-bridge",
    name: "MCP 工具桥",
    version: "1.0.1",
    apps: [
      {
        id: "mcp-bridge-main",
        name: "MCP 工具桥",
        icon: "zap",
        mount: async function (container, roche) {
          state.roche = roche;
          state.container = container;
          state.view = "chat";
          state.editingServer = undefined;

          // 注入样式
          var oldStyle = document.querySelector("style[data-mcp-bridge]");
          if (oldStyle) oldStyle.parentNode.removeChild(oldStyle);
          var styleEl = document.createElement("style");
          styleEl.textContent = getStyles();
          styleEl.setAttribute("data-mcp-bridge", "1");
          document.head.appendChild(styleEl);
          state.styleEl = styleEl;

          // 等待配置加载后再连接
          await loadConfig(roche);

          // 自动连接处于开启状态的服务器
          state.configs.forEach(function (cfg) {
            if (cfg.enabled !== false) connectServer(cfg);
          });

          render();
        },
        unmount: function (container) {
          // 退出时断开所有连接清理资源
          state.servers.forEach(function (s) { s.disconnect(); });
          state.servers = [];

          if (state.styleEl && state.styleEl.parentNode) {
            state.styleEl.parentNode.removeChild(state.styleEl);
          }
          container.innerHTML = "";
        }
      }
    ]
  });

})();
