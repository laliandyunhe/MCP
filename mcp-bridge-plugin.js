(function () {
  "use strict";

  /* ============================================================
     MCP Client Core（内嵌，无需额外文件）
     ============================================================ */
  var MCPClient = (function () {
    function uuid() {
      return "xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx".replace(/[xy]/g, function (c) {
        var r = (Math.random() * 16) | 0;
        return (c === "x" ? r : (r & 0x3) | 0x8).toString(16);
      });
    }

    function MCPConnection(config) {
      this.id = uuid();
      this.name = config.name || "未命名服务器";
      this.url = config.url;
      this.transport = config.transport || "streamable-http";
      this.headers = config.headers || {};
      this.sessionId = null;
      this.tools = [];
      this.initialized = false;
      this._requestId = 0;
      this._pendingRequests = {};
      this._sseSource = null;
    }

    MCPConnection.prototype._nextId = function () { return ++this._requestId; };

    MCPConnection.prototype._buildHeaders = function (extra) {
      var h = Object.assign({ "Content-Type": "application/json", "Accept": "application/json, text/event-stream" }, this.headers, extra || {});
      if (this.sessionId) h["Mcp-Session-Id"] = this.sessionId;
      return h;
    };

    MCPConnection.prototype._parseSSEResponse = async function (resp) {
      var reader = resp.body.getReader();
      var decoder = new TextDecoder();
      var buffer = "", lastResult = null;
      while (true) {
        var chunk = await reader.read();
        if (chunk.done) break;
        buffer += decoder.decode(chunk.value, { stream: true });
        var lines = buffer.split("\n");
        buffer = lines.pop();
        for (var i = 0; i < lines.length; i++) {
          var line = lines[i].trim();
          if (line.indexOf("data:") === 0) {
            var dataStr = line.substring(5).trim();
            if (!dataStr || dataStr === "[DONE]") continue;
            try {
              var parsed = JSON.parse(dataStr);
              if (parsed.error) throw new Error("MCP错误: " + (parsed.error.message || JSON.stringify(parsed.error)));
              if (parsed.result !== undefined) lastResult = parsed.result;
            } catch (e) { if (e.message && e.message.indexOf("MCP错误") === 0) throw e; }
          }
        }
      }
      return lastResult;
    };

    MCPConnection.prototype._rpcHTTP = async function (method, params) {
      var body = { jsonrpc: "2.0", id: this._nextId(), method: method, params: params || {} };
      var resp = await fetch(this.url, { method: "POST", headers: this._buildHeaders(), body: JSON.stringify(body) });
      var sid = resp.headers.get("Mcp-Session-Id");
      if (sid) this.sessionId = sid;
      if (!resp.ok) { var t = await resp.text().catch(function(){return "";}); throw new Error("HTTP " + resp.status + ": " + t.substring(0, 200)); }
      var ct = resp.headers.get("Content-Type") || "";
      if (ct.indexOf("text/event-stream") !== -1) return await this._parseSSEResponse(resp);
      var json = await resp.json();
      if (json.error) throw new Error("MCP错误: " + (json.error.message || JSON.stringify(json.error)));
      return json.result;
    };

    MCPConnection.prototype._rpcSSE = function (method, params) {
      var self = this;
      return new Promise(function (resolve, reject) {
        if (!self._sseSource) { reject(new Error("SSE未连接")); return; }
        var id = self._nextId();
        self._pendingRequests[id] = { resolve: resolve, reject: reject };
        setTimeout(function () {
          if (self._pendingRequests[id]) { delete self._pendingRequests[id]; reject(new Error("请求超时: " + method)); }
        }, 30000);
        fetch(self.url, { method: "POST", headers: self._buildHeaders(), body: JSON.stringify({ jsonrpc: "2.0", id: id, method: method, params: params || {} }) })
          .catch(function (e) { delete self._pendingRequests[id]; reject(e); });
      });
    };

    MCPConnection.prototype._request = function (method, params) {
      return this.transport === "sse" ? this._rpcSSE(method, params) : this._rpcHTTP(method, params);
    };

    MCPConnection.prototype.connect = async function () {
      if (this.transport === "sse") {
        var self = this;
        await new Promise(function (resolve, reject) {
          var es = new EventSource(self.url);
          self._sseSource = es;
          es.onopen = resolve;
          es.onerror = function () { reject(new Error("SSE连接失败")); };
          es.onmessage = function (evt) {
            try {
              var msg = JSON.parse(evt.data);
              if (msg.id && self._pendingRequests[msg.id]) {
                var p = self._pendingRequests[msg.id]; delete self._pendingRequests[msg.id];
                msg.error ? p.reject(new Error(msg.error.message)) : p.resolve(msg.result);
              }
            } catch (e) {}
          };
        });
      }
      await this._request("initialize", { protocolVersion: "2024-11-05", capabilities: {}, clientInfo: { name: "roche-mcp-bridge", version: "1.0.0" } });
      this.initialized = true;
      fetch(this.url, { method: "POST", headers: this._buildHeaders(), body: JSON.stringify({ jsonrpc: "2.0", method: "notifications/initialized", params: {} }) }).catch(function(){});
      await this.refreshTools();
    };

    MCPConnection.prototype.refreshTools = async function () {
      var r = await this._request("tools/list", {});
      this.tools = (r && r.tools) || [];
      return this.tools;
    };

    MCPConnection.prototype.callTool = async function (name, args) {
      return await this._request("tools/call", { name: name, arguments: args || {} });
    };

    MCPConnection.prototype.disconnect = function () {
      if (this._sseSource) { this._sseSource.close(); this._sseSource = null; }
      this.initialized = false;
    };

    MCPConnection.prototype.describeTools = function () {
      if (!this.tools.length) return "";
      return this.tools.map(function (t) {
        var schema = t.inputSchema || {};
        var props = schema.properties || {};
        var required = schema.required || [];
        var paramDesc = Object.keys(props).map(function (k) {
          var p = props[k];
          var req = required.indexOf(k) !== -1 ? "必填" : "可选";
          return k + "(" + (p.type || "string") + ", " + req + "): " + (p.description || "");
        }).join("; ");
        return "[" + t.name + "] " + (t.description || "") + (paramDesc ? "\n  参数: " + paramDesc : "");
      }).join("\n");
    };

    return { MCPConnection: MCPConnection };
  })();

  /* ============================================================
     插件状态
     ============================================================ */
  var state = {
    roche: null,
    styleEl: null,
    container: null,
    servers: [],        // MCPConnection[]
    configs: [],        // 服务器配置数组（持久化）
    mode: "manual",     // 'manual' | 'auto'
    showProcess: true,  // 是否在气泡中显示工具调用过程
    view: "chat",       // 'chat' | 'settings'
    connecting: {},     // serverId -> bool
    errors: {},         // serverId -> string
  };

 /* ============================================================
     持久化
     ============================================================ */
  var STORAGE_KEY = "mcp-bridge-v1";

  function loadConfig(roche, callback) {
    if (roche && roche.storage) {
      roche.storage.get(STORAGE_KEY).then(function(data) {
        if (data) {
          try {
            var parsed = typeof data === "string" ? JSON.parse(data) : data;
            state.configs = parsed.configs || [];
            state.mode = parsed.mode || "manual";
            state.showProcess = parsed.showProcess !== false;
          } catch(e) {}
        }
        if (callback) callback();
      }).catch(function() {
        if (callback) callback();
      });
    } else {
      if (callback) callback();
    }
  }

  function saveConfig(roche) {
    if (roche && roche.storage) {
      roche.storage.set(STORAGE_KEY, {
        configs: state.configs,
        mode: state.mode,
        showProcess: state.showProcess,
      });
    }
  }


  /* ============================================================
     样式
     ============================================================ */
  function getStyles() {
    return `
      /* ── 根容器 ── */
      .mcp-bridge { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", "PingFang SC", "Hiragino Sans GB", sans-serif; font-size: 15px; color: #1a1a1a; height: 100%; display: flex; flex-direction: column; background: #f2f2f7; }

      /* ── 导航栏 ── */
      .mcp-navbar { display: flex; align-items: center; padding: 12px 8px 0; background: #f2f2f7; flex-shrink: 0; gap: 4px; }
      .mcp-navbar-back { background: none; border: none; cursor: pointer; padding: 6px 8px; color: #3478f6; font-size: 17px; display: flex; align-items: center; gap: 2px; white-space: nowrap; }
      .mcp-navbar-back svg { width: 20px; height: 20px; }
      .mcp-navbar-title { flex: 1; text-align: center; font-size: 17px; font-weight: 600; color: #000; }
      .mcp-navbar-action { background: none; border: none; cursor: pointer; padding: 6px 8px; color: #3478f6; font-size: 17px; white-space: nowrap; }

      /* ── Tab 栏（基础设置 / 工具）── */
      .mcp-tabbar { display: flex; margin: 12px 16px 0; background: #e4e4eb; border-radius: 9px; padding: 2px; flex-shrink: 0; }
      .mcp-tab { flex: 1; padding: 7px 0; border: none; background: transparent; border-radius: 7px; font-size: 13px; font-weight: 500; color: #666; cursor: pointer; transition: all 0.18s; }
      .mcp-tab.active { background: #fff; color: #000; font-weight: 600; box-shadow: 0 1px 4px rgba(0,0,0,0.13); }

      /* ── 滚动内容区 ── */
      .mcp-scroll { flex: 1; overflow-y: auto; padding: 18px 16px 32px; display: flex; flex-direction: column; gap: 24px; }

      /* ── 区块（每个设置分组）── */
      .mcp-section {}
      .mcp-section-label { font-size: 13px; color: #6b6b6b; margin-bottom: 6px; padding-left: 4px; }
      .mcp-section-hint { font-size: 12px; color: #9a9a9a; margin-top: 4px; padding-left: 4px; }
      .mcp-card { background: #fff; border-radius: 12px; overflow: hidden; }

      /* ── 开关行 ── */
      .mcp-row { display: flex; align-items: center; padding: 13px 16px; border-bottom: 0.5px solid #e8e8ed; }
      .mcp-row:last-child { border-bottom: none; }
      .mcp-row-label { flex: 1; font-size: 17px; color: #000; }
      .mcp-row-sublabel { font-size: 13px; color: #8e8e93; margin-top: 2px; }
      .mcp-row-value { font-size: 17px; color: #8e8e93; }

      /* iOS 风格开关 */
      .mcp-ios-switch { position: relative; width: 51px; height: 31px; flex-shrink: 0; }
      .mcp-ios-switch input { opacity: 0; width: 0; height: 0; position: absolute; }
      .mcp-ios-track { position: absolute; inset: 0; border-radius: 31px; background: #e5e5ea; cursor: pointer; transition: background 0.22s; }
      .mcp-ios-switch input:checked + .mcp-ios-track { background: #34c759; }
      .mcp-ios-track::after { content: ""; position: absolute; width: 27px; height: 27px; border-radius: 50%; background: #fff; top: 2px; left: 2px; transition: transform 0.22s; box-shadow: 0 2px 5px rgba(0,0,0,0.25); }
      .mcp-ios-switch input:checked + .mcp-ios-track::after { transform: translateX(20px); }

      /* ── 文本输入 ── */
      .mcp-input-row { display: flex; align-items: center; padding: 0 16px; border-bottom: 0.5px solid #e8e8ed; min-height: 44px; }
      .mcp-input-row:last-child { border-bottom: none; }
      .mcp-input-row-label { font-size: 17px; color: #000; width: 80px; flex-shrink: 0; }
      .mcp-ios-input { flex: 1; border: none; outline: none; font-size: 17px; color: #000; font-family: inherit; background: transparent; padding: 12px 0; }
      .mcp-ios-input::placeholder { color: #c7c7cc; }

      /* 纯文本输入框（URL 等独占一行） */
      .mcp-full-input-wrap { padding: 0 16px; }
      .mcp-full-input { width: 100%; box-sizing: border-box; border: none; outline: none; font-size: 15px; color: #000; font-family: inherit; padding: 12px 0; background: transparent; }
      .mcp-full-input::placeholder { color: #c7c7cc; }

      /* ── 传输类型选择器（分段控件）── */
      .mcp-segment { display: flex; border-radius: 9px; background: #e4e4eb; padding: 2px; margin: 0; }
      .mcp-segment-btn { flex: 1; padding: 7px 0; border: none; background: transparent; border-radius: 7px; font-size: 13px; font-weight: 500; color: #555; cursor: pointer; transition: all 0.18s; display: flex; align-items: center; justify-content: center; gap: 5px; }
      .mcp-segment-btn.active { background: #fff; color: #000; font-weight: 600; box-shadow: 0 1px 4px rgba(0,0,0,0.13); }
      .mcp-segment-btn svg { width: 14px; height: 14px; }

      /* ── 请求头列表 ── */
      .mcp-header-item { display: flex; align-items: center; padding: 10px 16px; gap: 8px; border-bottom: 0.5px solid #e8e8ed; }
      .mcp-header-item:last-child { border-bottom: none; }
      .mcp-header-k { flex: 1; border: none; outline: none; font-size: 15px; font-family: inherit; color: #000; background: transparent; }
      .mcp-header-k::placeholder { color: #c7c7cc; }
      .mcp-header-sep { color: #d1d1d6; font-size: 18px; }
      .mcp-header-v { flex: 2; border: none; outline: none; font-size: 15px; font-family: inherit; color: #8e8e93; background: transparent; }
      .mcp-header-v::placeholder { color: #c7c7cc; }
      .mcp-header-del { background: none; border: none; cursor: pointer; color: #ff3b30; padding: 2px 4px; font-size: 20px; line-height: 1; flex-shrink: 0; }

      /* ── 添加按钮（大圆角）── */
      .mcp-add-big-btn { width: 100%; padding: 14px; background: #3478f6; color: #fff; border: none; border-radius: 12px; font-size: 17px; font-weight: 600; cursor: pointer; display: flex; align-items: center; justify-content: center; gap: 8px; transition: opacity 0.15s; }
      .mcp-add-big-btn:hover { opacity: 0.88; }
      .mcp-add-big-btn svg { width: 18px; height: 18px; }

      /* ── 保存按钮（右上角文字）── */
      .mcp-save-text-btn { background: none; border: none; cursor: pointer; color: #3478f6; font-size: 17px; font-weight: 500; padding: 6px 8px; }

      /* ── 服务器卡片（列表视图）── */
      .mcp-server-card { display: flex; align-items: center; padding: 13px 16px; border-bottom: 0.5px solid #e8e8ed; gap: 12px; }
      .mcp-server-card:last-child { border-bottom: none; }
      .mcp-server-dot { width: 10px; height: 10px; border-radius: 50%; flex-shrink: 0; }
      .mcp-server-dot.ok { background: #34c759; }
      .mcp-server-dot.err { background: #ff3b30; }
      .mcp-server-dot.connecting { background: #ff9500; animation: mcp-pulse 1s infinite; }
      .mcp-server-dot.idle { background: #c7c7cc; }
      @keyframes mcp-pulse { 0%,100%{opacity:1}50%{opacity:0.3} }
      .mcp-server-card-info { flex: 1; min-width: 0; }
      .mcp-server-card-name { font-size: 17px; color: #000; }
      .mcp-server-card-meta { font-size: 13px; color: #8e8e93; margin-top: 2px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
      .mcp-server-card-actions { display: flex; gap: 8px; }
      .mcp-icon-btn { background: none; border: none; cursor: pointer; color: #8e8e93; padding: 4px; border-radius: 8px; display: flex; align-items: center; }
      .mcp-icon-btn svg { width: 18px; height: 18px; }
      .mcp-icon-btn:hover { color: #3478f6; }

      /* ── 工具面板 ── */
      .mcp-tool-section-title { font-size: 13px; color: #6b6b6b; margin-bottom: 6px; padding-left: 4px; }
      .mcp-tool-card { background: #fff; border-radius: 12px; overflow: hidden; margin-bottom: 10px; }
      .mcp-tool-header { display: flex; align-items: center; gap: 10px; padding: 13px 16px; cursor: pointer; }
      .mcp-tool-header:active { background: #f5f5f5; }
      .mcp-tool-name { flex: 1; font-size: 17px; font-weight: 500; }
      .mcp-tool-desc { font-size: 13px; color: #8e8e93; margin-top: 2px; }
      .mcp-tool-chevron { color: #c7c7cc; transition: transform 0.2s; }
      .mcp-tool-chevron.open { transform: rotate(180deg); }
      .mcp-tool-body { display: none; border-top: 0.5px solid #e8e8ed; padding: 12px 16px; }
      .mcp-tool-body.open { display: block; }
      .mcp-param-label { font-size: 12px; font-weight: 600; color: #6b6b6b; margin: 8px 0 4px; display: flex; align-items: center; gap: 5px; }
      .mcp-required-badge { font-size: 10px; background: #ffeaea; color: #ff3b30; border-radius: 4px; padding: 1px 5px; }
      .mcp-param-input { width: 100%; box-sizing: border-box; padding: 9px 12px; border: 0.5px solid #e8e8ed; border-radius: 9px; font-size: 15px; outline: none; font-family: inherit; resize: vertical; min-height: 38px; background: #fafafa; }
      .mcp-param-input:focus { border-color: #3478f6; background: #fff; }
      .mcp-call-btn { margin-top: 12px; width: 100%; padding: 10px; background: #3478f6; color: #fff; border: none; border-radius: 10px; font-size: 15px; font-weight: 600; cursor: pointer; }
      .mcp-call-btn:disabled { opacity: 0.5; cursor: not-allowed; }

      /* ── 工具调用结果气泡 ── */
      .mcp-result-bubble { margin: 6px 0; padding: 10px 14px; background: #eef2ff; border-left: 3px solid #3478f6; border-radius: 0 10px 10px 0; font-size: 13px; }
      .mcp-result-bubble-header { font-weight: 600; color: #3478f6; margin-bottom: 5px; }
      .mcp-result-bubble-args { font-size: 11px; color: #3478f6; opacity: 0.7; margin-bottom: 4px; }
      .mcp-result-bubble-body { color: #374151; white-space: pre-wrap; word-break: break-word; max-height: 200px; overflow-y: auto; }
      .mcp-result-bubble.error { background: #fff1f0; border-left-color: #ff3b30; }
      .mcp-result-bubble.error .mcp-result-bubble-header { color: #ff3b30; }

      /* ── 模式切换栏 ── */
      .mcp-modebar { display: flex; align-items: center; gap: 10px; padding: 10px 16px; background: #fff; border-bottom: 0.5px solid #e8e8ed; flex-shrink: 0; }
      .mcp-mode-label { font-size: 13px; color: #8e8e93; }
      .mcp-mode-toggle { display: flex; background: #e4e4eb; border-radius: 9px; padding: 2px; }
      .mcp-mode-btn { padding: 5px 14px; border-radius: 7px; border: none; cursor: pointer; font-size: 13px; font-weight: 500; background: transparent; color: #555; transition: all 0.18s; }
      .mcp-mode-btn.active { background: #fff; color: #000; font-weight: 600; box-shadow: 0 1px 4px rgba(0,0,0,0.13); }

      /* ── 自动模式提示 ── */
      .mcp-auto-hint { flex: 1; display: flex; flex-direction: column; align-items: center; justify-content: center; gap: 10px; color: #8e8e93; padding: 30px 20px; text-align: center; }
      .mcp-auto-hint svg { width: 44px; height: 44px; color: #3478f6; opacity: 0.5; }
      .mcp-auto-hint-title { font-size: 17px; font-weight: 600; color: #555; }
      .mcp-auto-hint-desc { font-size: 14px; line-height: 1.6; color: #8e8e93; }

      /* ── 空状态 ── */
      .mcp-empty { display: flex; flex-direction: column; align-items: center; justify-content: center; gap: 10px; flex: 1; color: #c7c7cc; padding: 30px; text-align: center; }
      .mcp-empty svg { width: 44px; height: 44px; opacity: 0.35; }
      .mcp-empty-title { font-size: 17px; font-weight: 600; color: #8e8e93; }
      .mcp-empty-desc { font-size: 14px; color: #c7c7cc; }

      /* ── 服务器列表（主界面）── */
      .mcp-server-list { padding: 10px 16px; display: flex; flex-direction: column; gap: 8px; flex-shrink: 0; }
      .mcp-server-chip { display: flex; align-items: center; gap: 10px; padding: 10px 14px; background: #fff; border-radius: 12px; }
      .mcp-dot { width: 9px; height: 9px; border-radius: 50%; flex-shrink: 0; }
      .mcp-dot.ok { background: #34c759; }
      .mcp-dot.err { background: #ff3b30; }
      .mcp-dot.connecting { background: #ff9500; animation: mcp-pulse 1s infinite; }
      .mcp-dot.idle { background: #c7c7cc; }
      .mcp-chip-name { flex: 1; font-size: 15px; font-weight: 500; }
      .mcp-chip-count { font-size: 13px; color: #8e8e93; }

      /* ── 动画 ── */
      @keyframes mcp-spin { to { transform: rotate(360deg); } }
      .mcp-spin { animation: mcp-spin 0.8s linear infinite; display: inline-block; }
    `;
  }

  /* ============================================================
     SVG 图标
     ============================================================ */
  var I = {
    settings: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="3"/><path d="M19.4 15a1.65 1.65 0 0 0 .33 1.82l.06.06a2 2 0 0 1-2.83 2.83l-.06-.06a1.65 1.65 0 0 0-1.82-.33 1.65 1.65 0 0 0-1 1.51V21a2 2 0 0 1-4 0v-.09A1.65 1.65 0 0 0 9 19.4a1.65 1.65 0 0 0-1.82.33l-.06.06a2 2 0 0 1-2.83-2.83l.06-.06A1.65 1.65 0 0 0 4.68 15a1.65 1.65 0 0 0-1.51-1H3a2 2 0 0 1 0-4h.09A1.65 1.65 0 0 0 4.6 9a1.65 1.65 0 0 0-.33-1.82l-.06-.06a2 2 0 0 1 2.83-2.83l.06.06A1.65 1.65 0 0 0 9 4.68a1.65 1.65 0 0 0 1-1.51V3a2 2 0 0 1 4 0v.09a1.65 1.65 0 0 0 1 1.51 1.65 1.65 0 0 0 1.82-.33l.06-.06a2 2 0 0 1 2.83 2.83l-.06.06A1.65 1.65 0 0 0 19.4 9a1.65 1.65 0 0 0 1.51 1H21a2 2 0 0 1 0 4h-.09a1.65 1.65 0 0 0-1.51 1z"/></svg>',
    back: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polyline points="15 18 9 12 15 6"/></svg>',
    add: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><line x1="12" y1="5" x2="12" y2="19"/><line x1="5" y1="12" x2="19" y2="12"/></svg>',
    trash: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polyline points="3 6 5 6 21 6"/><path d="M19 6v14a2 2 0 0 1-2 2H7a2 2 0 0 1-2-2V6m3 0V4a1 1 0 0 1 1-1h4a1 1 0 0 1 1 1v2"/></svg>',
    refresh: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polyline points="23 4 23 10 17 10"/><path d="M20.49 15a9 9 0 1 1-2.12-9.36L23 10"/></svg>',
    plug: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M18 6L6 18"/><path d="m6 6 12 12"/><path d="M9 3v4"/><path d="M15 3v4"/><path d="M9 17v4"/><path d="M15 17v4"/><rect x="5" y="7" width="14" height="10" rx="2"/></svg>',
    zap: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polygon points="13 2 3 14 12 14 11 22 21 10 12 10 13 2"/></svg>',
    chevDown: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polyline points="6 9 12 15 18 9"/></svg>',
    spin: '<svg class="mcp-spin" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round"><path d="M12 2a10 10 0 0 1 0 20"/></svg>',
  };

  /* ============================================================
     渲染
     ============================================================ */
  function render() {
    if (!state.container) return;
    state.container.innerHTML = "";

    if (state.editingServer !== undefined) {
      // 表单视图自带完整 navbar+tabbar，直接挂载
      state.container.appendChild(renderServerForm());
      return;
    }

    var root = document.createElement("div");
    root.className = "mcp-bridge";

    if (state.view === "settings") {
      root.appendChild(renderTopbar(true));
      root.appendChild(renderSettings());
    } else {
      root.appendChild(renderTopbar(false));
      root.appendChild(renderModebar());
      root.appendChild(renderServerChips());
      if (state.mode === "manual") {
        root.appendChild(renderToolPanel());
      } else {
        root.appendChild(renderAutoHint());
      }
    }

    state.container.appendChild(root);
  }

  /* 顶栏 */
  function renderTopbar(isSettings) {
    var el = document.createElement("div");
    el.className = "mcp-navbar";
    el.style.cssText = "display:flex;align-items:center;padding:12px 8px 8px;background:#f2f2f7;flex-shrink:0;gap:4px;border-bottom:0.5px solid #e8e8ed;";

    if (isSettings) {
      // 设置页：左侧返回主界面
      var backBtn = document.createElement("button");
      backBtn.className = "mcp-navbar-back";
      backBtn.innerHTML = I.back + " 返回";
      backBtn.onclick = function () { state.view = "chat"; render(); };
      el.appendChild(backBtn);
    } else {
      // 主界面：左侧「关闭插件」返回软件本体
      var closeBtn = document.createElement("button");
      closeBtn.className = "mcp-navbar-back";
      closeBtn.innerHTML = I.back + " 返回";
      closeBtn.title = "返回软件";
      closeBtn.onclick = function () {
        if (isSettings) {
          // 设置页：左侧返回主界面
          var backBtn = document.createElement("button");
          backBtn.className = "mcp-navbar-back";
          backBtn.innerHTML = I.back + " 返回";
          backBtn.onclick = function () { state.view = "chat"; render(); };
          el.appendChild(backBtn);
        } else {
          // 主界面：左侧「关闭插件」返回软件本体
          var closeBtn = document.createElement("button");
          closeBtn.className = "mcp-navbar-back";
          closeBtn.innerHTML = I.back + " 返回";
          closeBtn.title = "返回软件";
          closeBtn.onclick = function () {
            // 调用 Roche 正确的退出插件方法
            if (state.roche && state.roche.ui && typeof state.roche.ui.closeApp === "function") {
              state.roche.ui.closeApp();
            } else {
              window.history.back();
            }
          };
          el.appendChild(closeBtn);
        }

    var navTitle = document.createElement("div");
    navTitle.className = "mcp-navbar-title";
    navTitle.textContent = isSettings ? "MCP 设置" : "MCP 工具桥";
    el.appendChild(navTitle);

    if (!isSettings) {
      var settingsBtn = document.createElement("button");
      settingsBtn.className = "mcp-save-text-btn";
      settingsBtn.textContent = "设置";
      settingsBtn.onclick = function () { state.view = "settings"; render(); };
      el.appendChild(settingsBtn);
    } else {
      // 占位保持对称
      var placeholder = document.createElement("div");
      placeholder.style.width = "52px";
      el.appendChild(placeholder);
    }

    return el;
  }

  /* 模式栏 */
  function renderModebar() {
    var el = document.createElement("div");
    el.className = "mcp-modebar";

    var label = document.createElement("span");
    label.className = "mcp-mode-label";
    label.textContent = "模式";
    el.appendChild(label);

    var toggle = document.createElement("div");
    toggle.className = "mcp-mode-toggle";

    [["manual", "手动调用"], ["auto", "AI 自主"]].forEach(function (pair) {
      var btn = document.createElement("button");
      btn.className = "mcp-mode-btn" + (state.mode === pair[0] ? " active" : "");
      btn.textContent = pair[1];
      btn.onclick = function () {
        state.mode = pair[0];
        saveConfig(state.roche);
        render();
      };
      toggle.appendChild(btn);
    });

    el.appendChild(toggle);
    return el;
  }

  /* 服务器状态芯片列表 */
  function renderServerChips() {
    var el = document.createElement("div");
    el.className = "mcp-server-list";

    if (!state.configs.length) {
      var hint = document.createElement("div");
      hint.style.cssText = "font-size:12px;color:#aaa;padding:4px 2px;";
      hint.textContent = "暂无服务器，请前往设置添加";
      el.appendChild(hint);
      return el;
    }

    state.configs.forEach(function (cfg) {
      var conn = state.servers.find(function (s) { return s.id === cfg.id; });
      var chip = document.createElement("div");
      chip.className = "mcp-server-chip";

      var dot = document.createElement("div");
      dot.className = "mcp-dot " + (
        state.connecting[cfg.id] ? "connecting" :
        conn && conn.initialized ? "ok" :
        state.errors[cfg.id] ? "err" : "idle"
      );
      chip.appendChild(dot);

      var name = document.createElement("div");
      name.className = "mcp-chip-name";
      name.textContent = cfg.name;
      chip.appendChild(name);

      var count = document.createElement("div");
      count.className = "mcp-chip-count";
      if (state.connecting[cfg.id]) {
        count.innerHTML = I.spin;
      } else if (conn && conn.initialized) {
        count.textContent = conn.tools.length + " 个工具";
      } else if (state.errors[cfg.id]) {
        count.textContent = "连接失败";
        count.style.color = "#ef4444";
      } else {
        var connectBtn = document.createElement("button");
        connectBtn.style.cssText = "font-size:11px;padding:3px 8px;background:#6366f1;color:#fff;border:none;border-radius:6px;cursor:pointer;";
        connectBtn.textContent = "连接";
        connectBtn.onclick = function () { connectServer(cfg); };
        count.appendChild(connectBtn);
      }
      chip.appendChild(count);

      el.appendChild(chip);
    });

    return el;
  }

  /* 手动模式：工具面板 */
  function renderToolPanel() {
    var el = document.createElement("div");
    el.className = "mcp-tool-panel";

    var connectedServers = state.servers.filter(function (s) { return s.initialized && s.tools.length; });

    if (!connectedServers.length) {
      var empty = document.createElement("div");
      empty.className = "mcp-empty";
      empty.innerHTML = I.plug + '<div class="mcp-empty-title">暂无可用工具</div><div class="mcp-empty-desc">连接 MCP 服务器后，工具列表将显示在此处</div>';
      el.appendChild(empty);
      return el;
    }

    connectedServers.forEach(function (server) {
      var serverCfg = state.configs.find(function (c) { return c.id === server.id; });
      var disabledTools = (serverCfg && serverCfg.disabledTools) || [];
      var enabledTools = server.tools.filter(function (t) { return disabledTools.indexOf(t.name) === -1; });
      if (!enabledTools.length) return;

      var section = document.createElement("div");
      section.className = "mcp-tool-section";

      var sTitle = document.createElement("div");
      sTitle.className = "mcp-tool-section-title";
      sTitle.textContent = server.name;
      section.appendChild(sTitle);

      enabledTools.forEach(function (tool) {
        section.appendChild(renderToolCard(server, tool));
      });

      el.appendChild(section);
    });

    return el;
  }

  /* 单个工具卡片 */
  function renderToolCard(server, tool) {
    var card = document.createElement("div");
    card.className = "mcp-tool-card";

    var header = document.createElement("div");
    header.className = "mcp-tool-header";

    var info = document.createElement("div");
    info.style.flex = "1";
    var nameEl = document.createElement("div");
    nameEl.className = "mcp-tool-name";
    nameEl.textContent = tool.name;
    var descEl = document.createElement("div");
    descEl.className = "mcp-tool-desc";
    descEl.textContent = tool.description || "";
    info.appendChild(nameEl);
    info.appendChild(descEl);
    header.appendChild(info);

    var chev = document.createElement("span");
    chev.className = "mcp-tool-chevron";
    chev.innerHTML = I.chevDown;
    header.appendChild(chev);

    var body = document.createElement("div");
    body.className = "mcp-tool-body";

    header.onclick = function () {
      var isOpen = body.classList.toggle("open");
      chev.classList.toggle("open", isOpen);
    };

    // 参数输入
    var schema = tool.inputSchema || {};
    var props = schema.properties || {};
    var required = schema.required || [];
    var paramInputs = {};

    Object.keys(props).forEach(function (key) {
      var prop = props[key];
      var label = document.createElement("div");
      label.className = "mcp-param-label";
      label.textContent = key;
      if (required.indexOf(key) !== -1) {
        var badge = document.createElement("span");
        badge.className = "mcp-required-badge";
        badge.textContent = "必填";
        label.appendChild(badge);
      }
      body.appendChild(label);

      if (prop.description) {
        var hint = document.createElement("div");
        hint.style.cssText = "font-size:11px;color:#aaa;margin-bottom:4px;";
        hint.textContent = prop.description;
        body.appendChild(hint);
      }

      var input = document.createElement("textarea");
      input.className = "mcp-param-input";
      input.rows = prop.type === "object" || prop.type === "array" ? 3 : 1;
      input.placeholder = prop.type === "object" ? '{"key": "value"}' : (prop.examples ? String(prop.examples[0]) : "");
      paramInputs[key] = input;
      body.appendChild(input);
    });

    // 调用按钮
    var callBtn = document.createElement("button");
    callBtn.className = "mcp-call-btn";
    callBtn.textContent = "调用工具";
    callBtn.onclick = async function () {
      callBtn.disabled = true;
      callBtn.innerHTML = I.spin + " 调用中…";

      // 收集参数
      var args = {};
      var valid = true;
      Object.keys(paramInputs).forEach(function (k) {
        var val = paramInputs[k].value.trim();
        if (!val && required.indexOf(k) !== -1) { valid = false; return; }
        if (!val) return;
        try {
          args[k] = JSON.parse(val);
        } catch (e) {
          args[k] = val;
        }
      });

      if (!valid) {
        callBtn.disabled = false;
        callBtn.textContent = "调用工具";
        alert("请填写所有必填参数");
        return;
      }

      try {
        var result = await server.callTool(tool.name, args);
        injectToolResult(server.name, tool.name, args, result, false);
        callBtn.textContent = "✓ 调用成功";
        setTimeout(function () { callBtn.disabled = false; callBtn.textContent = "调用工具"; }, 2000);
      } catch (e) {
        injectToolResult(server.name, tool.name, args, e.message, true);
        callBtn.disabled = false;
        callBtn.textContent = "调用工具";
      }
    };
    body.appendChild(callBtn);

    card.appendChild(header);
    card.appendChild(body);
    return card;
  }

  /* AI 自主模式提示 */
  function renderAutoHint() {
    var el = document.createElement("div");
    el.className = "mcp-auto-hint";
    el.innerHTML = I.zap + '<div class="mcp-auto-hint-title">AI 自主模式已启用</div><div class="mcp-auto-hint-desc">每次发送消息时，AI 会自动判断是否需要调用工具。工具调用过程和结果将以气泡形式显示在对话中。</div>';
    return el;
  }

  /* 设置视图 */
  function renderSettings() {
    var el = document.createElement("div");
    el.className = "mcp-settings";

    // 服务器列表区
    var serverSection = document.createElement("div");
    serverSection.className = "mcp-settings-section";
    var serverTitle = document.createElement("div");
    serverTitle.className = "mcp-settings-section-title";
    serverTitle.textContent = "MCP 服务器";
    serverSection.appendChild(serverTitle);

    state.configs.forEach(function (cfg) {
      var conn = state.servers.find(function (s) { return s.id === cfg.id; });
      var item = document.createElement("div");
      item.className = "mcp-server-item";

      var info = document.createElement("div");
      info.className = "mcp-server-item-info";
      var iname = document.createElement("div");
      iname.className = "mcp-server-item-name";
      iname.textContent = cfg.name;
      var iurl = document.createElement("div");
      iurl.className = "mcp-server-item-url";
      iurl.textContent = cfg.url;
      var imeta = document.createElement("div");
      imeta.className = "mcp-server-item-meta";
      imeta.textContent = cfg.transport + (conn && conn.initialized ? " · " + conn.tools.length + " 个工具" : state.errors[cfg.id] ? " · 连接失败" : " · 未连接");
      info.appendChild(iname); info.appendChild(iurl); info.appendChild(imeta);
      item.appendChild(info);

      var acts = document.createElement("div");
      acts.className = "mcp-server-item-actions";

      // 连接/刷新按钮
      var connBtn = document.createElement("button");
      connBtn.className = "mcp-icon-btn";
      connBtn.innerHTML = state.connecting[cfg.id] ? I.spin : I.refresh;
      connBtn.title = conn && conn.initialized ? "刷新工具列表" : "连接";
      connBtn.onclick = function () { connectServer(cfg); };
      acts.appendChild(connBtn);

      // 编辑按钮
      var editBtn = document.createElement("button");
      editBtn.className = "mcp-icon-btn";
      editBtn.innerHTML = I.settings;
      editBtn.title = "编辑";
      editBtn.onclick = function () {
        state.editingServer = Object.assign({}, cfg);
        render();
      };
      acts.appendChild(editBtn);

      // 删除按钮
      var delBtn = document.createElement("button");
      delBtn.className = "mcp-del-btn";
      delBtn.innerHTML = I.trash;
      delBtn.title = "删除";
      delBtn.onclick = function () {
        if (!confirm("确定删除「" + cfg.name + "」？")) return;
        var conn2 = state.servers.find(function (s) { return s.id === cfg.id; });
        if (conn2) { conn2.disconnect(); state.servers = state.servers.filter(function (s) { return s.id !== cfg.id; }); }
        state.configs = state.configs.filter(function (c) { return c.id !== cfg.id; });
        delete state.errors[cfg.id];
        saveConfig(state.roche);
        render();
      };
      acts.appendChild(delBtn);

      item.appendChild(acts);
      serverSection.appendChild(item);
    });

    var addBtn = document.createElement("button");
    addBtn.className = "mcp-add-server-btn";
    addBtn.innerHTML = I.add + " 添加服务器";
    addBtn.onclick = function () {
      state.editingServer = { id: null, name: "", url: "", transport: "streamable-http", headers: [] };
      render();
    };
    serverSection.appendChild(addBtn);
    el.appendChild(serverSection);

    // 行为设置区
    var behaviorSection = document.createElement("div");
    behaviorSection.className = "mcp-settings-section";
    var behaviorTitle = document.createElement("div");
    behaviorTitle.className = "mcp-settings-section-title";
    behaviorTitle.textContent = "行为设置";
    behaviorSection.appendChild(behaviorTitle);

    var showProcessRow = document.createElement("div");
    showProcessRow.className = "mcp-toggle-row";
    showProcessRow.innerHTML = '<div class="mcp-toggle-info"><div class="mcp-toggle-title">显示工具调用过程</div><div class="mcp-toggle-desc">在对话中以气泡显示工具名称、参数和返回结果</div></div>';
    var sw = makeSwitch(state.showProcess, function (v) {
      state.showProcess = v;
      saveConfig(state.roche);
    });
    showProcessRow.appendChild(sw);
    behaviorSection.appendChild(showProcessRow);
    el.appendChild(behaviorSection);

    return el;
  }

  /* 服务器编辑表单（iOS 风格，分「基础设置」/「工具」两个 Tab，和截图一致） */
  function renderServerForm() {
    var cfg = state.editingServer;
    var currentTransport = cfg.transport || "streamable-http";
    var currentEnabled = cfg.enabled !== false; // 默认启用
    var currentTab = state._formTab || "basic"; // 'basic' | 'tools'

    var currentHeaders = Array.isArray(cfg.headers)
      ? cfg.headers.slice()
      : Object.keys(cfg.headers || {}).map(function (k) { return { key: k, val: cfg.headers[k] }; });

    /* ── 根容器 ── */
    var root = document.createElement("div");
    root.className = "mcp-bridge";
    root.style.cssText = "display:flex;flex-direction:column;height:100%;";

    /* ── 导航栏 ── */
    var navbar = document.createElement("div");
    navbar.className = "mcp-navbar";

    var backBtn = document.createElement("button");
    backBtn.className = "mcp-navbar-back";
    backBtn.innerHTML = I.back + " 返回";
    backBtn.onclick = function () { state.editingServer = undefined; state._formTab = undefined; render(); };
    navbar.appendChild(backBtn);

    var navTitle = document.createElement("div");
    navTitle.className = "mcp-navbar-title";
    navTitle.textContent = cfg.id ? "编辑服务器" : "添加服务器";
    navbar.appendChild(navTitle);

    var saveTextBtn = document.createElement("button");
    saveTextBtn.className = "mcp-save-text-btn";
    saveTextBtn.textContent = "保存";
    saveTextBtn.onclick = doSave;
    navbar.appendChild(saveTextBtn);

    root.appendChild(navbar);

    /* ── Tab 栏 ── */
    var tabbar = document.createElement("div");
    tabbar.className = "mcp-tabbar";
    [["basic", "基础设置"], ["tools", "工具"]].forEach(function (pair) {
      var tab = document.createElement("button");
      tab.className = "mcp-tab" + (currentTab === pair[0] ? " active" : "");
      tab.textContent = pair[1];
      tab.onclick = function () {
        state._formTab = pair[0];
        currentTab = pair[0];
        // 切 Tab 时不重新 render 整页，只切换内容区
        tabbar.querySelectorAll(".mcp-tab").forEach(function (t) { t.classList.remove("active"); });
        tab.classList.add("active");
        basicScroll.style.display = pair[0] === "basic" ? "flex" : "none";
        toolsScroll.style.display = pair[0] === "tools" ? "flex" : "none";
      };
      tabbar.appendChild(tab);
    });
    root.appendChild(tabbar);

    /* ══════════════════════════════
       基础设置 Tab
       ══════════════════════════════ */
    var basicScroll = document.createElement("div");
    basicScroll.className = "mcp-scroll";
    basicScroll.style.display = currentTab === "basic" ? "flex" : "none";

    /* 区块 1：启用 */
    var enableSection = document.createElement("div");
    enableSection.className = "mcp-section";
    var enableLabel = document.createElement("div");
    enableLabel.className = "mcp-section-label";
    enableLabel.textContent = "启用";
    var enableHint = document.createElement("div");
    enableHint.className = "mcp-section-hint";
    enableHint.textContent = "是否启用此MCP服务器";
    enableSection.appendChild(enableLabel);
    enableSection.appendChild(enableHint);

    var enableCard = document.createElement("div");
    enableCard.className = "mcp-card";
    var enableRow = document.createElement("div");
    enableRow.className = "mcp-row";
    var enableRowLabel = document.createElement("div");
    enableRowLabel.className = "mcp-row-label";
    enableRowLabel.textContent = "启用";
    var enableSwitch = makeIosSwitch(currentEnabled, function (v) { currentEnabled = v; });
    enableRow.appendChild(enableRowLabel);
    enableRow.appendChild(enableSwitch);
    enableCard.appendChild(enableRow);
    enableSection.appendChild(enableCard);
    basicScroll.appendChild(enableSection);

    /* 区块 2：名称 */
    var nameSection = document.createElement("div");
    nameSection.className = "mcp-section";
    var nameLabel = document.createElement("div");
    nameLabel.className = "mcp-section-label";
    nameLabel.textContent = "名称";
    var nameHint = document.createElement("div");
    nameHint.className = "mcp-section-hint";
    nameHint.textContent = "MCP服务器的显示名称";
    nameSection.appendChild(nameLabel);
    nameSection.appendChild(nameHint);
    var nameCard = document.createElement("div");
    nameCard.className = "mcp-card";
    var nameInputRow = document.createElement("div");
    nameInputRow.className = "mcp-input-row";
    var nameInput = document.createElement("input");
    nameInput.className = "mcp-ios-input";
    nameInput.placeholder = "名称";
    nameInput.value = cfg.name || "";
    nameInputRow.appendChild(nameInput);
    nameCard.appendChild(nameInputRow);
    nameSection.appendChild(nameCard);
    basicScroll.appendChild(nameSection);

    /* 区块 3：传输类型 */
    var transSection = document.createElement("div");
    transSection.className = "mcp-section";
    var transLabel = document.createElement("div");
    transLabel.className = "mcp-section-label";
    transLabel.textContent = "传输类型";
    var transHint = document.createElement("div");
    transHint.className = "mcp-section-hint";
    transHint.textContent = "选择MCP服务器的传输协议类型";
    transSection.appendChild(transLabel);
    transSection.appendChild(transHint);

    var transCard = document.createElement("div");
    transCard.className = "mcp-card";
    transCard.style.padding = "10px 12px";
    var segment = document.createElement("div");
    segment.className = "mcp-segment";

    [["streamable-http", "✓ Streamable HTTP"], ["sse", "SSE"]].forEach(function (pair) {
      var btn = document.createElement("button");
      btn.className = "mcp-segment-btn" + (currentTransport === pair[0] ? " active" : "");
      btn.textContent = pair[1];
      btn.onclick = function () {
        currentTransport = pair[0];
        segment.querySelectorAll(".mcp-segment-btn").forEach(function (b) { b.classList.remove("active"); b.textContent = b.textContent.replace("✓ ", ""); });
        btn.classList.add("active");
        btn.textContent = "✓ " + btn.textContent.replace("✓ ", "");
      };
      segment.appendChild(btn);
    });
    transCard.appendChild(segment);
    transSection.appendChild(transCard);
    basicScroll.appendChild(transSection);

    /* 区块 4：服务器地址 */
    var urlSection = document.createElement("div");
    urlSection.className = "mcp-section";
    var urlLabel = document.createElement("div");
    urlLabel.className = "mcp-section-label";
    urlLabel.textContent = "服务器地址";
    var urlHint = document.createElement("div");
    urlHint.className = "mcp-section-hint";
    urlHint.textContent = currentTransport === "sse" ? "SSE服务器的URL地址" : "流式HTTP服务器的URL地址";
    urlSection.appendChild(urlLabel);
    urlSection.appendChild(urlHint);

    // 监听传输类型变更，实时更新提示
    segment.addEventListener("click", function () {
      urlHint.textContent = currentTransport === "sse" ? "SSE服务器的URL地址" : "流式HTTP服务器的URL地址";
    });

    var urlCard = document.createElement("div");
    urlCard.className = "mcp-card";
    var urlInputRow = document.createElement("div");
    urlInputRow.style.padding = "0 16px";
    var urlInput = document.createElement("input");
    urlInput.className = "mcp-full-input";
    urlInput.placeholder = "URL";
    urlInput.value = cfg.url || "";
    urlInput.type = "url";
    urlInput.autocomplete = "off";
    urlInput.autocapitalize = "none";
    urlInputRow.appendChild(urlInput);
    urlCard.appendChild(urlInputRow);
    urlSection.appendChild(urlCard);
    basicScroll.appendChild(urlSection);

    /* 区块 5：自定义请求头 */
    var headersSection = document.createElement("div");
    headersSection.className = "mcp-section";
    var headersLabel = document.createElement("div");
    headersLabel.className = "mcp-section-label";
    headersLabel.textContent = "自定义请求头";
    var headersHint = document.createElement("div");
    headersHint.className = "mcp-section-hint";
    headersHint.textContent = "为MCP服务器请求添加自定义HTTP头";
    headersSection.appendChild(headersLabel);
    headersSection.appendChild(headersHint);

    var headersCard = document.createElement("div");
    headersCard.className = "mcp-card";

    function renderHeaderItems() {
      headersCard.innerHTML = "";
      currentHeaders.forEach(function (h, idx) {
        var item = document.createElement("div");
        item.className = "mcp-header-item";
        var kInput = document.createElement("input");
        kInput.className = "mcp-header-k";
        kInput.placeholder = "名称（如 Authorization）";
        kInput.value = h.key || "";
        kInput.oninput = function () { currentHeaders[idx].key = kInput.value; };
        var sep = document.createElement("span");
        sep.className = "mcp-header-sep";
        sep.textContent = ":";
        var vInput = document.createElement("input");
        vInput.className = "mcp-header-v";
        vInput.placeholder = "值";
        vInput.value = h.val || "";
        vInput.oninput = function () { currentHeaders[idx].val = vInput.value; };
        var delBtn = document.createElement("button");
        delBtn.className = "mcp-header-del";
        delBtn.textContent = "−";
        delBtn.onclick = function () { currentHeaders.splice(idx, 1); renderHeaderItems(); };
        item.appendChild(kInput); item.appendChild(sep); item.appendChild(vInput); item.appendChild(delBtn);
        headersCard.appendChild(item);
      });
      headersSection.appendChild(headersCard);
    }
    renderHeaderItems();

    var addHeaderBtn = document.createElement("button");
    addHeaderBtn.className = "mcp-add-big-btn";
    addHeaderBtn.innerHTML = I.add + " 添加请求头";
    addHeaderBtn.onclick = function () { currentHeaders.push({ key: "", val: "" }); renderHeaderItems(); };
    headersSection.appendChild(addHeaderBtn);
    basicScroll.appendChild(headersSection);

    /* 底部操作按钮区域 */
    var saveSection = document.createElement("div");
    saveSection.className = "mcp-section";
    saveSection.style.cssText = "display:flex; flex-direction:column; gap:12px; margin-top:10px;";

    var saveBtn = document.createElement("button");
    saveBtn.className = "mcp-add-big-btn";
    saveBtn.style.background = "#34c759";
    saveBtn.innerHTML = "保存配置";
    saveBtn.onclick = doSave;
    saveSection.appendChild(saveBtn);

    // 如果是编辑已有服务器，增加删除按钮
    if (cfg.id) {
      var delBtn = document.createElement("button");
      delBtn.className = "mcp-add-big-btn";
      delBtn.style.background = "#ff3b30";
      delBtn.innerHTML = I.trash + " 删除服务器";
      delBtn.onclick = function () {
        if (!confirm("确定删除「" + cfg.name + "」？")) return;
        var existConn = state.servers.find(function (s) { return s.id === cfg.id; });
        if (existConn) { existConn.disconnect(); state.servers = state.servers.filter(function (s) { return s.id !== cfg.id; }); }
        state.configs = state.configs.filter(function (c) { return c.id !== cfg.id; });
        delete state.errors[cfg.id];
        saveConfig(state.roche);
        state.editingServer = undefined;
        state._formTab = undefined;
        state.view = "settings";
        render();
      };
      saveSection.appendChild(delBtn);
    }

    basicScroll.appendChild(saveSection);

    /* ══════════════════════════════
       工具 Tab（每个工具有启用/禁用开关，底部保存）
       ══════════════════════════════ */
    var toolsScroll = document.createElement("div");
    toolsScroll.className = "mcp-scroll";
    toolsScroll.style.display = currentTab === "tools" ? "flex" : "none";

    var conn = state.servers.find(function (s) { return s.id === cfg.id; });
    if (!conn || !conn.initialized) {
      var emptyTools = document.createElement("div");
      emptyTools.className = "mcp-empty";
      emptyTools.innerHTML = I.plug + '<div class="mcp-empty-title">未连接</div><div class="mcp-empty-desc">保存并连接服务器后，可在此处启用或禁用各工具</div>';
      toolsScroll.appendChild(emptyTools);
    } else if (!conn.tools.length) {
      var noTools = document.createElement("div");
      noTools.className = "mcp-empty";
      noTools.innerHTML = I.plug + '<div class="mcp-empty-title">无可用工具</div><div class="mcp-empty-desc">该服务器未返回任何工具</div>';
      toolsScroll.appendChild(noTools);
    } else {
      // 从 configs 中读取该服务器的禁用工具列表（用本地副本操作，保存时才写回）
      var existingCfgForTools = state.configs.find(function (c) { return c.id === cfg.id; });
      var disabledTools = ((existingCfgForTools && existingCfgForTools.disabledTools) || []).slice();

      var toolsTopSection = document.createElement("div");
      toolsTopSection.className = "mcp-section";
      var toolsSectionLabel = document.createElement("div");
      toolsSectionLabel.className = "mcp-section-label";
      toolsSectionLabel.textContent = "工具列表";
      var toolsSectionHint = document.createElement("div");
      toolsSectionHint.className = "mcp-section-hint";
      toolsSectionHint.textContent = "禁用的工具不会出现在手动调用面板和 AI 自主调用中";
      toolsTopSection.appendChild(toolsSectionLabel);
      toolsTopSection.appendChild(toolsSectionHint);

      var toolsCard = document.createElement("div");
      toolsCard.className = "mcp-card";

      conn.tools.forEach(function (tool, idx) {
        var isEnabled = disabledTools.indexOf(tool.name) === -1;

        var row = document.createElement("div");
        row.style.cssText = "display:flex;flex-direction:column;padding:12px 16px;" +
          (idx < conn.tools.length - 1 ? "border-bottom:0.5px solid #e8e8ed;" : "");

        var rowTop = document.createElement("div");
        rowTop.style.cssText = "display:flex;align-items:center;width:100%;";

        var rowInfo = document.createElement("div");
        rowInfo.style.flex = "1";
        var toolNameEl = document.createElement("div");
        toolNameEl.className = "mcp-row-label";
        toolNameEl.textContent = tool.name;
        rowInfo.appendChild(toolNameEl);
        if (tool.description) {
          var toolDescEl = document.createElement("div");
          toolDescEl.className = "mcp-row-sublabel";
          toolDescEl.textContent = tool.description;
          rowInfo.appendChild(toolDescEl);
        }

        // 工具参数预览
        if (tool.inputSchema && tool.inputSchema.properties) {
          var keys = Object.keys(tool.inputSchema.properties);
          if (keys.length) {
            var paramPreview = document.createElement("div");
            paramPreview.style.cssText = "font-size:11px;color:#c7c7cc;margin-top:3px;";
            paramPreview.textContent = "参数：" + keys.join("，");
            rowInfo.appendChild(paramPreview);
          }
        }

        // 每个工具独立的开关（闭包捕获 tool.name）
        (function (toolName) {
          var sw = makeIosSwitch(isEnabled, function (enabled) {
            if (enabled) {
              var i = disabledTools.indexOf(toolName);
              if (i !== -1) disabledTools.splice(i, 1);
            } else {
              if (disabledTools.indexOf(toolName) === -1) disabledTools.push(toolName);
            }
          });
          rowTop.appendChild(rowInfo);
          rowTop.appendChild(sw);
        })(tool.name);

        row.appendChild(rowTop);
        toolsCard.appendChild(row);
      });

      toolsTopSection.appendChild(toolsCard);
      toolsScroll.appendChild(toolsTopSection);

      // 保存按钮
      var toolsSaveSection = document.createElement("div");
      toolsSaveSection.className = "mcp-section";
      var toolsSaveBtn = document.createElement("button");
      toolsSaveBtn.className = "mcp-add-big-btn";
      toolsSaveBtn.style.background = "#34c759";
      toolsSaveBtn.textContent = "保存工具设置";
      toolsSaveBtn.onclick = function () {
        var targetCfg = state.configs.find(function (c) { return c.id === cfg.id; });
        if (targetCfg) {
          targetCfg.disabledTools = disabledTools.slice();
          saveConfig(state.roche);
          toolsSaveBtn.textContent = "✓ 已保存";
          toolsSaveBtn.style.background = "#8e8e93";
          setTimeout(function () {
            toolsSaveBtn.textContent = "保存工具设置";
            toolsSaveBtn.style.background = "#34c759";
          }, 1800);
        } else {
          alert("请先在「基础设置」Tab 保存服务器信息");
          state._formTab = "basic";
          render();
        }
      };
      toolsSaveSection.appendChild(toolsSaveBtn);
      toolsScroll.appendChild(toolsSaveSection);
    }
    root.appendChild(toolsScroll);

    /* ── 保存逻辑 ── */
    function doSave() {
      var name = nameInput.value.trim();
      var url = urlInput.value.trim();
      if (!name) { alert("请填写服务器名称"); return; }
      if (!url) { alert("请填写服务器地址"); return; }

      var headersObj = {};
      currentHeaders.forEach(function (h) {
        if (h.key && h.key.trim()) headersObj[h.key.trim()] = (h.val || "").trim();
      });

      if (cfg.id) {
        var existing = state.configs.find(function (c) { return c.id === cfg.id; });
        if (existing) {
          existing.name = name; existing.url = url;
          existing.transport = currentTransport; existing.headers = headersObj;
          existing.enabled = currentEnabled;
        }
        var existConn = state.servers.find(function (s) { return s.id === cfg.id; });
        if (existConn) { existConn.disconnect(); state.servers = state.servers.filter(function (s) { return s.id !== cfg.id; }); }
        delete state.errors[cfg.id];
      } else {
        var newId = String(Date.now()) + Math.random().toString(36).substring(2, 6);
        var newCfg = { id: newId, name: name, url: url, transport: currentTransport, headers: headersObj, enabled: currentEnabled };
        state.configs.push(newCfg);
      }

      saveConfig(state.roche);
      state.editingServer = undefined;
      state._formTab = undefined;

      // 如果启用，自动连接
      var savedCfg = state.configs.find(function (c) { return c.name === name && c.url === url; });
      if (currentEnabled && savedCfg) connectServer(savedCfg);

      state.view = "settings";
      render();
    }

    return root;
  }

  /* ── 工具函数 ── */
  function formInput(label, placeholder, value) {
    var group = document.createElement("div");
    group.className = "mcp-form-group";
    var labelEl = document.createElement("div");
    labelEl.className = "mcp-form-label";
    labelEl.textContent = label;
    group.appendChild(labelEl);
    var input = document.createElement("input");
    input.className = "mcp-form-input";
    input.placeholder = placeholder;
    input.value = value;
    group.appendChild(input);
    return { group: group, input: input };
  }

  function makeIosSwitch(initial, onChange) {
    var label = document.createElement("label");
    label.className = "mcp-ios-switch";
    var input = document.createElement("input");
    input.type = "checkbox";
    input.checked = initial;
    input.onchange = function () { onChange(input.checked); };
    var track = document.createElement("span");
    track.className = "mcp-ios-track";
    label.appendChild(input);
    label.appendChild(track);
    return label;
  }

  // 别名保持兼容
  function makeSwitch(initial, onChange) { return makeIosSwitch(initial, onChange); }

  /* ============================================================
     工具调用结果注入聊天
     ============================================================ */
  function injectToolResult(serverName, toolName, args, result, isError) {
    if (!state.showProcess) return;

    var bubble = document.createElement("div");
    bubble.className = "mcp-result-bubble" + (isError ? " mcp-result-bubble-error" : "");

    var header = document.createElement("div");
    header.className = "mcp-result-bubble-header";
    header.innerHTML = (isError ? "⚠ " : "🔧 ") + serverName + " · " + toolName;
    bubble.appendChild(header);

    if (args && Object.keys(args).length) {
      var argsEl = document.createElement("div");
      argsEl.style.cssText = "font-size:11px;color:#6366f1;margin-bottom:4px;";
      argsEl.textContent = "参数: " + JSON.stringify(args);
      bubble.appendChild(argsEl);
    }

    var body = document.createElement("div");
    body.className = "mcp-result-bubble-body";
    if (isError) {
      body.textContent = String(result);
    } else {
      var content = result && result.content;
      if (Array.isArray(content)) {
        body.textContent = content.map(function (c) { return c.text || JSON.stringify(c); }).join("\n");
      } else {
        body.textContent = typeof result === "object" ? JSON.stringify(result, null, 2) : String(result || "（无返回）");
      }
    }
    bubble.appendChild(body);

    // 找聊天容器注入
    var chatLog = document.querySelector(".drip-chat-log") || document.querySelector("[class*='chat-log']") || document.querySelector("[class*='message-list']");
    if (chatLog) {
      chatLog.appendChild(bubble);
      bubble.scrollIntoView({ behavior: "smooth", block: "end" });
    }
  }

  /* ============================================================
     AI 自主模式：拦截发送并注入工具上下文
     ============================================================ */
  var _autoObserver = null;

  function startAutoMode() {
    // 监听发送按钮点击，在消息发出前准备工具描述
    if (_autoObserver) return;

    // 方案：给 roche.ai.chat 包一层代理
    if (!state.roche || !state.roche.ai) return;
    var origChat = state.roche.ai.chat.bind(state.roche.ai);

    state.roche.ai.chat = async function (opts) {
      if (state.mode !== "auto") return origChat(opts);

      var connectedServers = state.servers.filter(function (s) { return s.initialized && s.tools.length; });
      if (!connectedServers.length) return origChat(opts);

      var toolsDesc = connectedServers.map(function (s) {
        var sCfg = state.configs.find(function (c) { return c.id === s.id; });
        var disabled = (sCfg && sCfg.disabledTools) || [];
        // 临时过滤掉禁用工具的描述
        var origTools = s.tools;
        s.tools = origTools.filter(function (t) { return disabled.indexOf(t.name) === -1; });
        var desc = s.tools.length ? "【" + s.name + "】\n" + s.describeTools() : "";
        s.tools = origTools; // 还原
        return desc;
      }).filter(Boolean).join("\n\n");

      var toolSystemPrompt = [
        "你可以调用以下 MCP 工具来辅助回答（如果需要）：",
        toolsDesc,
        "",
        "如需调用工具，请在回复的最开始输出以下 JSON（必须在 <tool_call> 标签内），否则直接回答：",
        "<tool_call>",
        '{"server":"服务器名称","tool":"工具名称","args":{"参数名":"参数值"}}',
        "</tool_call>",
      ].join("\n");

      // 在 messages 的第一个 system 消息中追加工具描述，或插入新的 system 消息
      var messages = (opts.messages || []).slice();
      var hasSystem = messages.length && messages[0].role === "system";
      if (hasSystem) {
        messages[0] = Object.assign({}, messages[0], { content: messages[0].content + "\n\n" + toolSystemPrompt });
      } else {
        messages.unshift({ role: "system", content: toolSystemPrompt });
      }

      var firstResp = await origChat(Object.assign({}, opts, { messages: messages, stream: false }));
      var respText = typeof firstResp === "string" ? firstResp : (firstResp && firstResp.text) || "";

      // 解析 <tool_call>
      var toolCallMatch = respText.match(/<tool_call>([\s\S]*?)<\/tool_call>/);
      if (!toolCallMatch) return firstResp; // 没有工具调用，直接返回

      var callSpec;
      try { callSpec = JSON.parse(toolCallMatch[1].trim()); } catch (e) { return firstResp; }

      var targetServer = state.servers.find(function (s) {
        return s.initialized && (s.name === callSpec.server || callSpec.server === undefined);
      });
      var targetTool = targetServer && targetServer.tools.find(function (t) { return t.name === callSpec.tool; });

      if (!targetServer || !targetTool) return firstResp;

      // 执行工具
      var toolResult, toolError;
      try {
        toolResult = await targetServer.callTool(callSpec.tool, callSpec.args || {});
        injectToolResult(targetServer.name, callSpec.tool, callSpec.args, toolResult, false);
      } catch (e) {
        toolError = e.message;
        injectToolResult(targetServer.name, callSpec.tool, callSpec.args, e.message, true);
      }

      if (toolError) return firstResp;

      // 把工具结果喂回 AI，发起第二轮
      var toolResultText = (function () {
        var content = toolResult && toolResult.content;
        if (Array.isArray(content)) return content.map(function (c) { return c.text || JSON.stringify(c); }).join("\n");
        return typeof toolResult === "object" ? JSON.stringify(toolResult, null, 2) : String(toolResult || "");
      })();

      var round2Messages = messages.concat([
        { role: "assistant", content: respText },
        { role: "user", content: "工具 [" + callSpec.tool + "] 返回结果：\n" + toolResultText + "\n\n请基于以上结果回答用户的问题。" },
      ]);

      return await origChat(Object.assign({}, opts, { messages: round2Messages }));
    };
  }

  function stopAutoMode() {
    // 恢复原 chat 函数（如果有备份）
    // 这里简单处理：unmount 后整个 state 清空，下次 mount 重新代理
  }

  /* ============================================================
     服务器连接
     ============================================================ */
  async function connectServer(cfg) {
    state.connecting[cfg.id] = true;
    delete state.errors[cfg.id];
    render();

    try {
      var conn = new MCPClient.MCPConnection(cfg);
      conn.id = cfg.id; // 复用配置 id
      await conn.connect();

      state.servers = state.servers.filter(function (s) { return s.id !== cfg.id; });
      state.servers.push(conn);
      delete state.connecting[cfg.id];
    } catch (e) {
      state.errors[cfg.id] = e.message || String(e);
      delete state.connecting[cfg.id];
    }
    render();
  }

  /* ============================================================
     插件注册
     ============================================================ */
  window.RochePlugin.register({
    id: "mcp-bridge",
    name: "MCP 工具桥",
    version: "1.0.0",
    apps: [
      {
        id: "mcp-bridge-main",
        name: "MCP 工具桥",
        icon: "zap",
        mount: function (container, roche) {
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

          // 使用异步回调确保数据加载完成后再连接和渲染
          loadConfig(roche, function() {
            // 自动连接上次已配置并启用的服务器，避免重复连接
            state.configs.forEach(function (cfg) { 
               var existing = state.servers.find(function(s) { return s.id === cfg.id; });
               if (cfg.enabled !== false && !existing) {
                  connectServer(cfg); 
               }
            });
            // 启动自主模式代理
            startAutoMode();
            render();
          });
        },
        unmount: function (container) {
          // 【注意】移除断开服务器连接的代码，让 MCP 服务器在后台保持常驻，以供 AI 调用
          
          // 移除样式
          if (state.styleEl && state.styleEl.parentNode) {
            state.styleEl.parentNode.removeChild(state.styleEl);
          }

          // 不断开 _autoObserver 的绑定，因为后台聊天仍然需要使用它
          if (container) container.innerHTML = "";
        },
      },
    ],
  });
})();
