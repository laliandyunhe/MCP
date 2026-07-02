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

  function loadConfig(roche) {
    try {
      var raw = roche.storage.getItem(STORAGE_KEY);
      if (!raw) return;
      var data = JSON.parse(raw);
      state.configs = data.configs || [];
      state.mode = data.mode || "manual";
      state.showProcess = data.showProcess !== false;
    } catch (e) {}
  }

  function saveConfig(roche) {
    try {
      roche.storage.setItem(STORAGE_KEY, JSON.stringify({
        configs: state.configs,
        mode: state.mode,
        showProcess: state.showProcess,
      }));
    } catch (e) {}
  }

  /* ============================================================
     样式
     ============================================================ */
  function getStyles() {
    return `
      .mcp-bridge { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; font-size: 14px; color: #1a1a1a; height: 100%; display: flex; flex-direction: column; background: #f7f7f8; }

      /* ── 顶栏 ── */
      .mcp-topbar { display: flex; align-items: center; justify-content: space-between; padding: 12px 16px; background: #fff; border-bottom: 1px solid #e5e5e5; flex-shrink: 0; }
      .mcp-topbar-title { font-size: 15px; font-weight: 600; color: #111; }
      .mcp-topbar-actions { display: flex; gap: 8px; }
      .mcp-icon-btn { background: none; border: none; cursor: pointer; padding: 5px; border-radius: 8px; color: #555; display: flex; align-items: center; transition: background 0.15s; }
      .mcp-icon-btn:hover { background: #f0f0f0; }
      .mcp-icon-btn svg { width: 18px; height: 18px; }

      /* ── 模式标签栏 ── */
      .mcp-modebar { display: flex; align-items: center; gap: 10px; padding: 10px 16px; background: #fff; border-bottom: 1px solid #e5e5e5; flex-shrink: 0; }
      .mcp-mode-label { font-size: 12px; color: #888; margin-right: 2px; }
      .mcp-mode-toggle { display: flex; border-radius: 8px; background: #f0f0f0; padding: 2px; gap: 2px; }
      .mcp-mode-btn { padding: 4px 12px; border-radius: 6px; border: none; cursor: pointer; font-size: 12px; font-weight: 500; background: transparent; color: #666; transition: all 0.15s; }
      .mcp-mode-btn.active { background: #fff; color: #111; box-shadow: 0 1px 3px rgba(0,0,0,0.12); }

      /* ── 服务器列表（聊天视图）── */
      .mcp-server-list { padding: 10px 12px; display: flex; flex-direction: column; gap: 8px; flex-shrink: 0; }
      .mcp-server-chip { display: flex; align-items: center; gap: 8px; padding: 8px 12px; background: #fff; border-radius: 10px; border: 1px solid #e5e5e5; }
      .mcp-dot { width: 8px; height: 8px; border-radius: 50%; flex-shrink: 0; }
      .mcp-dot.ok { background: #22c55e; }
      .mcp-dot.err { background: #ef4444; }
      .mcp-dot.connecting { background: #f59e0b; animation: mcp-pulse 1s infinite; }
      .mcp-dot.idle { background: #d1d5db; }
      @keyframes mcp-pulse { 0%,100%{opacity:1} 50%{opacity:0.4} }
      .mcp-chip-name { flex: 1; font-size: 13px; font-weight: 500; min-width: 0; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
      .mcp-chip-count { font-size: 11px; color: #888; flex-shrink: 0; }

      /* ── 工具面板（手动模式）── */
      .mcp-tool-panel { flex: 1; overflow-y: auto; padding: 0 12px 12px; }
      .mcp-tool-section { margin-bottom: 12px; }
      .mcp-tool-section-title { font-size: 11px; font-weight: 600; color: #888; text-transform: uppercase; letter-spacing: 0.05em; margin-bottom: 6px; padding: 0 2px; }
      .mcp-tool-card { background: #fff; border-radius: 10px; border: 1px solid #e5e5e5; overflow: hidden; margin-bottom: 6px; }
      .mcp-tool-header { display: flex; align-items: center; gap: 10px; padding: 10px 12px; cursor: pointer; user-select: none; }
      .mcp-tool-header:hover { background: #fafafa; }
      .mcp-tool-name { flex: 1; font-size: 13px; font-weight: 600; }
      .mcp-tool-desc { font-size: 12px; color: #888; margin-top: 1px; }
      .mcp-tool-chevron { color: #aaa; transition: transform 0.2s; flex-shrink: 0; }
      .mcp-tool-chevron.open { transform: rotate(180deg); }
      .mcp-tool-body { padding: 0 12px 12px; border-top: 1px solid #f0f0f0; display: none; }
      .mcp-tool-body.open { display: block; }
      .mcp-param-label { font-size: 11px; font-weight: 600; color: #555; margin: 8px 0 3px; display: flex; align-items: center; gap: 4px; }
      .mcp-required-badge { font-size: 10px; background: #fee2e2; color: #dc2626; border-radius: 4px; padding: 0 4px; font-weight: 500; }
      .mcp-param-input { width: 100%; box-sizing: border-box; padding: 7px 10px; border: 1px solid #e5e5e5; border-radius: 7px; font-size: 13px; outline: none; font-family: inherit; resize: vertical; min-height: 36px; transition: border-color 0.15s; }
      .mcp-param-input:focus { border-color: #6366f1; }
      .mcp-call-btn { margin-top: 10px; width: 100%; padding: 8px; background: #6366f1; color: #fff; border: none; border-radius: 8px; font-size: 13px; font-weight: 600; cursor: pointer; transition: opacity 0.15s; }
      .mcp-call-btn:hover { opacity: 0.88; }
      .mcp-call-btn:disabled { opacity: 0.5; cursor: not-allowed; }

      /* ── 调用结果气泡 ── */
      .mcp-result-bubble { margin: 6px 0; padding: 10px 12px; background: #eef2ff; border-left: 3px solid #6366f1; border-radius: 0 8px 8px 0; font-size: 12px; }
      .mcp-result-bubble-header { font-weight: 600; color: #4f46e5; margin-bottom: 4px; display: flex; align-items: center; gap: 6px; }
      .mcp-result-bubble-body { color: #374151; white-space: pre-wrap; word-break: break-word; max-height: 200px; overflow-y: auto; }
      .mcp-result-bubble-error { background: #fef2f2; border-left-color: #ef4444; }
      .mcp-result-bubble-error .mcp-result-bubble-header { color: #dc2626; }

      /* ── 自动模式提示 ── */
      .mcp-auto-hint { flex: 1; display: flex; flex-direction: column; align-items: center; justify-content: center; gap: 8px; color: #888; padding: 20px; text-align: center; }
      .mcp-auto-hint svg { width: 36px; height: 36px; color: #6366f1; opacity: 0.6; }
      .mcp-auto-hint-title { font-size: 13px; font-weight: 600; color: #555; }
      .mcp-auto-hint-desc { font-size: 12px; line-height: 1.5; }

      /* ── 设置视图 ── */
      .mcp-settings { flex: 1; overflow-y: auto; padding: 12px; display: flex; flex-direction: column; gap: 12px; }
      .mcp-settings-section { background: #fff; border-radius: 12px; border: 1px solid #e5e5e5; overflow: hidden; }
      .mcp-settings-section-title { font-size: 11px; font-weight: 700; color: #888; text-transform: uppercase; letter-spacing: 0.06em; padding: 10px 14px 6px; border-bottom: 1px solid #f0f0f0; }
      .mcp-server-item { padding: 12px 14px; border-bottom: 1px solid #f5f5f5; display: flex; align-items: flex-start; gap: 10px; }
      .mcp-server-item:last-child { border-bottom: none; }
      .mcp-server-item-info { flex: 1; min-width: 0; }
      .mcp-server-item-name { font-size: 13px; font-weight: 600; }
      .mcp-server-item-url { font-size: 11px; color: #888; margin-top: 2px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
      .mcp-server-item-meta { font-size: 11px; color: #aaa; margin-top: 2px; }
      .mcp-server-item-actions { display: flex; gap: 6px; flex-shrink: 0; }
      .mcp-add-server-btn { display: flex; align-items: center; gap: 6px; width: 100%; padding: 12px 14px; background: none; border: none; cursor: pointer; font-size: 13px; font-weight: 500; color: #6366f1; text-align: left; }
      .mcp-add-server-btn:hover { background: #fafafa; }

      /* ── 服务器编辑表单 ── */
      .mcp-form { padding: 14px; display: flex; flex-direction: column; gap: 12px; }
      .mcp-form-group { display: flex; flex-direction: column; gap: 4px; }
      .mcp-form-label { font-size: 12px; font-weight: 600; color: #555; }
      .mcp-form-hint { font-size: 11px; color: #aaa; margin-top: 1px; }
      .mcp-form-input { padding: 8px 10px; border: 1px solid #e5e5e5; border-radius: 8px; font-size: 13px; outline: none; font-family: inherit; transition: border-color 0.15s; width: 100%; box-sizing: border-box; }
      .mcp-form-input:focus { border-color: #6366f1; }
      .mcp-transport-toggle { display: flex; border-radius: 8px; background: #f0f0f0; padding: 2px; }
      .mcp-transport-btn { flex: 1; padding: 6px; border-radius: 6px; border: none; cursor: pointer; font-size: 12px; font-weight: 500; background: transparent; color: #666; transition: all 0.15s; }
      .mcp-transport-btn.active { background: #fff; color: #111; box-shadow: 0 1px 3px rgba(0,0,0,0.12); }
      .mcp-header-row { display: flex; gap: 6px; align-items: center; }
      .mcp-header-key { flex: 1; }
      .mcp-header-val { flex: 2; }
      .mcp-del-btn { background: none; border: none; cursor: pointer; color: #ef4444; padding: 4px; border-radius: 6px; flex-shrink: 0; }
      .mcp-del-btn:hover { background: #fee2e2; }
      .mcp-form-actions { display: flex; gap: 8px; }
      .mcp-btn-primary { flex: 1; padding: 9px; background: #6366f1; color: #fff; border: none; border-radius: 9px; font-size: 13px; font-weight: 600; cursor: pointer; transition: opacity 0.15s; }
      .mcp-btn-primary:hover { opacity: 0.88; }
      .mcp-btn-secondary { flex: 1; padding: 9px; background: #f0f0f0; color: #555; border: none; border-radius: 9px; font-size: 13px; font-weight: 600; cursor: pointer; transition: background 0.15s; }
      .mcp-btn-secondary:hover { background: #e5e5e5; }
      .mcp-btn-danger { padding: 9px 14px; background: #fee2e2; color: #dc2626; border: none; border-radius: 9px; font-size: 13px; font-weight: 600; cursor: pointer; }

      /* ── 全局开关行 ── */
      .mcp-toggle-row { display: flex; align-items: center; justify-content: space-between; padding: 12px 14px; }
      .mcp-toggle-row + .mcp-toggle-row { border-top: 1px solid #f5f5f5; }
      .mcp-toggle-info { flex: 1; }
      .mcp-toggle-title { font-size: 13px; font-weight: 500; }
      .mcp-toggle-desc { font-size: 11px; color: #aaa; margin-top: 2px; }
      .mcp-switch { position: relative; width: 42px; height: 24px; flex-shrink: 0; }
      .mcp-switch input { opacity: 0; width: 0; height: 0; }
      .mcp-switch-track { position: absolute; inset: 0; background: #d1d5db; border-radius: 24px; cursor: pointer; transition: background 0.2s; }
      .mcp-switch input:checked + .mcp-switch-track { background: #6366f1; }
      .mcp-switch-track::after { content: ""; position: absolute; width: 18px; height: 18px; border-radius: 50%; background: #fff; top: 3px; left: 3px; transition: transform 0.2s; box-shadow: 0 1px 3px rgba(0,0,0,0.2); }
      .mcp-switch input:checked + .mcp-switch-track::after { transform: translateX(18px); }

      /* ── 空状态 ── */
      .mcp-empty { display: flex; flex-direction: column; align-items: center; justify-content: center; gap: 8px; flex: 1; color: #aaa; padding: 20px; text-align: center; }
      .mcp-empty svg { width: 40px; height: 40px; opacity: 0.4; }
      .mcp-empty-title { font-size: 14px; font-weight: 600; color: #888; }
      .mcp-empty-desc { font-size: 12px; }

      /* ── 微动画 ── */
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

    var root = document.createElement("div");
    root.className = "mcp-bridge";

    if (state.view === "settings") {
      root.appendChild(renderTopbar(true));
      root.appendChild(renderSettings());
    } else if (state.editingServer !== undefined) {
      root.appendChild(renderTopbar(false, true));
      root.appendChild(renderServerForm());
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
  function renderTopbar(isSettings, isEdit) {
    var el = document.createElement("div");
    el.className = "mcp-topbar";

    var titleEl = document.createElement("span");
    titleEl.className = "mcp-topbar-title";
    titleEl.textContent = isEdit
      ? (state.editingServer && state.editingServer.id ? "编辑服务器" : "添加服务器")
      : isSettings ? "MCP 设置"
      : "MCP 工具桥";
    el.appendChild(titleEl);

    var actions = document.createElement("div");
    actions.className = "mcp-topbar-actions";

    if (isSettings || isEdit) {
      var backBtn = document.createElement("button");
      backBtn.className = "mcp-icon-btn";
      backBtn.innerHTML = I.back;
      backBtn.title = "返回";
      backBtn.onclick = function () {
        state.view = "chat";
        state.editingServer = undefined;
        render();
      };
      actions.appendChild(backBtn);
    } else {
      var settingsBtn = document.createElement("button");
      settingsBtn.className = "mcp-icon-btn";
      settingsBtn.innerHTML = I.settings;
      settingsBtn.title = "设置";
      settingsBtn.onclick = function () { state.view = "settings"; render(); };
      actions.appendChild(settingsBtn);
    }

    el.appendChild(actions);
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
      var section = document.createElement("div");
      section.className = "mcp-tool-section";

      var sTitle = document.createElement("div");
      sTitle.className = "mcp-tool-section-title";
      sTitle.textContent = server.name;
      section.appendChild(sTitle);

      server.tools.forEach(function (tool) {
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

  /* 服务器编辑表单 */
  function renderServerForm() {
    var el = document.createElement("div");
    el.className = "mcp-settings";

    var section = document.createElement("div");
    section.className = "mcp-settings-section";

    var form = document.createElement("div");
    form.className = "mcp-form";
    var cfg = state.editingServer;

    // 名称
    var nameInput = formInput("名称", "服务器显示名称", cfg.name || "");
    form.appendChild(nameInput.group);

    // URL
    var urlInput = formInput("服务器地址", "https://your-mcp-server.com/mcp", cfg.url || "");
    form.appendChild(urlInput.group);

    // 传输类型
    var transGroup = document.createElement("div");
    transGroup.className = "mcp-form-group";
    var transLabel = document.createElement("div");
    transLabel.className = "mcp-form-label";
    transLabel.textContent = "传输类型";
    transGroup.appendChild(transLabel);
    var transToggle = document.createElement("div");
    transToggle.className = "mcp-transport-toggle";
    var currentTransport = cfg.transport || "streamable-http";

    [["streamable-http", "Streamable HTTP"], ["sse", "SSE"]].forEach(function (pair) {
      var btn = document.createElement("button");
      btn.className = "mcp-transport-btn" + (currentTransport === pair[0] ? " active" : "");
      btn.textContent = pair[1];
      btn.onclick = function () {
        currentTransport = pair[0];
        transToggle.querySelectorAll(".mcp-transport-btn").forEach(function (b) { b.classList.remove("active"); });
        btn.classList.add("active");
      };
      transToggle.appendChild(btn);
    });
    transGroup.appendChild(transToggle);
    form.appendChild(transGroup);

    // 自定义请求头
    var headersGroup = document.createElement("div");
    headersGroup.className = "mcp-form-group";
    var headersLabel = document.createElement("div");
    headersLabel.className = "mcp-form-label";
    headersLabel.textContent = "自定义请求头（可选，用于鉴权）";
    headersGroup.appendChild(headersLabel);

    var headersList = document.createElement("div");
    headersList.style.display = "flex";
    headersList.style.flexDirection = "column";
    headersList.style.gap = "6px";

    var currentHeaders = Array.isArray(cfg.headers)
      ? cfg.headers.slice()
      : Object.keys(cfg.headers || {}).map(function (k) { return { key: k, val: cfg.headers[k] }; });

    function renderHeaderRows() {
      headersList.innerHTML = "";
      currentHeaders.forEach(function (h, idx) {
        var row = document.createElement("div");
        row.className = "mcp-header-row";
        var kInput = document.createElement("input");
        kInput.className = "mcp-form-input mcp-header-key";
        kInput.placeholder = "Header 名称";
        kInput.value = h.key || "";
        kInput.oninput = function () { currentHeaders[idx].key = kInput.value; };
        var vInput = document.createElement("input");
        vInput.className = "mcp-form-input mcp-header-val";
        vInput.placeholder = "值";
        vInput.value = h.val || "";
        vInput.oninput = function () { currentHeaders[idx].val = vInput.value; };
        var delBtn = document.createElement("button");
        delBtn.className = "mcp-del-btn";
        delBtn.innerHTML = I.trash;
        delBtn.onclick = function () { currentHeaders.splice(idx, 1); renderHeaderRows(); };
        row.appendChild(kInput); row.appendChild(vInput); row.appendChild(delBtn);
        headersList.appendChild(row);
      });
    }
    renderHeaderRows();
    headersGroup.appendChild(headersList);

    var addHeaderBtn = document.createElement("button");
    addHeaderBtn.className = "mcp-btn-secondary";
    addHeaderBtn.style.marginTop = "6px";
    addHeaderBtn.textContent = "+ 添加请求头";
    addHeaderBtn.onclick = function () { currentHeaders.push({ key: "", val: "" }); renderHeaderRows(); };
    headersGroup.appendChild(addHeaderBtn);
    form.appendChild(headersGroup);

    // 操作按钮
    var actions = document.createElement("div");
    actions.className = "mcp-form-actions";

    var saveBtn = document.createElement("button");
    saveBtn.className = "mcp-btn-primary";
    saveBtn.textContent = "保存";
    saveBtn.onclick = function () {
      var name = nameInput.input.value.trim();
      var url = urlInput.input.value.trim();
      if (!name || !url) { alert("名称和地址不能为空"); return; }

      var headersObj = {};
      currentHeaders.forEach(function (h) {
        if (h.key && h.key.trim()) headersObj[h.key.trim()] = h.val || "";
      });

      if (cfg.id) {
        // 编辑现有
        var existing = state.configs.find(function (c) { return c.id === cfg.id; });
        if (existing) { existing.name = name; existing.url = url; existing.transport = currentTransport; existing.headers = headersObj; }
        var existConn = state.servers.find(function (s) { return s.id === cfg.id; });
        if (existConn) { existConn.disconnect(); state.servers = state.servers.filter(function (s) { return s.id !== cfg.id; }); }
        delete state.errors[cfg.id];
      } else {
        // 新增
        var newCfg = { id: MCPClient.uuid ? MCPClient.uuid() : String(Date.now()), name: name, url: url, transport: currentTransport, headers: headersObj };
        state.configs.push(newCfg);
      }

      saveConfig(state.roche);
      state.editingServer = undefined;
      state.view = "settings";
      render();
    };
    actions.appendChild(saveBtn);

    var cancelBtn = document.createElement("button");
    cancelBtn.className = "mcp-btn-secondary";
    cancelBtn.textContent = "取消";
    cancelBtn.onclick = function () { state.editingServer = undefined; render(); };
    actions.appendChild(cancelBtn);

    form.appendChild(actions);
    section.appendChild(form);
    el.appendChild(section);
    return el;
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

  function makeSwitch(initial, onChange) {
    var label = document.createElement("label");
    label.className = "mcp-switch";
    var input = document.createElement("input");
    input.type = "checkbox";
    input.checked = initial;
    input.onchange = function () { onChange(input.checked); };
    var track = document.createElement("span");
    track.className = "mcp-switch-track";
    label.appendChild(input);
    label.appendChild(track);
    return label;
  }

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
        return "【" + s.name + "】\n" + s.describeTools();
      }).join("\n\n");

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

          loadConfig(roche);

          // 自动连接上次已配置的服务器
          state.configs.forEach(function (cfg) { connectServer(cfg); });

          // 启动自主模式代理
          startAutoMode();

          render();
        },
        unmount: function (container) {
          // 断开所有服务器
          state.servers.forEach(function (s) { s.disconnect(); });
          state.servers = [];

          // 移除样式
          if (state.styleEl && state.styleEl.parentNode) {
            state.styleEl.parentNode.removeChild(state.styleEl);
          }

          if (_autoObserver) { _autoObserver.disconnect(); _autoObserver = null; }
          if (container) container.innerHTML = "";
        },
      },
    ],
  });
})();
