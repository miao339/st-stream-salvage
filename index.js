/**
 * 流式抢救 · Stream Salvage
 * ------------------------------------------------------------------
 * 解决的问题：
 *   部分中转站是「假流式」——先在服务器上把整段写完，最后一次性推回来。
 *   这一下没推完整时，酒馆的流解析器凑不出完整事件，界面一片空白（空回）。
 *
 * 这个插件做两件事：
 *   1. 自动兜底：断在行内时，把已经写完整的字段捞出来，重封成合法事件。
 *   2. 手动抢救：缓存最近一次响应，「空回抢救」按钮可随时把正文救进聊天。
 *
 * 对正常响应：字节原样透传，不解析、不改写、不加延迟。
 */

(function () {
    'use strict';

    const SCRIPT_NAME = '流式抢救 · Stream Salvage';
    const VERSION = '1.3.0';
    const WRAP_MARK = '__streamSalvageOuter__';
    const GUARD_MS = 800;
    const MAX_WRAP_DEPTH = 3;
    const CAPTURE_LIMIT = 8 * 1024 * 1024;
    const REPO = 'miao339/st-stream-salvage';
    // 双马尾（流式抗截断）把 window.fetch 直接赋值成自己的包装，
    // 但会保留 original 链。顺着这条链就能判断我们还在不在。
    const PEER_MARKER = '__unicodeFloorInterceptor_fetch_v3__';
    const UPDATE_CHECK_INTERVAL = 6 * 60 * 60 * 1000;
    // 直连 GitHub 国内基本不通，按顺序试镜像，第一个成功的就用
    const UPDATE_MIRRORS = [
        `https://ghproxy.net/https://raw.githubusercontent.com/${REPO}/master/manifest.json`,
        `https://gh-proxy.com/https://raw.githubusercontent.com/${REPO}/master/manifest.json`,
        `https://cdn.jsdelivr.net/gh/${REPO}@master/manifest.json`,
        `https://raw.githubusercontent.com/${REPO}/master/manifest.json`,
    ];
    // 只接管聊天补全端点。别的请求（模型列表、存档、图片、世界书）一个字都不碰，
    // 原样调用 inner.fetch，不解析也不拷贝，避免和任何其他插件抢活。
    const CHAT_ENDPOINT = /\/(?:api\/backends\/chat-completions\/generate|api\/backends\/text-completions\/generate|v1\/chat\/completions|chat\/completions|generateContent)(?:[?#]|$)/i;

    const PREF_KEY = 'streamSalvage.enabled';
    const PREF_AUTO_UPDATE = 'streamSalvage.autoUpdate';
    const PREF_LAST_CHECK = 'streamSalvage.lastCheck';

    function loadEnabled() {
        try {
            const raw = localStorage.getItem(PREF_KEY);
            return raw === null ? true : raw === '1';
        } catch (_) { return true; }
    }

    function loadAutoUpdate() {
        try {
            const raw = localStorage.getItem(PREF_AUTO_UPDATE);
            return raw === null ? true : raw === '1';
        } catch (_) { return true; }
    }

    const state = {
        installedAt: Date.now(),
        rescued: 0,
        lastRescue: null,
        wrapDepth: 0,
        lastStream: null,      // { at, url, bytes, complete, data: Uint8Array, tool }
        menuInstalled: false,
        enabled: loadEnabled(),
        autoUpdate: loadAutoUpdate(),
        update: { checkedAt: 0, latest: null, source: '', error: null, checking: false },
    };

    function setAutoUpdate(value) {
        state.autoUpdate = Boolean(value);
        try { localStorage.setItem(PREF_AUTO_UPDATE, state.autoUpdate ? '1' : '0'); } catch (_) {}
        notify(state.autoUpdate ? '自动检查更新：已开启' : '自动检查更新：已关闭');
    }

    function cmpVersion(a, b) {
        const pa = String(a || '0').split('.').map(n => parseInt(n, 10) || 0);
        const pb = String(b || '0').split('.').map(n => parseInt(n, 10) || 0);
        for (let i = 0; i < Math.max(pa.length, pb.length); i++) {
            const x = pa[i] || 0;
            const y = pb[i] || 0;
            if (x > y) return 1;
            if (x < y) return -1;
        }
        return 0;
    }

    async function checkUpdate(silent = false) {
        if (state.update.checking) return state.update;
        state.update.checking = true;
        let result = null;
        for (const url of UPDATE_MIRRORS) {
            try {
                const r = await fetch(url, { cache: 'no-store' });
                if (!r.ok) continue;
                const j = await r.json();
                if (j && j.version) { result = { version: String(j.version), source: url }; break; }
            } catch (_) {}
        }
        state.update = {
            checkedAt: Date.now(),
            latest: result ? result.version : null,
            source: result ? result.source : '',
            error: result ? null : '所有镜像都取不到版本信息',
            checking: false,
        };
        try { localStorage.setItem(PREF_LAST_CHECK, String(state.update.checkedAt)); } catch (_) {}

        if (!silent) {
            if (!result) notify('检查更新失败：网络不通，稍后再试');
            else if (cmpVersion(result.version, VERSION) > 0) notify(`发现新版本 v${result.version}（当前 v${VERSION}）`);
            else notify(`已是最新版本 v${VERSION}`);
        }
        return state.update;
    }

    async function doUpdate() {
        const ctx = stContext();
        if (!ctx) { notify('拿不到酒馆上下文，无法更新'); return; }
        const headers = typeof ctx.getRequestHeaders === 'function' ? ctx.getRequestHeaders() : {};
        try {
            notify('正在更新，请稍候…');
            const r = await fetch('/api/extensions/update', {
                method: 'POST',
                headers: { ...headers, 'Content-Type': 'application/json' },
                body: JSON.stringify({ extensionName: 'st-stream-salvage', global: false }),
            });
            const j = await r.json().catch(() => ({}));
            if (r.ok) notify('更新完成，刷新酒馆页面生效');
            else notify('更新失败：' + (j.error || j.message || ('HTTP ' + r.status)) + '（GitHub 不通时可能拉不到，可手动重装）');
        } catch (error) {
            notify('更新失败：' + error.message + '（可手动重装）');
        }
    }

    function setEnabled(value) {
        state.enabled = Boolean(value);
        try { localStorage.setItem(PREF_KEY, state.enabled ? '1' : '0'); } catch (_) {}
        notify(state.enabled ? '空回自动抢救：已开启' : '空回自动抢救：已关闭');
    }

    function log(...args) {
        try { console.log(`[${SCRIPT_NAME}]`, ...args); } catch (_) {}
    }

    function warn(...args) {
        try { console.warn(`[${SCRIPT_NAME}]`, ...args); } catch (_) {}
    }

    function hosts() {
        const list = [];
        try { list.push(window); } catch (_) {}
        try { if (window.top && window.top !== window) list.push(window.top); } catch (_) {}
        return list;
    }

    function notify(message, level = 'warning') {
        for (const host of hosts()) {
            try {
                const t = host.toastr;
                const fn = t && typeof t[level] === 'function' ? t[level].bind(t) : null;
                if (fn) { fn(message, SCRIPT_NAME, { timeOut: 8000 }); return; }
            } catch (_) {}
        }
        warn(message);
    }

    function stContext() {
        for (const host of hosts()) {
            try {
                if (host.SillyTavern && typeof host.SillyTavern.getContext === 'function') {
                    return host.SillyTavern.getContext();
                }
            } catch (_) {}
        }
        return null;
    }

    // ── 转义还原（中转站常把 arguments 套两层转义，循环解到稳定）─────────────
    function unescapeJsonString(input) {
        let s = String(input ?? '');
        if (!s) return '';
        for (let round = 0; round < 3; round++) {
            const before = s;
            try {
                s = JSON.parse('"' + s + '"');
            } catch (_) {
                s = s
                    .replace(/\\u([0-9a-fA-F]{4})/g, (_, hex) => String.fromCharCode(parseInt(hex, 16)))
                    .replace(/\\n/g, '\n')
                    .replace(/\\r/g, '\r')
                    .replace(/\\t/g, '\t')
                    .replace(/\\"/g, '"')
                    .replace(/\\\\/g, '\\');
            }
            if (s === before) break;
        }
        return s;
    }

    // ── 从残缺的 arguments 里捞出已经闭合的字段 ────────────────────────────
    // \\[^"] 吃掉「反斜杠 + 非引号」，但不会吃掉 \"，所以 \" 只作为字段结束标志
    function extractClosedFields(argsRaw) {
        const out = {};
        if (!argsRaw) return out;
        const re = /\\"([A-Za-z0-9_]+)\\"\s*:\s*\\"((?:\\[^"]|[^\\"])*)\\"/g;
        let m;
        while ((m = re.exec(argsRaw)) !== null) {
            const key = m[1];
            if (key === 'arguments' || key === 'type' || key === 'name' || key === 'index' || key === 'id') continue;
            const value = unescapeJsonString(m[2]);
            if (value && value.length) out[key] = value;
        }
        return out;
    }

    // ── 把抢救出来的内容重封成合法 SSE ──────────────────────────────────────
    function buildSalvageEvents({ fields, toolName, text }) {
        const id = 'chatcmpl-salvage-' + Date.now().toString(36);
        const created = Math.floor(Date.now() / 1000);
        const base = { id, created, model: 'salvaged', object: 'chat.completion.chunk' };
        const event = obj => 'data: ' + JSON.stringify(obj) + '\n\n';
        let out = '';

        out += event({
            ...base,
            choices: [{ delta: { content: '', role: 'assistant' }, finish_reason: null, index: 0 }],
        });

        if (toolName) {
            const args = JSON.stringify(fields || {});
            out += event({
                ...base,
                choices: [{
                    delta: {
                        tool_calls: [{
                            index: 0,
                            id: 'call_salvage',
                            type: 'function',
                            function: { name: toolName, arguments: args },
                        }],
                    },
                    finish_reason: null,
                    index: 0,
                }],
            });
            out += event({ ...base, choices: [{ delta: {}, finish_reason: 'tool_calls', index: 0 }] });
        } else {
            const body = String(text ?? Object.values(fields || {}).join('\n\n'));
            out += event({
                ...base,
                choices: [{ delta: { content: body }, finish_reason: null, index: 0 }],
            });
            out += event({ ...base, choices: [{ delta: {}, finish_reason: 'stop', index: 0 }] });
        }

        out += 'data: [DONE]\n\n';
        return out;
    }

    // ── 抢救一段原始 SSE 文本 ───────────────────────────────────────────────
    function salvageTail(tail, toolHint) {
        const trimmed = String(tail ?? '').trim();
        if (!trimmed) return null;

        const dataLines = trimmed
            .split('\n')
            .filter(line => line.startsWith('data:'))
            .map(line => line.slice(5).trim());
        if (!dataLines.length) return null;

        const raw = dataLines.join('');

        let toolName = null;
        const nameMatch = raw.match(/"name"\s*:\s*"([A-Za-z0-9_.\-]+)"/);
        if (nameMatch) toolName = nameMatch[1];
        if (!toolName && toolHint) toolName = toolHint;

        // 1) tool_calls.arguments
        const argsMatch = raw.match(/"arguments"\s*:\s*"((?:\\.|[^"\\])*)/);
        if (argsMatch) {
            const fields = extractClosedFields(argsMatch[1]);
            const keys = Object.keys(fields);
            if (keys.length) {
                const chars = keys.reduce((sum, key) => sum + fields[key].length, 0);
                const body = fields.game_content || keys.map(k => fields[k]).join('\n\n');
                return {
                    text: buildSalvageEvents({ fields, toolName }),
                    info: { mode: 'tool_calls', chars, fields: keys, tool: toolName },
                    fields,
                    body,
                };
            }
        }

        // 2) 普通 content
        const contentMatch = raw.match(/"content"\s*:\s*"((?:\\.|[^"\\])*)/);
        if (contentMatch) {
            const text = unescapeJsonString(contentMatch[1].replace(/\\+$/, ''));
            if (text && text.trim()) {
                return {
                    text: buildSalvageEvents({ fields: {}, toolName: null, text }),
                    info: { mode: 'content', chars: text.length },
                    fields: {},
                    body: text,
                };
            }
        }

        return null;
    }

    // ── 从请求体里取工具名和 stream 标记 ───────────────────────────────────
    // 残缺响应常常只给 arguments 不给 name；另外部分后端转发的响应没有 content-type，
    // 只能靠请求体里的 stream:true 判断这是不是流式请求。
    function extractRequestMeta(args) {
        const meta = { toolName: null, stream: null };
        try {
            const options = args && args[1];
            if (!options) return meta;
            let body = options.body;
            if (typeof body !== 'string') {
                if (body instanceof ArrayBuffer) body = new TextDecoder().decode(body);
                else if (ArrayBuffer.isView(body)) body = new TextDecoder().decode(body);
                else return meta;
            }
            if (!body || body.length > 4 * 1024 * 1024) return meta;
            const parsed = JSON.parse(body);
            if (parsed && typeof parsed.stream === 'boolean') meta.stream = parsed.stream;
            const tools = parsed && parsed.tools;
            if (!Array.isArray(tools)) return meta;
            for (const tool of tools) {
                const name = tool && ((tool.function && tool.function.name) || tool.name);
                if (typeof name === 'string' && name) { meta.toolName = name; return meta; }
            }
        } catch (_) {}
        return meta;
    }

    // ── 缓存最近一次响应，供手动抢救 ────────────────────────────────────────
    function rememberStream(url, toolHint, chunks, total, ignored) {
        try {
            const data = new Uint8Array(total);
            let offset = 0;
            for (const chunk of chunks) { data.set(chunk, offset); offset += chunk.length; }

            // 「完整」的判据是原始字节里有没有 [DONE]，而不是「有没有抢救过」。
            // 只看尾巴 4KB，别为了判个真假把 8MB 全解一遍。
            let tailText = '';
            try {
                tailText = new TextDecoder('utf-8').decode(data.slice(Math.max(0, total - 4096)));
            } catch (_) {}

            state.lastStream = {
                at: Date.now(),
                url: String(url || ''),
                bytes: total,
                complete: tailText.includes('[DONE]'),
                tool: toolHint || null,
                data,
            };
        } catch (error) {
            warn('缓存响应失败', error);
        }
    }

    // ── 包装 SSE 响应 ───────────────────────────────────────────────────────
    // ── 旁听捕获：克隆一份响应读下来存档，原流一个字节都不碰 ────────────────
    // 这是最安全的形态：不管读成功还是失败，酒馆收到的永远是原始响应。
    function captureResponse(response, toolHint, url, streamRequested) {
        let contentType = '';
        try { contentType = String(response.headers.get('content-type') || '').toLowerCase(); } catch (_) {}
        const looksSse = contentType.includes('text/event-stream');
        if (!looksSse && streamRequested !== true) return response;
        if (!response.body) return response;

        // 用 tee 把流分成两支：一支交给酒馆，一支留给自己读。
        // 比 response.clone() 稳，clone 对某些流会直接抛错。
        let branches;
        try {
            branches = response.body.tee();
        } catch (error) {
            warn('分流失败，放弃捕获', error);
            state.captureError = 'tee: ' + (error && error.message);
            return response;
        }
        const [forClient, forUs] = branches;

        (async () => {
            try {
                const reader = forUs.getReader();
                const chunks = [];
                let total = 0;
                while (true) {
                    const { done, value } = await reader.read();
                    if (done) break;
                    if (total < CAPTURE_LIMIT) { chunks.push(value.slice()); total += value.length; }
                }

                const data = new Uint8Array(total);
                let offset = 0;
                for (const chunk of chunks) { data.set(chunk, offset); offset += chunk.length; }

                let tailText = '';
                try { tailText = new TextDecoder('utf-8').decode(data.slice(Math.max(0, total - 4096))); } catch (_) {}
                const complete = tailText.includes('[DONE]');

                state.lastStream = { at: Date.now(), url: String(url || ''), bytes: total, complete, tool: toolHint || null, data };

                if (!complete) {
                    let text = '';
                    try { text = new TextDecoder('utf-8').decode(data); } catch (_) {}
                    const rescued = salvageTail(text, toolHint);
                    if (rescued) {
                        state.lastRescue = { at: Date.now(), ...rescued.info };
                        notify(`检测到断流，打开「空回助手」可救回 ${rescued.info.chars} 字`);
                    } else {
                        warn('检测到断流，但没有完整的正文可救', { bytes: total });
                    }
                }
            } catch (error) {
                warn('旁听捕获中断', error);
                state.captureError = 'read: ' + (error && error.message);
            }
        })();

        // 返回给酒馆的那一支：内容和原响应完全一样
        const headers = new Headers(response.headers);
        headers.delete('content-length');
        return new Response(forClient, {
            status: response.status,
            statusText: response.statusText,
            headers,
        });
    }

    function wrapSseResponse(response, toolHint, url, streamRequested) {
        return captureResponse(response, toolHint, url, streamRequested);
    }

    // ── 安装 fetch 钩子：只要还在链里就不动，掉出链才重包 ────────────────────
    function chainHasOurs(fn) {
        let cur = fn;
        let depth = 0;
        while (typeof cur === 'function' && depth++ < 60) {
            if (cur[WRAP_MARK]) return true;
            const meta = cur[PEER_MARKER];
            if (meta && typeof meta.original === 'function') { cur = meta.original; continue; }
            break;
        }
        return false;
    }

    function installOnce() {
        const current = window.fetch;
        if (typeof current !== 'function') return;
        if (current[WRAP_MARK]) return;          // 已经是最外层，就是我们
        if (chainHasOurs(current)) return;       // 在别人的 original 链里，仍然生效，不用重包

        const inner = current;
        const wrapped = async function (...args) {
            const url = typeof args[0] === 'string' ? args[0] : (args[0] && args[0].url) || '';
            // 开关关闭时完全不介入
            if (!state.enabled) return inner.apply(this, args);
            // 只处理聊天补全，其余请求原样放行
            if (!CHAT_ENDPOINT.test(url)) return inner.apply(this, args);
            const meta = extractRequestMeta(args);
            const response = await inner.apply(this, args);
            try {
                state.lastSeen = {
                    at: Date.now(),
                    url: String(url).slice(0, 100),
                    ct: response.headers.get('content-type'),
                    hasBody: Boolean(response.body),
                    streamReq: meta.stream,
                    alreadyWrapped: Boolean(response.__streamSalvage),
                };
            } catch (_) {}
            try {
                return wrapSseResponse(response, meta.toolName, url, meta.stream);
            } catch (error) {
                warn('包装响应失败，按原样返回', error);
                return response;
            }
        };
        try { Object.defineProperty(wrapped, WRAP_MARK, { value: true }); } catch (_) {}
        try { Object.defineProperty(wrapped, 'name', { value: 'fetch' }); } catch (_) {}

        window.fetch = wrapped;
        state.wrapDepth += 1;
    }

    function ensureOutermost() {
        try { installOnce(); } catch (error) { warn('安装失败', error); }
    }

    // ── 手动抢救：把缓存里的正文写进聊天 ────────────────────────────────────
    async function rescueIntoChat() {
        const record = state.lastStream;
        if (!record || !record.bytes) {
            notify('还没有抓到可抢救的响应。先在酒馆发一次消息，空回之后再点这里');
            return;
        }

        if (record.complete) {
            notify('上一次响应是正常完成的，没有断流，不需要抢救（硬点会重复插入一份正文）');
            return;
        }

        let text = '';
        try {
            text = new TextDecoder('utf-8').decode(record.data);
        } catch (error) {
            notify('缓存读取失败：' + error.message);
            return;
        }

        const rescued = salvageTail(text, record.tool);
        if (!rescued || !rescued.body || !rescued.body.trim()) {
            notify('这次缓存里没有完整的正文，无法抢救（可能上游一个字都没推）');
            return;
        }

        const ctx = stContext();
        if (!ctx || !Array.isArray(ctx.chat)) {
            notify('没拿到酒馆上下文，无法写入聊天');
            return;
        }

        const message = {
            name: ctx.name2 || 'Assistant',
            is_user: false,
            is_system: false,
            send_date: Date.now(),
            mes: rescued.body,
            extra: { streamSalvage: true },
        };

        try {
            ctx.chat.push(message);
            if (typeof ctx.saveChat === 'function') await ctx.saveChat();
            if (typeof ctx.addOneMessage === 'function') {
                ctx.addOneMessage(message, { scroll: true });
            } else if (typeof ctx.reloadCurrentChat === 'function') {
                await ctx.reloadCurrentChat();
            }
            notify(`已把 ${rescued.body.length} 字正文救进当前聊天`);
            log('手动抢救完成', rescued.info);
        } catch (error) {
            notify('写入聊天失败：' + error.message);
        }
    }

    // ── 空回助手：说明 + 开关 + 状态 ────────────────────────────────────────
    function openAssistant() {
        try {
            buildAssistantPanel();
            return { ok: true };
        } catch (error) {
            warn('打开空回助手失败', error);
            notify('打开空回助手失败：' + (error && error.message ? error.message : error));
            return { ok: false, error: String((error && error.stack) || error) };
        }
    }

    function buildAssistantPanel() {
        const existing = document.getElementById('ss-assistant-panel');
        if (existing) { existing.remove(); return; }

        const last = state.lastRescue;
        const cached = state.lastStream;

        const html = `
        <div style="text-align:left;line-height:1.6;font-size:.94em;position:relative;">
            <button id="ss-assistant-close" title="关闭" style="position:absolute;top:-2px;right:-4px;background:none;border:none;color:inherit;opacity:.5;cursor:pointer;font-size:1.05em;line-height:1;padding:4px 6px;">✕</button>
            <h3 style="margin:0 0 6px;font-size:1.05em;padding-right:24px;">空回助手</h3>
            <p style="margin:0 0 6px;opacity:.85;">
                站点是假流式，最后一次性把整段推回来。推不完整酒馆就整段丢掉，于是空白。
                插件在这一截没收尾时，把已经写好的部分救出来交给酒馆。
            </p>
            <p style="margin:0 0 10px;opacity:.55;font-size:.9em;">
                只接管聊天补全请求，其它一概不碰。
            </p>

            <label style="display:flex;align-items:center;gap:8px;padding:9px 0;border-top:1px solid var(--SmartThemeBorderColor);cursor:pointer;">
                <input type="checkbox" id="ss-assistant-enable" ${state.enabled ? 'checked' : ''} style="margin:0;">
                <span>启用自动抢救</span>
            </label>

            <div style="padding:8px 0;border-top:1px solid var(--SmartThemeBorderColor);font-size:.9em;opacity:.85;line-height:1.7;">
                <div style="display:flex;justify-content:space-between;gap:12px;"><span>已抢救</span><span>${state.rescued} 次</span></div>
                <div style="display:flex;justify-content:space-between;gap:12px;"><span>最近一次</span><span>${last ? `${last.chars} 字 · ${last.mode}` : '—'}</span></div>
                <div style="display:flex;justify-content:space-between;gap:12px;"><span>已缓存</span><span>${cached ? `${cached.bytes} 字节 · ${cached.complete ? '完整' : '断流'}` : '—'}</span></div>
            </div>

            <div class="menu_button" id="ss-assistant-rescue" style="margin-top:10px;width:fit-content;">
                从缓存立即抢救一次
            </div>
            <div style="font-size:.85em;opacity:.55;margin-top:7px;line-height:1.55;">
                空回之后、还没发下一条消息时点它，会把上一次响应里写好的正文直接写进聊天。再发一次缓存就被覆盖了。
            </div>

            <div style="display:flex;align-items:center;justify-content:space-between;gap:10px;padding-top:9px;margin-top:9px;border-top:1px solid var(--SmartThemeBorderColor);">
                <label style="display:flex;align-items:center;gap:8px;cursor:pointer;">
                    <input type="checkbox" id="ss-auto-update" ${state.autoUpdate ? 'checked' : ''} style="margin:0;">
                    <span style="font-size:.9em;">自动检查更新</span>
                </label>
                <div class="menu_button" id="ss-check-update" style="margin:0;width:fit-content;font-size:.88em;padding:2px 10px;">检查更新</div>
            </div>
            <div id="ss-update-info" style="font-size:.82em;opacity:.55;margin-top:5px;line-height:1.5;">
                当前 v${VERSION}
            </div>
        </div>`;

        // 贴底部输出框：插到 form_sheld 前面，跟着布局走，不弹居中窗
        const panel = document.createElement('div');
        panel.id = 'ss-assistant-panel';
        panel.style.cssText = [
            'position:relative',
            'margin:0 0 6px 0',
            'padding:10px 14px',
            'border:1px solid var(--SmartThemeBorderColor)',
            'border-radius:10px',
            'background:var(--SmartThemeBlurTintColor,rgba(20,20,20,.94))',
            'box-shadow:0 6px 18px rgba(0,0,0,.35)',
            'font-size:.94em',
            'line-height:1.6',
            'text-align:left',
        ].join(';');
        panel.innerHTML = html;

        const host = document.getElementById('form_sheld') || document.getElementById('send_form');
        const parent = (host && host.parentElement) || document.getElementById('sheld') || document.body;
        if (host && host.parentElement === parent) parent.insertBefore(panel, host);
        else parent.appendChild(panel);

        const cb = panel.querySelector('#ss-assistant-enable');
        if (cb) cb.addEventListener('change', event => setEnabled(event.target.checked));
        const btn = panel.querySelector('#ss-assistant-rescue');
        if (btn) btn.addEventListener('click', () => rescueIntoChat());
        const closeBtn = panel.querySelector('#ss-assistant-close');
        if (closeBtn) closeBtn.addEventListener('click', event => {
            event.stopPropagation();
            panel.remove();
        });

        // 更新区
        const autoCb = panel.querySelector('#ss-auto-update');
        if (autoCb) autoCb.addEventListener('change', event => setAutoUpdate(event.target.checked));

        const upInfo = panel.querySelector('#ss-update-info');
        const renderUpdateInfo = () => {
            if (!upInfo) return;
            const u = state.update;
            if (u.checking) { upInfo.textContent = '正在检查…'; return; }
            if (!u.checkedAt) { upInfo.textContent = `当前 v${VERSION}`; return; }
            if (u.error) { upInfo.textContent = `当前 v${VERSION} · 检查失败：${u.error}`; return; }
            if (cmpVersion(u.latest, VERSION) > 0) {
                upInfo.innerHTML = `当前 v${VERSION} · 发现新版本 v${u.latest} ` +
                    `<span class="menu_button" id="ss-do-update" style="display:inline-block;margin:0 0 0 6px;padding:1px 8px;font-size:.95em;">立即更新</span>`;
                const doBtn = upInfo.querySelector('#ss-do-update');
                if (doBtn) doBtn.addEventListener('click', event => { event.stopPropagation(); doUpdate(); });
            } else {
                upInfo.textContent = `当前 v${VERSION} · 已是最新`;
            }
        };

        const checkBtn = panel.querySelector('#ss-check-update');
        if (checkBtn) checkBtn.addEventListener('click', async event => {
            event.stopPropagation();
            if (upInfo) upInfo.textContent = '正在检查…';
            await checkUpdate(false);
            renderUpdateInfo();
        });
        renderUpdateInfo();

        let detached = false;
        const onOutside = event => {
            if (detached) return;
            const current = document.getElementById('ss-assistant-panel');
            if (!current) { document.removeEventListener('mousedown', onOutside, true); return; }
            if (current.contains(event.target)) return;
            detached = true;
            current.remove();
            document.removeEventListener('mousedown', onOutside, true);
        };
        setTimeout(() => document.addEventListener('mousedown', onOutside, true), 80);
    }

    // ── 在魔法棒菜单里挂「空回助手」入口 ────────────────────────────────────
    function installMenuButton() {
        try {
            if (state.menuInstalled && document.getElementById('ss-rescue-menu-item')) return;
            const menu = document.getElementById('extensionsMenu');
            if (!menu) return;
            if (document.getElementById('ss-rescue-menu-item')) { state.menuInstalled = true; return; }

            const container = document.createElement('div');
            container.id = 'ss-rescue-menu-item';
            container.className = 'extension_container interactable';
            container.title = '空回助手：查看说明与开关';
            container.innerHTML = `
                <div class="list-group-item flex-container flexGap5">
                    <div class="fa-solid fa-kit-medical extensionsMenuExtensionButton"></div>
                    <span>空回助手</span>
                </div>`;
            // 不要 stopPropagation：ST 靠 click 冒泡到 <html> 来收起魔法棒菜单
            container.addEventListener('click', () => {
                openAssistant();
            });
            menu.appendChild(container);
            state.menuInstalled = true;
            log('「空回抢救」按钮已挂到扩展菜单');
        } catch (error) {
            warn('挂按钮失败', error);
        }
    }

    // ── 设置面板状态刷新 ────────────────────────────────────────────────────
    function refreshPanel() {
        try {
            if (!document.getElementById('stream-salvage-panel')) return;
            const set = (id, text) => {
                const el = document.getElementById(id);
                if (el && el.textContent !== text) el.textContent = text;
            };
            set('ss-rescued', String(state.rescued));
            set('ss-hooks', String(state.wrapDepth));
            set('ss-outer', window.fetch && window.fetch[WRAP_MARK] ? '是' : '否');
            const last = state.lastRescue;
            set('ss-last', last ? `${last.chars} 字 · ${last.mode}${last.tool ? ' · ' + last.tool : ''}` : '—');

            const cached = state.lastStream;
            set('ss-cached', cached ? `${cached.bytes} 字节 · ${cached.complete ? '完整' : '断流'}` : '—');

            const btn = document.getElementById('ss-reinstall');
            if (btn && !btn.__ssBound) {
                btn.__ssBound = true;
                btn.addEventListener('click', () => {
                    state.wrapDepth = 0;
                    ensureOutermost();
                    refreshPanel();
                });
            }
        } catch (_) {}
    }

    // ── 启动 ────────────────────────────────────────────────────────────────
    ensureOutermost();
    setInterval(ensureOutermost, GUARD_MS);
    setInterval(() => { installMenuButton(); refreshPanel(); }, 1200);

    // 自动检查更新：开页面时若超过间隔就查一次，之后每 30 分钟看一次是否到期
    (function scheduleUpdateCheck() {
        const due = () => {
            if (!state.autoUpdate) return false;
            let last = 0;
            try { last = Number(localStorage.getItem(PREF_LAST_CHECK) || 0); } catch (_) {}
            return Date.now() - last > UPDATE_CHECK_INTERVAL;
        };
        setTimeout(() => { if (due()) checkUpdate(true); }, 6000);
        setInterval(() => { if (due()) checkUpdate(true); }, 30 * 60 * 1000);
    })();
    setTimeout(installMenuButton, 300);
    setTimeout(installMenuButton, 1500);
    setTimeout(installMenuButton, 4000);

    try {
        window.__streamSalvage__ = {
            version: VERSION,
            status() {
                const cached = state.lastStream;
                return {
                    installedAt: state.installedAt,
                    enabled: state.enabled,
                    rescued: state.rescued,
                    lastRescue: state.lastRescue,
                    outermost: Boolean(window.fetch && window.fetch[WRAP_MARK]),
                    wrapDepth: state.wrapDepth,
                    menuInstalled: state.menuInstalled,
                    lastSeen: state.lastSeen || null,
                    captureError: state.captureError || null,
                    cached: cached ? { bytes: cached.bytes, complete: cached.complete, at: cached.at } : null,
                };
            },
            rescue: rescueIntoChat,
            openAssistant,
            checkUpdate,
            update: doUpdate,
            salvageTail,
            reinstall() { state.wrapDepth = 0; ensureOutermost(); },
        };
    } catch (_) {}

    log(`v${VERSION} 已加载`);
})();
