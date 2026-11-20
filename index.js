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
    const VERSION = '1.1.0';
    const WRAP_MARK = '__streamSalvageOuter__';
    const GUARD_MS = 800;
    const MAX_WRAP_DEPTH = 3;
    const CAPTURE_LIMIT = 8 * 1024 * 1024;
    // 只接管聊天补全端点。别的请求（模型列表、存档、图片、世界书）一个字都不碰，
    // 原样调用 inner.fetch，不解析也不拷贝，避免和任何其他插件抢活。
    const CHAT_ENDPOINT = /\/(?:api\/backends\/chat-completions\/generate|api\/backends\/text-completions\/generate|v1\/chat\/completions|chat\/completions|generateContent)(?:[?#]|$)/i;

    const PREF_KEY = 'streamSalvage.enabled';

    function loadEnabled() {
        try {
            const raw = localStorage.getItem(PREF_KEY);
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
    };

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

    // ── 从请求体里取工具名（残缺响应常常只给 arguments，不给 name）──────────
    function extractToolName(args) {
        try {
            const options = args && args[1];
            if (!options) return null;
            let body = options.body;
            if (typeof body !== 'string') {
                if (body instanceof ArrayBuffer) body = new TextDecoder().decode(body);
                else if (ArrayBuffer.isView(body)) body = new TextDecoder().decode(body);
                else return null;
            }
            if (!body || body.length > 4 * 1024 * 1024) return null;
            const parsed = JSON.parse(body);
            const tools = parsed && parsed.tools;
            if (!Array.isArray(tools)) return null;
            for (const tool of tools) {
                const name = tool && ((tool.function && tool.function.name) || tool.name);
                if (typeof name === 'string' && name) return name;
            }
        } catch (_) {}
        return null;
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
    function wrapSseResponse(response, toolHint, url) {
        let contentType = '';
        try { contentType = String(response.headers.get('content-type') || '').toLowerCase(); } catch (_) {}
        if (!contentType.includes('text/event-stream')) return response;
        if (!response.body) return response;
        if (response.__streamSalvage) return response;

        const reader = response.body.getReader();
        const decoder = new TextDecoder('utf-8');
        const encoder = new TextEncoder();
        const captured = [];
        let capturedBytes = 0;
        let buffer = '';
        let finished = false;

        const pushCapture = chunk => {
            if (capturedBytes >= CAPTURE_LIMIT) return;
            try {
                captured.push(chunk.slice());
                capturedBytes += chunk.length;
            } catch (_) {}
        };

        const emitSalvage = (controller, reason) => {
            const pending = buffer;
            buffer = '';
            if (!pending.trim()) return false;
            const rescued = salvageTail(pending, toolHint);
            if (!rescued) {
                // 救不了就把原始尾巴原样吐出去，绝不吞数据。
                // 等于退化回「没装这个插件」的状态，下游爱怎么处理怎么处理。
                warn(`${reason}：尾巴里没有可抢救的已闭合字段，原样放行`);
                try { controller.enqueue(encoder.encode(pending)); } catch (_) {}
                return false;
            }
            try { controller.enqueue(encoder.encode(rescued.text)); } catch (_) { return false; }
            state.rescued += 1;
            state.lastRescue = { at: Date.now(), ...rescued.info };
            warn(`${reason}：已抢救`, rescued.info);
            notify(`上游断在行内，已自动救回 ${rescued.info.chars} 字内容`);
            return true;
        };

        const stream = new ReadableStream({
            async pull(controller) {
                if (finished) return;
                try {
                    const { done, value } = await reader.read();
                    if (done) {
                        finished = true;
                        const rescuedOk = emitSalvage(controller, '流出完但尾巴未闭合');
                        rememberStream(url, toolHint, captured, capturedBytes, !rescuedOk);
                        try { controller.close(); } catch (_) {}
                        return;
                    }
                    pushCapture(value);
                    buffer += decoder.decode(value, { stream: true });
                    let ready = '';
                    let idx;
                    while ((idx = buffer.indexOf('\n')) >= 0) {
                        ready += buffer.slice(0, idx + 1);
                        buffer = buffer.slice(idx + 1);
                    }
                    if (ready) controller.enqueue(encoder.encode(ready));
                } catch (error) {
                    finished = true;
                    emitSalvage(controller, '连接中断(' + (error && error.message ? error.message : error) + ')');
                    rememberStream(url, toolHint, captured, capturedBytes, false);
                    try { controller.close(); } catch (_) {}
                }
            },
            cancel(reason) {
                finished = true;
                rememberStream(url, toolHint, captured, capturedBytes, false);
                try { reader.cancel(reason); } catch (_) {}
            },
        });

        const headers = new Headers(response.headers);
        headers.delete('content-length');

        const wrapped = new Response(stream, {
            status: response.status,
            statusText: response.statusText,
            headers,
        });
        try { Object.defineProperty(wrapped, '__streamSalvage', { value: true }); } catch (_) {}
        return wrapped;
    }

    // ── 安装 fetch 钩子，保证在最外层 ───────────────────────────────────────
    function installOnce() {
        if (state.wrapDepth >= MAX_WRAP_DEPTH) return;
        const current = window.fetch;
        if (typeof current !== 'function') return;
        if (current[WRAP_MARK]) return;

        const inner = current;
        const wrapped = async function (...args) {
            const url = typeof args[0] === 'string' ? args[0] : (args[0] && args[0].url) || '';
            // 开关关闭时完全不介入
            if (!state.enabled) return inner.apply(this, args);
            // 只处理聊天补全，其余请求原样放行
            if (!CHAT_ENDPOINT.test(url)) return inner.apply(this, args);
            const toolHint = extractToolName(args);
            const response = await inner.apply(this, args);
            try {
                return wrapSseResponse(response, toolHint, url);
            } catch (error) {
                warn('包装响应失败，按原样返回', error);
                return response;
            }
        };
        try { Object.defineProperty(wrapped, WRAP_MARK, { value: true }); } catch (_) {}

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
    async function openAssistant() {
        const ctx = stContext();
        const last = state.lastRescue;
        const cached = state.lastStream;

        const html = `
        <div style="text-align:left;line-height:1.6;font-size:.94em;">
            <h3 style="margin:0 0 6px;font-size:1.05em;">空回助手</h3>
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
        </div>`;

        if (!ctx || typeof ctx.callGenericPopup !== 'function') {
            setEnabled(!state.enabled);
            notify('当前酒馆不支持弹窗，已直接切换开关');
            return;
        }

        // 点面板外面（酒馆空白处）就关掉
        let closing = false;
        const detach = () => document.removeEventListener('mousedown', onOutside, true);
        const closePopup = () => {
            if (closing) return;
            closing = true;
            try {
                if (typeof ctx.hidePopup === 'function') ctx.hidePopup();
                else {
                    const ok = document.querySelector('.popup .popup-button-ok');
                    if (ok) ok.click();
                }
            } catch (_) {}
            detach();
        };
        const onOutside = event => {
            const popup = document.querySelector('.popup');
            if (!popup) { detach(); return; }
            if (popup.contains(event.target)) return;
            closePopup();
        };

        let popupPromise;
        try {
            popupPromise = ctx.callGenericPopup(html, ctx.POPUP_TYPE.TEXT, '', {
                wide: true,
                okButton: '关闭',
                cancelButton: false,
                allowVerticalScrolling: true,
            });
        } catch (error) {
            notify('打开面板失败：' + error.message);
            return;
        }

        setTimeout(() => {
            document.addEventListener('mousedown', onOutside, true);
            const cb = document.getElementById('ss-assistant-enable');
            if (cb) cb.addEventListener('change', event => setEnabled(event.target.checked));
            const btn = document.getElementById('ss-assistant-rescue');
            if (btn) btn.addEventListener('click', () => rescueIntoChat());
        }, 200);

        Promise.resolve(popupPromise).finally(detach);
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
            container.addEventListener('click', event => {
                event.stopPropagation();
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
                    cached: cached ? { bytes: cached.bytes, complete: cached.complete, at: cached.at } : null,
                };
            },
            rescue: rescueIntoChat,
            salvageTail,
            reinstall() { state.wrapDepth = 0; ensureOutermost(); },
        };
    } catch (_) {}

    log(`v${VERSION} 已加载`);
})();
