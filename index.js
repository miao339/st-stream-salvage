/**
 * 流式抢救 · Stream Salvage
 * ------------------------------------------------------------------
 * 解决什么问题：
 *   某些中转站在生成结束时，会把整段结果塞进一个超长的 SSE 事件里一次性推过来。
 *   如果这一下没推完整（断在字符串中间、或者整条事件没有换行收尾），
 *   酒馆的流解析器就凑不出一个完整事件，JSON.parse 失败，界面一片空白。
 *
 * 本插件做什么：
 *   在 window.fetch 层接管 SSE 响应，按行缓冲。
 *   完整的行立刻原样转发（不影响正常流式体验），
 *   不完整的尾巴留在本地。等流结束时，从残缺尾巴里把已经闭合的字段捞出来，
 *   重新封成一个合法的 tool_calls 事件 + finish_reason + [DONE] 再交给下游。
 *
 * 对正常响应做什么：
 *   什么都不做。字节原样透传，不解析、不改写、不加延迟。
 *
 * 优先级：
 *   每 800ms 检查一次 window.fetch。如果被别的插件重新包装过，
 *   就再包一层，保证我们永远在最外层，能拿到最原始的字节流。
 */

(function () {
    'use strict';

    const SCRIPT_NAME = '流式抢救 · Stream Salvage';
    const VERSION = '1.0.0';
    const WRAP_MARK = '__streamSalvageOuter__';
    const GUARD_MS = 800;

    const state = {
        installedAt: Date.now(),
        rescued: 0,
        lastRescue: null,
        wrapDepth: 0,
    };

    function log(...args) {
        try { console.log(`[${SCRIPT_NAME}]`, ...args); } catch (_) {}
    }

    function warn(...args) {
        try { console.warn(`[${SCRIPT_NAME}]`, ...args); } catch (_) {}
    }

    function notify(message) {
        const hosts = [];
        try { hosts.push(window); } catch (_) {}
        try { if (window.top && window.top !== window) hosts.push(window.top); } catch (_) {}
        for (const host of hosts) {
            try {
                const t = host.toastr;
                if (t && typeof t.warning === 'function') {
                    t.warning(message, SCRIPT_NAME, { timeOut: 8000 });
                    return;
                }
            } catch (_) {}
        }
        warn(message);
    }

    // ── 转义还原 ────────────────────────────────────────────────────────────
    // 把 JSON 字符串内部的内容还原成真实文本：
    //   \u003c -> <   \n -> 换行   \" -> "   \\ -> \
    // 注意：部分中转站把 arguments 套了两层转义（\\u003c），所以这里循环解到稳定为止。
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

    // ── 从残缺的 arguments 里捞出已经闭合的字符串字段 ──────────────────────
    // 残缺 JSON 长这样（注意 key 外面还套着一层转义引号）：
    //   {\"game_content\":\"\u003cgame\u003e...已闭合...\",\"think_nya\":\"写到一半就断了
    // 我们的目标：只要某个字段的收尾引号还在，就把它的内容完整取出来。
    function extractClosedFields(argsRaw) {
        const out = {};
        if (!argsRaw) return out;
        // \\[^"] 吃掉「反斜杠 + 非引号」（如 \n \u003c \\），但不允许吃掉 \"
        // 这样 \" 只会被当作字段结束的标志，不会被误当转义序列
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

    // ── 把抢救出来的字段重新封成合法 SSE ────────────────────────────────────
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

    // ── 抢救尾巴 ────────────────────────────────────────────────────────────
    // tail = 流结束时缓冲区里还没换行的那一坨原始文本
    function salvageTail(tail, toolHint) {
        const trimmed = String(tail ?? '').trim();
        if (!trimmed) return null;

        const dataLines = trimmed
            .split('\n')
            .filter(line => line.startsWith('data:'))
            .map(line => line.slice(5).trim());

        if (!dataLines.length) return null;
        const raw = dataLines.join('');

        // 工具名：优先从残缺数据里找，找不到就用请求体里的工具名兜底
        let toolName = null;
        const nameMatch = raw.match(/"name"\s*:\s*"([A-Za-z0-9_.\-]+)"/);
        if (nameMatch) toolName = nameMatch[1];
        if (!toolName && toolHint) toolName = toolHint;

        // 1) 优先抢救 tool_calls.arguments
        const argsMatch = raw.match(/"arguments"\s*:\s*"((?:\\.|[^"\\])*)/);
        if (argsMatch) {
            const fields = extractClosedFields(argsMatch[1]);
            const keys = Object.keys(fields);
            if (keys.length) {
                const chars = keys.reduce((sum, key) => sum + fields[key].length, 0);
                return {
                    text: buildSalvageEvents({ fields, toolName }),
                    info: { mode: 'tool_calls', chars, fields: keys, tool: toolName },
                };
            }
        }

        // 2) 退一步，抢救普通 content
        const contentMatch = raw.match(/"content"\s*:\s*"((?:\\.|[^"\\])*)/);
        if (contentMatch) {
            const text = unescapeJsonString(contentMatch[1].replace(/\\+$/, ''));
            if (text && text.trim()) {
                return {
                    text: buildSalvageEvents({ fields: {}, toolName: null, text }),
                    info: { mode: 'content', chars: text.length },
                };
            }
        }

        return null;
    }

    // ── 从请求体里读工具名（残缺响应常常只给 arguments，不给 name）──────────
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

    // ── 包装一个 SSE 响应 ───────────────────────────────────────────────────
    function wrapSseResponse(response, toolHint) {
        let contentType = '';
        try { contentType = String(response.headers.get('content-type') || '').toLowerCase(); } catch (_) {}
        if (!contentType.includes('text/event-stream')) return response;
        if (!response.body) return response;
        if (response.__streamSalvage) return response;

        const reader = response.body.getReader();
        const decoder = new TextDecoder('utf-8');
        const encoder = new TextEncoder();
        let buffer = '';
        let finished = false;

        const emitSalvage = (controller, reason) => {
            const pending = buffer;
            buffer = '';
            if (!pending.trim()) return false;
            const rescued = salvageTail(pending, toolHint);
            if (!rescued) {
                warn(`${reason}：尾巴里没有可抢救的已闭合字段，原样放弃`, pending.slice(0, 200));
                return false;
            }
            try { controller.enqueue(encoder.encode(rescued.text)); } catch (_) { return false; }
            state.rescued += 1;
            state.lastRescue = { at: Date.now(), ...rescued.info };
            warn(`${reason}：已抢救`, rescued.info);
            notify(`上游断在行内，已从残缺响应里救回 ${rescued.info.chars} 字内容`);
            return true;
        };

        const stream = new ReadableStream({
            async pull(controller) {
                if (finished) return;
                try {
                    const { done, value } = await reader.read();
                    if (done) {
                        finished = true;
                        emitSalvage(controller, '流出完但尾巴未闭合');
                        try { controller.close(); } catch (_) {}
                        return;
                    }
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
                    try { controller.close(); } catch (_) {}
                }
            },
            cancel(reason) {
                finished = true;
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

    // ── 安装 fetch 钩子，并保证永远在最外层 ─────────────────────────────────
    function installOnce() {
        const current = window.fetch;
        if (typeof current !== 'function') return;
        if (current[WRAP_MARK]) return; // 已经是我们，且在最外层

        const inner = current;
        const wrapped = async function (...args) {
            const toolHint = extractToolName(args);
            const response = await inner.apply(this, args);
            try {
                return wrapSseResponse(response, toolHint);
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

    ensureOutermost();
    setInterval(ensureOutermost, GUARD_MS);

    // ── 对外暴露一点调试信息 ────────────────────────────────────────────────
    try {
        window.__streamSalvage__ = {
            version: VERSION,
            status() {
                return {
                    installedAt: state.installedAt,
                    rescued: state.rescued,
                    lastRescue: state.lastRescue,
                    outermost: Boolean(window.fetch && window.fetch[WRAP_MARK]),
                    wrapDepth: state.wrapDepth,
                };
            },
            salvageTail,
            reinstall: ensureOutermost,
        };
    } catch (_) {}

    log(`v${VERSION} 已加载，正在监视 SSE 流（每 ${GUARD_MS}ms 校准一次最外层）`);
})();
