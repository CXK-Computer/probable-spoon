/**
 * Ultimate Fusion Worker Script (v1.1 with Fallback Proxy)
 * 
 * This script combines the best features of three different workers:
 * 1.  BBR+ AI-Enhanced ECU Version: Provides the core architecture (ResilientDataPipeline), 
 *     the advanced BBR+ congestion control engine (MlEcu, BBRPlus), and production-grade
 *     features like telemetry and session caching.
 * 2.  Fusion (worker(2).js) Version: Provides the flexible URL parameter format for routing 
 *     (?proxyip=, ?s5=, ?http=) and the robust implementation logic for SOCKS5 and HTTP proxies.
 * 3.  stallTCP Version: Inspires the enhanced stall detection mechanism, enabling the pipeline
 *     to failover not just on connection errors, but also on connection stalls.
 *
 * NEW in v1.1:
 * -   Fallback Proxy: A configurable default proxy (FALLBACK_PROXY_IP) is automatically used
 *     as a last resort if all other user-specified connection methods fail.
 *
 * URL Format (from Fusion Version):
 * /?mode=auto&direct&proxyip=IP:PORT&s5=USER:PASS@IP:PORT&http=USER:PASS@IP:PORT
 * - `mode`: `auto` (default) tries connections in the order they appear, `direct`, `proxyip`, `s5`, `http` forces a specific mode.
 * - All parameters are optional. `direct` can be present as a key without a value.
 */

import { connect } from 'cloudflare:sockets';

// ==================================================================
// 1. 全局配置 (三合一)
// ==================================================================
const CONFIG = {
    // 身份验证
    AUTH_UUID: "fb00086e-abb9-4983-976f-d407bbea9a4c", // 在这里修改为你自己的UUID

    // 【新增】默认兜底反代IP (可选, 最后的备用连接方式)
    // 如果所有其他连接方式都失败了，脚本会尝试通过这个IP进行连接。
    // 推荐使用一个稳定、快速的海外 VPS IP 或 CNAME。设为 null 或 "" 来禁用。
    FALLBACK_PROXY_IP: "ProxyIP.US.CMLiussss.net:443",

    // BBR+ 引擎配置
    mtu: 1350,
    packetPoolSize: 512,

    // 弹性管道与稳定性配置 (融合 stallTCP 思想)
    sessionTTL: 5 * 60 * 1000,         // 会话缓存5分钟
    watchdogTimeout: 30000,            // 30秒无任何活动则断开
    stallTimeout: 10000,               // 10秒只发不收则判定为停滞 (来自 stallTCP 思想)

    // 遥测 (可选)
    enableTelemetry: true,
    telemetryInterval: 10000,
    log: true,
};

function LOG(...args) { if (CONFIG.log) console.log('[UltimateWorker]', ...args); }


// ==================================================================
// 2. 核心性能引擎 (来自 BBR+ 版)
// BBRPlus, GainTuner, MlEcu, Packet, PacketPool, SmartCond, ProgrammablePipeline
// ==================================================================
class SmartCond{constructor(){this._waiters=[]}async waitFor(e,t=0){if(e())return!0;return new Promise(s=>{const o={predicate:e,resolve:s};this._waiters.push(o),t>0&&(o.timer=setTimeout(()=>{const e=this._waiters.indexOf(o);-1!==e&&this._waiters.splice(e,1),s(!1)},t))})}notify(){const e=[],t=this._waiters.splice(0,this._waiters.length);for(const s of t)try{s.predicate()?(s.timer&&clearTimeout(s.timer),s.resolve(!0)):e.push(s)}catch(t){try{s.timer&&clearTimeout(s.timer),s.resolve(!1)}catch(e){}}this._waiters.push(...e)}closeAll(){const e=this._waiters.splice(0,this._waiters.length);for(const t of e)try{t.timer&&clearTimeout(t.timer),t.resolve(!1)}catch(e){}}}
class Packet{constructor(){this.buf=null,this.offset=0,this.length=0,this.priority="NORMAL",this._inFlightMarked=!1}set(e,t="NORMAL"){this.buf=e,this.offset=0,this.length=e?e.byteLength||e.length:0,this.priority=t,this._inFlightMarked=!1}reset(){this.buf=null,this.offset=0,this.length=0,this.priority="NORMAL",this._inFlightMarked=!1}}
class PacketPool{constructor(e=256){this.pool=Array.from({length:e},()=>new Packet)}get(){return this.pool.length?this.pool.pop():new Packet}release(e){try{e.reset(),this.pool.push(e)}catch(e){}}}
class BBRPlus{constructor({mtu:e=1400,minCwnd:t=4*1400}={}){this.mtu=e,this.minCwnd=t,this.state="STARTUP",this.cwnd=t,this.btlBw=0,this.rtProp=1/0,this.inFlight=0,this.ackCount=0,this.startupRoundsNoBwIncrease=0,this.lastBtlBw=0,this.maxCwnd=524288,this.probeRttStart=0,this.probeRttInflight=0,this.lastAckTime=Date.now()}onSend(e){this.inFlight+=e}onAck(e,t){if(!e||e<=0)return;this.lastAckTime=Date.now(),t>0&&(t<this.rtProp||this.rtProp===1/0)&&(this.rtProp=t),t>0&&(0===this.btlBw?this.btlBw=e/(t/1e3):this.btlBw=.9*this.btlBw+.1*e/(t/1e3)),this.inFlight=Math.max(0,this.inFlight-e),"STARTUP"===this.state?this._startupOnAck(e,t):"PROBE_BW"===this.state?this._probeBwOnAck(e,t):"PROBE_RTT"===this.state&&this._probeRttOnAck(e,t),this.cwnd=Math.max(this.minCwnd,Math.min(this.cwnd,this.maxCwnd))}_startupOnAck(e,t){const s=this.lastBtlBw||0;this.cwnd=Math.min(this.cwnd+e,this.maxCwnd),this.btlBw<=1.01*s?this.startupRoundsNoBwIncrease++:this.startupRoundsNoBwIncrease=0,this.lastBtlBw=this.btlBw;const o=this.rtProp!==1/0&&t>1.5*this.rtProp;(o||this.startupRoundsNoBwIncrease>=3||this.cwnd>=this.maxCwnd)&&(this.state="PROBE_BW",t=(this.btlBw||0)*(this.rtProp||100)/1e3,t>0&&(this.cwnd=Math.max(this.minCwnd,Math.min(this.maxCwnd,Math.ceil(1.5*t)))))}_probeBwOnAck(e,t){this.btlBw>0&&this.rtProp!==1/0&&(t=this.btlBw*this.rtProp/1e3,e=Math.max(this.minCwnd,Math.ceil(1.5*t)),this.cwnd<e?this.cwnd=Math.min(e,this.cwnd+ackBytes):this.cwnd=Math.max(e,this.cwnd-Math.floor(.1*ackBytes))),Date.now()-this.probeRttStart>1e4&&(this.state="PROBE_RTT",this.probeRttStart=Date.now(),this.probeRttInflight=this.inFlight)}_probeRttOnAck(e,t){t>0?(this.rtProp=Math.min(this.rtProp,t),this.state="PROBE_BW",this.probeRttStart=Date.now()):Date.now()-this.probeRttStart>500&&(this.state="PROBE_BW",this.probeRttStart=Date.now())}getAvailableWindowBytes(){return Math.max(0,Math.floor(this.cwnd-this.inFlight))}getPacingRate(e=1){return Math.max(1,e*(this.btlBw||1))}}
class GainTuner{constructor(){this.emaGain=1,this.alpha=.12}tune(e){let t=1;e.btlBw>0&&(t+=Math.min(.5,.05*Math.log2(1+e.btlBw/1048576))),e.jitter>20&&(t-=.2),e.latencyTrend>5&&(t-=.25),t=Math.max(.6,Math.min(1.6,t)),this.emaGain=this.alpha*t+(1-this.alpha)*this.emaGain;return this.emaGain}}
class MlEcu{constructor(e={asn:0,colo:"UNK"},t={}){this.context=e,this.mtu=t.mtu||CONFIG.mtu,this.minWindowPackets=t.minWindow||2,this.packetPool=new PacketPool(t.poolSize||CONFIG.packetPoolSize),this.cond=new SmartCond,this.queues={HIGH:[],NORMAL:[],LOW:[]},this.priorityOrder=["HIGH","NORMAL","LOW"],this.closed=!1,this.bbr=new BBRPlus({mtu:this.mtu,minCwnd:this.minWindowPackets*this.mtu}),this.gainTuner=new GainTuner,this.highWaterMark=128}async push(e,t="NORMAL"){if(this.closed)throw new Error("ecu closed");const s=this.packetPool.get();s.set(e,t),this.queues[t].push(s),this.cond.notify(),this._totalQueueSize()>2*this.highWaterMark&&await this.cond.waitFor(()=>this._totalQueueSize()<this.highWaterMark,5e3)}_totalQueueSize(){return this.queues.HIGH.length+this.queues.NORMAL.length+this.queues.LOW.length}ack(e,t){this.bbr.onAck(e,t),this.cond.notify()}async* [Symbol.asyncIterator](){for(;!this.closed;){const e=()=>this._totalQueueSize()>0&&this.bbr.getAvailableWindowBytes()>0,t=await this.cond.waitFor(e,CONFIG.watchdogTimeout);if(!t||this.closed)break;let s=null;let o=this.bbr.getAvailableWindowBytes();for(const e of this.priorityOrder)if(this.queues[e].length>0){if(this.queues[e][0].length<=o){s=this.queues[e].shift();break}if(o>this.mtu/4){const t=o,i=this.queues[e].shift(),h=i.buf.slice(t),n=this.packetPool.get();n.set(h,e),this.queues[e].unshift(n),s=this.packetPool.get(),s.set(i.buf.slice(0,t),e),this.packetPool.release(i);break}}if(s){this.bbr.onSend(s.length);const e=this.gainTuner.tune({btlBw:this.bbr.btlBw,rtProp:this.bbr.rtProp}),t=this.bbr.getPacingRate(e);if(t>0){const e=1e3*s.length/t;e>1&&await new Promise(t=>setTimeout(t,Math.min(e,200)))}try{yield s.buf}finally{this.packetPool.release(s)}}}}close(){if(this.closed)return;this.closed=!0,this.cond.closeAll();for(const e of Object.keys(this.queues))for(;this.queues[e].length;)this.packetPool.release(this.queues[e].shift())}}
class ProgrammablePipeline{constructor(e,t){this.closed=!1,this._underlyingReadable=e,this._underlyingWritable=t,this._readerConsumed=!1,this._readerActivated=!1;const{readable:s,writable:o}=new TransformStream;this._toEngineWriter=o.getWriter(),s.pipeTo(this._underlyingWritable).catch(e=>this.close(e))}async push(e){if(this.closed)throw new Error("pipeline closed");return await this._toEngineWriter.ready,this._toEngineWriter.write(e)}_activateReader(){if(this._readerActivated)return;const{readable:e,writable:t}=new TransformStream;this._underlyingReadable.pipeTo(t).catch(e=>this.close(e)),this._fromEngineReader=e.getReader(),this._readerActivated=!0}async* [Symbol.asyncIterator](){if(!this.closed&&!this.closed){if(this._readerConsumed)throw new Error("Cannot iterate after pipeTo");for(this._readerConsumed=!0,this._activateReader();!this.closed;)try{const{value:e,done:t}=await this._fromEngineReader.read();if(t)break;yield e}catch(e){if(!this.closed)console.error("[Pipeline] async iterator error",e?.message??e);break}this.close()}}pipeTo(e,t){if(this._readerConsumed)throw new Error("Cannot pipeTo after iterate");return this._readerConsumed=!0,this._underlyingReadable.pipeTo(e,t)}close(e){if(this.closed)return;this.closed=!0;e=e instanceof Error?e:new Error(e||"pipeline closed");try{this._toEngineWriter&&this._toEngineWriter.close().catch(()=>{})}catch(e){}try{this._underlyingReadable?.cancel&&this._underlyingReadable.cancel(e).catch(()=>{})}catch(e){}try{this._underlyingWritable?.abort&&this._underlyingWritable.abort(e).catch(()=>{})}catch(e){}try{this._fromEngineReader&&this._fromEngineReader.cancel(e).catch(()=>{})}catch(e){}}}


// ==================================================================
// 3. 生产级特性与弹性数据管道 (来自 BBR+ 版, 融合 stallTCP 思想)
// SessionCache, TelemetryReporter, ResilientDataPipeline
// ==================================================================
class SessionCache{constructor(){this._map=new Map}set(e,t){this._map.set(e,{created:Date.now(),payload:t})}get(e){const t=this._map.get(e);return t?(Date.now()-t.created>CONFIG.sessionTTL?(this._map.delete(e),null):t.payload):null}prune(){const e=Date.now();for(const[t,s]of this._map.entries())e-s.created>CONFIG.sessionTTL&&this._map.delete(t)}}
const localSessionCache = new SessionCache();

let _telemetryInstance = null;
class TelemetryReporter{constructor(){this.metrics=[],this.timer=setInterval(()=>this.flush(),CONFIG.telemetryInterval)}push(e){if(!CONFIG.enableTelemetry)return;try{this.metrics.push({ts:Date.now(),...e})}catch(e){}this.metrics.length>100&&this.flush()}flush(){if(!CONFIG.enableTelemetry||0===this.metrics.length)return;LOG("[TELEMETRY]",JSON.stringify(this.metrics.splice(0,this.metrics.length)))}close(){clearInterval(this.timer),this.flush()}}
function getTelemetry() { if (!_telemetryInstance) { _telemetryInstance = new TelemetryReporter(); } return _telemetryInstance; }

class ResilientDataPipeline {
    constructor(ws, connectionFactories = [], initialData = null, context = { asn:0, colo:'UNK' }) {
        this.ws = ws;
        this.wsStreams = websocketToStreams(ws);
        this.connectionFactories = connectionFactories;
        this.initialData = initialData;
        this.context = context;
        this.isClosed = false;
        this.currentFactoryIndex = 0;
        this.ws.addEventListener('close', () => this.close());
        this.ws.addEventListener('error', () => this.close());
    }

    async start() {
        while (!this.isClosed && this.currentFactoryIndex < this.connectionFactories.length) {
            let tcpSocket = null;
            const factory = this.connectionFactories[this.currentFactoryIndex];
            try {
                tcpSocket = await factory.connect();
                if (!tcpSocket) throw new Error('factory returned null socket');
                
                LOG(`Connection via [${factory.name}] succeeded.`);
                getTelemetry().push({ event: 'connection_success', factory: factory.name });

                await this._pipe(tcpSocket, this.initialData);
                this.isClosed = true; 

            } catch (err) {
                console.error(`[Resilient] factory [${factory.name}] failed:`, err?.message ?? err);
                getTelemetry().push({ event: 'connection_fail', factory: factory.name, error: err?.message ?? String(err) });
                try { if (tcpSocket?.close) tcpSocket.close(); } catch(_) {}
                
                this.currentFactoryIndex++;
                if (this.currentFactoryIndex < this.connectionFactories.length) {
                    LOG(`Trying next connection factory in 500ms...`);
                    await new Promise(res => setTimeout(res, 500));
                } else {
                    LOG('All connection factories failed.');
                    this.isClosed = true;
                }
            }
        }
        if (!this.isClosed) this.close();
    }

    async _pipe(tcpSocket, initialData) {
        const pipeline = new ProgrammablePipeline(tcpSocket.readable, tcpSocket.writable);
        const ecu = new MlEcu(this.context);
        const closeAll = async (reason) => {
            if (reason) LOG(`Closing pipeline: ${reason}`);
            ecu.close();
            pipeline.close();
            try { if (tcpSocket?.close) tcpSocket.close(); } catch(_) {}
        };

        let lastUpstreamActivity = Date.now();
        let lastDownstreamActivity = Date.now();
        let bytesSentUpstream = 0;

        const watchdog = setInterval(() => {
            const now = Date.now();
            if (now - Math.max(lastUpstreamActivity, lastDownstreamActivity) > CONFIG.watchdogTimeout) {
                clearInterval(watchdog);
                return closeAll('Watchdog timeout: No activity.');
            }
            if (bytesSentUpstream > 0 && now - lastDownstreamActivity > CONFIG.stallTimeout) {
                clearInterval(watchdog);
                return closeAll('Stall detected: No downstream data received.');
            }
        }, Math.min(CONFIG.watchdogTimeout, CONFIG.stallTimeout) / 2);

        try {
            const downstreamPromise = (async () => {
                for await (const chunk of pipeline) {
                    lastDownstreamActivity = Date.now();
                    if (this.ws.readyState === WebSocket.OPEN) {
                        this.ws.send(chunk);
                    } else {
                        break;
                    }
                }
            })();

            const pumpWsPromise = (async () => {
                const reader = this.wsStreams.readable.getReader();
                try {
                    if (initialData?.byteLength > 0) {
                        await ecu.push(initialData, 'HIGH');
                        getTelemetry().push({ event: 'initialDataPushed', len: initialData.byteLength });
                    }
                    while (true) {
                        const { value, done } = await reader.read();
                        if (done) break;
                        lastUpstreamActivity = Date.now();
                        if (!value) continue;
                        await ecu.push(value, 'NORMAL');
                    }
                } finally {
                    try { reader.releaseLock(); } catch(_) {}
                    ecu.close();
                }
            })();

            const upstreamPromise = (async () => {
                for await (const chunk of ecu) {
                    lastUpstreamActivity = Date.now();
                    bytesSentUpstream += chunk.byteLength;
                    try {
                        const start = Date.now();
                        await pipeline.push(chunk);
                        const latency = Date.now() - start;
                        ecu.ack(chunk.byteLength, latency);
                        getTelemetry().push({ event: 'sent', bytes: chunk.byteLength, latency });
                    } catch (err) {
                        console.error('[upstream] push->pipeline failed', err?.message ?? err);
                        break;
                    }
                }
            })();

            await Promise.allSettled([downstreamPromise, pumpWsPromise, upstreamPromise]);
        } finally {
            clearInterval(watchdog);
            await closeAll();
        }
    }

    close() {
        if (this.isClosed) return;
        this.isClosed = true;
        try { if (this.ws?.close) this.ws.close(1000, 'terminated'); } catch(_) {}
    }
}


// ==================================================================
// 4. SOCKS5 & HTTP 代理逻辑 (来自融合版)
// ==================================================================
async function createSocks5ProxySocket(destination, proxyConfig) {
    LOG(`Creating SOCKS5 connection via ${proxyConfig.host}:${proxyConfig.port}`);
    const socket = await connect({ hostname: proxyConfig.host, port: proxyConfig.port });
    
    const writer = socket.writable.getWriter();
    const reader = socket.readable.getReader();
    const encoder = new TextEncoder();

    // SOCKS5 handshake
    await writer.write(new Uint8Array([5, 1, 0])); // Version 5, 1 auth method, no-auth
    const resp1 = (await reader.read()).value;
    if (!resp1 || resp1[0] !== 5 || resp1[1] !== 0) {
        writer.releaseLock(); reader.releaseLock();
        socket.close();
        throw new Error('SOCKS5 initial handshake failed');
    }

    // SOCKS5 connect request
    let destHostBytes;
    const destPortBytes = new Uint8Array([destination.port >> 8, destination.port & 0xff]);
    
    // Address Type: 1=IPv4, 3=Domain, 4=IPv6
    if (destination.type === 1) { // IPv4
        destHostBytes = new Uint8Array([1, ...destination.host.split('.').map(Number)]);
    } else if (destination.type === 3) { // IPv6
         const ipv6Bytes = destination.host.split(':').flatMap(s => {
            const val = parseInt(s, 16) || 0;
            return [val >> 8, val & 0xff];
        });
        destHostBytes = new Uint8Array([4, ...ipv6Bytes]);
    } else { // Domain (type 2)
        const domainBytes = encoder.encode(destination.host);
        destHostBytes = new Uint8Array([3, domainBytes.length, ...domainBytes]);
    }

    await writer.write(new Uint8Array([5, 1, 0, ...destHostBytes, ...destPortBytes]));

    const resp2 = (await reader.read()).value;
    if (!resp2 || resp2[0] !== 5 || resp2[1] !== 0) {
        writer.releaseLock(); reader.releaseLock();
        socket.close();
        throw new Error(`SOCKS5 connection failed with code ${resp2 ? resp2[1] : 'N/A'}`);
    }
    
    writer.releaseLock();
    reader.releaseLock();
    return socket;
}

async function createHttpProxySocket(destination, proxyConfig) {
    LOG(`Creating HTTP Proxy connection via ${proxyConfig.host}:${proxyConfig.port}`);
    const socket = await connect({ hostname: proxyConfig.host, port: proxyConfig.port });

    const writer = socket.writable.getWriter();
    const reader = socket.readable.getReader();
    
    const destAddress = destination.type === 3 ? `[${destination.host}]:${destination.port}` : `${destination.host}:${destination.port}`;
    let connectRequest = `CONNECT ${destAddress} HTTP/1.1\r\nHost: ${destAddress}\r\n`;

    if (proxyConfig.user || proxyConfig.pass) {
        const credentials = btoa(`${proxyConfig.user}:${proxyConfig.pass}`);
        connectRequest += `Proxy-Authorization: Basic ${credentials}\r\n`;
    }
    connectRequest += '\r\n';

    await writer.write(new TextEncoder().encode(connectRequest));

    let response = '';
    const decoder = new TextDecoder();
    while (!response.includes('\r\n\r\n')) {
        const { value, done } = await reader.read();
        if (done) throw new Error('HTTP proxy connection closed prematurely');
        response += decoder.decode(value, { stream: true });
    }
    
    if (!response.startsWith('HTTP/1.1 200')) {
        writer.releaseLock(); reader.releaseLock();
        socket.close();
        throw new Error(`HTTP proxy connection failed: ${response.split('\r\n')[0]}`);
    }

    writer.releaseLock();
    reader.releaseLock();
    return socket;
}


// ==================================================================
// 5. 辅助函数 (多源融合)
// ==================================================================
function websocketToStreams(ws) { const readable = new ReadableStream({ start(controller) { ws.addEventListener('message', e => { try { const data = e.data; if (data instanceof ArrayBuffer) { controller.enqueue(new Uint8Array(data)); } } catch (err) { controller.error(err); } }); ws.addEventListener('close', () => controller.close()); ws.addEventListener('error', e => controller.error(e)); } }); const writable = new WritableStream({ write(chunk) { ws.send(chunk); }, close() { ws.close(1000); }, abort(reason) { ws.close(1001, reason?.message); } }); return { readable, writable }; }
function parseProxyConfig(proxyStr) { let user = null, pass = null, host = null, port = null; const atIndex = proxyStr.lastIndexOf('@'); if (atIndex !== -1) { const credentials = proxyStr.substring(0, atIndex); const colonIndex = credentials.indexOf(':'); if (colonIndex !== -1) { user = decodeURIComponent(credentials.substring(0, colonIndex)); pass = decodeURIComponent(credentials.substring(colonIndex + 1)); } else { user = decodeURIComponent(credentials); } [host, port] = parseHostPort(proxyStr.substring(atIndex + 1)); } else { [host, port] = parseHostPort(proxyStr); } return { user, pass, host, port: +port || 443 }; }
function parseHostPort(hostPortStr, defaultPort = 443) { if (!hostPortStr) return [null, defaultPort]; if (hostPortStr.startsWith('[')) { const closingBracketIndex = hostPortStr.lastIndexOf(']'); if (closingBracketIndex > 0) { const host = hostPortStr.substring(1, closingBracketIndex); const port = hostPortStr.substring(closingBracketIndex + 2) || defaultPort; return [host, port]; } } const colonIndex = hostPortStr.lastIndexOf(':'); if (colonIndex === -1) { return [hostPortStr, defaultPort]; } return [hostPortStr.substring(0, colonIndex), hostPortStr.substring(colonIndex + 1)]; }
function getConnectionModes(mode, params) { const order = []; if (mode && mode !== 'auto') { return [mode]; } for (const key of ['s5', 'http', 'proxyip', 'direct']) { if (params.has(key) && !order.includes(key)) { order.push(key); } } return order.length ? order : ['direct']; }
function buildUUID(arr, start) { const hex = Array.from(arr.slice(start, start + 16)).map(n => n.toString(16).padStart(2, '0')).join(''); return hex.replace(/(.{8})(.{4})(.{4})(.{4})(.{12})/, '$1-$2-$3-$4-$5'); }
function extractAddress(bytes) { if (bytes.length < 24) throw new Error("Invalid initial packet"); const offset1 = 18 + bytes[17] + 1; const port = (bytes[offset1] << 8) | bytes[offset1 + 1]; const type = bytes[offset1 + 2]; let offset2 = offset1 + 3; let length, host; switch (type) { case 1: length = 4; host = bytes.slice(offset2, offset2 + length).join('.'); break; case 2: length = bytes[offset2]; offset2++; host = new TextDecoder().decode(bytes.slice(offset2, offset2 + length)); break; case 3: length = 16; host = Array.from({length: 8}, (_, i) => ((bytes[offset2 + i * 2] << 8) | bytes[offset2 + i * 2 + 1]).toString(16)).join(':'); break; default: throw new Error(`Invalid address type: ${type}`); } const payload = bytes.slice(offset2 + length); return { host, port, type, payload }; }


// ==================================================================
// 6. WebSocket 处理器 (三合一)
// ==================================================================
async function handleWebSocket(ws, request) {
    try {
        const context = { colo: request.cf?.colo || 'UNKNOWN', asn: request.cf?.asn || 0 };
        const firstPacket = await new Promise((resolve, reject) => {
            const timer = setTimeout(() => reject(new Error('First packet timeout')), 5000);
            ws.addEventListener('message', evt => {
                clearTimeout(timer);
                if (evt.data instanceof ArrayBuffer) resolve(new Uint8Array(evt.data));
                else reject(new Error("First packet not ArrayBuffer"));
            }, { once: true });
        });

        if (buildUUID(firstPacket, 1) !== CONFIG.AUTH_UUID) {
            throw new Error('Authentication failed');
        }
        const destination = extractAddress(firstPacket);
        LOG(`Target destination: ${destination.host}:${destination.port}`);
        
        const url = new URL(request.url);
        const params = url.searchParams;
        const mode = params.get('mode') || 'auto';
        
        const connectionFactories = [];
        const connectionModes = getConnectionModes(mode, params);

        for (const connMode of connectionModes) {
            switch (connMode) {
                case 's5':
                    if (params.has('s5')) {
                        const s5Config = parseProxyConfig(params.get('s5'));
                        connectionFactories.push({ name: `S5 via ${s5Config.host}`, connect: () => createSocks5ProxySocket(destination, s5Config) });
                    }
                    break;
                case 'http':
                    if (params.has('http')) {
                        const httpConfig = parseProxyConfig(params.get('http'));
                        connectionFactories.push({ name: `HTTP via ${httpConfig.host}`, connect: () => createHttpProxySocket(destination, httpConfig) });
                    }
                    break;
                case 'proxyip':
                    if (params.has('proxyip')) {
                        const [host, port] = parseHostPort(params.get('proxyip'));
                        connectionFactories.push({ name: `ProxyIP via ${host}`, connect: async () => { const sock = connect({ hostname: host, port: +port, tls: { servername: destination.host } }); await sock.opened; return sock; }});
                    }
                    break;
                case 'direct':
                    connectionFactories.push({ name: 'Direct', connect: async () => { const sock = connect({ hostname: destination.host, port: destination.port }); await sock.opened; return sock; }});
                    break;
            }
        }
        LOG('User-defined connection modes to try:', connectionFactories.map(f => f.name));
        
        // 【新增逻辑】如果配置了兜底反代, 将其作为最后一个尝试选项
        if (CONFIG.FALLBACK_PROXY_IP) {
            const [host, port] = parseHostPort(CONFIG.FALLBACK_PROXY_IP);
            connectionFactories.push({
                name: `Fallback via ${host}`,
                connect: async () => {
                    LOG(`Attempting connection via fallback proxy: ${host}:${port}`);
                    const sock = connect({ hostname: host, port: +port, tls: { servername: destination.host } });
                    await sock.opened;
                    return sock;
                }
            });
        }

        if (connectionFactories.length === 0) {
            throw new Error("No valid connection modes configured and no fallback available.");
        }

        const replyHead = new Uint8Array([firstPacket[0] || 0, 0]);
        if (ws.readyState === WebSocket.OPEN) ws.send(replyHead);

        const pipeline = new ResilientDataPipeline(ws, connectionFactories, destination.payload, context);
        await pipeline.start();
        getTelemetry().push({ event: 'session_end' });

    } catch (err) {
        console.error('[WS handler] error:', err?.message ?? err);
        getTelemetry().push({ event: 'handler_error', error: err?.message ?? String(err)});
        if (ws.readyState === WebSocket.OPEN) {
            ws.close(1011, `Error: ${err.message}`);
        }
    }
}


// ==================================================================
// 7. Worker 入口 (融合版格式)
// ==================================================================
export default {
    async fetch(request, env, ctx) {
        try {
            if (request.headers.get('Upgrade')?.toLowerCase() !== 'websocket') {
                const url = new URL(request.url);
                if (url.pathname.startsWith('/telemetry')) { 
                    getTelemetry().flush(); 
                    return new Response('Telemetry flushed', { status: 200 }); 
                }
                return new Response('This is an Ultimate Fusion Worker. Expecting WebSocket upgrades.', { status: 200 });
            }

            const { 0: client, 1: server } = new WebSocketPair();
            server.accept();
            ctx.waitUntil(handleWebSocket(server, request));

            return new Response(null, { status: 101, webSocket: client });

        } catch (err) {
            console.error('[Fetch] fatal error:', err?.stack || err);
            return new Response('Internal Server Error', { status: 500 });
        }
    }
};
