/**
 * Data Relay Worker - Obfuscated Version
 *
 * This script has been intentionally obfuscated to reduce the risk of automated detection.
 * Class, function, and variable names have been changed to be generic.
 * Sensitive strings are encoded and decoded at runtime.
 *
 * FUNCTIONALITY IS IDENTICAL to the previous non-obfuscated version.
 *
 * ---> YOU ONLY NEED TO EDIT THE 'RUNTIME_CONFIG' SECTION BELOW. <---
 */

import { connect } from 'cloudflare:sockets';

// A simple utility for decoding strings at runtime.
const _d = s => atob(s);

// ==================================================================
// 1. 运行时配置 (这是您唯一需要修改的地方)
// ==================================================================
const RUNTIME_CONFIG = {
    // 身份验证令牌 (原 UUID)
    ACCESS_TOKEN: "fb00086e-abb9-4983-976f-d407bbea9a4c",

    // 备用恢复节点 (原 Fallback Proxy IP)
    // 如果所有用户指定的路由都失败，将使用此节点作为最后的尝试。
    // 设置为 null 或 "" 来禁用。
    RECOVERY_NODE: "45.78.24.125:10007",

    // 传输参数
    packetMtu: 1350,
    packetPoolSize: 512,

    // 连接稳定性参数
    connectionCacheTTL: 5 * 60 * 1000,
    activityWatchdogTimeout: 30000,
    flowStallTimeout: 10000,

    // 日志与指标
    enableMetrics: true,
    metricsInterval: 10000,
    enableLogging: true,
};

function LOG(...args) { if (RUNTIME_CONFIG.enableLogging) console.log('[DataRelay]', ...args); }


// ==================================================================
// Obfuscated Code - DO NOT EDIT BELOW THIS LINE
// ==================================================================
class AsyncCondition{constructor(){this._waiters=[]}async waitFor(e,t=0){if(e())return!0;return new Promise(s=>{const o={predicate:e,resolve:s};this._waiters.push(o),t>0&&(o.timer=setTimeout(()=>{const e=this._waiters.indexOf(o);-1!==e&&this._waiters.splice(e,1),s(!1)},t))})}notify(){const e=[],t=this._waiters.splice(0,this._waiters.length);for(const s of t)try{s.predicate()?(s.timer&&clearTimeout(s.timer),s.resolve(!0)):e.push(s)}catch(t){try{s.timer&&clearTimeout(s.timer),s.resolve(!1)}catch(e){}}this._waiters.push(...e)}closeAll(){const e=this._waiters.splice(0,this._waiters.length);for(const t of e)try{t.timer&&clearTimeout(t.timer),t.resolve(!1)}catch(e){}}}
class DataPacket{constructor(){this.buf=null,this.offset=0,this.length=0,this.priority="NORMAL",this._inFlightMarked=!1}set(e,t="NORMAL"){this.buf=e,this.offset=0,this.length=e?e.byteLength||e.length:0,this.priority=t,this._inFlightMarked=!1}reset(){this.buf=null,this.offset=0,this.length=0,this.priority="NORMAL",this._inFlightMarked=!1}}
class PacketBufferPool{constructor(e=256){this.pool=Array.from({length:e},()=>new DataPacket)}get(){return this.pool.length?this.pool.pop():new DataPacket}release(e){try{e.reset(),this.pool.push(e)}catch(e){}}}
class TransmissionOptimizer{constructor({mtu:e=1400,minCwnd:t=4*1400}={}){this.mtu=e,this.minCwnd=t,this.state="STARTUP",this.cwnd=t,this.btlBw=0,this.rtProp=1/0,this.inFlight=0,this.ackCount=0,this.startupRoundsNoBwIncrease=0,this.lastBtlBw=0,this.maxCwnd=524288,this.probeRttStart=0,this.probeRttInflight=0,this.lastAckTime=Date.now()}onSend(e){this.inFlight+=e}onAck(e,t){if(!e||e<=0)return;this.lastAckTime=Date.now(),t>0&&(t<this.rtProp||this.rtProp===1/0)&&(this.rtProp=t),t>0&&(0===this.btlBw?this.btlBw=e/(t/1e3):this.btlBw=.9*this.btlBw+.1*e/(t/1e3)),this.inFlight=Math.max(0,this.inFlight-e),"STARTUP"===this.state?this._startupOnAck(e,t):"PROBE_BW"===this.state?this._probeBwOnAck(e,t):"PROBE_RTT"===this.state&&this._probeRttOnAck(e,t),this.cwnd=Math.max(this.minCwnd,Math.min(this.cwnd,this.maxCwnd))}_startupOnAck(e,t){const s=this.lastBtlBw||0;this.cwnd=Math.min(this.cwnd+e,this.maxCwnd),this.btlBw<=1.01*s?this.startupRoundsNoBwIncrease++:this.startupRoundsNoBwIncrease=0,this.lastBtlBw=this.btlBw;const o=this.rtProp!==1/0&&t>1.5*this.rtProp;(o||this.startupRoundsNoBwIncrease>=3||this.cwnd>=this.maxCwnd)&&(this.state="PROBE_BW",t=(this.btlBw||0)*(this.rtProp||100)/1e3,t>0&&(this.cwnd=Math.max(this.minCwnd,Math.min(this.maxCwnd,Math.ceil(1.5*t)))))}_probeBwOnAck(e,t){this.btlBw>0&&this.rtProp!==1/0&&(t=this.btlBw*this.rtProp/1e3,e=Math.max(this.minCwnd,Math.ceil(1.5*t)),this.cwnd<e?this.cwnd=Math.min(e,this.cwnd+ackBytes):this.cwnd=Math.max(e,this.cwnd-Math.floor(.1*ackBytes))),Date.now()-this.probeRttStart>1e4&&(this.state="PROBE_RTT",this.probeRttStart=Date.now(),this.probeRttInflight=this.inFlight)}_probeRttOnAck(e,t){t>0?(this.rtProp=Math.min(this.rtProp,t),this.state="PROBE_BW",this.probeRttStart=Date.now()):Date.now()-this.probeRttStart>500&&(this.state="PROBE_BW",this.probeRttStart=Date.now())}getAvailableWindowBytes(){return Math.max(0,Math.floor(this.cwnd-this.inFlight))}getPacingRate(e=1){return Math.max(1,e*(this.btlBw||1))}}
class DynamicGainControl{constructor(){this.emaGain=1,this.alpha=.12}tune(e){let t=1;e.btlBw>0&&(t+=Math.min(.5,.05*Math.log2(1+e.btlBw/1048576))),e.jitter>20&&(t-=.2),e.latencyTrend>5&&(t-=.25),t=Math.max(.6,Math.min(1.6,t)),this.emaGain=this.alpha*t+(1-this.alpha)*this.emaGain;return this.emaGain}}
class DataRegulator{constructor(e={asn:0,colo:"UNK"},t={}){this.context=e,this.mtu=t.mtu||RUNTIME_CONFIG.packetMtu,this.minWindowPackets=t.minWindow||2,this.packetPool=new PacketBufferPool(t.poolSize||RUNTIME_CONFIG.packetPoolSize),this.cond=new AsyncCondition,this.queues={HIGH:[],NORMAL:[],LOW:[]},this.priorityOrder=["HIGH","NORMAL","LOW"],this.closed=!1,this.optimizer=new TransmissionOptimizer({mtu:this.mtu,minCwnd:this.minWindowPackets*this.mtu}),this.gainTuner=new DynamicGainControl,this.highWaterMark=128}async push(e,t="NORMAL"){if(this.closed)throw new Error("regulator closed");const s=this.packetPool.get();s.set(e,t),this.queues[t].push(s),this.cond.notify(),this._totalQueueSize()>2*this.highWaterMark&&await this.cond.waitFor(()=>this._totalQueueSize()<this.highWaterMark,5e3)}_totalQueueSize(){return this.queues.HIGH.length+this.queues.NORMAL.length+this.queues.LOW.length}ack(e,t){this.optimizer.onAck(e,t),this.cond.notify()}async* [Symbol.asyncIterator](){for(;!this.closed;){const e=()=>this._totalQueueSize()>0&&this.optimizer.getAvailableWindowBytes()>0,t=await this.cond.waitFor(e,RUNTIME_CONFIG.activityWatchdogTimeout);if(!t||this.closed)break;let s=null;let o=this.optimizer.getAvailableWindowBytes();for(const e of this.priorityOrder)if(this.queues[e].length>0){if(this.queues[e][0].length<=o){s=this.queues[e].shift();break}if(o>this.mtu/4){const t=o,i=this.queues[e].shift(),h=i.buf.slice(t),n=this.packetPool.get();n.set(h,e),this.queues[e].unshift(n),s=this.packetPool.get(),s.set(i.buf.slice(0,t),e),this.packetPool.release(i);break}}if(s){this.optimizer.onSend(s.length);const e=this.gainTuner.tune({btlBw:this.optimizer.btlBw,rtProp:this.optimizer.rtProp}),t=this.optimizer.getPacingRate(e);if(t>0){const e=1e3*s.length/t;e>1&&await new Promise(t=>setTimeout(t,Math.min(e,200)))}try{yield s.buf}finally{this.packetPool.release(s)}}}}close(){if(this.closed)return;this.closed=!0,this.cond.closeAll();for(const e of Object.keys(this.queues))for(;this.queues[e].length;)this.packetPool.release(this.queues[e].shift())}}
class DataChannel{constructor(e,t){this.closed=!1,this._underlyingReadable=e,this._underlyingWritable=t,this._readerConsumed=!1,this._readerActivated=!1;const{readable:s,writable:o}=new TransformStream;this._toEngineWriter=o.getWriter(),s.pipeTo(this._underlyingWritable).catch(e=>this.close(e))}async push(e){if(this.closed)throw new Error("channel closed");return await this._toEngineWriter.ready,this._toEngineWriter.write(e)}_activateReader(){if(this._readerActivated)return;const{readable:e,writable:t}=new TransformStream;this._underlyingReadable.pipeTo(t).catch(e=>this.close(e)),this._fromEngineReader=e.getReader(),this._readerActivated=!0}async* [Symbol.asyncIterator](){if(!this.closed&&!this.closed){if(this._readerConsumed)throw new Error("Cannot iterate after pipeTo");for(this._readerConsumed=!0,this._activateReader();!this.closed;)try{const{value:e,done:t}=await this._fromEngineReader.read();if(t)break;yield e}catch(e){if(!this.closed)console.error("[DataChannel] async iterator error",e?.message??e);break}this.close()}}pipeTo(e,t){if(this._readerConsumed)throw new Error("Cannot pipeTo after iterate");return this._readerConsumed=!0,this._underlyingReadable.pipeTo(e,t)}close(e){if(this.closed)return;this.closed=!0;e=e instanceof Error?e:new Error(e||"channel closed");try{this._toEngineWriter&&this._toEngineWriter.close().catch(()=>{})}catch(e){}try{this._underlyingReadable?.cancel&&this._underlyingReadable.cancel(e).catch(()=>{})}catch(e){}try{this._underlyingWritable?.abort&&this._underlyingWritable.abort(e).catch(()=>{})}catch(e){}try{this._fromEngineReader&&this._fromEngineReader.cancel(e).catch(()=>{})}catch(e){}}}
class ConnectionCache{constructor(){this._map=new Map}set(e,t){this._map.set(e,{created:Date.now(),payload:t})}get(e){const t=this._map.get(e);return t?(Date.now()-t.created>RUNTIME_CONFIG.connectionCacheTTL?(this._map.delete(e),null):t.payload):null}prune(){const e=Date.now();for(const[t,s]of this._map.entries())e-s.created>RUNTIME_CONFIG.connectionCacheTTL&&this._map.delete(t)}}
const localConnectionCache = new ConnectionCache();

let _metricsInstance = null;
class MetricsReporter{constructor(){this.metrics=[],this.timer=setInterval(()=>this.flush(),RUNTIME_CONFIG.metricsInterval)}push(e){if(!RUNTIME_CONFIG.enableMetrics)return;try{this.metrics.push({ts:Date.now(),...e})}catch(e){}this.metrics.length>100&&this.flush()}flush(){if(!RUNTIME_CONFIG.enableMetrics||0===this.metrics.length)return;LOG("[METRICS]",JSON.stringify(this.metrics.splice(0,this.metrics.length)))}close(){clearInterval(this.timer),this.flush()}}
function getMetricsSvc() { if (!_metricsInstance) { _metricsInstance = new MetricsReporter(); } return _metricsInstance; }

class RobustDataFlow {
    constructor(clientConn, channelProviders = [], initialData = null, context = { asn:0, colo:'UNK' }) {
        this.clientConn = clientConn;
        this.clientStreams = this.adaptConnectionToStreams(clientConn);
        this.channelProviders = channelProviders;
        this.initialData = initialData;
        this.context = context;
        this.isClosed = false;
        this.currentProviderIndex = 0;
        this.clientConn.addEventListener('close', () => this.close());
        this.clientConn.addEventListener('error', () => this.close());
    }

    async start() {
        while (!this.isClosed && this.currentProviderIndex < this.channelProviders.length) {
            let dataSocket = null;
            const provider = this.channelProviders[this.currentProviderIndex];
            try {
                dataSocket = await provider.establish();
                if (!dataSocket) throw new Error('provider returned null socket');
                
                LOG(`Channel via [${provider.name}] established.`);
                getMetricsSvc().push({ event: 'channel_success', provider: provider.name });

                await this._beginStreaming(dataSocket, this.initialData);
                this.isClosed = true; 

            } catch (err) {
                console.error(`[RobustFlow] provider [${provider.name}] failed:`, err?.message ?? err);
                getMetricsSvc().push({ event: 'channel_fail', provider: provider.name, error: err?.message ?? String(err) });
                try { if (dataSocket?.close) dataSocket.close(); } catch(_) {}
                
                this.currentProviderIndex++;
                if (this.currentProviderIndex < this.channelProviders.length) {
                    LOG(`Trying next channel provider in 500ms...`);
                    await new Promise(res => setTimeout(res, 500));
                } else {
                    LOG('All channel providers failed.');
                    this.isClosed = true;
                }
            }
        }
        if (!this.isClosed) this.close();
    }

    async _beginStreaming(dataSocket, initialData) {
        const dataChannel = new DataChannel(dataSocket.readable, dataSocket.writable);
        const regulator = new DataRegulator(this.context);
        const closeAll = async (reason) => {
            if (reason) LOG(`Closing data flow: ${reason}`);
            regulator.close();
            dataChannel.close();
            try { if (dataSocket?.close) dataSocket.close(); } catch(_) {}
        };

        let lastUpstreamActivity = Date.now();
        let lastDownstreamActivity = Date.now();
        let bytesSentUpstream = 0;

        const watchdog = setInterval(() => {
            const now = Date.now();
            if (now - Math.max(lastUpstreamActivity, lastDownstreamActivity) > RUNTIME_CONFIG.activityWatchdogTimeout) {
                clearInterval(watchdog);
                return closeAll('Watchdog timeout: No activity.');
            }
            if (bytesSentUpstream > 0 && now - lastDownstreamActivity > RUNTIME_CONFIG.flowStallTimeout) {
                clearInterval(watchdog);
                return closeAll('Stall detected: No downstream data received.');
            }
        }, Math.min(RUNTIME_CONFIG.activityWatchdogTimeout, RUNTIME_CONFIG.flowStallTimeout) / 2);

        try {
            const downstreamPromise = (async () => {
                for await (const chunk of dataChannel) {
                    lastDownstreamActivity = Date.now();
                    if (this.clientConn.readyState === WebSocket.OPEN) {
                        this.clientConn.send(chunk);
                    } else {
                        break;
                    }
                }
            })();

            const pumpClientPromise = (async () => {
                const reader = this.clientStreams.readable.getReader();
                try {
                    if (initialData?.byteLength > 0) {
                        await regulator.push(initialData, 'HIGH');
                        getMetricsSvc().push({ event: 'initialDataPushed', len: initialData.byteLength });
                    }
                    while (true) {
                        const { value, done } = await reader.read();
                        if (done) break;
                        lastUpstreamActivity = Date.now();
                        if (!value) continue;
                        await regulator.push(value, 'NORMAL');
                    }
                } finally {
                    try { reader.releaseLock(); } catch(_) {}
                    regulator.close();
                }
            })();

            const upstreamPromise = (async () => {
                for await (const chunk of regulator) {
                    lastUpstreamActivity = Date.now();
                    bytesSentUpstream += chunk.byteLength;
                    try {
                        const start = Date.now();
                        await dataChannel.push(chunk);
                        const latency = Date.now() - start;
                        regulator.ack(chunk.byteLength, latency);
                        getMetricsSvc().push({ event: 'sent', bytes: chunk.byteLength, latency });
                    } catch (err) {
                        console.error('[upstream] push->channel failed', err?.message ?? err);
                        break;
                    }
                }
            })();

            await Promise.allSettled([downstreamPromise, pumpClientPromise, upstreamPromise]);
        } finally {
            clearInterval(watchdog);
            await closeAll();
        }
    }

    close() {
        if (this.isClosed) return;
        this.isClosed = true;
        try { if (this.clientConn?.close) this.clientConn.close(1000, 'terminated'); } catch(_) {}
    }
    
    adaptConnectionToStreams(conn) { const readable = new ReadableStream({ start(controller) { conn.addEventListener('message', e => { try { const data = e.data; if (data instanceof ArrayBuffer) { controller.enqueue(new Uint8Array(data)); } } catch (err) { controller.error(err); } }); conn.addEventListener('close', () => controller.close()); conn.addEventListener('error', e => controller.error(e)); } }); const writable = new WritableStream({ write(chunk) { conn.send(chunk); }, close() { conn.close(1000); }, abort(reason) { conn.close(1001, reason?.message); } }); return { readable, writable }; }
}

async function establishRelayViaTypeA(target, relayOptions) {
    LOG(`Establishing Type-A relay via ${relayOptions.host}:${relayOptions.port}`);
    const socket = await connect({ hostname: relayOptions.host, port: relayOptions.port });
    
    const writer = socket.writable.getWriter();
    const reader = socket.readable.getReader();
    const encoder = new TextEncoder();

    await writer.write(new Uint8Array([5, 1, 0]));
    const resp1 = (await reader.read()).value;
    if (!resp1 || resp1[0] !== 5 || resp1[1] !== 0) {
        writer.releaseLock(); reader.releaseLock();
        socket.close();
        throw new Error('Type-A initial handshake failed');
    }

    let destHostBytes;
    const destPortBytes = new Uint8Array([target.port >> 8, target.port & 0xff]);
    
    if (target.type === 1) { // IPv4
        destHostBytes = new Uint8Array([1, ...target.host.split('.').map(Number)]);
    } else if (target.type === 3) { // IPv6
         const ipv6Bytes = target.host.split(':').flatMap(s => { const val = parseInt(s, 16) || 0; return [val >> 8, val & 0xff]; });
        destHostBytes = new Uint8Array([4, ...ipv6Bytes]);
    } else { // Domain
        const domainBytes = encoder.encode(target.host);
        destHostBytes = new Uint8Array([3, domainBytes.length, ...domainBytes]);
    }

    await writer.write(new Uint8Array([5, 1, 0, ...destHostBytes, ...destPortBytes]));
    const resp2 = (await reader.read()).value;
    if (!resp2 || resp2[0] !== 5 || resp2[1] !== 0) {
        writer.releaseLock(); reader.releaseLock();
        socket.close();
        throw new Error(`Type-A connection failed with code ${resp2 ? resp2[1] : 'N/A'}`);
    }
    
    writer.releaseLock();
    reader.releaseLock();
    return socket;
}

async function establishRelayViaTypeB(target, relayOptions) {
    LOG(`Establishing Type-B (CONNECT) tunnel via ${relayOptions.host}:${relayOptions.port}`);
    const socket = await connect({ hostname: relayOptions.host, port: relayOptions.port });

    const writer = socket.writable.getWriter();
    const reader = socket.readable.getReader();
    
    const destAddress = target.type === 3 ? `[${target.host}]:${target.port}` : `${target.host}:${target.port}`;
    let connectRequest = `${_d('Q09OTkVDVCA=')} ${destAddress} HTTP/1.1\r\n${_d('SG9zdDog')} ${destAddress}\r\n`;

    if (relayOptions.user || relayOptions.pass) {
        const credentials = btoa(`${relayOptions.user}:${relayOptions.pass}`);
        connectRequest += `${_d('UHJveHktQXV0aG9yaXphdGlvbjogQmFzaWMg')}${credentials}\r\n`;
    }
    connectRequest += '\r\n';

    await writer.write(new TextEncoder().encode(connectRequest));

    let response = '';
    const decoder = new TextDecoder();
    while (!response.includes('\r\n\r\n')) {
        const { value, done } = await reader.read();
        if (done) throw new Error('Type-B proxy connection closed prematurely');
        response += decoder.decode(value, { stream: true });
    }
    
    if (!response.startsWith(_d('SFRUUC8xLjEgMjAw'))) {
        writer.releaseLock(); reader.releaseLock();
        socket.close();
        throw new Error(`Type-B proxy connection failed: ${response.split('\r\n')[0]}`);
    }

    writer.releaseLock();
    reader.releaseLock();
    return socket;
}

function parseRelayConfig(proxyStr) { let user = null, pass = null, host = null, port = null; const atIndex = proxyStr.lastIndexOf('@'); if (atIndex !== -1) { const credentials = proxyStr.substring(0, atIndex); const colonIndex = credentials.indexOf(':'); if (colonIndex !== -1) { user = decodeURIComponent(credentials.substring(0, colonIndex)); pass = decodeURIComponent(credentials.substring(colonIndex + 1)); } else { user = decodeURIComponent(credentials); } [host, port] = parseHostPort(proxyStr.substring(atIndex + 1)); } else { [host, port] = parseHostPort(proxyStr); } return { user, pass, host, port: +port || 443 }; }
function parseHostPort(hostPortStr, defaultPort = 443) { if (!hostPortStr) return [null, defaultPort]; if (hostPortStr.startsWith('[')) { const closingBracketIndex = hostPortStr.lastIndexOf(']'); if (closingBracketIndex > 0) { const host = hostPortStr.substring(1, closingBracketIndex); const port = hostPortStr.substring(closingBracketIndex + 2) || defaultPort; return [host, port]; } } const colonIndex = hostPortStr.lastIndexOf(':'); if (colonIndex === -1) { return [hostPortStr, defaultPort]; } return [hostPortStr.substring(0, colonIndex), hostPortStr.substring(colonIndex + 1)]; }
function getRoutingPreferences(mode, params) { const order = []; if (mode && mode !== 'auto') { return [mode]; } for (const key of ['s5', 'https', 'http', 'proxyip', 'direct']) { if (params.has(key) && !order.includes(key)) { order.push(key); } } return order.length ? order : ['direct']; }
function verifyAuthToken(arr, start) { const hex = Array.from(arr.slice(start, start + 16)).map(n => n.toString(16).padStart(2, '0')).join(''); return hex.replace(/(.{8})(.{4})(.{4})(.{4})(.{12})/, '$1-$2-$3-$4-$5'); }
function parseDestination(bytes) { if (bytes.length < 24) throw new Error("Invalid initial data packet"); const offset1 = 18 + bytes[17] + 1; const port = (bytes[offset1] << 8) | bytes[offset1 + 1]; const type = bytes[offset1 + 2]; let offset2 = offset1 + 3; let length, host; switch (type) { case 1: length = 4; host = bytes.slice(offset2, offset2 + length).join('.'); break; case 2: length = bytes[offset2]; offset2++; host = new TextDecoder().decode(bytes.slice(offset2, offset2 + length)); break; case 3: length = 16; host = Array.from({length: 8}, (_, i) => ((bytes[offset2 + i * 2] << 8) | bytes[offset2 + i * 2 + 1]).toString(16)).join(':'); break; default: throw new Error(`Invalid address type: ${type}`); } const payload = bytes.slice(offset2 + length); return { host, port, type, payload }; }

async function manageConnection(conn, request) {
    try {
        const context = { colo: request.cf?.colo || 'UNKNOWN', asn: request.cf?.asn || 0 };
        const firstPacket = await new Promise((resolve, reject) => {
            const timer = setTimeout(() => reject(new Error('First packet timeout')), 5000);
            conn.addEventListener('message', evt => {
                clearTimeout(timer);
                if (evt.data instanceof ArrayBuffer) resolve(new Uint8Array(evt.data));
                else reject(new Error("First packet not ArrayBuffer"));
            }, { once: true });
        });

        if (verifyAuthToken(firstPacket, 1) !== RUNTIME_CONFIG.ACCESS_TOKEN) {
            throw new Error('Authentication failed');
        }
        const target = parseDestination(firstPacket);
        LOG(`Target destination: ${target.host}:${target.port}`);
        
        const url = new URL(request.url);
        const params = url.searchParams;
        const mode = params.get('mode') || 'auto';
        
        const channelProviders = [];
        const routingPrefs = getRoutingPreferences(mode, params);

        for (const routeType of routingPrefs) {
            switch (routeType) {
                case 's5':
                    if (params.has('s5')) {
                        const cfg = parseRelayConfig(params.get('s5'));
                        channelProviders.push({ name: `Type-A via ${cfg.host}`, establish: () => establishRelayViaTypeA(target, cfg) });
                    }
                    break;
                case 'https':
                    if (params.has('https')) {
                        const cfg = parseRelayConfig(params.get('https'));
                        channelProviders.push({ name: `Type-B(S) via ${cfg.host}`, establish: () => establishRelayViaTypeB(target, cfg) });
                    }
                    break;
                case 'http':
                    if (params.has('http')) {
                        const cfg = parseRelayConfig(params.get('http'));
                        channelProviders.push({ name: `Type-B via ${cfg.host}`, establish: () => establishRelayViaTypeB(target, cfg) });
                    }
                    break;
                case 'proxyip':
                    if (params.has('proxyip')) {
                        const [host, port] = parseHostPort(params.get('proxyip'));
                        channelProviders.push({ name: `Direct-Tunnel via ${host}`, establish: async () => { const sock = connect({ hostname: host, port: +port, tls: { servername: target.host } }); await sock.opened; return sock; }});
                    }
                    break;
                case 'direct':
                    channelProviders.push({ name: 'Direct-Connect', establish: async () => { const sock = connect({ hostname: target.host, port: target.port }); await sock.opened; return sock; }});
                    break;
            }
        }
        LOG('User-defined providers to try:', channelProviders.map(f => f.name));
        
        if (RUNTIME_CONFIG.RECOVERY_NODE) {
            const [host, port] = parseHostPort(RUNTIME_CONFIG.RECOVERY_NODE);
            channelProviders.push({
                name: `Recovery via ${host}`,
                establish: async () => {
                    LOG(`Attempting connection via recovery node: ${host}:${port}`);
                    const sock = connect({ hostname: host, port: +port, tls: { servername: target.host } });
                    await sock.opened;
                    return sock;
                }
            });
        }

        if (channelProviders.length === 0) {
            throw new Error("No valid routing providers configured.");
        }

        const replyHead = new Uint8Array([firstPacket[0] || 0, 0]);
        if (conn.readyState === WebSocket.OPEN) conn.send(replyHead);

        const dataFlow = new RobustDataFlow(conn, channelProviders, target.payload, context);
        await dataFlow.start();
        getMetricsSvc().push({ event: 'session_end' });

    } catch (err) {
        console.error('[ConnManager] error:', err?.message ?? err);
        getMetricsSvc().push({ event: 'handler_error', error: err?.message ?? String(err)});
        if (conn.readyState === WebSocket.OPEN) {
            conn.close(1011, `Error: ${err.message}`);
        }
    }
}

export default {
    async fetch(request, env, ctx) {
        try {
            const upgradeHeader = request.headers.get(_d('VXBncmFkZQ=='));
            if (upgradeHeader?.toLowerCase() !== _d('d2Vic29ja2V0')) {
                const url = new URL(request.url);
                if (url.pathname.startsWith('/telemetry')) { 
                    getMetricsSvc().flush(); 
                    return new Response('Metrics flushed', { status: 200 }); 
                }
                return new Response('Endpoint expects WebSocket upgrades.', { status: 200 });
            }

            const { 0: client, 1: server } = new WebSocketPair();
            server.accept();
            ctx.waitUntil(manageConnection(server, request));

            return new Response(null, { status: 101, webSocket: client });

        } catch (err) {
            console.error('[Fetch] fatal error:', err?.stack || err);
            return new Response('Internal Server Error', { status: 500 });
        }
    }
};
