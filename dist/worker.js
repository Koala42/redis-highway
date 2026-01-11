"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.Worker = void 0;
const events_1 = require("events");
const lua_1 = require("./lua");
const keys_1 = require("./keys");
class Worker {
    constructor(redis, groupName, streamName, concurrency = 1, blockTimeMs = 2000) {
        this.redis = redis;
        this.groupName = groupName;
        this.streamName = streamName;
        this.concurrency = concurrency;
        this.blockTimeMs = blockTimeMs;
        this.isRunning = false;
        this.activeCount = 0;
        this.events = new events_1.EventEmitter();
        this.MAX_RETRIES = 3;
        this.events.setMaxListeners(100);
        this.keys = new keys_1.KeyManager(streamName);
    }
    /**
     * Start worker
     * @returns
     */
    async start() {
        if (this.isRunning) {
            return;
        }
        this.isRunning = true;
        try {
            await this.redis.xgroup('CREATE', this.streamName, this.groupName, '0', 'MKSTREAM');
        }
        catch (e) {
            if (!e.message.includes('BUSYGROUP')) {
                throw e;
            }
        }
        this.fetchLoop();
    }
    stop() {
        this.isRunning = false;
    }
    async fetchLoop() {
        while (this.isRunning) {
            const freeSlots = this.concurrency - this.activeCount;
            if (freeSlots <= 0) {
                await new Promise((resolve) => this.events.once('job_finished', resolve));
                continue;
            }
            try {
                const results = await this.redis.xreadgroup('GROUP', this.groupName, this.consumerName(), 'COUNT', freeSlots, 'BLOCK', this.blockTimeMs, 'STREAMS', this.streamName, '>');
                if (results) {
                    const messages = results[0][1];
                    for (const msg of messages) {
                        this.spawnWorker(msg);
                    }
                }
            }
            catch (err) {
                console.error(`[${this.groupName}] Fetch Error:`, err);
                await new Promise((resolve) => setTimeout(resolve, 1000));
            }
        }
    }
    spawnWorker(msg) {
        this.activeCount++;
        this.processInternal(msg).finally(() => {
            this.activeCount--;
            this.events.emit('job_finished');
        });
    }
    async processInternal(msg) {
        const msgId = msg[0];
        const messageFields = msg[1];
        const fields = {};
        for (let i = 0; i < messageFields.length; i += 2) {
            fields[messageFields[i]] = messageFields[i + 1];
        }
        const messageUuid = fields['id'];
        const routes = fields['target'];
        const retryCount = parseInt(fields['retryCount'] || '0', 10);
        if (!routes.includes(this.groupName)) {
            await this.redis.xack(this.streamName, this.groupName, msgId);
            return;
        }
        try {
            const dataKey = this.keys.getJobDataKey(messageUuid);
            const payload = await this.redis.get(dataKey);
            if (!payload) {
                // Data missing or expired
                await this.finalize(messageUuid, msgId);
                return;
            }
            await this.process(JSON.parse(payload));
            await this.finalize(messageUuid, msgId);
        }
        catch (err) {
            console.error(`[${this.groupName}] Job failed ${messageUuid}`, err);
            await this.handleFailure(messageUuid, msgId, retryCount, err.message);
        }
    }
    async handleFailure(uuid, msgId, currentRetries, errorMsg) {
        // 1. ACK the failed message (we are done with THIS stream entry)
        await this.redis.xack(this.streamName, this.groupName, msgId);
        if (currentRetries < this.MAX_RETRIES) {
            // 2a. RETRY: Re-queue to main stream targeting ONLY this group
            console.log(`[${this.groupName}] Retrying job ${uuid} (Attempt ${currentRetries + 1}/${this.MAX_RETRIES})`);
            const pipeline = this.redis.pipeline();
            // Refresh TTL to ensure data persists through retries (e.g., +1 hour)
            pipeline.expire(this.keys.getJobDataKey(uuid), 3600);
            pipeline.expire(this.keys.getJobStatusKey(uuid), 3600);
            pipeline.xadd(this.streamName, '*', 'id', uuid, 'target', this.groupName, // Only target THIS group
            'retryCount', currentRetries + 1);
            await pipeline.exec();
        }
        else {
            // 2b. DEAD LETTER QUEUE (DLQ)
            console.error(`[${this.groupName}] Job ${uuid} exhausted retries. Moving to DLQ.`);
            const dlqStream = `${this.streamName}:dlq`;
            // We need the payload to store in DLQ permanently?
            // Or just reference the key? Creating a self-contained DLQ entry is safer.
            const payload = await this.redis.get(this.keys.getJobDataKey(uuid));
            await this.redis.xadd(dlqStream, '*', 'id', uuid, 'group', this.groupName, 'error', errorMsg, 'payload', payload || 'MISSING', 'failedAt', Date.now());
            // We still run finalize to clean up atomic counters/status
            // (Treat it as "done" for the main flow, even though it failed)
            await this.finalize(uuid, msgId, true); // true = skip Lua DEL? No, finalize deletes keys if everyone is done.
            // Actually, if we move to DLQ, we should probably let finalize clean up the hot keys.
            // The DLQ entry contains the payload copy, so losing the hot key is fine.
        }
    }
    async finalize(messageUuid, msgId, fromError = false) {
        // If we call finalize from error handler, we already ACKed inside handleFailure.
        // But the Lua script does ACK too. It is idempotent so it's fine.
        const timestamp = Date.now();
        const statusKey = this.keys.getJobStatusKey(messageUuid);
        const dataKey = this.keys.getJobDataKey(messageUuid);
        const throughputKey = this.keys.getThroughputKey(this.groupName, timestamp);
        const totalKey = this.keys.getTotalKey(this.groupName);
        await this.redis.eval(lua_1.LUA_MARK_DONE, 6, statusKey, dataKey, this.streamName, this.groupName, throughputKey, totalKey, this.groupName, timestamp, msgId);
    }
    consumerName() {
        return `${this.groupName}-${process.pid}`;
    }
}
exports.Worker = Worker;
