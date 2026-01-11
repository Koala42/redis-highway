"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.Producer = void 0;
const uuid_1 = require("uuid");
const keys_1 = require("./keys");
class Producer {
    constructor(redis, streamName) {
        this.redis = redis;
        this.streamName = streamName;
        this.keys = new keys_1.KeyManager(streamName);
    }
    /**
     * Push message to queue
     * @param payload - serialized string payload
     * @param targetGroups - target consumers
     * @param opts - Job options
     * @returns Created job ID (uuidv7)
     */
    async push(payload, targetGroups, opts) {
        const id = (0, uuid_1.v7)();
        const ttl = opts?.ttl || null; // 24 hours in seconds
        const pipeline = this.redis.pipeline();
        const dataKey = this.keys.getJobDataKey(id);
        const statusKey = this.keys.getJobStatusKey(id);
        // Create job data
        if (ttl) {
            pipeline.set(dataKey, payload, 'EX', ttl);
        }
        else {
            pipeline.set(dataKey, payload);
        }
        // Initialize job metadata - status
        pipeline.hset(statusKey, '__target', targetGroups.length);
        if (ttl) {
            pipeline.expire(statusKey, ttl);
        }
        // Push message to stream
        pipeline.xadd(this.streamName, '*', 'id', id, 'target', targetGroups.join(','));
        await pipeline.exec();
        return id;
    }
}
exports.Producer = Producer;
