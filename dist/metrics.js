"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.Metrics = void 0;
const keys_1 = require("./keys");
class Metrics {
    constructor(redis, streamName) {
        this.redis = redis;
        this.streamName = streamName;
        this.keys = new keys_1.KeyManager(streamName);
    }
    /**
     * Get current metrics for the queue
     * @param groupNames - List of consumer groups to fetch throughput for
     */
    async getMetrics(groupNames) {
        const pipeline = this.redis.pipeline();
        // 1. Stream Length
        pipeline.xlen(this.streamName);
        // 2. DLQ Length
        pipeline.xlen(this.keys.getDlqStreamKey());
        // 3. Throughput keys for current minute
        const timestamp = Date.now();
        groupNames.forEach(group => {
            pipeline.get(this.keys.getThroughputKey(group, timestamp));
        });
        const results = await pipeline.exec();
        if (!results) {
            throw new Error("Pipeline execution failed");
        }
        // Helper to safely extract result
        const getResult = (index) => {
            const [err, res] = results[index];
            if (err)
                throw err;
            return res;
        };
        const streamLength = getResult(0);
        const dlqLength = getResult(1);
        const throughput = {};
        groupNames.forEach((group, index) => {
            // Offset by 2 because first two are xlen calls
            const val = getResult(index + 2);
            throughput[group] = parseInt(val || '0', 10);
        });
        return {
            streamLength,
            dlqLength,
            throughput
        };
    }
    /**
     * Get prometheus compatible metrics
     * @param groupNames target group names for throughput metrics
     * @param prefix - export prefix
     * @returns metrics as string
     */
    async getPrometheusMetrics(groupNames, prefix = 'redis_highway_queue') {
        const pipeline = this.redis.pipeline();
        pipeline.xlen(this.streamName);
        pipeline.xlen(this.keys.getDlqStreamKey());
        const timestamp = Date.now();
        groupNames.forEach(group => {
            pipeline.get(this.keys.getThroughputKey(group, timestamp));
            pipeline.get(this.keys.getTotalKey(group));
        });
        const results = await pipeline.exec();
        if (!results)
            throw new Error("Pipeline execution failed");
        const getResult = (index) => {
            const [err, res] = results[index];
            if (err)
                throw err;
            return res;
        };
        const streamLength = getResult(0);
        const dlqLength = getResult(1);
        const response = [];
        response.push(`# HELP ${prefix}_waiting_jobs Total jobs waiting in stream`, `# TYPE ${prefix}_waiting_jobs gauge`, `${prefix}_waiting_jobs{stream="${this.streamName}"} ${streamLength}`, `# HELP ${prefix}_dlq_jobs Total jobs in DLQ`, `# TYPE ${prefix}_dlq_jobs gauge`, `${prefix}_dlq_jobs{stream="${this.streamName}"} ${dlqLength}`);
        groupNames.forEach((group, index) => {
            const baseIndex = 2 + (index * 2);
            const throughputVal = parseInt(getResult(baseIndex) || '0', 10);
            const totalVal = parseInt(getResult(baseIndex + 1) || '0', 10);
            response.push(`# HELP ${prefix}_throughput_1m Jobs processed in the last minute`, `# TYPE ${prefix}_throughput_1m gauge`, `${prefix}_throughput_1m{stream="${this.streamName}", group="${group}"} ${throughputVal}`, `# HELP ${prefix}_jobs_total Total jobs processed`, `# TYPE ${prefix}_jobs_total counter`, `${prefix}_jobs_total{stream="${this.streamName}", group="${group}"} ${totalVal}`);
        });
        return response.join('\n');
    }
}
exports.Metrics = Metrics;
