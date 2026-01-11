"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.KeyManager = void 0;
class KeyManager {
    constructor(streamName) {
        this.streamName = streamName;
    }
    getStreamKey() {
        return this.streamName;
    }
    getJobStatusKey(id) {
        return `${this.streamName}:status:${id}`;
    }
    getJobDataKey(id) {
        return `${this.streamName}:data:${id}`;
    }
    getDlqStreamKey() {
        return `${this.streamName}:dlq`;
    }
    getThroughputKey(groupName, timestamp) {
        const minute = Math.floor(timestamp / 60000) * 60000;
        return `metrics:throughput:${this.streamName}:${groupName}:${minute}`;
    }
    getTotalKey(groupName) {
        return `metrics:total:${this.streamName}:${groupName}`;
    }
}
exports.KeyManager = KeyManager;
