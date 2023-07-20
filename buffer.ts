import { Document } from 'mongodb'
import EventEmitter from 'node:events'

export class DBRecordBuffer extends EventEmitter {
    private records: Document[] = []
    private timer: NodeJS.Timeout | null = null
    private mode: 'waiting' | 'started' = 'waiting'

    constructor(
        private chunkSize: number,
        private timeout: number
    ) {
        super()
    }

    addRecord(record: Document) {
        this.records.push(record)
        if (this.records.length >= this.chunkSize) this.releaseRecords()
    }

    addRecords(records: Document[]) {
        this.records = [...records, ...this.records]
        if (this.records.length >= 1000) this.releaseRecords()
    }

    releaseRecords() {
        if (this.timer && this.mode === 'started') {
            clearTimeout(this.timer)
            if (this.records.length) {
                const chunk = this.records.splice(0, this.chunkSize)
                this.emit('data', chunk)
            }
            this.setTimer()
        }
    }

    recordsChunkProcessed() {
        if (!this.records.length) this.emit('records ended')
    }

    setTimer() {
        if (this.mode === 'waiting') this.mode = 'started'
        this.timer = setTimeout(() => {
            this.releaseRecords()
        }, this.timeout)
    }
}
