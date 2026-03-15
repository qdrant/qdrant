export class QdrantWasm {
    constructor(options = {}) {
        const baseUrl = options.baseUrl || window.location.origin + '/';
        this.workerUrl = baseUrl + "qdrant-worker.js";
        this.wasmUrl = baseUrl + "qdrant.wasm";
        this.worker = null;
        this.callbacks = new Map();
        this.msgId = 0;
        this.isInitialized = false;
    }

    async initialize() {
        if (this.isInitialized) return;

        try {
            const response = await fetch(this.workerUrl);
            if (!response.ok) throw new Error(`Failed to fetch worker: ${response.statusText}`);

            const code = await response.text();
            const blob = new Blob([code], { type: 'application/javascript' });
            const blobUrl = URL.createObjectURL(blob);

            this.worker = new Worker(blobUrl, { type: 'module' });

            this.worker.onmessage = (e) => this._onMessage(e);
            this.worker.onerror = (err) => console.error("Worker Error:", err);

            return new Promise((resolve, reject) => {
                this._resolveReady = resolve;
                this._rejectReady = reject;

                this.worker.postMessage({
                    action: 'init',
                    data: { wasmUrl: this.wasmUrl }
                });

                setTimeout(() => reject(new Error("Initialization timed out")), 10000);
            });
        } catch (err) {
            console.error("Qdrant SDK Init Error:", err);
            throw err;
        }
    }

    async prepareShard() {
        return this._send('create');
    }

    async getInfo() {
        return this._send('info');
    }

    async upsert(id, vector, payload = {}) {
        return this._send('upsert', { id, vector, payload });
    }

    async query(vector, topK = 10) {
        return this._send('query', { vector, topK });
    }

    async retrieve(ids, withPayload = true, withVector = false) {
        return this._send('retrieve', { ids, withPayload, withVector });
    }

    async count(filter = null, exact = true) {
        return this._send('count', { filter, exact });
    }

    async scroll(request = {}) {
        return this._send('scroll', request);
    }

    _onMessage(e) {
        const { id, result, error, type } = e.data;

        if (type === 'ready') {
            this.isInitialized = true;
            if (this._resolveReady) this._resolveReady(true);
            return;
        }

        const cb = this.callbacks.get(id);
        if (cb) {
            this.callbacks.delete(id);
            if (error) cb.reject(new Error(error));
            else cb.resolve(result);
        }
    }

    _send(action, data) {
        if (!this.isInitialized) {
            throw new Error("Client not initialized. Call .initialize() first.");
        }

        const id = this.msgId++;
        return new Promise((resolve, reject) => {
            this.callbacks.set(id, { resolve, reject });
            this.worker.postMessage({ action, data, id });
        });
    }
}
