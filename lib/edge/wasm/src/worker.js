import { WASI, File, Directory, PreopenDirectory, ConsoleStdout } from "@bjorn3/browser_wasi_shim";

const QDRANT_PATH = "/qdrant_data";
const QDRANT_DIR = "qdrant_data";
const encoder = new TextEncoder();
const decoder = new TextDecoder();

async function syncToOPFS(memDir, opfsHandle) {
  for (const [name, entry] of memDir.contents) {
    if (entry instanceof Directory) {
      const subHandle = await opfsHandle.getDirectoryHandle(name, { create: true });
      await syncToOPFS(entry, subHandle);
    } else if (entry instanceof File) {
      const fileHandle = await opfsHandle.getFileHandle(name, { create: true });
      const writable = await fileHandle.createWritable();
      await writable.write(entry.data);
      await writable.close();
    }
  }
}

async function syncFromOPFS(opfsHandle, memDir) {
  for await (const [name, handle] of opfsHandle.entries()) {
    if (handle.kind === "directory") {
      const subMemDir = new Directory(/* @__PURE__ */ new Map());
      memDir.contents.set(name, subMemDir);
      await syncFromOPFS(handle, subMemDir);
    } else if (handle.kind === "file") {
      const file = await handle.getFile();
      const arrayBuffer = await file.arrayBuffer();
      const memFile = new File(new Uint8Array(arrayBuffer));
      memDir.contents.set(name, memFile);
    }
  }
}

let wasm = null;
let wasi = null;
let memory = null;
let qdrantFs = null;
let opfsHandle = null;
let shard = null;
let mmaps = new Map();

function copyToWasm(src) {
  const ptr = wasm.qdrant_malloc(src.byteLength);
  new Uint8Array(memory.buffer, ptr, src.byteLength).set(src);
  return { ptr, len: src.byteLength };
}

function withString(str, fn) {
  const bytes = encoder.encode(str);
  const { ptr, len } = copyToWasm(bytes);
  try { return fn(ptr, len); }
  finally { wasm.qdrant_free(ptr, len); }
}

function withF32Array(arr, fn) {
  const f32 = new Float32Array(arr);
  const { ptr } = copyToWasm(new Uint8Array(f32.buffer));
  try { return fn(ptr, f32.length); }
  finally { wasm.qdrant_free(ptr, f32.byteLength); }
}

function readResultString() {
  const ptr = wasm.get_result_ptr(), len = wasm.get_result_len();
  if (!ptr || !len) return null;
  return decoder.decode(new Uint8Array(memory.buffer, ptr, len));
}

function callWasm(fn, ...args) {
  const ok = fn(...args);
  const result = readResultString();
  if (!ok) throw new Error(result || "WASM call failed");
  return result;
}

function callWasmJson(fn, ...args) {
  return JSON.parse(callWasm(fn, ...args));
}

function requireShard() {
  if (!shard) throw new Error("Shard not created");
}

function flushMmap(ptr, len, file) {
  const src = new Uint8Array(memory.buffer, ptr, len);
  if (file.data.byteLength !== len) {
    const resized = new Uint8Array(len);
    resized.set(file.data.subarray(0, Math.min(file.data.byteLength, len)));
    file.data = resized;
  }
  file.data.set(src);
}

async function persistToOPFS() {
  if (memory) for (const [ptr, reg] of mmaps) flushMmap(ptr, reg.len, reg.file);
  await syncToOPFS(qdrantFs, opfsHandle);
}

async function initWasm(wasmUrl) {
  if (wasm) return;

  const rootHandle = await navigator.storage.getDirectory();
  opfsHandle = await rootHandle.getDirectoryHandle(QDRANT_DIR, { create: true });
  qdrantFs = new Directory(new Map());
  await syncFromOPFS(opfsHandle, qdrantFs);
  const preopenDir = new PreopenDirectory(QDRANT_PATH, qdrantFs.contents);
  qdrantFs = preopenDir.dir;
  wasi = new WASI([], [], [
    new File(new ArrayBuffer(0)),
    ConsoleStdout.lineBuffered(msg => console.log(`[WASM STDOUT] ${msg}`)),
    ConsoleStdout.lineBuffered(msg => console.error(`[WASM STDERR] ${msg}`)),
    preopenDir
  ]);

  const cache = await caches.open('qdrant-wasm-cache');
  let response = await cache.match(wasmUrl);
  if (!response) {
    response = await fetch(wasmUrl);
    if (!response.ok) throw new Error(`Failed to fetch WASM: ${response.statusText}`);
    await cache.put(wasmUrl, response.clone());
  }

  const wasmBytes = await response.arrayBuffer();
  const { instance } = await WebAssembly.instantiate(wasmBytes, {
    wasi_snapshot_preview1: wasi.wasiImport,
    env: {
      wasi_mmap_register: (fd, ptr, len) => {
        const fdObj = wasi.fds[fd];
        if (fdObj?.file) mmaps.set(ptr, { file: fdObj.file, len });
      },
      wasi_mmap_flush: (ptr, len) => {
        const reg = mmaps.get(ptr);
        if (reg) flushMmap(ptr, len, reg.file);
      }
    }
  });
  wasm = instance.exports;
  wasi.initialize(instance);
  memory = wasm.memory;
  postMessage({ type: "ready", message: "WASM Initialized" });
}

function shardExists() {
  return qdrantFs.contents.has("wal") || qdrantFs.contents.has("segments");
}

function createShard() {
  if (!shardExists()) {
    console.log("No shard found. Building a new one.");
    withString(QDRANT_PATH, (p, l) => callWasm(wasm.shard_build, p, l));
    // shard_build drops the temporary EdgeShard which flushes mmaps; clear stale
    // registrations so persistToOPFS won't overwrite fresh data with stale zeros.
    mmaps.clear();
  }
  console.log(`Loading Qdrant shard from ${QDRANT_PATH}`);
  const ptr = withString(QDRANT_PATH, (p, l) => wasm.shard_new(p, l));
  if (!ptr) throw new Error(readResultString() || "shard_new failed");
  return ptr;
}

const actions = {
  async init(data) { await initWasm(data.wasmUrl); },
  async create() {
    shard = createShard();
    await persistToOPFS();
    return "ready";
  },
  async info() {
    requireShard();
    return callWasmJson(wasm.shard_info, shard);
  },
  async upsert({ id, vector, payload }) {
    requireShard();
    const payloadStr = payload && Object.keys(payload).length > 0 ? JSON.stringify(payload) : "";
    withF32Array(vector, (vPtr, vLen) =>
      withString(payloadStr, (pPtr, pLen) =>
        callWasm(wasm.shard_upsert, shard, BigInt(id), vPtr, vLen, pPtr, pLen)
      )
    );
    await persistToOPFS();
    return true;
  },
  async query({ vector, topK }) {
    requireShard();
    return JSON.parse(withF32Array(vector, (p, l) => callWasm(wasm.shard_search, shard, p, l, topK)));
  },
  async retrieve({ ids, withPayload, withVector }) {
    requireShard();
    const u64s = new BigUint64Array(ids.map(id => BigInt(id)));
    const { ptr, len } = copyToWasm(new Uint8Array(u64s.buffer));
    try {
      return callWasmJson(wasm.shard_retrieve, shard, ptr, ids.length, withPayload, withVector);
    } finally {
      wasm.qdrant_free(ptr, len);
    }
  },
  async count({ filter, exact }) {
    requireShard();
    const filterJson = filter ? JSON.stringify(filter) : "";
    const res = withString(filterJson, (p, l) => callWasm(wasm.shard_count, shard, p, l, exact || false));
    return parseInt(res, 10);
  },
  async scroll(req) {
    requireShard();
    const snakeReq = {
      offset: req.offset,
      limit: req.limit,
      filter: req.filter,
      with_payload: req.withPayload,
      with_vector: req.withVector
    };
    return JSON.parse(withString(JSON.stringify(snakeReq), (p, l) => callWasm(wasm.shard_scroll, shard, p, l)));
  }
};

onmessage = async (e) => {
  const { action, data, id } = e.data;
  try {
    const handler = actions[action];
    if (!handler) throw new Error(`Unknown action: ${action}`);
    const result = await handler(data);
    if (result !== undefined) postMessage({ id, result });
  } catch (err) {
    postMessage({ id, error: err.message });
  }
};
