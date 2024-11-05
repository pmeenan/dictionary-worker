// Pick ONE - either brotli or zstd

// Zstd - ~100k compressed
//import zstdlib from "../zstd-wasm-compress/bin/zstdlib.js";
//import zstdwasm from "../zstd-wasm-compress/bin/zstdlib.wasm";
// Brotli - ~900k compressed
import brotlienc from "../brotli-wasm-compress/bin/brotlienc.js";
import BrotliEncoder from "../brotli-wasm-compress/bin/brotlienc.wasm";

// File name of the current dictionary asset (TODO: see if there is a way to get this dynamically)
const currentDictionary = "HWl0A6pNEHO4AeCdArQj53JlvZKN8Fcwk3JcGv3tak8";

// Psuedo-path where the dictionaries will be served from (shouldn't collide with a real directory)
const dictionaryPath = "/dictionary/";

// Dictionary options
const match = 'match="/*", match-dest=("document" "frame")'; // Match pattern for the URLs to be compressed
const dictionaryExpiration = 30 * 24 * 3600;                 // 30 day expiration on the dictionary itself

// Compression options
const blocking = true;   // Block requests until wasm and the dictionary have loaded
const compressionLevel = 10;
const compressionWindowLog = 20;  // Compression window should be at least as long as the dictionary + typical response - 2 ^ 20 = 1MB

// Internal globals for managing state while waiting for the dictionary and zstd wasm to load
let zstd = null;
let brotli = null;
let dictionaryLoaded = null;
let wasmLoaded = null;
let initialized = false;
let dictionary = null;
let dictionarySize = 0;
let dictionaryJS = null;
const currentHash = atob(currentDictionary.replaceAll('-', '+').replaceAll('_', '/'));
const dictionaryPathname = dictionaryPath + currentDictionary + '.dat';
const dczHeader = new Uint8Array([0x5e, 0x2a, 0x4d, 0x18, 0x20, 0x00, 0x00, 0x00, ...Uint8Array.from(currentHash, c => c.charCodeAt(0))]);
const dcbHeader = new Uint8Array([0xff, 0x44, 0x43, 0x42, ...Uint8Array.from(currentHash, c => c.charCodeAt(0))]);

export default {

  /*
    Main entrypoint.
    - If a fetch comes in for /dictionary/<hash>.dat, return the in-memory dictionary with an appropriate Use-As-Dictionary header.
    - If a fetch comes in with "Sec-Fetch-Dest: document" or 'frame':
      * Pass-through the fetch to get the original response.
      * Add a <link> header to trigger the dictionary fetch.
      * If the request has an "Available-Dictionary" request header that matches the current dictionary then dictionary-compress the response.
    - Otherwise, pass the fetch through to the origin.
  */
  async fetch(request, env, ctx) {
    // Handle the request for the dictionary itself
    const url = new URL(request.url);
    const isDictionary = url.pathname == dictionaryPathname;
    if (isDictionary) {
      return await fetchDictionary(env, url);
    } else {
      const dest = request.headers.get("sec-fetch-dest");
      if (dest && (dest.indexOf("document") !== -1 || dest.indexOf("frame") !== -1)) {
        if (request.headers.get("available-dictionary")) {
          // Trigger the async dictionary load
          dictionaryInit(request, env, ctx).catch(E => console.log(E));
          zstdInit(ctx).catch(E => console.log(E));
          brotliInit(ctx).catch(E => console.log(E));

          // Fetch the actual content
          const original = await fetch(request);

          // block on the dictionary/wasm init if necessary
          if (blocking) {
            if (zstd === null && brotli === null) { await wasmLoaded; }
            if (dictionary === null) { await dictionaryLoaded; }
          }
          
          if (original.ok && supportsCompression(request)) {
            return await compressResponse(original, ctx);
          } else {
            return original;
          }
        } else {
          // Just add the link header
          const response = new Response(original.body, original);
          response.headers.append("Link", '<' + dictionaryPathname + '>; rel="compression-dictionary"',);
          return response;
        }
      } else {
        return await fetch(request);
      }
    }
  }
}

/*
  Dictionary-compress the response
*/
async function compressResponse(original, ctx) {
  const { readable, writable } = new TransformStream();
  if (zstd !== null) {
    ctx.waitUntil(compressStreamZstd(original.body, writable));
  } else if (brotli !== null) {
    ctx.waitUntil(compressStreamBrotli(original.body, writable));
  }

  // Add the appropriate headers
  const response = new Response(readable, original);
  if (zstd !== null) {
    let ver = zstd.versionNumber();
    response.headers.set("X-Zstd-Version", ver);
    response.headers.set("Content-Encoding", 'dcz',);
    response.encodeBody = "manual";
  } else if (brotli !== null) {
    let ver = brotli.Version();
    response.headers.set("X-Brotli-Version", ver);
    response.headers.set("Content-Encoding", 'dcb',);
    response.encodeBody = "manual";
  }
  response.headers.set("Vary", 'Accept-Encoding, Available-Dictionary',);
  return response;
}

async function compressStreamZstd(readable, writable) {
  const reader = readable.getReader();
  const writer = writable.getWriter();

  // allocate a compression context and buffers before the stream starts
  let cctx = null;
  let zstdInBuff = null;
  let zstdOutBuff = null;
  let inSize = 0;
  let outSize = 0;
  try {
    cctx = zstd.createCCtx();
    if (cctx !== null) {
      inSize = zstd.CStreamInSize();
      outSize = zstd.CStreamOutSize();
      zstdInBuff = zstd._malloc(inSize);
      zstdOutBuff = zstd._malloc(outSize);

      // configure the zstd parameters
      zstd.CCtx_setParameter(cctx, zstd.cParameter.c_compressionLevel, compressionLevel);
      zstd.CCtx_setParameter(cctx, zstd.cParameter.c_windowLog, compressionWindowLog );
      
      zstd.CCtx_refCDict(cctx, dictionary);
    }
  } catch (E) {
    console.log(E);
  }

  // write the dcz header
  await writer.write(dczHeader);
  
  let isFirstChunk = true;
  let chunksGathered = 0;

  // streaming compression modeled after https://github.com/facebook/zstd/blob/dev/examples/streaming_compression.c
  while (true) {
    const { value, done } = await reader.read();
    const size = done ? 0 : value.byteLength;

    // Grab chunks of the input stream in case it is bigger than the zstd buffer
    let pos = 0;
    while (pos < size || done) {
      const endPos = Math.min(pos + inSize, size);
      const chunkSize = done ? 0 : endPos - pos;
      const chunk = done ? null : value.subarray(pos, endPos);
      pos = endPos;

      try {
        if (chunkSize > 0) {
          zstd.HEAPU8.set(chunk, zstdInBuff);
        }

        const inBuffer = new zstd.inBuffer();
        inBuffer.src = zstdInBuff;
        inBuffer.size = chunkSize;
        inBuffer.pos = 0;
        let finished = false;
        do {
          const outBuffer = new zstd.outBuffer();
          outBuffer.dst = zstdOutBuff;
          outBuffer.size = outSize;
          outBuffer.pos = 0;

          // Use a naive flushing strategy for now. Flush the first chunk immediately and then let zstd decide
          // when each chunk should be emitted (likey accumulate until complete).
          // Also, every 5 chunks that were gathered, flush irregardless.
          let mode = zstd.EndDirective.e_continue;
          if (done) {
            mode = zstd.EndDirective.e_end;
          } else if (isFirstChunk || chunksGathered >= 4) {
            mode = zstd.EndDirective.e_flush;
            isFirstChunk = false;
            chunksGathered = 0;
          }

          const remaining = zstd.compressStream2(cctx, outBuffer, inBuffer, mode);

          // Keep track of the number of chunks processed where we didn't send any response.
          if (outBuffer.pos == 0) chunksGathered++;

          if (outBuffer.pos > 0) {
            const data = new Uint8Array(zstd.HEAPU8.buffer, outBuffer.dst, outBuffer.pos);
            await writer.write(data);
          }

          finished = done ? (remaining == 0) : (inBuffer.pos == inBuffer.size);
        } while (!finished);
      } catch (E) {
        console.log(E);
      }
      if (done) break;
    }
    if (done) break;
  }

  await writer.close();

  // Free the zstd context and buffers
  if (zstdInBuff !== null) zstd._free(zstdInBuff);
  if (zstdOutBuff !== null) zstd._free(zstdOutBuff);
  if (cctx !== null) zstd.freeCCtx(cctx);
}

// The current brotli sample below doesn't handle streaming incremental chunks
// (it will flush whenever the encoder decides to)
async function compressStreamBrotli(readable, writable) {
  const reader = readable.getReader();
  const writer = writable.getWriter();

  // allocate a compression context and buffers before the stream starts
  let state = null;
  let inBuff = null;
  let outBuff = null;
  let inSize = 102400;
  let outSize = 204800;
  try {
    state = brotli.CreateInstance();
    if (state !== null) {
      inBuff = brotli._malloc(inSize);
      outBuff = brotli._malloc(outSize);

      // configure the brotli parameters
      brotli.SetParameter(state, brotli.Parameter.QUALITY, compressionLevel);
      brotli.SetParameter(state, brotli.Parameter.LGWIN, compressionWindowLog);
      
      brotli.AttachPreparedDictionary(state, dictionary);
    }
  } catch (E) {
    console.log(E);
  }
  
  // write the dcb header
  await writer.write(dcbHeader);
  
  while (true) {
    const { value, done } = await reader.read();
    const size = done ? 0 : value.byteLength;

    // Grab chunks of the input stream in case it is bigger than the zstd buffer
    let pos = 0;
    while (pos < size || done) {
      const endPos = Math.min(pos + inSize, size);
      const chunkSize = done ? 0 : endPos - pos;
      const chunk = done ? null : value.subarray(pos, endPos);
      pos = endPos;

      try {
        if (chunkSize > 0) {
          brotli.HEAPU8.set(chunk, inBuff);
        }

        let finished = false;
        do {
          const inBuffer = new brotli.Buffer();
          inBuffer.ptr = inBuff;
          inBuffer.size = chunkSize;
          const outBuffer = new brotli.Buffer();
          outBuffer.ptr = outBuff;
          outBuffer.size = outSize;
          finished = true;

          let mode = done ? brotli.Operation.FINISH : brotli.Operation.PROCESS;
          if (brotli.CompressStream(state, mode, inBuffer, outBuffer)) {
            const available = outSize - outBuffer.size;
            if (available > 0) {
              const data = new Uint8Array(brotli.HEAPU8.buffer, outBuff, available);
              await writer.write(data);
            }
            if (done && brotli.HasMoreOutput(state)) {
              finished = false;
            }
          } else {
            console.log("brotli.Compress failed");
          }
        } while(!finished);
      } catch (E) {
        console.log(E);
      }
      if (done) break;
    }
    if (done) break;
  }
  await writer.close();

  // Free the brotli context and buffers
  if (inBuff !== null) brotli._free(inBuff);
  if (outBuff !== null) brotli._free(outBuff);
  if (state !== null) brotli.DestroyInstance(state);
}

/*
 Handle the client request for a dictionary
*/
async function fetchDictionary(env, url) {
  // Just pass the request through to the assets fetch
  url.pathname = '/' + currentDictionary + '.dat';
  let asset = await env.ASSETS.fetch(url);
  return new Response(asset.body, {
    headers: {
      "content-type": "text/plain; charset=UTF-8",  /* Can be anything but text/plain will allow for Cloudflare to apply compression */
      "cache-control": "public, max-age=" + dictionaryExpiration,
      "use-as-dictionary": match
    }
  });
}

/*
 See if the client advertized a matching dictionary and the appropriate encoding
*/
function supportsCompression(request) {
  let hasDictionary = false;
  const availableDictionary = request.headers.get("available-dictionary");
  if (availableDictionary && dictionary !== null) {
    const availableHash = atob(availableDictionary.trim().replaceAll(':', ''));
    if (availableHash == currentHash) {
      hasDictionary = true;
    }
  }
  const supportsDCZ = request.cf.clientAcceptEncoding.indexOf("dcz") !== -1 && zstd !== null;
  const supportsDCB = request.cf.clientAcceptEncoding.indexOf("dcb") !== -1 && brotli !== null;
  return hasDictionary && (supportsDCZ || supportsDCB);
}

/*
  Make sure the dictionary is loaded and cached into the isolate global.
  The current implementation blocks all requests until the dictionary has been loaded.
  This can be modified to fail fast and only use dictionaries after they have loaded.
 */
async function dictionaryInit(request, env, ctx) {
  if (dictionaryJS === null && dictionaryLoaded === null) {
    let resolve;
    dictionaryLoaded = new Promise((res, rej) => {
      resolve = res;
    });
    // Keep the request alive until the dictionary loads
    ctx.waitUntil(dictionaryLoaded);
    const url = new URL(request.url);
    url.pathname = '/' + currentDictionary + '.dat';
    const response = await env.ASSETS.fetch(url);
    if (response.ok) {
      dictionaryJS = await response.bytes();
    }
    postInit();
    resolve(true);
  }
}

// wasm setup
async function zstdInit(ctx) {
  if (zstd === null && wasmLoaded === null && (typeof zstdlib !== 'undefined')) {
    let resolve;
    wasmLoaded = new Promise((res, rej) => {
      resolve = res;
    });
    // Keep the request alive until wasm loads
    ctx.waitUntil(wasmLoaded);
    zstd = await zstdlib({
      instantiateWasm(info, receive) {
        let instance = new WebAssembly.Instance(zstdwasm, info);
        receive(instance);
        return instance.exports;
      },
      locateFile(path, scriptDirectory) {
        return path
      },
    }).catch(E => console.log(E));
    postInit();
    resolve(true);
  }
}

async function brotliInit(ctx) {
  if (brotli === null && wasmLoaded === null && (typeof brotlienc !== 'undefined') && (typeof zstdlib === 'undefined')) {
    let resolve;
    wasmLoaded = new Promise((res, rej) => {
      resolve = res;
    });
    // Keep the request alive until wasm loads
    ctx.waitUntil(wasmLoaded);
    brotli = await brotlienc({
      instantiateWasm(info, receive) {
        let instance = new WebAssembly.Instance(BrotliEncoder, info);
        receive(instance);
        return instance.exports;
      },
      locateFile(path, scriptDirectory) {
        return path
      },
    }).catch(E => console.log(E));
    postInit();
    resolve(true);
  }
}

// After both the dictionary and wasm have initialized, prepare the dictionary into zstd
// memory so it can be reused efficiently.
function postInit() {
  if (!initialized && dictionaryJS !== null) {
    if (zstd !== null) {
      try {
        let d = zstd._malloc(dictionaryJS.byteLength)
        dictionarySize = dictionaryJS.byteLength;
        zstd.HEAPU8.set(dictionaryJS, d);
        dictionaryJS = null;
        dictionary = zstd.createCDict_byReference(d, dictionarySize, compressionLevel);
        initialized = true;
      } catch (E) {
        console.log(E);
      }
    } else if (brotli !== null) {
      try {
        let d = brotli._malloc(dictionaryJS.byteLength)
        dictionarySize = dictionaryJS.byteLength;
        brotli.HEAPU8.set(dictionaryJS, d);
        dictionaryJS = null;
        dictionary = brotli.PrepareDictionary(brotli.SharedDictionaryType.Raw, dictionarySize, d, compressionLevel);
        initialized = true;
      } catch (E) {
        console.log(E);
      }
    }
  }
}