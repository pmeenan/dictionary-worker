// Pick ONE - either brotli or zstd

// Zstd - ~100k compressed
import zstdlib from "../zstd-wasm-compress/bin/zstdlib.js";
import zstdwasm from "../zstd-wasm-compress/bin/zstdlib.wasm";

// Brotli - ~300k compressed
//import brotlienc from "../brotli-wasm-compress/bin/brotlienc.js";
//import BrotliEncoder from "../brotli-wasm-compress/bin/brotlienc.wasm";

const expire_days = 7; // Default dictionary expiration (can be overriden per-dictionary with "expire-days")

// List of dictionaries that need to be loaded with "Link rel=compression-dictionary"
// These can either be "asset" file paths under the "assets" directory or
// "url" pointing to a path to fetch the dictionary.
const dynamic_dictionaries = {
  "roadtrip2": {
    "asset": "/HWl0A6pNEHO4AeCdArQj53JlvZKN8Fcwk3JcGv3tak8.dat",
    "match": "/*",
    "match-dest": ["document", "frame"],
    "expire-days": 30
  }
}

// List of URL patterns for static resources for delta-encoding version updates
// where the new version of a file will match the same pattern.
// The dictionary ID will be set as the original URL for the dictionary.
// An optional "match-dest" param can restrict the dictionary to a specific destination.
let static_dictionaries = {
  "script": [
    "/wp-includes/js/jquery/jquery.min.js?ver=*",
    "/wp-includes/js/jquery/jquery-migrate.min.js?ver=*",
    "/wp-content/plugins/nextgen-gallery/products/photocrati_nextgen/modules/nextgen_basic_gallery/static/slideshow/slick/slick-*-modded.js?ver=*",
    "/wp-content/plugins/nextgen-gallery/products/photocrati_nextgen/modules/ajax/static/ajax.min.js?ver=*",
    "/wp-content/plugins/nextgen-gallery/products/photocrati_nextgen/modules/nextgen_basic_gallery/static/slideshow/ngg_basic_slideshow.js?ver=*",
    "/*/fontawesome/js/*-shims.min.js?ver=*",
    "/wp-content/plugins/jquery-t-countdown-widget/js/jquery.t-countdown.js?ver=*",
    "/wp-content/themes/scrappy/js/small-menu.js?ver=*",
    "/wp-content/plugins/nextgen-gallery/products/photocrati_nextgen/modules/nextgen_gallery_display/static/common.js?ver=*",
    "/wp-content/plugins/nextgen-gallery/products/photocrati_nextgen/modules/lightbox/static/lightbox_context.js?ver=*",
    "/wp-content/plugins/nextgen-gallery/products/photocrati_nextgen/modules/lightbox/static/shutter/shutter.js?ver=*",
    "/wp-content/plugins/nextgen-gallery/products/photocrati_nextgen/modules/lightbox/static/shutter/nextgen_shutter.js?ver=*",
    "/*/fontawesome/js/all.min.js?ver=*",
  ],
  "style": [
    "/wp-includes/css/dist/block-library/style.min.css?ver=*",
    "/wp-content/plugins/social-media-widget/social_widget.css?ver=*",
    "/wp-content/themes/scrappy/style.css?ver=*",
    "/wp-content/plugins/nextgen-gallery/products/photocrati_nextgen/modules/nextgen_basic_gallery/static/slideshow/ngg_basic_slideshow.css?ver=*",
    "/wp-content/plugins/nextgen-gallery/products/photocrati_nextgen/modules/nextgen_basic_gallery/static/slideshow/slick/slick.css?ver=*",
    "/wp-content/plugins/nextgen-gallery/products/photocrati_nextgen/modules/nextgen_basic_gallery/static/slideshow/slick/slick-theme.css?ver=*",
    "/wp-content/plugins/nextgen-gallery/products/photocrati_nextgen/modules/nextgen_gallery_display/static/trigger_buttons.css?ver=*",
    "/wp-content/plugins/nextgen-gallery/products/photocrati_nextgen/modules/lightbox/static/shutter/shutter.css?ver=*",
    "/*/fontawesome/css/*-shims.min.css?ver=*",
    "/*/fontawesome/css/all.min.css?ver=*",
    "/wp-content/plugins/nextgen-gallery/products/photocrati_nextgen/modules/widget/static/widgets.css?ver=*",
    "/wp-includes/css/dashicons.min.css?ver=*",
    "/wp-content/plugins/gallery-plugin/css/frontend_style.css?ver=*",
    "/*/fancybox/jquery.fancybox.min.css?ver=*",
    "/wp-content/plugins/jquery-t-countdown-widget/css/hoth/style.css?ver=*",
  ]
};
//static_dictionaries = {};

// Psuedo-path where the dictionaries will be served from (shouldn't collide with a real directory)
const dictionaryPath = "/dictionary/";

// Compression options
const compressionLevel = 10;      // 1-19 for ZStandard, 5-11 for Brotli
const compressionWindowLog = 20;  // Compression window should be at least as long as the dictionary + typical response - 2 ^ 20 = 1MB

// Internal globals for managing state while waiting for the dictionary and zstd wasm to load
let zstd = null;
let brotli = null;
let wasm = null;
let wasmLoaded = null;
const dictionaries = {};  // in-memory dictionaries, indexed by ID
const bufferSize = 10240; // 10k malloc buffers for response chunks (usually 4kb max)
const buffers = [];       // Spare buffers

export default {
  /*
    Main entrypoint.
    - If a fetch comes in with "Sec-Fetch-Dest: document" or 'frame', Add a <link> header to trigger the dictionary fetch.
    - If a fetch comes in for /dictionary/<id>, return the dictionary with an appropriate Use-As-Dictionary header.
    - If a fetch comes in matching a known static pattern, add an appropriate Use-As-Dictionary header.
    - Pass-through the fetch to get the original response.
    - If the request has an "Available-Dictionary" request header that matches a known dictionary ID then dictionary-compress the response.
    - Otherwise, return the original response.
  */
  async fetch(request, env, ctx) {
    // Handle the request for the dictionary itself
    const url = new URL(request.url);
    const isDictionary = url.pathname.startsWith(dictionaryPath);
    if (isDictionary) {
      return await fetchDictionary(env, request);
    } else {
      // List of headers that need to be added to the response
      const headers = [];
      const dest = request.headers.get("sec-fetch-dest");
      const targetDestinations = ['document', 'frame', ...Object.keys(static_dictionaries)];

      addLinkHeaders(request, headers);
      addDictionaryHeader(request, headers);

      if (dest && targetDestinations.includes(dest)) {
        headers.push(["Vary", "Available-Dictionary"]);
        if (request.headers.get("available-dictionary") && request.headers.get("dictionary-id")) {
          // Use the dictionary and URL combined as the cache key for the entry
          const cacheKey = url + " " + request.headers.get("available-dictionary");
          const cache = caches.default;
          const cached = await cache.match(cacheKey);
          if (cached) {
            const response = new Response(cached.body, cached);
            return response;
          } else {
            // Trigger the async dictionary load
            const dictionaryPromise = loadDictionary(request, env, ctx).catch(E => console.log(E));

            // Fetch the actual content
            const original = await fetch(request);

            // Wait for the dictionary to finish loading
            const dictionary = await dictionaryPromise;

            if (original.status == 200 && dictionary !== null) {
              const flushChunks = dest == "document";
              const response = compressResponse(original, dictionary, headers, ctx, flushChunks);
              ctx.waitUntil(cache.put(cacheKey, response.clone()));
              return response;
            } else {
              return addHeaders(original, headers);
            }
          }
        } else {
          const original = await fetch(request);
          return addHeaders(original, headers);
        }
      } else {
        const original = await fetch(request);
        return addHeaders(original, headers);
      }
    }
  }
}

function addHeaders(original, headers) {
  const response = new Response(original.body, original);
  for (const header of headers) {
    response.headers.append(header[0], header[1]);
  }
  return response;
}

// Add the Link headers for all dynamic_dictionaries to any document or frame requests
function addLinkHeaders(request, headers) {
  // TODO: don't send the link header if the client already has a dictionary (maybe)
  const dest = request.headers.get("sec-fetch-dest");
  if (dest && ["document", "frame"].includes(dest)) {
    for (const id in dynamic_dictionaries) {
      headers.push(["Link", '<' + dictionaryPath + id + '>; rel="compression-dictionary"']);
    }
  }
}

// Add the Use-As-Dictionary for any matching static_dictionaries
function addDictionaryHeader(request, headers) {
  const url = new URL(request.url);
  const dest = request.headers.get("sec-fetch-dest");
  let match = findMatch(url, dest);
  if (match !== null) {
    const id = '"' + url.pathname + url.search + '"';
    match = '"' + match + '"';
    const matchDest = '("' + dest + '")';
    headers.push(["Use-As-Dictionary", 'id=' + id + ', match=' + match + ', match-dest=' + matchDest]);
  }
}

/*
  Dictionary-compress the response
*/
function compressResponse(original, dictionary, headers, ctx, flushChunks) {
  const { readable, writable } = new TransformStream();

  const init = {
    "cf": original.cf,
    "encodeBody": "manual",
    "headers": original.headers,
    "status": original.status,
    "statusText": original.statusText
  }
  const response = new Response(readable, init);
  for (const header of headers) {
    response.headers.append(header[0], header[1]);
  }
  if (zstd !== null) {
    response.headers.set("Content-Encoding", 'dcz',);
    ctx.waitUntil(compressStreamZstd(original, writable, dictionary, flushChunks));
  } else if (brotli !== null) {
    response.headers.set("Content-Encoding", 'dcb',);
    ctx.waitUntil(compressStreamBrotli(original, writable, dictionary, flushChunks));
  }
  return response;
}

async function compressStreamZstd(original, writable, dictionary, flushChunks) {
  const reader = original.body.getReader();
  const writer = writable.getWriter();

  // allocate a compression context and buffers before the stream starts
  let cctx = null;
  let inBuff = null;
  let outBuff = null;
  try {
    cctx = zstd.createCCtx();
    inBuff = getBuffer();
    outBuff = getBuffer();
  if (cctx !== null) {
      // configure the zstd parameters
      zstd.CCtx_setParameter(cctx, zstd.cParameter.c_compressionLevel, compressionLevel);
      zstd.CCtx_setParameter(cctx, zstd.cParameter.c_windowLog, compressionWindowLog );
      zstd.CCtx_refCDict(cctx, dictionary.dictionary);
    }
  } catch (E) {
    console.log(E);
  }

  let headerWritten = false;

  // streaming compression modeled after https://github.com/facebook/zstd/blob/dev/examples/streaming_compression.c
  while (true) {
    const { value, done } = await reader.read();
    const size = done ? 0 : value.byteLength;

    // Grab chunks of the input stream in case it is bigger than the zstd buffer
    let pos = 0;
    const inBuffer = new zstd.inBuffer();
    const outBuffer = new zstd.outBuffer();
    while (pos < size || done) {
      const endPos = Math.min(pos + bufferSize, size);
      const chunkSize = done ? 0 : endPos - pos;
      const chunk = done ? null : value.subarray(pos, endPos);
      pos = endPos;

      try {
        if (chunkSize > 0) {
          wasm.HEAPU8.set(chunk, inBuff);
        }

        inBuffer.src = inBuff;
        inBuffer.size = chunkSize;
        inBuffer.pos = 0;
        let finished = false;
        do {
          outBuffer.dst = outBuff;
          outBuffer.size = bufferSize;
          outBuffer.pos = 0;

          // Use a naive flushing strategy for now. Flush the first chunk immediately and then let zstd decide
          // when each chunk should be emitted (likey accumulate until complete).
          // Also, every 5 chunks that were gathered, flush irregardless.
          let mode = flushChunks ? zstd.EndDirective.e_flush : zstd.EndDirective.e_continue;
          if (done) {
            mode = zstd.EndDirective.e_end;
          }

          const remaining = zstd.compressStream2(cctx, outBuffer, inBuffer, mode);

          if (outBuffer.pos > 0) {
            if (!headerWritten) {
              const dczHeader = new Uint8Array([0x5e, 0x2a, 0x4d, 0x18, 0x20, 0x00, 0x00, 0x00, ...dictionary.hash]);
              await writer.write(dczHeader);
              headerWritten = true;
            }
            const data = new Uint8Array(wasm.HEAPU8.buffer, outBuff, outBuffer.pos);
            await writer.write(data.slice(0));  // Write a copy of the buffer so it doesn't get overwritten
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

  // Free the zstd context and buffers
  releaseBuffer(inBuff);
  releaseBuffer(outBuff);
  if (cctx !== null) zstd.freeCCtx(cctx);

  await writer.close();
  await cleanup();
}

// The current brotli sample below doesn't handle streaming incremental chunks
// (it will flush whenever the encoder decides to)
async function compressStreamBrotli(original, writable, dictionary, flushChunks) {
  const reader = original.body.getReader();
  const writer = writable.getWriter();

  // allocate a compression context and buffers before the stream starts
  let state = null;
  let inBuff = null;
  let outBuff = null;
  try {
    state = brotli.CreateInstance();
    if (state !== null) {
      inBuff = getBuffer();
      outBuff = getBuffer();

      // configure the brotli parameters
      brotli.SetParameter(state, brotli.Parameter.QUALITY, compressionLevel);
      brotli.SetParameter(state, brotli.Parameter.LGWIN, compressionWindowLog);
      
      brotli.AttachPreparedDictionary(state, dictionary.dictionary);
    }
  } catch (E) {
    console.log(E);
  }

  let headerWritten = false;
  
  while (true) {
    const { value, done } = await reader.read();
    const size = done ? 0 : value.byteLength;

    // Grab chunks of the input stream in case it is bigger than the zstd buffer
    let pos = 0;
    while (pos < size || done) {
      const endPos = Math.min(pos + bufferSize, size);
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
          outBuffer.size = bufferSize;
          finished = true;

          let mode = flushChunks ? brotli.Operation.FLUSH : brotli.Operation.PROCESS;
          if (done) {
            mode = brotli.Operation.FINISH;
          }

          if (brotli.CompressStream(state, mode, inBuffer, outBuffer)) {
            const available = bufferSize - outBuffer.size;
            if (available > 0) {
              if (!headerWritten) {
                const dcbHeader = new Uint8Array([0xff, 0x44, 0x43, 0x42, ...dictionary.hash]);
                await writer.write(dcbHeader);
                headerWritten = true;
              }
              const data = new Uint8Array(brotli.HEAPU8.buffer, outBuff, available);
              await writer.write(data.slice(0));
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

  // Free the brotli context and buffers
  releaseBuffer(inBuff);
  releaseBuffer(outBuff);
  if (state !== null) brotli.DestroyInstance(state);

  await writer.close();
  await cleanup();
}

/*
 Handle the client request for a dictionary
*/
async function fetchDictionary(env, request) {
  const url = new URL(request.url);
  const id = url.pathname.slice(dictionaryPath.length).replace(/\/+$/, "").replace(/^\/+/, "");
  if (id in dynamic_dictionaries && "match" in dynamic_dictionaries[id]) {
    const info = dynamic_dictionaries[id];
    let expires = "expire-days" in info ? info["expire-days"] : expire_days;
    expires = expires * 24 * 60 * 60;
    let response = null;
    if ("asset" in info) {
      const assetUrl = new URL(info.asset, url);
      response = await env.ASSETS.fetch(assetUrl);
    } else if ("url" in info) {
      response = await fetch(info.url, request);
    }
    if (response !== null) {
      let match = 'id="' + id + '", match="' + info["match"] + '"';
      if ("match-dest" in info && info["match-dest"].length) {
        let m = '(';
        for (const dest of info["match-dest"]) {
          if (m.length > 2)
            m += " ";
          m += '"' + dest + '"';
        }
        m += ')';
        match += ', match-dest=' + m;
      }
      const bytes = await response.bytes();
      return new Response(bytes, {
        headers: {
          //"content-type": "text/plain; charset=UTF-8",  /* Can be anything but text/plain will allow for Cloudflare to apply compression */
          "content-type": "application/octet-stream",  /* Use uncompressed responses for now - something weird is going on otherwise */
          "cache-control": "public, max-age=" + expires,
          "use-as-dictionary": match
        }
      });
    } else {
      return await fetch(request);
    }
  } else {
    console.log("Matching dictionary not found");
    return await fetch(request);
  }
}

/*
  See if there is a match path in static_dictionaries that matches the given url
*/
function findMatch(url, dest) {
  if (dest in static_dictionaries) {
    for (const pattern of static_dictionaries[dest]) {
      const urlPattern = new URLPattern(pattern, url.toString());
      if (urlPattern.test(url)) {
        return pattern;
      }
    }
  }
  return null;
}

/*
  Initialize wasm and load the matching dictionary in parallel
*/
async function loadDictionary(request, env, ctx) {
  let dictionary = null;
  const availableDictionary = request.headers.get("available-dictionary").trim().replaceAll(':', '')
  const hash = base64ToUint8Array(availableDictionary);
  const id = request.headers.get("dictionary-id").trim().replaceAll('"', '');

  // Initialize wasm
  let loadingWasm = false;
  if (brotli === null && zstd === null) {
    loadingWasm = true;
    zstdInit(ctx).catch(E => console.log(E));
    brotliInit(ctx).catch(E => console.log(E));
  }

  // Fetch the dictionary if we don't already have it
  if (!(id in dictionaries)) {
    // The ID will either be an absolute path starting with / or a key from dynamic_dictionaries
    let response = null;
    let dictionaryURL = null;
    let dictionaryAsset = null;
    if (!id.startsWith("/")) {
      if (id in dynamic_dictionaries) {
        if ("asset" in dynamic_dictionaries[id]) {
          dictionaryAsset = dynamic_dictionaries[id].asset;
        } else if ("url" in dynamic_dictionaries[id]) {
          dictionaryURL = dynamic_dictionaries[id].url;
        }
      }
    } else {
      dictionaryURL = new URL(id, request.url);
      const match = findMatch(dictionaryURL, request.headers.get("sec-fetch-dest"));
      if (match === null) {
        dictionaryURL = null;
      }
    }

    if (dictionaryAsset !== null) {
      const url = new URL(dictionaryAsset, request.url);
      response = await env.ASSETS.fetch(url);
    } else if (dictionaryURL !== null) {
      response = await fetch(dictionaryURL);
    }

    if (response !== null && response.ok) {
      const bytes = await response.bytes();
      if (loadingWasm && wasmLoaded !== null) {
        await wasmLoaded;
        loadingWasm = false;
      }
      // Get the hash of the dictionary and store it in encoder-specific format
      const dictionaryHash = await crypto.subtle.digest({name: 'SHA-256'}, bytes);
      const raw = prepareDictionary(bytes);
      dictionaries[id] = {
        "hash": new Uint8Array(dictionaryHash),
        "dictionary": raw
      };
    }
  }

  // wait for wasm to finish
  if (loadingWasm && wasmLoaded !== null) {
    await wasmLoaded;
  }
  
  let supportsDCZ = false;
  let supportsDCB = false;
  if ("cf" in request && "clientAcceptEncoding" in request.cf) {
    supportsDCZ = request.cf.clientAcceptEncoding.indexOf("dcz") !== -1 && zstd !== null;
    supportsDCB = request.cf.clientAcceptEncoding.indexOf("dcb") !== -1 && brotli !== null;
  }
  if ((supportsDCZ || supportsDCB) && id in dictionaries && "hash" in dictionaries[id] && areUint8ArraysEqual(dictionaries[id]["hash"], hash)) {
    dictionaries[id]['lastUsed'] = performance.now();
    dictionary = dictionaries[id];
  } else {
    console.log("Dictionary mismatch");
    if (!supportsDCZ) console.log("Does not support dcz");
    if (!(id in dictionaries)) {
      console.log("Dictionary " + id + " not found")
    } else if (!areUint8ArraysEqual(dictionaries[id]["hash"], hash)) {
      console.log("Hash mismatch");
      console.log(dictionaries[id].hash);
      console.log(hash);
    }
  }

  return dictionary;
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
    wasm = zstd;
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
    wasm = brotli;
    resolve(true);
  }
}

function prepareDictionary(bytes) {
  let prepared = null;
  try {
    if (bytes !== null) {
      const d = wasm._malloc(bytes.byteLength)
      wasm.HEAPU8.set(bytes, d);
      if (zstd !== null) {
        prepared = zstd.createCDict(d, bytes.byteLength, compressionLevel);
      } else if (brotli !== null) {
        prepared = brotli.PrepareDictionary(brotli.SharedDictionaryType.Raw, bytes.byteLength, d, compressionLevel);
      }
      wasm._free(d);
    }
  } catch (E) {
    console.log(E);
  }
  return prepared;
}

function base64ToUint8Array(base64String) {
  const decodedString = atob(base64String);
  const uint8Array = new Uint8Array(decodedString.length);

  for (let i = 0; i < decodedString.length; i++) {
    uint8Array[i] = decodedString.charCodeAt(i);
  }

  return uint8Array;
}

function areUint8ArraysEqual(arr1, arr2) {
  if (arr1.length !== arr2.length) {
    return false;
  }

  for (let i = 0; i < arr1.length; i++) {
    if (arr1[i] !== arr2[i]) {
      return false;
    }
  }

  return true;
}

function getBuffer() {
  let buffer = buffers.pop();
  if (!buffer && wasm !== null) {
    buffer = wasm._malloc(bufferSize);
  }
  if (!buffer) {
    console.log("Error allocating buffer");
  }
  return buffer
}

function releaseBuffer(buffer) {
  if (buffer !== null) {
    buffers.push(buffer);
  }
}

function toHex(buffer) {
  return Array.prototype.map.call(buffer, x => ('00' + x.toString(16)).slice(-2)).join('');
}

// TODO: Free any buffers that haven't been used in a while
let lastCleanup = null;
const CLEANUP_INTERVAL = 600 * 1000; // Every 5 minutes
const DICTIONARY_TTL = 3600 * 1000;  // Keep unused dictionaries for an hour
async function cleanup() {
  const now = performance.now();
  if (!lastCleanup || now - lastCleanup >= CLEANUP_INTERVAL) {
    try {
      lastCleanup = now;
      const keys = [];
      for (const id in dictionaries) {
        if ("lastUsed" in dictionaries[id] && now - dictionaries[id]["lastUsed"] > DICTIONARY_TTL) {
          keys.push(id);
        }
      }
      for (const key in keys) {
        delete dictionaries[key];
      }
    } catch (E) {
      console.log(E);
    }
  }
}