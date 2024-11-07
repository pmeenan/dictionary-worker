// Pick ONE - either brotli or zstd

// Zstd - ~100k compressed
import zstdlib from "../zstd-wasm-compress/bin/zstdlib.js";
import zstdwasm from "../zstd-wasm-compress/bin/zstdlib.wasm";

/* Brotli - ~900k compressed
import brotlienc from "../brotli-wasm-compress/bin/brotlienc.js";
import BrotliEncoder from "../brotli-wasm-compress/bin/brotlienc.wasm";
*/

const expire_days = 7; // Default dictionary expiration (can be overriden per-dictionary with "expire-days")

// List of dictionaries that need to be loaded with "Link rel=compression-dictionary"
// These can either be "asset" file paths under the "assets" directory or
// "url" pointing to a path to fetch the dictionary.
const dynamic_dictionaries = {
  "roadtrip": {
    "asset": "/roadtrip.dat",
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
}
static_dictionaries = {}

// Psuedo-path where the dictionaries will be served from (shouldn't collide with a real directory)
const dictionaryPath = "/dictionary/";

// Compression options
const compressionLevel = 10;      // 1-19 for ZStandard, 5-11 for Brotli
const compressionWindowLog = 20;  // Compression window should be at least as long as the dictionary + typical response - 2 ^ 20 = 1MB

// Internal globals for managing state while waiting for the dictionary and zstd wasm to load
let zstd = null;
let brotli = null;
let wasmLoaded = null;
let dictionaries = {};  // in-memory dictionaries, indexed by ID

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
          const cacheKey = url + " " + request.headers.get("available-dictionary");
          //const cache = caches.default;
          //const cached = await cache.match(cacheKey);
          const cached = false;
          if (cached) {
            const response = new Response(cached.body, cached);
            response.headers.append("x-Dictionary", "cached");
            return response;
          } else {
            // Trigger the async dictionary load
            const dictionaryPromise = loadDictionary(request, env, ctx).catch(E => console.log(E));

            // Fetch the actual content
            const original = await fetch(request);

            // Wait for the dictionary to finish loading
            const dictionary = await dictionaryPromise;

            if (original.ok && dictionary !== null) {
              const response = await compressResponse(original, dictionary, headers, ctx);
              //ctx.waitUntil(cache.put(cacheKey, response.clone()));
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
        console.log("destination mismatch");
        console.log(dest);
        console.log(targetDestinations);
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
async function compressResponse(original, dictionary, headers, ctx) {
  const { readable, writable } = new TransformStream();
  if (zstd !== null) {
    ctx.waitUntil(compressStreamZstd(original.body, writable, dictionary));
  } else if (brotli !== null) {
    ctx.waitUntil(compressStreamBrotli(original.body, writable, dictionary));
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
  for (const header of headers) {
    response.headers.append(header[0], header[1]);
  }
  return response;
}

async function compressStreamZstd(readable, writable, dictionary) {
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
      
      zstd.CCtx_refCDict(cctx, dictionary.dictionary);
    }
  } catch (E) {
    console.log(E);
  }

  // write the dcz header
  const dczHeader = new Uint8Array([0x5e, 0x2a, 0x4d, 0x18, 0x20, 0x00, 0x00, 0x00, ...dictionary.hash]);
  await writer.write(dczHeader);
  console.log("Wrote dcz header");
  console.log(dczHeader);
  
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

          console.log("Chunk: " + chunkSize +", out: " + outBuffer.pos);

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
async function compressStreamBrotli(readable, writable, dictionary) {
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
      
      brotli.AttachPreparedDictionary(state, dictionary.dictionary);
    }
  } catch (E) {
    console.log(E);
  }
  
  // write the dcb header
  const dcbHeader = new Uint8Array([0xff, 0x44, 0x43, 0x42, ...dictionary.hash]);
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
      return new Response(response.body, {
        headers: {
          "content-type": "text/plain; charset=UTF-8",  /* Can be anything but text/plain will allow for Cloudflare to apply compression */
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
      const urlPattern = new URLPattern(pattern, url);
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
    dictionary = dictionaries[id];
  } else {
    console.log("Dictionary mismatch");
    if (!supportsDCZ) console.log("Does not support dcz");
    if (!(id in dictionaries)) {
      console,log("Dictionary " + id + " not found")
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
    resolve(true);
  }
}

function prepareDictionary(bytes) {
  let prepared = null;
  try {
    if (bytes !== null) {
      if (zstd !== null) {
        const d = zstd._malloc(bytes.byteLength)
        zstd.HEAPU8.set(bytes, d);
        prepared = zstd.createCDict(d, bytes.byteLength, compressionLevel);
        zstd._free(d);
      } else if (brotli !== null) {
        const d = brotli._malloc(bytes.byteLength)
        brotli.HEAPU8.set(bytes, d);
        prepared = brotli.PrepareDictionary(brotli.SharedDictionaryType.Raw, bytes.byteLength, d, compressionLevel);
        brotli._free(d);
      }
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