# meikipop Mining Bridge (browser extension) — experimental groundwork

Lets you mine Japanese subtitles from YouTube (and, best-effort, other `<video>`-based
sites) straight into Anki via meikipop, without needing screen-OCR: the extension reads
caption text directly from the page DOM, grabs a video frame, and records a short clip
of tab audio, then sends all three to a local bridge server that meikipop runs.

**Status:** this is a first working skeleton, not a polished release. Tab-audio capture
in particular relies on Chrome APIs (`tabCapture`, `getUserMedia` with a tab stream id)
that are known to be finicky across Chrome versions/sites (especially DRM-protected
video, which will also blank out captured frames due to canvas tainting).

**Browser support:**

| Feature | Chrome / Edge | Firefox |
|---|---|---|
| Caption text mining | Yes | Yes |
| Video frame screenshot | Yes | Yes |
| Tab-audio clip (via the extension) | Yes | **No** (Firefox has no extension API for tab/system audio capture at all — not just `chrome.tabCapture`, but `getDisplayMedia`'s audio option is unimplemented there too) |

On Firefox, mining still works for text + screenshot; the extension-captured audio clip
is silently omitted (a one-time console note explains why).

**Firefox audio workaround:** if you enable meikipop's own **Sentence Audio Capture**
(Settings → General — an OS-level loopback recorder, independent of any browser), the
mining bridge automatically falls back to it whenever the extension can't supply a clip.
This works for both browsers, but on Firefox it's the *only* way to get audio into mined
cards.

**Folder layout:** Chrome and Firefox need different `manifest.json` files — Firefox's
manifest schema doesn't even recognize the `tabCapture` permission string (Chrome-only
API), so a single shared manifest can't satisfy both. Use the matching subfolder:

```
browser_extension/
  chrome/    <- load this in Chrome/Edge (has the tabCapture permission)
  firefox/   <- load this in Firefox (no tabCapture permission; audio clip omitted)
```

`background.js`, `content.js`, `options.html`, `options.js` are identical in both
folders (the code already feature-detects whether tab-audio capture is available), only
`manifest.json` differs between them.

## Setup

1. In meikipop: **Settings → Mining Bridge (Browser Extension)** → check **Enabled**,
   set a port if you like, click **Save**, then reopen Settings and click
   **Copy Pairing Token**.
2. Load the unpacked extension from the folder matching your browser:
   - **Chrome/Edge:** go to `chrome://extensions` (or `edge://extensions`), enable
     **Developer mode**, click **Load unpacked**, and select the `browser_extension/chrome`
     folder.
   - **Firefox:** go to `about:debugging#/runtime/this-firefox` → **Load Temporary
     Add-on…** → select `browser_extension/firefox/manifest.json`.
     Firefox only keeps temporary add-ons until the browser restarts — reload it from
     that same page after each restart. (A permanent install requires the extension
     to be signed by Mozilla, which this experimental build isn't.)
3. Click the extension's icon → paste the pairing JSON you copied → **Save** →
   **Test Connection** (should say "Connected to meikipop").
4. On a YouTube video with Japanese captions turned on, press **Alt+A** to mine the
   current line. Check meikipop's log / your Anki deck for the new card.

## How it works

```
content.js (page)  --caption text, video frame, tab-audio clip-->  background.js (service worker)
                                                                           |
                                                                (adds Authorization: Bearer <token>)
                                                                           v
                                                    http://127.0.0.1:<port>/mine  (meikipop, loopback only)
                                                                           |
                                                          dictionary lookup + AnkiConnect note
```

## Security notes

- The bridge only listens on `127.0.0.1` — it is never reachable from the network.
- Every request needs the pairing token; a malicious webpage cannot call the bridge
  even though it's local, because CORS is restricted to `chrome-extension://`/
  `moz-extension://` origins and the token isn't exposed to page JS (only to the
  extension's background service worker).
- Don't share your pairing token; regenerate it by clearing `token` in `config.ini`'s
  `[Bridge]` section and restarting meikipop.
- CORS is restricted to `chrome-extension://` and `moz-extension://` origins, so the
  Firefox build is covered by the same origin allowlist as Chrome.
