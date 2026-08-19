// meikipop Mining Bridge - background service worker.
//
// Acts as the network boundary between the page (content script) and the local
// meikipop bridge server: it holds the pairing token (kept out of the page's
// isolated-world reach) and performs the actual fetch() calls, and it brokers
// chrome.tabCapture (an extension-privileged API the content script can't call
// directly) so the content script can record a short tab-audio clip.
//
// Cross-browser note: Firefox has no equivalent of chrome.tabCapture.getMediaStreamId
// (its tabCapture API doesn't support the stream-id hand-off used here), so tab-audio
// capture is Chrome/Chromium-only. This file is shared between browser_extension/chrome
// and browser_extension/firefox verbatim: on Firefox, the "tabCapture" permission simply
// isn't declared in that folder's manifest.json, so chrome.tabCapture is undefined and
// the feature-detection below degrades gracefully (text + video screenshot still mine).
const TAB_CAPTURE_SUPPORTED = typeof chrome !== 'undefined'
  && !!chrome.tabCapture
  && typeof chrome.tabCapture.getMediaStreamId === 'function';

async function getBridgeConfig() {
  const { bridgePort, bridgeToken } = await chrome.storage.local.get(['bridgePort', 'bridgeToken']);
  return { port: bridgePort || 8850, token: bridgeToken || '' };
}

chrome.runtime.onMessage.addListener((message, sender, sendResponse) => {
  if (message.type === 'GET_TAB_STREAM_ID') {
    if (!TAB_CAPTURE_SUPPORTED) {
      // Firefox (or any browser without tabCapture.getMediaStreamId): no audio clip this time.
      sendResponse({ streamId: null, unsupported: true });
      return false;
    }
    // consumerTabId must be set explicitly: it tells Chrome which tab is allowed to call
    // getUserMedia() with the returned stream id. Since content.js calls getUserMedia() in
    // the same tab it's injected into, consumer and target are the same tab here.
    chrome.tabCapture.getMediaStreamId({ targetTabId: sender.tab.id, consumerTabId: sender.tab.id }, (streamId) => {
      if (chrome.runtime.lastError) {
        console.warn('[meikipop] tabCapture.getMediaStreamId failed:', chrome.runtime.lastError.message);
        sendResponse({ streamId: null });
        return;
      }
      sendResponse({ streamId });
    });
    return true; // keep the message channel open for the async response
  }

  if (message.type === 'MINE') {
    (async () => {
      const { port, token } = await getBridgeConfig();
      if (!token) {
        sendResponse({ ok: false, error: 'No pairing token set. Open the extension options and paste the token copied from meikipop Settings.' });
        return;
      }
      try {
        const resp = await fetch(`http://127.0.0.1:${port}/mine`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${token}` },
          body: JSON.stringify({
            token,
            text: message.text,
            imageBase64: message.imageBase64,
            audioBase64: message.audioBase64,
            sourceUrl: message.sourceUrl,
          }),
        });
        sendResponse(await resp.json());
      } catch (e) {
        sendResponse({ ok: false, error: `Could not reach meikipop on 127.0.0.1:${port}. Is the Mining Bridge enabled in Settings? (${e})` });
      }
    })();
    return true;
  }

  if (message.type === 'LOOKUP') {
    (async () => {
      const { port, token } = await getBridgeConfig();
      try {
        const resp = await fetch(`http://127.0.0.1:${port}/lookup`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${token}` },
          body: JSON.stringify({ token, text: message.text }),
        });
        sendResponse(await resp.json());
      } catch (e) {
        sendResponse({ error: String(e) });
      }
    })();
    return true;
  }

  return false;
});
