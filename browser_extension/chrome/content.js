// meikipop Mining Bridge - content script.
//
// Press Alt+A while a video with subtitles is playing (or with text selected as a
// generic fallback) to mine the current moment: caption text + a video frame +
// (Chrome/Edge only) a short tab-audio clip get sent to meikipop's local bridge to
// look the word up and create an Anki card, without needing meikipop's screen-OCR at all.

const CLIP_DURATION_MS = 3000;

function findActiveVideo() {
  const videos = Array.from(document.querySelectorAll('video'));
  return videos.find(v => !v.paused && v.readyState >= 2) || videos[0] || null;
}

function captureFrame(video) {
  try {
    const canvas = document.createElement('canvas');
    canvas.width = video.videoWidth;
    canvas.height = video.videoHeight;
    const ctx = canvas.getContext('2d');
    ctx.drawImage(video, 0, 0, canvas.width, canvas.height);
    return canvas.toDataURL('image/jpeg', 0.85);
  } catch (e) {
    // Most commonly a CORS-tainted canvas (DRM'd or cross-origin video source).
    console.warn('[meikipop] Failed to capture video frame:', e);
    return null;
  }
}

function getCaptionText() {
  // YouTube's live caption track.
  const ytCaption = document.querySelector('.ytp-caption-segment, .caption-window');
  if (ytCaption && ytCaption.textContent.trim()) {
    return ytCaption.textContent.trim();
  }
  // Generic fallback for sites without a recognized caption DOM: use the current
  // text selection, so the user can select subtitle/on-page text manually.
  const selection = window.getSelection().toString().trim();
  return selection || '';
}

function recordTabAudioClip(durationMs) {
  return new Promise((resolve) => {
    chrome.runtime.sendMessage({ type: 'GET_TAB_STREAM_ID' }, async (response) => {
      if (response && response.unsupported) {
        // Firefox (or any browser without chrome.tabCapture): log this once per page load
        // instead of on every mine, since it's an expected/permanent limitation there.
        if (!window.__meikipopAudioUnsupportedLogged) {
          window.__meikipopAudioUnsupportedLogged = true;
          console.info('[meikipop] Tab-audio capture is not available in this browser (Chrome/Edge only). Text + screenshot mining still works.');
        }
        resolve(null);
        return;
      }
      const streamId = response && response.streamId;
      if (!streamId) { resolve(null); return; }
      try {
        const stream = await navigator.mediaDevices.getUserMedia({
          audio: { mandatory: { chromeMediaSource: 'tab', chromeMediaSourceId: streamId } },
        });
        const recorder = new MediaRecorder(stream);
        const chunks = [];
        recorder.ondataavailable = (e) => { if (e.data.size > 0) chunks.push(e.data); };
        recorder.onstop = () => {
          stream.getTracks().forEach((t) => t.stop());
          const blob = new Blob(chunks, { type: 'audio/webm' });
          const reader = new FileReader();
          reader.onloadend = () => resolve(reader.result); // data URL
          reader.onerror = () => resolve(null);
          reader.readAsDataURL(blob);
        };
        recorder.start();
        setTimeout(() => recorder.stop(), durationMs);
      } catch (e) {
        console.warn('[meikipop] Tab audio capture failed (this API is finicky across Chrome versions):', e);
        resolve(null);
      }
    });
  });
}

async function mineCurrentMoment() {
  const video = findActiveVideo();
  const text = getCaptionText();
  if (!text) {
    console.warn('[meikipop] Nothing to mine: no caption text found. Select the subtitle text manually and try again.');
    return;
  }

  const imageBase64 = video ? captureFrame(video) : null;
  const audioBase64 = await recordTabAudioClip(CLIP_DURATION_MS);

  chrome.runtime.sendMessage({
    type: 'MINE',
    text,
    imageBase64,
    audioBase64,
    sourceUrl: location.href,
  }, (result) => {
    if (result && result.ok) {
      console.info('[meikipop] Mined note added:', result);
    } else {
      console.error('[meikipop] Mining failed:', result && result.error);
    }
  });
}

document.addEventListener('keydown', (e) => {
  if (e.altKey && e.code === 'KeyA') {
    mineCurrentMoment();
  }
});
