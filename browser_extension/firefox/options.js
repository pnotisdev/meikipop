const pairingInput = document.getElementById('pairing');
const portInput = document.getElementById('port');
const tokenInput = document.getElementById('token');
const statusEl = document.getElementById('status');

chrome.storage.local.get(['bridgePort', 'bridgeToken'], ({ bridgePort, bridgeToken }) => {
  if (bridgePort) portInput.value = bridgePort;
  if (bridgeToken) tokenInput.value = bridgeToken;
});

pairingInput.addEventListener('change', () => {
  try {
    const parsed = JSON.parse(pairingInput.value);
    if (parsed.port) portInput.value = parsed.port;
    if (parsed.token) tokenInput.value = parsed.token;
  } catch (e) {
    // Ignore invalid JSON while the user is still typing/pasting.
  }
});

document.getElementById('save').addEventListener('click', () => {
  chrome.storage.local.set({
    bridgePort: parseInt(portInput.value, 10) || 8850,
    bridgeToken: tokenInput.value.trim(),
  }, () => {
    statusEl.textContent = 'Saved.';
  });
});

document.getElementById('test').addEventListener('click', async () => {
  statusEl.textContent = 'Testing...';
  try {
    const resp = await fetch(`http://127.0.0.1:${portInput.value}/health`);
    const data = await resp.json();
    statusEl.textContent = data.status === 'ok' ? '\u2705 Connected to meikipop.' : '\u26a0\ufe0f Unexpected response.';
  } catch (e) {
    statusEl.textContent = '\u274c Could not reach meikipop. Is the Mining Bridge enabled in Settings?';
  }
});
