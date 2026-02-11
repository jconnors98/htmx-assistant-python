/* global QRCode */
// QR helper.
// The HTML tries to load QRCode via CDN, but CDNs can be blocked/offline.
// We lazy-load with a fallback before failing.

const QR_CODE_CDN_URLS = [
  // Prefer local vendored copy (works offline / behind strict networks).
  "assets/qrcode.min.js",
  "https://cdn.jsdelivr.net/npm/qrcode@1.5.3/build/qrcode.min.js",
  "https://unpkg.com/qrcode@1.5.3/build/qrcode.min.js",
];

function getQrCodeGlobal() {
  try {
    return globalThis.QRCode;
  } catch {
    return typeof QRCode === "undefined" ? undefined : QRCode;
  }
}

function installNayukiAdapterIfPresent() {
  // If we vendored Nayuki's qrcodegen (which exposes `qrcodegen`), adapt it to
  // the API we use elsewhere: QRCode.toCanvas(...) / QRCode.toDataURL(...).
  try {
    if (globalThis.QRCode) return globalThis.QRCode;
    const qg = globalThis.qrcodegen;
    if (!qg || !qg.QrCode || !qg.QrCode.Ecc) return undefined;

    const eccFromOption = (level) => {
      const v = String(level || "M").toUpperCase();
      if (v === "L") return qg.QrCode.Ecc.LOW;
      if (v === "Q") return qg.QrCode.Ecc.QUARTILE;
      if (v === "H") return qg.QrCode.Ecc.HIGH;
      return qg.QrCode.Ecc.MEDIUM;
    };

    const toBool = (qr, x, y) => {
      try {
        return !!qr.getModule(x, y);
      } catch {
        return false;
      }
    };

    const renderToCanvas = (canvasEl, text, options = {}) => {
      if (!canvasEl) throw new Error("Missing canvas element.");
      if (!text) throw new Error("Missing QR text.");

      const ecc = eccFromOption(options.errorCorrectionLevel);
      const qr = qg.QrCode.encodeText(String(text), ecc);

      const margin = typeof options.margin === "number" ? options.margin : 1;
      const width = typeof options.width === "number" ? options.width : 320;
      const dark = (options.color && options.color.dark) || options.darkColor || "#111827";
      const light = (options.color && options.color.light) || options.lightColor || "#ffffff";

      const modules = qr.size;
      const pixelsPerModule = Math.max(1, Math.floor(width / (modules + margin * 2)));
      const sizePx = (modules + margin * 2) * pixelsPerModule;

      canvasEl.width = sizePx;
      canvasEl.height = sizePx;

      const ctx = canvasEl.getContext("2d");
      if (!ctx) throw new Error("Canvas 2D context not available.");

      ctx.fillStyle = light;
      ctx.fillRect(0, 0, sizePx, sizePx);

      ctx.fillStyle = dark;
      for (let y = 0; y < modules; y++) {
        for (let x = 0; x < modules; x++) {
          if (toBool(qr, x, y)) {
            ctx.fillRect(
              (x + margin) * pixelsPerModule,
              (y + margin) * pixelsPerModule,
              pixelsPerModule,
              pixelsPerModule
            );
          }
        }
      }
    };

    globalThis.QRCode = {
      toCanvas: async (...args) => {
        // Supported forms:
        // - toCanvas(canvasEl, text, options)
        // - toCanvas(text, options) -> creates a canvas
        const [a0, a1, a2] = args;
        if (a0 && typeof a0.getContext === "function") {
          renderToCanvas(a0, a1, a2 || {});
          return;
        }
        const canvas = document.createElement("canvas");
        renderToCanvas(canvas, a0, a1 || {});
        return canvas;
      },
      toDataURL: async (...args) => {
        // Supported forms:
        // - toDataURL(text, options)
        // - toDataURL(canvasEl, text, options)
        const [a0, a1, a2] = args;
        let canvasEl, text, options;
        if (a0 && typeof a0.getContext === "function") {
          canvasEl = a0;
          text = a1;
          options = a2 || {};
        } else {
          canvasEl = document.createElement("canvas");
          text = a0;
          options = a1 || {};
        }
        renderToCanvas(canvasEl, text, options);
        return canvasEl.toDataURL((options && options.type) || "image/png");
      },
    };

    return globalThis.QRCode;
  } catch {
    return undefined;
  }
}

function loadScript(src) {
  return new Promise((resolve, reject) => {
    const s = document.createElement("script");
    s.src = src;
    s.async = true;
    s.onload = () => resolve(true);
    s.onerror = () => reject(new Error(`Failed to load QR library from ${src}`));
    document.head.appendChild(s);
  });
}

async function ensureQrCodeLoaded() {
  if (typeof document === "undefined") {
    throw new Error("QR rendering requires a browser environment.");
  }
  if (getQrCodeGlobal() || installNayukiAdapterIfPresent()) return;

  if (!globalThis.__buildexQrCodeLoadPromise) {
    globalThis.__buildexQrCodeLoadPromise = (async () => {
      for (const url of QR_CODE_CDN_URLS) {
        try {
          await loadScript(url);
          if (getQrCodeGlobal() || installNayukiAdapterIfPresent()) return;
        } catch {
          // try next
        }
      }
      throw new Error(
        "QR library not loaded (QRCode global missing). If you are offline or CDNs are blocked, allow access to jsdelivr/unpkg or host qrcode.min.js locally."
      );
    })();
  }

  await globalThis.__buildexQrCodeLoadPromise;
}

export async function renderQrToCanvas(canvasEl, text, options = {}) {
  if (!canvasEl) throw new Error("Missing canvas element.");
  if (!text) throw new Error("Missing QR text.");
  await ensureQrCodeLoaded();
  const QR = getQrCodeGlobal();
  if (!QR) throw new Error("QR library not loaded (QRCode global missing).");

  const opts = {
    errorCorrectionLevel: options.errorCorrectionLevel || "M",
    margin: typeof options.margin === "number" ? options.margin : 1,
    width: typeof options.width === "number" ? options.width : 320,
    color: {
      dark: options.darkColor || "#111827",
      light: options.lightColor || "#ffffff",
    },
  };

  await QR.toCanvas(canvasEl, text, opts);
}

export async function qrDataUrl(text, options = {}) {
  if (!text) throw new Error("Missing QR text.");
  await ensureQrCodeLoaded();
  const QR = getQrCodeGlobal();
  if (!QR) throw new Error("QR library not loaded (QRCode global missing).");

  const opts = {
    errorCorrectionLevel: options.errorCorrectionLevel || "M",
    margin: typeof options.margin === "number" ? options.margin : 1,
    width: typeof options.width === "number" ? options.width : 512,
    color: {
      dark: options.darkColor || "#111827",
      light: options.lightColor || "#ffffff",
    },
  };
  return await QR.toDataURL(text, opts);
}

export function downloadDataUrl(dataUrl, filename) {
  const a = document.createElement("a");
  a.href = dataUrl;
  a.download = filename || "qr.png";
  document.body.appendChild(a);
  a.click();
  a.remove();
}

