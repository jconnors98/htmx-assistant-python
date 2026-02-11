function $(id) {
  return document.getElementById(id);
}

function getConfig() {
  const cfg = window.APP_CONFIG;
  if (!cfg || typeof cfg !== "object") {
    throw new Error(
      "Missing APP_CONFIG. Create config.js from config.example.js and set backendSubmitUrl + apiKey header."
    );
  }
  return cfg;
}

function setStatus(kind, message) {
  const el = $("status");
  if (!el) return;
  el.classList.remove("good", "bad");
  if (kind === "good") el.classList.add("status", "good");
  else if (kind === "bad") el.classList.add("status", "bad");
  else el.classList.add("status");
  el.textContent = message || "";
  el.hidden = !message;
}

function isLikelyEmail(value) {
  const v = String(value || "").trim();
  if (v.length < 5) return false;
  // intentionally simple and permissive
  return /^[^\s@]+@[^\s@]+\.[^\s@]{2,}$/.test(v);
}

function normalizePhone(value) {
  // Store a minimally normalized version while allowing international input.
  const raw = String(value || "").trim();
  // collapse whitespace
  return raw.replace(/\s+/g, " ");
}

function requiredTrimmed(value) {
  return String(value || "").trim();
}

function setFieldError(inputEl, message) {
  if (!inputEl) return;
  inputEl.setAttribute("aria-invalid", message ? "true" : "false");
  const id = inputEl.id ? `${inputEl.id}Error` : "";
  if (id) {
    const err = $(id);
    if (err) {
      err.textContent = message || "";
      err.hidden = !message;
    }
  }
}

function clearAllFieldErrors() {
  ["fullName", "companyName", "email", "phone"].forEach((k) => {
    const el = $(k);
    setFieldError(el, "");
  });
}

async function submitEntry(payload) {
  const cfg = getConfig();
  if (!cfg.backendSubmitUrl) throw new Error("APP_CONFIG.backendSubmitUrl is missing.");

  const headers = {
    "Content-Type": "application/json",
  };

  if (cfg.apiKeyHeaderName && cfg.apiKey) {
    headers[cfg.apiKeyHeaderName] = cfg.apiKey;
  }

  const res = await fetch(cfg.backendSubmitUrl, {
    method: "POST",
    headers,
    body: JSON.stringify(payload),
  });

  if (!res.ok) {
    let detail = "";
    try {
      const text = await res.text();
      detail = text ? ` (${text.slice(0, 180)})` : "";
    } catch {
      // ignore
    }
    throw new Error(`Submission failed: ${res.status} ${res.statusText}${detail}`);
  }
}

export function initEntryForm() {
  const form = $("entryForm");
  if (!form) return;

  // Populate event text if present.
  try {
    const cfg = getConfig();
    const titleEl = $("eventTitle");
    const subEl = $("eventSubtitle");
    if (titleEl && cfg.eventTitle) titleEl.textContent = cfg.eventTitle;
    if (subEl && cfg.eventSubtitle) subEl.textContent = cfg.eventSubtitle;
  } catch {
    // config may be missing; handled on submit with a clear error
  }

  form.addEventListener("submit", async (e) => {
    e.preventDefault();

    const fullName = requiredTrimmed($("fullName")?.value);
    const companyName = requiredTrimmed($("companyName")?.value);
    const email = requiredTrimmed($("email")?.value);
    const phone = normalizePhone($("phone")?.value);
    const honeypot = requiredTrimmed($("website")?.value);

    // optional checkboxes (default required for age)
    const ageOk = Boolean($("ageOk")?.checked);
    const contactOk = Boolean($("contactOk")?.checked);

    setStatus("", "");
    clearAllFieldErrors();

    if (honeypot) {
      // likely bot
      setStatus("bad", "Submission blocked.");
      return;
    }

    const fieldErrors = {};
    if (!fullName) fieldErrors.fullName = "Please enter your full name.";
    if (!companyName) fieldErrors.companyName = "Please enter your company name.";
    if (!email) fieldErrors.email = "Please enter your email.";
    else if (!isLikelyEmail(email)) fieldErrors.email = "Please enter a valid email.";
    if (!phone) fieldErrors.phone = "Please enter your phone number.";

    if (fieldErrors.fullName) setFieldError($("fullName"), fieldErrors.fullName);
    if (fieldErrors.companyName) setFieldError($("companyName"), fieldErrors.companyName);
    if (fieldErrors.email) setFieldError($("email"), fieldErrors.email);
    if (fieldErrors.phone) setFieldError($("phone"), fieldErrors.phone);

    if (!ageOk) {
      setStatus("bad", "You must confirm you’re of legal drinking age.");
      return;
    }

    const firstError = fieldErrors.fullName || fieldErrors.companyName || fieldErrors.email || fieldErrors.phone;
    if (firstError) {
      setStatus("bad", firstError);
      const firstEl = $("fullName")?.getAttribute("aria-invalid") === "true"
        ? $("fullName")
        : $("companyName")?.getAttribute("aria-invalid") === "true"
          ? $("companyName")
          : $("email")?.getAttribute("aria-invalid") === "true"
            ? $("email")
            : $("phone");
      firstEl?.focus?.();
      return;
    }

    const submitBtn = $("submitBtn");
    if (submitBtn) {
      submitBtn.disabled = true;
      submitBtn.textContent = "Submitting…";
    }

    try {
      const payload = {
        fullName,
        companyName,
        email,
        phone,
        source: "qr",
        submittedAt: new Date().toISOString(),
        consent: {
          ageOk,
          contactOk,
        },
      };

      await submitEntry(payload);
      window.location.href = "thank-you.html";
    } catch (err) {
      setStatus("bad", err?.message || "Submission failed. Please try again.");
      if (submitBtn) {
        submitBtn.disabled = false;
        submitBtn.textContent = "Enter draw";
      }
    }
  });
}

