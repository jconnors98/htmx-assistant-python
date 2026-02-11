// Copy to config.js and edit values.
// This file is safe to commit; config.js should contain real secrets.

window.APP_CONFIG = {
  // Where to POST draw entries.
  backendSubmitUrl: "https://YOUR_BACKEND.example.com/api/tequila-draw/entries",

  // Static API key header (note: discoverable in a public site).
  apiKeyHeaderName: "x-api-key",
  apiKey: "REPLACE_ME",

  // Optional UI text
  eventTitle: "Tequila Bottle Draw",
  eventSubtitle: "Enter for a chance to win. Takes 10 seconds.",

  // Optional: the final public entry URL (used by index.html default QR).
  // If empty, the site will use the current origin + /enter.html.
  publicEntryUrl: "",
};

