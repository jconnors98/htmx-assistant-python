# Buildex Tequila Draw (QR + Entry Form)

Static site that:
- Shows a **QR code** people can scan to enter a draw
- Collects: **full name**, **company name**, **email**, **phone**
- **POSTs** the entry to your backend endpoint with a static API key header
- Includes a **print-friendly poster generator** page

## Files
- `index.html`: QR page (good for quick printing)
- `enter.html`: entry form (what the QR should point to)
- `qr.html`: poster / QR generator (best for print-ready QR + PNG download)
- `thank-you.html`: confirmation page
- `assets/styles.css`: styling
- `assets/app.js`: form validation + submit
- `assets/qr.js`: QR helpers
- `config.example.js`: config template
- `config.js`: your real config (contains API key)

## Configure
1. Open `config.js`
2. Set:
   - `backendSubmitUrl`
   - `apiKeyHeaderName`
   - `apiKey`
3. (Optional) Set:
   - `eventTitle`
   - `eventSubtitle`
   - `publicEntryUrl` (recommended after you deploy)

## Backend contract (frontend sends this)
**POST** to `APP_CONFIG.backendSubmitUrl`

**Headers**
- `Content-Type: application/json`
- `{apiKeyHeaderName}: {apiKey}` (only sent if both are set)

**JSON body**
```json
{
  "fullName": "Jane Doe",
  "companyName": "Acme",
  "email": "jane@acme.com",
  "phone": "+1 555 555 5555",
  "source": "qr",
  "submittedAt": "2026-02-11T12:34:56.000Z",
  "consent": {
    "ageOk": true,
    "contactOk": false
  }
}
```

Success response: any `200` or `201` → redirects to `thank-you.html`.

## Run locally (simple)
You can open the HTML files directly, but for best results use a tiny web server.

### Option A: Python
```bash
python -m http.server 5173
```
Then open `http://localhost:5173/index.html`

### Option B: Node
```bash
npx serve .
```

## Deploy
Host as a static site anywhere (GitHub Pages / Netlify / Azure Static Web Apps / S3).

After deploying, set `publicEntryUrl` in `config.js` to your final public URL:
`https://YOUR_DOMAIN/enter.html`

## Print a poster + QR
1. Open `qr.html`
2. Paste the final entry URL
3. Click **Generate**
4. Click **Print** (or **Download PNG** if you want to drop it into a flyer design)

## Security note
A static API key embedded in a public website is discoverable. If you want stronger protection later, switch to:
- a public submission endpoint with server-side rate limiting + bot filtering, or
- CAPTCHA verification.

