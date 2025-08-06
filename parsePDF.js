import fs from "fs/promises";

export async function parseResume(filePath) {
  const pdf = await import('pdf-parse'); // ESM-safe dynamic import

  const buffer = await fs.readFile(filePath);
  const data = await pdf.default(buffer); // Access .default if using import()

  return data.text;
}