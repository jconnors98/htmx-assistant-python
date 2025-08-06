// parsePDF.js
import fs from "fs";
import pdf from "pdf-parse";

export async function parseResume(filePath) {
  const buffer = fs.readFileSync(filePath);
  const data = await pdf(buffer);
  return data.text;
}
