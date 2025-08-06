// parsePDF.js
import fs from "fs";
import pdfParse from 'pdf-parse/lib/pdf-parse.js';

export async function parseResume(filePath) {
  const buffer = fs.readFileSync(filePath);
  const data = await pdf(buffer);
  return data.text;
}
