// parsePDF.js
import fs from "fs/promises";

/**
 * Parses a PDF resume and returns extracted text.
 * Works with ESM via dynamic import of pdf-parse.
 * @param {string} filePath - The path to the uploaded PDF file.
 * @returns {Promise<string>} Parsed resume text.
 */
export async function parseResume(filePath) {
  try {
    const pdf = await import("pdf-parse"); // dynamic import for ESM compatibility
    const buffer = await fs.readFile(filePath);
    const data = await pdf.default(buffer); // use .default for CommonJS interop
    return data.text;
  } catch (error) {
    console.error("❌ Error parsing PDF:", error);
    return "⚠️ Failed to extract resume text from the PDF.";
  }
}
