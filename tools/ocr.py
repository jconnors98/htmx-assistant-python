from __future__ import annotations

import tempfile
from pathlib import Path

import cv2  # type: ignore
import numpy as np  # type: ignore
import pytesseract  # type: ignore
from PIL import Image  # type: ignore


def enhance_blueprint_for_ocr(image_path: str) -> str:
    """
    Applies a sequence of OpenCV operations to improve blueprint legibility prior to OCR.
    Returns the path to the processed image.
    """
    original = cv2.imread(image_path)
    if original is None:
        return image_path

    grayscale = cv2.cvtColor(original, cv2.COLOR_BGR2GRAY)
    blurred = cv2.GaussianBlur(grayscale, (3, 3), 0)
    # Adaptive threshold emphasises lines/text
    threshold = cv2.adaptiveThreshold(
        blurred,
        255,
        cv2.ADAPTIVE_THRESH_MEAN_C,
        cv2.THRESH_BINARY,
        31,
        15,
    )
    # Sharpen edges
    kernel = np.array([[0, -1, 0], [-1, 5, -1], [0, -1, 0]])
    sharpened = cv2.filter2D(threshold, -1, kernel)

    # Deskew using moments when possible
    coords = np.column_stack(np.where(sharpened > 0))
    angle = 0.0
    if coords.size:
        rect = cv2.minAreaRect(coords)
        angle = rect[-1]
        if angle < -45:
            angle = -(90 + angle)
        else:
            angle = -angle
    (h, w) = sharpened.shape[:2]
    center = (w // 2, h // 2)
    matrix = cv2.getRotationMatrix2D(center, angle, 1.0)
    deskewed = cv2.warpAffine(sharpened, matrix, (w, h), flags=cv2.INTER_CUBIC, borderMode=cv2.BORDER_REPLICATE)

    temp_file = Path(tempfile.mkstemp(prefix="blueprint_", suffix=".png")[1])
    cv2.imwrite(str(temp_file), deskewed)
    return str(temp_file)


def ocr_image(image_path: str) -> str:
    """
    Runs Tesseract OCR on the provided image and returns the extracted text.
    """
    img = Image.open(image_path)
    return pytesseract.image_to_string(img)

