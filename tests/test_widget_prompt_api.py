import re
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
LOADER_JS = ROOT / "public" / "widget-loader.js"
CHAT_WIDGET_HTML = ROOT / "public" / "chat-widget.html"


class WidgetPromptApiTests(unittest.TestCase):
    def test_loader_exposes_send_prompt_command(self):
        loader = LOADER_JS.read_text(encoding="utf-8")

        self.assertIn("sendPrompt", loader)
        self.assertIn("SEND_PROMPT", loader)
        self.assertRegex(
            loader,
            re.compile(
                r"sendPrompt:\s*function\s*\(\s*prompt\s*\).*?"
                r"postWidgetCommand\s*\(\s*['\"]SEND_PROMPT['\"]\s*,\s*\{\s*prompt",
                re.DOTALL,
            ),
        )

    def test_iframe_handles_send_prompt_by_waiting_opening_and_sending(self):
        widget = CHAT_WIDGET_HTML.read_text(encoding="utf-8")

        self.assertIn("SEND_PROMPT", widget)
        self.assertRegex(
            widget,
            re.compile(
                r"if\s*\(\s*data\.command\s*===\s*['\"]SEND_PROMPT['\"]\s*\).*?"
                r"sendExternalPrompt\s*\(\s*data\.prompt\s*\)",
                re.DOTALL,
            ),
        )
        self.assertRegex(
            widget,
            re.compile(
                r"async\s+function\s+sendExternalPrompt\s*\(\s*prompt\s*\).*?"
                r"await\s+waitForWidgetReady\s*\(\s*\).*?"
                r"openWidget\s*\(\s*\).*?"
                r"await\s+sendMessage\s*\(\s*promptText\s*\)",
                re.DOTALL,
            ),
        )


if __name__ == "__main__":
    unittest.main()
