"""
Hub for all regex strings, which are compiled up at the application start time.
"""
import re

# Find variations of 'Re:' and 'Fwd:' in message subject header.
# https://stackoverflow.com/a/11640925
SUBJECT_PREFIX_STR = (
    r"([\[\(] *)?(RE?S?|FYI|RIF|I|FS|VB|RV|ENC|ODP|PD|YNT|ILT|SV|VS|VL|AW|WG|ΑΠ|ΣΧΕΤ|ΠΡΘ|תגובה|הועבר|主题|转"
    r"发|FWD?) *([-:;)\]][ :;\])-]*|$)|\]+ *$"
)
SUBJECT_PREFIX = re.compile(SUBJECT_PREFIX_STR, re.IGNORECASE)

# Identify separator lines in text suggesting a inline message
INLINE_MESSAGE_SEPARATOR = re.compile(r"[-_*=]{4,}.*")

# Identify email headers in text, suggesting a inline message
SUSPICIOUS_INLINE = re.compile(r"(To:|Subject:|From:|Cc:|Bcc?)", re.IGNORECASE)
