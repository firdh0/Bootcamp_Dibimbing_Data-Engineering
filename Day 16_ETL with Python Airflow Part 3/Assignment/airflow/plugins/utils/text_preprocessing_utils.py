from __future__ import annotations

import logging
import pandas as pd # For pd.isna check
import re
import string

log = logging.getLogger(__name__)

def case_folding_text(text: str) -> str:
    if pd.isna(text) or text is None:
        return ""
    return str(text).lower()

def cleaning_text(text: str) -> str:
    """Cleans text by removing URLs, mentions, hashtags, special characters, and extra spaces."""
    if pd.isna(text) or text is None:
        return ""
    text = str(text) 
    text = re.sub(r'(tempo\.co\s*,?\s*jakarta\s*-?|kompas\.com\s*,?\s*-?|cnnindonesia\.com\s*,?\s*-?)', '', text, flags=re.IGNORECASE)
    text = re.sub(r'@\w+', '', text) 
    text = re.sub(r'#\w+', '', text) 
    text = re.sub(r"https?://\S+|www\.\S+", '', text)
    text = text.replace('/', ' ') 
    text = re.sub(r'\.+', ' ', text) 
    text = re.sub(r'[^a-zA-Z0-9\s]', '', text) 
    text = text.replace('\n', ' ')
    text = text.translate(str.maketrans('', '', string.punctuation)) 
    text = text.strip() 
    text = re.sub(r'\s+', ' ', text) 
    return text