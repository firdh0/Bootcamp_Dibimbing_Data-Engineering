from __future__ import annotations

import re
import logging

log = logging.getLogger(__name__)

def count_sentences(text: str) -> int:
    """Counts the number of sentences in a given text."""
    if not text or not isinstance(text, str):
        return 0
    # Sentences are typically ended by '.', '!', or '?'
    # This regex handles multiple delimiters and ensures non-empty sentences are counted.
    sentences = re.split(r'[.!?]+', text)
    return len([s for s in sentences if s.strip()])

def count_words(text: str) -> int:
    """Counts the number of words in a given text."""
    if not text or not isinstance(text, str):
        return 0
    # \b matches word boundaries, \w+ matches one or more word characters
    words = re.findall(r'\b\w+\b', text.lower())
    return len(words)

def count_syllables_in_word(word: str) -> int:
    """Estimates the number of syllables in a single word (basic English-centric heuristic)."""
    if not word or not isinstance(word, str):
        return 0
    
    word = word.lower()
    if len(word) == 0:
        return 0
        
    # Simple vowel group counting, can be improved for accuracy esp. for Indonesian
    syllable_count = len(re.findall(r'[aeiouy]+', word))
    
    # Avoid counting 0 for words like "rhythm"
    if syllable_count == 0 and len(word) > 0:
        return 1
        
    return max(1, syllable_count) # Ensure at least one syllable for any word

def count_total_syllables(text: str) -> int:
    """Counts the total number of syllables in a given text."""
    if not text or not isinstance(text, str):
        return 0
    words = re.findall(r'\b\w+\b', text.lower())
    if not words:
        return 0
    return sum(count_syllables_in_word(w) for w in words)

def flesch_reading_ease(text: str) -> float:
    """
    Calculates the Flesch Reading Ease score for a given text.
    Score is capped between 0 and 100.
    """
    if not text or not isinstance(text, str) or text.strip() == "":
        log.warning("Flesch Reading Ease: Input text is empty or invalid. Returning 0.0.")
        return 0.0

    num_sentences = count_sentences(text)
    num_words = count_words(text)
    num_syllables = count_total_syllables(text)

    log.debug(f"Flesch calculation: Sentences={num_sentences}, Words={num_words}, Syllables={num_syllables}")

    if num_sentences == 0 or num_words == 0:
        log.warning("Flesch Reading Ease: Text has 0 sentences or 0 words. Score set to 0.0.")
        return 0.0 

    average_sentence_length = num_words / num_sentences
    average_syllables_per_word = num_syllables / num_words

    # Original Flesch Reading Ease formula (typically for English)
    # FRE = 206.835 - (1.015 * ASL) - (84.6 * ASW)
    score = 206.835 - (1.015 * average_sentence_length) - (84.6 * average_syllables_per_word)
    
    # Cap the score between 0 and 100
    capped_score = max(0.0, min(100.0, score))
    
    log.info(f"Calculated Flesch Reading Ease score: {score:.2f}, Capped score: {capped_score:.2f}")
    return round(capped_score, 2)

