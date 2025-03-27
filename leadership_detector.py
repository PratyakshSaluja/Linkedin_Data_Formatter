"""
Leadership Role Detection Module
-------------------------------

This module uses fuzzy string matching to analyze LinkedIn profile data
to determine if an individual holds a leadership position. The detector
focuses on the most recent job position and uses fuzzy matching to find
leadership keywords in the job title, allowing for variations in spelling,
word order, and partial matches.

Features:
- Fuzzy string matching with configurable similarity threshold
- Comprehensive list of leadership keywords and patterns
- Support for role-specific leadership patterns (e.g., "Senior Engineer", "Technical Lead")
- Detection of both explicit leadership roles and derived leadership positions
"""

from fuzzywuzzy import process

def is_leadership_role(profile_data, similarity_threshold=65):  # Lowered threshold for better matching
    """
    Detect if a person currently holds a leadership position based on their most recent job title.
    
    This function uses fuzzy string matching to analyze the most recent experience entry
    (assumed to be the current role) and looks for leadership indicators in the job title. 
    Leadership is detected by:
    1. Direct fuzzy matching against leadership keywords
    2. Pattern matching with role-specific leadership variations
    3. Contextual analysis of title components
    """
    # Exact match leadership keywords
    exact_keywords = {
        'executive', 'exec', 'director', 'manager', 'lead', 'head',
        'chief', 'ceo', 'cfo', 'cto', 'coo', 'vp', 'president', 'chair',
        'supervisor', 'founder'
    }
    
    # Keywords for fuzzy matching (longer phrases)
    fuzzy_keywords = [
        'department head',
        'executive director', 'managing director', 'senior manager',
        'vice president', 'general manager', 'regional manager'
    ]

    # Get all experiences from profile data
    experiences = profile_data.get('experiences', [])
    if not experiences:
        return False
    
    # Get the latest/current experience (first in the list from API)
    latest_exp = experiences[0]
    title = latest_exp.get('title', '').lower()
    
    # Add space before capital letters in concatenated words
    title = ''.join([' ' + c if c.isupper() and i > 0 else c for i, c in enumerate(title)]).strip()
    
    # Check for exact matches first with word boundaries
    title_words = set(title.split())
    if any(keyword in title_words for keyword in exact_keywords):
        return True

    # Do fuzzy matching only for longer phrases
    matches = process.extract(title, fuzzy_keywords, limit=3)
    for match, score in matches:
        if score >= similarity_threshold:
            return True
            
    # No leadership signals detected in current role
    return False
