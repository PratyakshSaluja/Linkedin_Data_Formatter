"""
Fortune 500 Company Detection Module
----------------------------------

This module determines whether a LinkedIn profile indicates employment at a Fortune 500 company.
It loads company names from a JSON file and performs both exact and fuzzy matching against
the person's current company to detect Fortune 500 employment status.

The module implements:
1. Loading Fortune 500 company names from JSON
2. Company name normalization for better matching
3. Both exact and fuzzy matching algorithms

 
"""
import json
import os
from fuzzywuzzy import fuzz

def load_fortune500_companies():
    """
    Load Fortune 500 company names from JSON file.
    
    This function loads the Fortune 500 company names from a JSON file in the same
    directory and returns them as a set of lowercase names for efficient matching.
    
    Returns:
    --------
    set
        Set of lowercase company names for case-insensitive matching
        
    Notes:
    ------
    - Uses the local 'fortune500_data.json' file
    - Converts all company names to lowercase for consistent matching
    """
    current_dir = os.path.dirname(os.path.abspath(__file__))
    json_path = os.path.join(current_dir, 'fortune500_data.json')
    
    with open(json_path, 'r') as f:
        data = json.load(f)
    return {company.lower() for company in data['companies']}

# Company name variations mapping
COMPANY_VARIATIONS = {
    # Alphabet companies
    'google': 'alphabet',          # Main company
    'google inc': 'alphabet',      # Old legal name
    'google llc': 'alphabet',      # Current legal name
    'youtube': 'alphabet',         # Subsidiary
    'waymo': 'alphabet',           # Subsidiary
    'deepmind': 'alphabet',        # Subsidiary
    'calico': 'alphabet',          # Subsidiary
    'verily': 'alphabet',          # Subsidiary
    
    # Meta companies
    'meta': 'meta platforms',      # Current name
    'facebook': 'meta platforms',  # Old name
    'instagram': 'meta platforms', # Subsidiary
    'whatsapp': 'meta platforms',  # Subsidiary
    'oculus': 'meta platforms',    # Subsidiary
    
    # Common variations of other companies
    'microsoft corporation': 'microsoft',
    'apple inc': 'apple',
    'amazon.com': 'amazon'
}

def normalize_company_name(name):
    """
    Normalize company name for comparison.
    
    This function standardizes company names by:
    1. Converting to lowercase
    2. Stripping whitespace
    3. Removing common suffixes (Inc, Corp, LLC, etc.)
    4. Mapping common variations to parent company names
    
    Parameters:
    -----------
    name : str or None
        Company name to normalize
        
    Returns:
    --------
    str
        Normalized company name for more accurate matching
        
    Notes:
    ------
    - Handles None values by returning an empty string
    - Removes common suffixes to improve matching across variations
    """
    if not name:
        return ""
    if not name:
        return ""
    name = name.lower().strip()
    
    # First check for company variations
    # Try exact match first
    if name in COMPANY_VARIATIONS:
        return COMPANY_VARIATIONS[name]
    # Then try first word match
    name_simple = name.split()[0]
    if name_simple in COMPANY_VARIATIONS:
        return COMPANY_VARIATIONS[name_simple]
    
    suffixes = [
        ' inc', ' corp', ' corporation', ' ltd', ' llc', 
        ' company', ' co', '.com', ' group', ' plc',
        ' holdings', ' holding', ' technologies', ' technology'
    ]
    for suffix in suffixes:
        name = name.replace(suffix, '')
    return name.strip()

def is_fortune_500(profile_data, debug=False):
    """
    Check if a person currently works at a Fortune 500 company.
    
    This function examines the person's most recent work experience (assumed to be
    the current position) and determines if the company is in the Fortune 500 list.
    It uses both direct matching and fuzzy string matching for accurate detection.
    
    Parameters:
    -----------
    profile_data : dict
        Complete LinkedIn profile data dictionary with experiences list
        
    Returns:
    --------
    bool
        True if the person's current company is in the Fortune 500 list, False otherwise
        
    Notes:
    ------
    - Only examines the most recent/current job position
    - Uses lazy loading and caching of Fortune 500 company data
    - Employs both direct matching and fuzzy matching (with 85% threshold)
    """
    # Get all experiences from profile data
    experiences = profile_data.get('experiences', [])
    if not experiences:
        return False
        
    # Get the latest/current experience (first in the list from API)
    latest_exp = experiences[0]
    company_name = latest_exp.get('company', '')
    
    if not company_name:
        return False
        
    try:
        # Load companies only once and cache them using function attribute
        if not hasattr(is_fortune_500, 'companies'):
            is_fortune_500.companies = load_fortune500_companies()
            
        # Normalize company name for better matching
        normalized_name = normalize_company_name(company_name)
        
        # Direct match against normalized company names
        if normalized_name in is_fortune_500.companies:
            if debug: print(f"Direct match found for: {normalized_name}")
            return True
            
        # Fuzzy matching for handling variations in company names
        for fortune_company in is_fortune_500.companies:
            # Check if one name contains the other or if they have high similarity
            if (normalized_name in fortune_company or 
                fortune_company in normalized_name or 
                fuzz.ratio(normalized_name, fortune_company) > 85):
                if debug: print(f"Fuzzy match: {normalized_name} -> {fortune_company}")
                return True
                
        return False
        
    except Exception as e:
        print(f"Error checking Fortune 500 status: {e}")
        return False
