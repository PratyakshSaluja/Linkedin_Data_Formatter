"""
Entrepreneur Detection Module
---------------------------

This module analyzes LinkedIn profile data to determine if an individual 
has entrepreneurial characteristics. It looks for entrepreneurship signals in:
1. Latest job title
2. Company name relationships to personal name

 
"""

def is_entrepreneur(profile_data):
    """
    Detect if a person is likely an entrepreneur based on their LinkedIn profile.
    
    The function uses multiple signals to identify entrepreneurial characteristics:
    1. Keywords in the latest job title from work experience
    2. Person's name appearing in company names (suggesting eponymous businesses)
    
    Parameters:
    -----------
    profile_data : dict
        Complete LinkedIn profile data dictionary with experiences and full_name
        
    Returns:
    --------
    bool
        True if entrepreneurial signals are detected, False otherwise
        
    Notes:
    ------
    - Case-insensitive matching is used for all comparisons
    - Only checks most recent experience for entrepreneurial signals
    """
    # Keywords that suggest entrepreneurial activities
    entrepreneur_keywords = {
        'founder', 'co-founder', 'cofounder', 'owner',
        'entrepreneur', 'startup', 'start-up', 'serial entrepreneur',
        'business owner'
    }
    
    experiences = profile_data.get('experiences', [])
    if not experiences:
        return False
        
    # Check only the latest experience (first in the list) for entrepreneurial job titles
    latest_exp = experiences[0]
    title = latest_exp.get('title', '').lower()
    if any(keyword in title for keyword in entrepreneur_keywords):
        return True
            
    # Check if person's name appears in company name (suggesting they founded it)
    company = latest_exp.get('company', '').lower()
    full_name = profile_data.get('full_name', '').lower()
    if full_name and full_name in company:
        return True
    
    # No entrepreneurial signals detected
    return False
