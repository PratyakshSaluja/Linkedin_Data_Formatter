"""
LinkedIn Data Fetcher Module
---------------------------

This module handles communication with the Proxycurl API to fetch LinkedIn profile data.
It formats and structures the raw API responses into a standardized format that can be
used by the rest of the application.
"""
import requests
import os
import re
import pandas as pd
from dotenv import load_dotenv
from fortune500 import is_fortune_500
from entrepreneur_detector import is_entrepreneur
from leadership_detector import is_leadership_role
from process_logger import process_logger
from db_utils import DatabaseManager  # Add this import

# Load environment variables
load_dotenv()

# API authentication token for Proxycurl service
PROXYCURL_API_KEY = os.getenv("PROXYCURL_API_KEY")

def format_date(date_dict):
    """Format date dictionary from Proxycurl API into a consistent string format."""
    if not date_dict:
        return "Present"
    month = str(date_dict.get('month', '')).zfill(2)
    year = str(date_dict.get('year', ''))
    return f"{month}/{year}" if month and year else "N/A"

def extract_profile_url_from_search(search_url):
    """Try to extract the actual profile URL from a LinkedIn search URL."""
    if "/in/" in search_url:
        return search_url
    
    name_match = re.search(r'keywords=([^&]+)', search_url)
    extracted_name = name_match.group(1).replace('%20', ' ') if name_match else "Unknown"
    
    print(f"Warning: Cannot process search URL for '{extracted_name}'. Please provide direct profile URL.")
    return None

def process_excel_file(file_path):
    """Process an Excel file containing LinkedIn profile information."""
    try:
        # Read the Excel file
        df = pd.read_excel(file_path)
        
        # Check if required columns exist
        if 'Linkedin Profile' not in df.columns or 'Full Name' not in df.columns:
            print("Error: Excel file must contain 'Linkedin Profile' and 'Full Name' columns")
            return []
        
        # Create a list of profile data
        profiles = []
        
        for _, row in df.iterrows():
            # Skip rows with missing URLs
            if pd.isna(row['Linkedin Profile']) or not row['Linkedin Profile'].strip():
                print(f"Warning: Missing LinkedIn URL for {row.get('Full Name', 'Unknown')}")
                continue
                
            # Extract profile URL from search URL if needed
            url = extract_profile_url_from_search(row['Linkedin Profile'])
            
            # Skip if URL extraction failed
            if not url:
                continue
                
            # Create profile metadata
            profile_data = {
                'url': url,
                'name': row.get('Full Name', ''),
                'batch': row.get('Batch', ''),
                'program': row.get('Programme', ''),
                'gender': row.get('Gender', ''),
                'graduation_year': row.get('Passing Year', ''),
                'admission_year': row.get('Admission Year Year', '')
            }
            
            profiles.append(profile_data)
            
        return profiles
        
    except Exception as e:
        print(f"Error processing Excel file: {str(e)}")
        return []

def get_existing_profiles(output_file="SampleData.xlsx"):
    """Get set of already processed LinkedIn profile URLs."""
    try:
        if os.path.exists(output_file):
            df = pd.read_excel(output_file, sheet_name='Profiles')
            return set(df['profile_url'].dropna().values)
    except Exception as e:
        print(f"Error reading existing profiles: {e}")
    return set()

def fetch_linkedin_data(linkedin_url):
    """Fetch LinkedIn profile data from Proxycurl API and structure it."""
    profile_url = extract_profile_url_from_search(linkedin_url)
    if not profile_url:
        return None
        
    api_endpoint = 'https://nubela.co/proxycurl/api/v2/linkedin'
    headers = {'Authorization': f'Bearer {PROXYCURL_API_KEY}'}

    params = {
        'url': profile_url,
        'fallback_to_cache': 'on-error',
        'use_cache': 'if-present',
        'skills': 'include',
        'inferred_salary': 'include',
        'personal_email': 'include',
        'personal_contact_number': 'include',
        'twitter_profile_id': 'include',
        'facebook_profile_id': 'include',
        'github_profile_id': 'include',
        'certifications': 'include'
    }

    try:
        response = requests.get(
            url=api_endpoint,
            params=params,
            headers=headers,
            timeout=30
        )
        response.raise_for_status()
        data = response.json()
        
        print(f"Successfully fetched data for: {profile_url}")
        
        # Add fallback/default values for required fields
        result = {
            'full_name': data.get('full_name', 'Unknown Name'),
            'public_identifier': data.get('public_identifier', ''),
            'profile_pic_url': data.get('profile_pic_url', ''),
            'headline': data.get('headline', ''),
            'summary': data.get('summary', ''),
            'country': data.get('country_full_name', ''),
            'city': data.get('city', ''),
            'email': data.get('personal_email', ''),
            'contact_number': data.get('personal_contact_number', ''),
            'github': data.get('github_profile_id', ''),
            'twitter': data.get('twitter_profile_id', ''),
            'facebook': data.get('facebook_profile_id', ''),
            'skills': ", ".join(data.get('skills', [])),
            'connections': data.get('connections', 0),
            'languages': data.get('languages', []),
            'follower_count': data.get('follower_count', 0),
            'industry': data.get('industry', ''),
            'certifications': data.get('certifications', []),
            'educations': [],
            'experiences': []
        }
        
        # Process education data
        for edu in data.get('education', []):
            education = {
                'institution_name': edu.get('school', ''),
                'degree': edu.get('degree_name', ''),
                'field_of_study': edu.get('field_of_study', ''),
                'start_date': format_date(edu.get('starts_at')),
                'end_date': format_date(edu.get('ends_at'))
            }
            result['educations'].append(education)
        
        # Process experience data
        for exp in data.get('experiences', []):
            # Ensure all fields are strings with empty string fallback
            experience = {
                'title': str(exp.get('title') or ''),
                'company': str(exp.get('company') or ''),
                'location': str(exp.get('location') or ''),
                'description': str(exp.get('description') or ''),
                'start_date': format_date(exp.get('starts_at')),
                'end_date': format_date(exp.get('ends_at'))
            }
            result['experiences'].append(experience)
            
        return result

    except requests.exceptions.RequestException as err:
        print(f"An error occurred while fetching LinkedIn data: {err}")
        return None
    except Exception as e:
        print(f"Unexpected error processing LinkedIn data: {e}")
        return None

def batch_process_excel(file_path, limit=None, output_file="SampleData.xlsx", append_mode=True):
    """Process multiple LinkedIn profiles from an Excel file."""
    profiles_list = process_excel_file(file_path)
    
    if not profiles_list:
        print("No valid LinkedIn profiles found in Excel file")
        return [], [], [], []
    
    # Get already processed profiles
    existing_profiles = get_existing_profiles(output_file)
    
    # Filter out already processed profiles
    new_profiles = [p for p in profiles_list if p['url'] not in existing_profiles]
    
    if not new_profiles:
        print("All profiles have already been processed")
        return [], [], [], []
    
    print(f"Found {len(new_profiles)} new profiles to process")
    
    # Apply limit if specified
    if limit and limit > 0:
        new_profiles = new_profiles[:limit]
        print(f"Processing first {limit} new profiles")
        
    try:
        # Initialize database connection
        db = DatabaseManager()
    except Exception as e:
        print(f"Database connection failed: {e}")
        return [], [], [], []

    # Get the current max profile ID from database
    max_profile_id = db.get_max_profile_id()

    profiles_data = []
    educations_data = []
    experiences_data = []
    club_experiences_data = []
    certifications_data = []

    # Process each profile one at a time to maintain consistency
    for profile_meta in new_profiles:
        print(f"Processing {profile_meta['name']} ({profile_meta['url']})")
        
        # First check if profile exists in DB
        existing_id = db.profile_exists(profile_meta['url'])
        if existing_id:
            print(f"Profile {profile_meta['url']} already exists with ID {existing_id}, skipping...")
            continue
            
        profile_data = fetch_linkedin_data(profile_meta['url'])
        if not profile_data:
            continue

        # Increment max ID for new profile
        max_profile_id += 1
        current_profile_id = max_profile_id
        
        # Prepare profile data
        profile_data['profile_id'] = current_profile_id
        profile_data['profile_url'] = profile_meta['url']
        
        # Add metadata from Excel
        profile_data['admission_year'] = profile_meta.get('admission_year', '')
        profile_data['graduation_year'] = profile_meta.get('graduation_year', '')
        profile_data['program'] = profile_meta.get('program', '')
        profile_data['gender'] = profile_meta.get('gender', '')
        profile_data['batch'] = profile_meta.get('batch', '')
        
        # Add analysis flags
        profile_data['leadership_role'] = is_leadership_role(profile_data)
        profile_data['fortune_500'] = is_fortune_500(profile_data)
        profile_data['entrepreneur'] = is_entrepreneur(profile_data)
        
        # Prepare related data with correct profile ID
        current_educations = []
        current_experiences = []
        current_club_experiences = []
        
        # Process educations
        for education in profile_data.get('educations', []):
            education['profile_id'] = current_profile_id
            current_educations.append(education)
        
        # Process experiences
        for experience in profile_data.get('experiences', []):
            experience['profile_id'] = current_profile_id
            
            # Get company and title, ensuring they are strings
            company = str(experience.get('company') or '')
            title = str(experience.get('title') or '')
            
            # Split between regular and club experiences
            company_lower = company.lower()
            title_lower = title.lower()
            club_keywords = ['club', 'society', 'association', 'committee', 'chapter']
            
            is_club = any(keyword in company_lower or keyword in title_lower for keyword in club_keywords)
            
            if is_club:
                club_exp = {
                    'profile_id': current_profile_id,
                    'club_name': company,
                    'role': title,
                    'description': str(experience.get('description') or ''),
                    'start_date': str(experience.get('start_date') or ''),
                    'end_date': str(experience.get('end_date') or ''),
                    'location': str(experience.get('location') or ''),
                    'position': ''
                }
                current_club_experiences.append(club_exp)
            else:
                current_experiences.append(experience)
        # Process certifications
        current_certifications = []
        for certification in profile_data.get('certifications', []):
            cert_data = {
                'profile_id': current_profile_id,
                'name': certification.get('name', ''),
                'issuing_organization': certification.get('issuing_organization', ''),
                'issue_date': format_date(certification.get('issue_date')),
                'expiration_date': format_date(certification.get('expiration_date')),
                'credential_id': certification.get('credential_id', ''),
                'credential_url': certification.get('credential_url', '')
            }
            current_certifications.append(cert_data)
        # Save this profile to database immediately
        if db.insert_single_profile(profile_data, current_educations, current_experiences, current_club_experiences,current_certifications):
            profiles_data.append(profile_data)
            educations_data.extend(current_educations)
            experiences_data.extend(current_experiences)
            club_experiences_data.extend(current_club_experiences)
            certifications_data.extend(current_certifications)
            print(f"Successfully processed and saved profile: {profile_meta['url']}")
        else:
            print(f"Failed to save profile to database: {profile_meta['url']}")
    
    return profiles_data, educations_data, experiences_data, club_experiences_data, certifications_data
