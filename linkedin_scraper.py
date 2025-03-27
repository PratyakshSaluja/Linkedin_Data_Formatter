"""
LinkedIn Data Processing Module
------------------------------

This module serves as the core processor for LinkedIn profile data. It handles the retrieval,
analysis, and organization of LinkedIn profile information from either individual URLs or
a CSV file containing multiple URLs.

The module integrates with various detector modules to analyze profile data for:
- Fortune 500 company employment
- Entrepreneurial signals
- Leadership roles

This information is structured and then exported to an Excel file with three sheets:
Profiles, Educations, and Experiences.

 
"""
import pandas as pd
import os
from io import StringIO
from fortune500 import is_fortune_500
from entrepreneur_detector import is_entrepreneur
from leadership_detector import is_leadership_role
from linkedin_data_fetcher import fetch_linkedin_data
from excel_writer import write_to_excel
from db_utils import DatabaseManager  # Add this import

def convert_numpy_types(data):
    """
    Convert numpy data types to Python native types.
    
    Parameters:
    -----------
    data : dict or list
        Data structure that may contain numpy types
        
    Returns:
    --------
    dict or list
        Data structure with numpy types converted to native Python types
    """
    if isinstance(data, dict):
        return {k: convert_numpy_types(v) for k, v in data.items()}
    elif isinstance(data, list):
        return [convert_numpy_types(v) for v in data]
    elif str(type(data)).startswith("<class 'numpy"):  # Check if numpy type
        return data.item()  # Convert to native Python type
    return data

from process_logger import process_logger

def process_linkedin_data(file_content=None, linkedin_url=None, output_file="SampleData.xlsx"):
    """
    Process LinkedIn data from either a CSV file or a single URL.
    
    This function handles the core workflow of the LinkedIn data scraping process:
    1. Parses input (file or URL)
    2. Fetches LinkedIn profile data using Proxycurl API
    3. Analyzes profiles for Fortune 500, leadership, and entrepreneurship signals
    4. Organizes data into structured lists for profiles, educations, and experiences
    5. Exports data to Excel file
    
    Parameters:
    -----------
    file_content : bytes, optional
        CSV file content containing LinkedIn URLs in first column
    linkedin_url : str, optional
        Single LinkedIn profile URL to process
    output_file : str, optional
        Path to the output Excel file
        
    Returns:
    --------
    str or None
        Path to the generated Excel file if successful, None if processing failed
        
    Notes:
    ------
    - Maintains profile IDs to ensure data integrity when appending to existing files
    - Detects if output file exists and appends to it rather than overwriting
    """
    if not file_content and not linkedin_url:
        print("No content or URL provided to process")
        return None

    if file_content:
        try:
            df = pd.read_csv(StringIO(file_content.decode('utf-8')))
            urls = df.iloc[:, 0].tolist()
            print(f"Processing {len(urls)} URLs from CSV file")
        except Exception as e:
            print(f"Error processing CSV file: {e}")
            return None
    else:
        urls = [linkedin_url]
        print(f"Processing single URL: {linkedin_url}")

    profiles = []
    educations = []
    experiences = []
    club_experiences = []

    # Track profile IDs to ensure uniqueness when appending to existing data
    max_profile_id = 0
    if os.path.exists(output_file):
        try:
            existing_profiles = pd.read_excel(output_file, sheet_name='Profiles')
            if not existing_profiles.empty and "profile_id" in existing_profiles.columns:
                max_profile_id = existing_profiles["profile_id"].max()
                process_logger.log_db_operation('read', 'profiles', 'success', f"Found existing data with max profile ID: {max_profile_id}")
            else:
                process_logger.log_db_operation('read', 'profiles', 'warning', "Found existing file but no profile IDs")
        except Exception as e:
            process_logger.log_db_operation('read', 'profiles', 'failed', f"Error reading profile IDs: {str(e)}")
            # Start from 0 if there's an error
    else:
        print(f"No existing file found at {output_file}, will create new file")

    try:
        # Initialize database connection
        db = DatabaseManager()
        db.create_tables()  # Ensure tables exist
    except Exception as e:
        process_logger.log_db_operation('initialize', 'all', 'failed', str(e))
        return None

    # Process each LinkedIn URL
    processed_urls = 0
    for url in urls:
        process_logger.log_profile_processing(url, None, None, "Started fetching data")
        data = fetch_linkedin_data(url)
        if data:
            max_profile_id += 1
            processed_urls += 1
            process_logger.log_profile_processing(url, None, None, f"Data fetched successfully, assigned ID: {max_profile_id}")
            
            # Create profile dictionary with native Python types
            profile_dict = convert_numpy_types({
                "profile_url": f"https://www.linkedin.com/in/{data.get('public_identifier', '')}/",
                "profile_pic_url": data.get("profile_pic_url", ""),  # Add this line
                "full_name": data["full_name"],
                "headline": data["headline"],
                "summary": data["summary"],
                "country": data["country"],
                "city": data["city"],
                "email": data["email"],
                "contact_number": data["contact_number"],
                "github": data["github"],
                "twitter": data["twitter"],
                "facebook": data["facebook"],
                "skills": data["skills"],
                "profile_id": max_profile_id,
                "connections": data["connections"],
                "languages": ", ".join(data.get("languages", [])),
                "follower_count": data.get("follower_count", 0),
                "industry": data.get("industry", ""),
                "certifications": ", ".join([cert.get("name", "") for cert in data.get("certifications", [])]),
                "fortune_500": 1 if is_fortune_500(data) else 0,  # Boolean flags stored as integers
                "entrepreneur": 1 if is_entrepreneur(data) else 0,
                "leadership_role": 1 if is_leadership_role(data) else 0
            })
            profiles.append(profile_dict)
            process_logger.log_profile_processing(url, None, None, f"Profile data processed for {data['full_name']}")

            # Associate educations with profile ID - convert numpy types
            for edu in data["educations"]:
                edu = convert_numpy_types(edu)
                edu["profile_id"] = max_profile_id
                educations.append(edu)
            process_logger.log_profile_processing(url, None, None, f"Added {len(data['educations'])} education records")

            # Process experiences - convert numpy types
            for exp in data["experiences"]:
                exp = convert_numpy_types(exp)
                exp["profile_id"] = max_profile_id
                
                # Check if experience is club-related
                company_lower = exp.get("company", "").lower()
                title_lower = exp.get("title", "").lower()
                club_keywords = ["club", "society", "association", "committee", "chapter"]
                
                is_club = any(keyword in company_lower or keyword in title_lower for keyword in club_keywords)
                
                if is_club:
                    club_exp = convert_numpy_types({
                        "profile_id": max_profile_id,
                        "club_name": exp.get("company", ""),
                        "role": exp.get("title", ""),
                        "description": exp.get("description", ""),
                        "start_date": exp.get("start_date", ""),
                        "end_date": exp.get("end_date", ""),
                        "location": exp.get("location", "")
                    })
                    club_experiences.append(club_exp)
                else:
                    experiences.append(exp)
            process_logger.log_profile_processing(url, None, None, f"Added {len(data['experiences'])} experience records")
        else:
            process_logger.log_profile_processing(url, False, False, "Failed to fetch data")

    # Write data to database before Excel
    if profiles:
        try:
            process_logger.log_db_operation('save', 'all', 'started', f"Saving {len(profiles)} profiles")
            db.insert_data(
                profiles_data=profiles,
                educations_data=educations,
                experiences_data=experiences,
                club_experiences_data=club_experiences
            )
            process_logger.log_db_operation('save', 'all', 'success', f"Saved {len(profiles)} profiles")
            # Update processing status for all profiles
            for profile in profiles:
                process_logger.log_profile_processing(
                    profile.get('profile_url', ''),
                    True,  # Will be written to Excel next
                    True   # Successfully saved to DB
                )
        except Exception as e:
            process_logger.log_db_operation('save', 'all', 'failed', str(e))
            # Update processing status for all profiles
            for profile in profiles:
                process_logger.log_profile_processing(
                    profile.get('profile_url', ''),
                    True,  # Will be written to Excel next
                    False  # Failed to save to DB
                )

        # Continue with Excel writing
        process_logger.log_excel_operation('write', output_file, 'started', 
            f"Writing {len(profiles)} profiles, {len(educations)} educations, {len(experiences)} experiences")
        return write_to_excel(profiles, educations, experiences, club_experiences, output_file)
    else:
        process_logger.log_profile_processing('all', False, False, "No profiles were processed successfully")
        return None
