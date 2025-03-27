"""
Excel Writer Module
-----------------

This module handles writing processed LinkedIn profile data to Excel format.
It organizes data into three distinct sheets within an Excel file.

The module supports both creating new Excel files and appending to existing ones,
with logging of all operations.
It organizes data into three distinct sheets within an Excel file:
1. Profiles - Contains personal/professional information and analysis flags
2. Educations - Contains educational history linked to profile IDs
3. Experiences - Contains work experience history linked to profile IDs

The module supports both creating new Excel files and appending to existing ones.

 
"""
import pandas as pd
import os
from process_logger import process_logger

def write_to_excel(profiles, educations, experiences, club_experiences, certifications, output_file="SampleData.xlsx", append_mode=True):
    """Write LinkedIn data to Excel file."""
    if not os.path.isabs(output_file):
        current_dir = os.path.dirname(os.path.abspath(__file__))
        output_file = os.path.join(current_dir, output_file)
        
    try:
        # Convert data to DataFrames
        new_profiles_df = pd.DataFrame(profiles)
        new_educations_df = pd.DataFrame(educations)
        new_experiences_df = pd.DataFrame(experiences)
        new_club_experiences_df = pd.DataFrame(club_experiences)
        new_certifications_df = pd.DataFrame(certifications)
        
        # If file exists and append_mode is True, read and append
        if os.path.exists(output_file) and append_mode:
            with pd.ExcelFile(output_file) as xls:
                existing_profiles = pd.read_excel(xls, 'Profiles')
                existing_educations = pd.read_excel(xls, 'Educations')
                existing_experiences = pd.read_excel(xls, 'Experiences')
                try:
                    existing_club_experiences = pd.read_excel(xls, 'Club Experiences')
                except:
                    existing_club_experiences = pd.DataFrame()
                try:
                    existing_certifications = pd.read_excel(xls, 'Certifications')
                except:
                    existing_certifications = pd.DataFrame()
                    
            # Remove duplicates based on profile_url before concatenating
            if not new_profiles_df.empty and 'profile_url' in new_profiles_df.columns:
                existing_profiles = existing_profiles[
                    ~existing_profiles['profile_url'].isin(new_profiles_df['profile_url'])
                ]
            
            # Concatenate with existing data
            df_profiles = pd.concat([existing_profiles, new_profiles_df], ignore_index=True)
            df_educations = pd.concat([existing_educations, new_educations_df], ignore_index=True)
            df_experiences = pd.concat([existing_experiences, new_experiences_df], ignore_index=True)
            df_club_experiences = pd.concat([existing_club_experiences, new_club_experiences_df], ignore_index=True)
            df_certifications = pd.concat([existing_certifications, new_certifications_df], ignore_index=True)
        else:
            df_profiles = new_profiles_df
            df_educations = new_educations_df
            df_experiences = new_experiences_df
            df_club_experiences = new_club_experiences_df
            df_certifications = new_certifications_df
            
        # Write to Excel
        with pd.ExcelWriter(output_file, engine='xlsxwriter') as writer:
            df_profiles.to_excel(writer, sheet_name='Profiles', index=False)
            df_educations.to_excel(writer, sheet_name='Educations', index=False)
            df_experiences.to_excel(writer, sheet_name='Experiences', index=False)
            df_club_experiences.to_excel(writer, sheet_name='Club Experiences', index=False)
            df_certifications.to_excel(writer, sheet_name='Certifications', index=False)
            
        return output_file
    except Exception as e:
        process_logger.log_excel_operation('write', output_file, 'failed', str(e))
        return None
