"""
Excel Processor Module
-------------------

This module serves as a simple command-line interface for processing 
Excel files containing LinkedIn profile URLs.

Usage:
python process_excel.py path/to/excel_file.xlsx

The script will:
1. Parse the Excel file for LinkedIn profile URLs
2. Process each profile URL, handling search URLs if needed
3. Store the processed data in the database
4. Export the results to an Excel file
"""

import sys
import os
from linkedin_data_fetcher import batch_process_excel
from db_utils import DatabaseManager
from excel_writer import write_to_excel
from process_logger import process_logger

def main():
    # Check if file path is provided
    if len(sys.argv) < 2:
        print("Please provide the path to an Excel file:")
        print("python process_excel.py path/to/excel_file.xlsx")
        return
    
    # Get file path from command line argument
    excel_file = sys.argv[1]
    
    # Check if file exists
    if not os.path.exists(excel_file):
        print(f"Error: File '{excel_file}' not found")
        return
    
    # Set output file name
    output_file = "SampleData.xlsx"
    
    print(f"Processing Excel file: {excel_file}")
    print(f"Output will be saved to: {output_file}")
    
    # Process Excel file
    profiles_data, educations_data, experiences_data, club_experiences_data = batch_process_excel(excel_file)
    
    # Check if any data was processed
    if not profiles_data:
        print("No profile data was processed successfully")
        return
    
    print(f"Successfully processed {len(profiles_data)} profiles")
    
    # Save to database
    try:
        db = DatabaseManager()
        db.create_tables()
        db.insert_data(
            profiles_data=profiles_data,
            educations_data=educations_data,
            experiences_data=experiences_data,
            club_experiences_data=club_experiences_data
        )
        print("Data saved to database successfully")
    except Exception as e:
        print(f"Error saving to database: {str(e)}")
    
    # Write to Excel file
    output_path = write_to_excel(
        profiles_data, 
        educations_data, 
        experiences_data, 
        club_experiences_data, 
        output_file
    )
    
    if output_path:
        print(f"Data exported to Excel file: {output_path}")
    else:
        print("Error exporting data to Excel file")

if __name__ == "__main__":
    main()
