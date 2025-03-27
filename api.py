"""
LinkedIn Profile Scraper API
----------------------------

This module defines a FastAPI application that serves as the interface for scraping and processing LinkedIn profiles.
It provides an endpoint to process LinkedIn profile data from either a CSV file containing multiple URLs
or a single LinkedIn profile URL.

Endpoints:
- POST /process: Processes LinkedIn profiles from either a CSV file or single URL and returns an Excel file with results
- GET /download: Downloads the latest Excel file with all processed data

 
"""
from fastapi import FastAPI, UploadFile, Form, Request, BackgroundTasks
from fastapi.responses import FileResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
import uvicorn
import os
from linkedin_scraper import process_linkedin_data
from db_utils import DatabaseManager
from excel_writer import write_to_excel
from linkedin_data_fetcher import batch_process_excel
from process_logger import process_logger

# Initialize FastAPI application
app = FastAPI(
    title="LinkedIn Profile Scraper API",
    description="API for scraping LinkedIn profiles and analyzing professional data",
    version="1.0.0"
)

# Get the current directory path for consistent file handling
current_dir = os.path.dirname(os.path.abspath(__file__))
output_file = os.path.join(current_dir, "SampleData.xlsx")

# Mount templates directory
templates = Jinja2Templates(directory="templates")

@app.get("/", response_class=HTMLResponse)
async def root(request: Request):
    """Serve the main HTML interface"""
    return templates.TemplateResponse("index.html", {"request": request})

@app.post("/process")
async def process_csv(file: UploadFile = None, linkedin_url: str = Form(None)):
    """
    Process LinkedIn profiles from either a CSV file or single URL.
    
    This endpoint accepts either:
    1. A CSV file with LinkedIn URLs in the first column, or
    2. A direct LinkedIn profile URL as a form field
    
    It scrapes the profiles, analyzes them for Fortune 500 employment,
    leadership roles, and entrepreneurship signals, and returns an Excel file
    with the processed data (not for immediate download, but for storage).
    
    Parameters:
    -----------
    file : UploadFile, optional
        CSV file containing LinkedIn URLs in the first column
    linkedin_url : str, optional
        Single LinkedIn profile URL to process
        
    Returns:
    --------
    FileResponse
        Excel file containing processed data
        
    Error Response:
    --------------
    JSON object with error message if processing fails
    """
    if not file and not linkedin_url:
        return {"error": "No input provided. Please provide either a CSV file or LinkedIn URL."}
        
    try:
        # Test database connection first
        try:
            db = DatabaseManager()
            db.create_tables()
        except Exception as e:
            return {
                "error": "Database connection failed",
                "details": [
                    f"- Database error: {str(e)}",
                    "- Check your database credentials",
                    "- Ensure the database server is accessible"
                ]
            }

        file_content = await file.read() if file else None
        result_file = process_linkedin_data(file_content, linkedin_url, output_file)
        
        print(f"Processing complete. Result file: {result_file}")
        
        if result_file and os.path.exists(result_file):
            return {
                "success": True, 
                "message": "Data processed successfully and saved to both database and Excel file"
            }
        else:
            return {
                "error": "Failed to process profile data. This could be due to:",
                "details": [
                    "- Invalid or expired API key",
                    "- API rate limit exceeded",
                    "- Profile not accessible",
                    "- Network connectivity issues",
                    "- Database connection issues"
                ]
            }
    except Exception as e:
        print(f"Error processing request: {str(e)}")
        return {"error": f"An unexpected error occurred: {str(e)}"}

@app.post("/process-excel")
async def process_excel_file(file: UploadFile = None, limit: int = Form(None)):
    """
    Process LinkedIn profiles from an Excel file.
    
    This endpoint accepts an Excel file with a specific format containing:
    - Full Name column
    - Linkedin Profile column (URLs)
    - Additional metadata columns (batch, program, etc.)
    
    It processes the profiles, analyzes them, and returns success/failure info.
    
    Parameters:
    -----------
    file : UploadFile
        Excel file containing LinkedIn data
        
    Returns:
    --------
    JSON
        Result information with success/failure status
    """
    if not file:
        return {"error": "No Excel file provided"}
        
    try:
        # Test database connection first
        try:
            db = DatabaseManager()
            db.create_tables()
            process_logger.log_db_operation('init', 'all', 'success')
        except Exception as e:
            return {
                "error": "Database connection failed",
                "details": [
                    f"- Database error: {str(e)}",
                    "- Check your database credentials",
                    "- Ensure the database server is accessible"
                ]
            }
            
        # Save uploaded file temporarily
        temp_file_path = f"temp_{file.filename}"
        with open(temp_file_path, "wb") as f:
            f.write(await file.read())
            
        # Process the Excel file with optional limit
        profiles_data, educations_data, experiences_data, club_experiences_data, current_certifications = batch_process_excel(
            temp_file_path, 
            limit=limit,
            output_file=output_file
        )
        
        # Clean up temp file
        if os.path.exists(temp_file_path):
            os.remove(temp_file_path)
            
        # Check if profiles were processed
        if not profiles_data:
            return {
                "error": "No profiles could be processed from the Excel file",
                "details": [
                    "- Check that your Excel file has a 'Linkedin Profile' column",
                    "- Ensure the URLs are properly formatted",
                    "- Some URLs might be search URLs rather than profile URLs"
                ]
            }
            
        # Save processed data
        result_file = write_to_excel(
            profiles_data, 
            educations_data, 
            experiences_data, 
            club_experiences_data, 
            current_certifications,
            output_file
        )
        
        # Save to database
        db_success = db.insert_data(
            profiles_data=profiles_data,
            educations_data=educations_data,
            experiences_data=experiences_data,
            club_experiences_data=club_experiences_data,
            certifications_data=current_certifications
        )

        process_logger.log_db_operation('save', 'all', 'success' if db_success else 'failed',
            f"Processed {len(profiles_data)} profiles" if db_success else "Failed to save to database")
        
        # Log Excel operation result
        if result_file and os.path.exists(result_file):
            process_logger.log_excel_operation('write', result_file, 'success', 
                f"Wrote {len(profiles_data)} profiles to Excel")
            return {
                "success": True,
                "message": f"Successfully processed {len(profiles_data)} profiles from Excel file",
                "processed_count": len(profiles_data)
            }
        else:
            return {
                "error": "Failed to save processed data to Excel file",
            }
            
    except Exception as e:
        process_logger.log_excel_operation('process', file.filename, 'failed', str(e))
        return {"error": f"An unexpected error occurred: {str(e)}"}

@app.get("/download")
async def download_excel():
    """
    Download the Excel file with all processed LinkedIn data.
    
    This endpoint serves the current Excel file containing all previously processed
    LinkedIn profile data for download.
    
    Returns:
    --------
    FileResponse
        Excel file download response
    """
    if os.path.exists(output_file):
        return FileResponse(
            path=output_file,
            filename="SampleData.xlsx",
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
    return {"error": "No data file found. Please process some profiles first."}

if __name__ == "__main__":
    uvicorn.run(app, host="localhost", port=8000)
