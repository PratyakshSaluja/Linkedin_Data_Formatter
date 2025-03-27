# LinkedIn Profile Analyzer

A powerful tool to extract, process, and analyze LinkedIn profiles in bulk. This application allows you to input LinkedIn profile URLs individually or in bulk, extracts comprehensive information from each profile, and stores the data in a structured database. The extracted data can be exported to Excel for further analysis.

## Features

- **Individual Profile Processing**: Input a single LinkedIn profile URL for processing
- **Bulk Profile Processing**: Upload a CSV file containing multiple LinkedIn profile URLs
- **Data Extraction**: Extract comprehensive information from LinkedIn profiles
- **Database Storage**: All extracted data is stored in a PostgreSQL database for persistence
- **Excel Export**: Export processed data to Excel files for analysis and reporting
- **Web Interface**: User-friendly web interface for easy interaction

## Data Extracted

The application extracts the following information from LinkedIn profiles:

- Basic profile information
- Education history
- Work experience
- Club and organization experiences
- Certifications and licenses

## Installation

### Prerequisites

- Python 3.7+
- PostgreSQL database
- ProxyCurl API key (for LinkedIn data extraction)

### Setup

1. Clone this repository:
   ```
   git clone <repository-url>
   cd final
   ```

2. Install required dependencies:
   ```
   pip install -r requirements.txt
   ```

3. Create a `.env` file in the project root with the following variables:
   ```
   DATABASE_URL=postgresql://username:password@host:port/database
   PROXYCURL_API_KEY=your_api_key_here
   OUTPUT_FILE=SampleData.xlsx
   ```

## Usage

### Starting the Application

Run the application with:
