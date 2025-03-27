import os
import pandas as pd
from db_utils import DatabaseManager
from datetime import datetime
from process_logger import process_logger
from sqlalchemy.exc import SQLAlchemyError

def create_export_directory():
    """Create an exports directory with timestamp"""
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    export_dir = os.path.join(os.path.dirname(__file__), f'exports_{timestamp}')
    os.makedirs(export_dir, exist_ok=True)
    return export_dir

def export_table_to_files(df, table_name, export_dir):
    """Export a dataframe to Excel, CSV and SQL files"""
    # Excel export
    excel_path = os.path.join(export_dir, f'{table_name}.xlsx')
    df.to_excel(excel_path, index=False)
    
    # CSV export
    csv_path = os.path.join(export_dir, f'{table_name}.csv')
    df.to_csv(csv_path, index=False)
    
    # SQL export (as INSERT statements)
    sql_path = os.path.join(export_dir, f'{table_name}.sql')
    with open(sql_path, 'w', encoding='utf-8') as f:
        f.write(f'-- {table_name} table data\n\n')
        # Fix handling of NaN and string escaping
        for _, row in df.iterrows():
            columns = ', '.join(f'"{col}"' for col in df.columns)
            values = []
            for val in row.values:
                if pd.isna(val):
                    values.append('NULL')
                elif isinstance(val, (int, float)):
                    values.append(str(val))
                else:
                    # Replace single quotes and escape special characters
                    safe_val = str(val).replace("'", "''")
                    values.append(f"'{safe_val}'")
            f.write(f'INSERT INTO {table_name} ({columns}) VALUES ({", ".join(values)});\n')

def export_all_data():
    """Export all database tables to different file formats"""
    try:
        # Connect to database
        db = DatabaseManager()
        process_logger.log_db_operation("export", "all", "success", "Starting data export process")

        # Create export directory
        export_dir = create_export_directory()
        process_logger.log_excel_operation("create", export_dir, "success", f"Created export directory")

        # Get all data from database using DatabaseManager's get_all_data method
        profiles_df, educations_df, experiences_df, club_experiences_df, certifications_df = db.get_all_data()
        
        # Create a dictionary of table names and their respective dataframes
        tables_data = {
            'profiles': profiles_df,
            'educations': educations_df,
            'experiences': experiences_df, 
            'club_experiences': club_experiences_df,
            'certifications': certifications_df
        }
        
        # Count exported tables for reporting
        exported_tables = 0
        
        # Export each non-empty dataframe
        for table_name, df in tables_data.items():
            if df is not None and not df.empty:
                export_table_to_files(df, table_name, export_dir)
                process_logger.log_excel_operation("export", f"{export_dir}/{table_name}", "success", f"Exported {table_name} table")
                exported_tables += 1
            else:
                print(f"No data in table: {table_name}")
                process_logger.log_excel_operation("export", table_name, "skipped", f"Table {table_name} is empty or not accessible")
        
        if exported_tables > 0:
            print(f"Successfully exported {exported_tables} tables to: {export_dir}")
            process_logger.log_db_operation("export", "all", "success", f"Data export completed successfully - {exported_tables} tables exported")
            return export_dir
        else:
            print("No tables were exported. Database might be empty.")
            process_logger.log_db_operation("export", "all", "warning", "No tables were exported. Database might be empty.")
            return None

    except SQLAlchemyError as e:
        error_msg = f"Database error exporting data: {str(e)}"
        print(error_msg)
        process_logger.log_db_operation("export", "all", "failed", error_msg)
        return None
    except Exception as e:
        error_msg = f"Error exporting data: {str(e)}"
        print(error_msg)
        process_logger.log_db_operation("export", "all", "failed", error_msg)
        return None

if __name__ == "__main__":
    export_all_data()
