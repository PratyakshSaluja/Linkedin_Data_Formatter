"""
Process Logger Module
-------------------

This module handles logging of all data processing operations including:
- Database operations (insert, update, delete)
- Excel file operations (create, append)
- Success/failure status of operations
- Timestamps of all operations
"""

import logging
from datetime import datetime
import os

class ProcessLogger:
    def __init__(self, log_file="process_logs.txt"):
        self.log_file = log_file
        
        # Configure logging
        logging.basicConfig(
            filename=log_file,
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        self.logger = logging.getLogger(__name__)
        
        # Also add console handler for immediate feedback
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        console_handler.setFormatter(formatter)
        self.logger.addHandler(console_handler)

    def log_excel_operation(self, operation_type, file_path, status, details=None):
        """
        Log Excel file operations
        
        Parameters:
        -----------
        operation_type : str
            Type of operation (e.g., 'create', 'append')
        file_path : str
            Path to the Excel file
        status : str
            'success' or 'failed'
        details : str, optional
            Additional details about the operation
        """
        msg = f"Excel Operation - Type: {operation_type}, File: {file_path}, Status: {status}"
        if details:
            msg += f", Details: {details}"
        
        if status == 'success':
            self.logger.info(msg)
        else:
            self.logger.error(msg)

    def log_db_operation(self, operation_type, table_name, status, details=None):
        """
        Log database operations
        
        Parameters:
        -----------
        operation_type : str
            Type of operation (e.g., 'insert', 'update', 'delete')
        table_name : str
            Name of the database table
        status : str
            'success' or 'failed'
        details : str, optional
            Additional details about the operation
        """
        msg = f"Database Operation - Type: {operation_type}, Table: {table_name}, Status: {status}"
        if details:
            msg += f", Details: {details}"
        
        if status == 'success':
            self.logger.info(msg)
        else:
            self.logger.error(msg)

    def log_profile_processing(self, profile_id, excel_status, db_status, details=None):
        """
        Log complete profile processing status
        
        Parameters:
        -----------
        profile_id : str
            ID of the profile being processed
        excel_status : bool
            Whether Excel save was successful
        db_status : bool
            Whether database save was successful
        details : str, optional
            Additional processing details
        """
        excel_msg = "saved to Excel" if excel_status else "failed Excel save"
        db_msg = "saved to database" if db_status else "failed database save"
        
        msg = f"Profile {profile_id} processing completed - {excel_msg}, {db_msg}"
        if details:
            msg += f", Details: {details}"
        
        if excel_status and db_status:
            self.logger.info(msg)
        else:
            self.logger.warning(msg)

# Create global logger instance
process_logger = ProcessLogger()
