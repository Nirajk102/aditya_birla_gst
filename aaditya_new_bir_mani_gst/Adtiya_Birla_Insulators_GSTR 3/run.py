#!/usr/bin/env python3
"""
Script to run the GSTR filing application with environment-specific configuration.
Usage: python run.py [environment]
Example: python run.py production
"""

import sys
import os
from application.cleartax_GSTR import cleartax_GSTR
from config import load_config

def main():
    # Get environment from command line argument or environment variable
    if len(sys.argv) > 1:
        env = sys.argv[1].lower()
    else:
        env = os.environ.get('ENVIRONMENT', '').lower()
    
    # Set environment variable for other modules
    os.environ['ENVIRONMENT'] = env
    
    # Load configuration based on environment
    config = load_config(env)
    
    # Print environment and connection details (for verification)
    print(f"Running in {env or 'default'} environment")
    print(f"Database: {config['DB_CONNECTION']['db_host']}")
    print(f"API URL: {config['API_DETAILS']['BASE_URL']}")
    
    # Run the GSTR filing processes
    print("Starting GSTR1 filing...")
    cleartax_GSTR.fileGSTR1()
    
    print("Starting GSTR2 filing...")
    cleartax_GSTR.fileGSTR2()
    
    print("GSTR filing completed.")

if __name__ == "__main__":
    main()