#!/usr/bin/env python3
"""
Setup script for Aditya Birla Insulators GSTR Filing Application.
This script helps with installing dependencies and setting up the environment.
"""

import subprocess
import sys
import os
import platform

def check_python_version():
    """Check if Python version is compatible."""
    required_version = (3, 6)
    current_version = sys.version_info
    
    if current_version < required_version:
        print(f"Error: Python {required_version[0]}.{required_version[1]} or higher is required.")
        print(f"Current Python version: {current_version[0]}.{current_version[1]}.{current_version[2]}")
        sys.exit(1)
    
    print(f"Python version check passed: {current_version[0]}.{current_version[1]}.{current_version[2]}")

def install_dependencies():
    """Install required packages from requirements.txt."""
    try:
        print("Installing dependencies from requirements.txt...")
        subprocess.check_call([sys.executable, "-m", "pip", "install", "-r", "requirements.txt"])
        print("Dependencies installed successfully.")
    except subprocess.CalledProcessError as e:
        print(f"Error installing dependencies: {e}")
        sys.exit(1)

def check_oracle_client():
    """Check if Oracle client is installed and configured."""
    try:
        import cx_Oracle
        print("cx_Oracle module is installed.")
        
        # Try to get Oracle client version
        try:
            client_version = cx_Oracle.clientversion()
            print(f"Oracle client version: {'.'.join(map(str, client_version))}")
        except Exception as e:
            print(f"Warning: Oracle client is not properly configured: {e}")
            print("Please ensure Oracle Instant Client is installed and configured correctly.")
            
            # Provide platform-specific instructions
            system = platform.system()
            if system == "Windows":
                print("\nWindows installation instructions:")
                print("1. Download Oracle Instant Client from https://www.oracle.com/database/technologies/instant-client/winx64-64-downloads.html")
                print("2. Extract the files to a directory (e.g., C:\\oracle\\instantclient_19_9)")
                print("3. Add the directory to your PATH environment variable")
            elif system == "Darwin":  # macOS
                print("\nmacOS installation instructions:")
                print("1. Install Oracle Instant Client using Homebrew:")
                print("   brew install instantclient-basic")
                print("   brew install instantclient-sqlplus")
            elif system == "Linux":
                print("\nLinux installation instructions:")
                print("1. Download Oracle Instant Client from https://www.oracle.com/database/technologies/instant-client/linux-x86-64-downloads.html")
                print("2. Extract the files and install the packages")
                print("3. Set the LD_LIBRARY_PATH environment variable")
            
    except ImportError:
        print("Error: cx_Oracle module is not installed.")
        print("Please run 'pip install cx_Oracle' to install it.")
        sys.exit(1)

def setup_environment():
    """Set up the environment for the application."""
    # Create logs directory if it doesn't exist
    logs_dir = os.path.join(os.path.dirname(__file__), "logs")
    if not os.path.exists(logs_dir):
        os.makedirs(logs_dir)
        print(f"Created logs directory: {logs_dir}")
    
    # Check if configuration files exist
    config_files = ["property.ini", "property.staging.ini", "property.production.ini"]
    for config_file in config_files:
        if os.path.exists(config_file):
            print(f"Configuration file exists: {config_file}")
        else:
            print(f"Warning: Configuration file not found: {config_file}")

def main():
    """Main setup function."""
    print("Setting up Aditya Birla Insulators GSTR Filing Application...")
    
    check_python_version()
    install_dependencies()
    check_oracle_client()
    setup_environment()
    
    print("\nSetup completed successfully!")
    print("\nTo run the application:")
    print("1. For development environment: python run.py")
    print("2. For staging environment: python run.py staging")
    print("3. For production environment: python run.py production")

if __name__ == "__main__":
    main()