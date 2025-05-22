from application.cleartax_GSTR import cleartax_GSTR
import schedule
import time
import os
import sys
from config import load_config

class Scheduler:
    @staticmethod
    def daily_job():
        """Run the daily GSTR filing job"""
        print(f"Running scheduled job at {time.strftime('%Y-%m-%d %H:%M:%S')}")
        cleartax_GSTR.fileGSTR1()
        cleartax_GSTR.fileGSTR2()
        print(f"Completed scheduled job at {time.strftime('%Y-%m-%d %H:%M:%S')}")
    
    @staticmethod
    def run_scheduler(env=None):
        """
        Run the scheduler with the specified environment configuration
        
        Args:
            env (str, optional): Environment name ('production', 'staging', or None for default)
        """
        # Set environment variable
        if env:
            os.environ['ENVIRONMENT'] = env
            
        # Load configuration based on environment
        config = load_config(env)
        print(f"Scheduler running in {env or 'default'} environment")
        print(f"Database: {config['DB_CONNECTION']['db_host']}")
        print(f"API URL: {config['API_DETAILS']['BASE_URL']}")
        
        # Schedule the job to run every day at midnight
        schedule.every().day.at("00:00").do(Scheduler.daily_job)
        print("Job scheduled to run daily at midnight")
        
        # Run the job immediately for testing
        print("Running job immediately for testing...")
        Scheduler.daily_job()
        
        # Keep the scheduler running
        while True:
            schedule.run_pending()
            time.sleep(60)  # Check every minute

if __name__ == "__main__":
    # Get environment from command line argument
    env = sys.argv[1].lower() if len(sys.argv) > 1 else None
    Scheduler.run_scheduler(env)
