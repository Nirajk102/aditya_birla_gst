import configparser
import os

def load_config(env=None):
    """
    Load configuration based on environment.
    
    Args:
        env (str, optional): Environment name ('production', 'staging', or None for default).
            If None, it will try to read from ENVIRONMENT environment variable.
            If ENVIRONMENT is not set, it will use the default property.ini.
    
    Returns:
        ConfigParser: Loaded configuration
    """
    config = configparser.ConfigParser(interpolation=None)
    
    # If env is not provided, try to get it from environment variable
    if env is None:
        env = os.environ.get('ENVIRONMENT', '').lower()
    
    # Determine which config file to use
    if env == 'production':
        filename = 'property.production.ini'
    elif env == 'sandbox':
        filename = 'property.sandbox.ini'
    else:
        filename = 'property.ini'  # Default/development config
    
    # Load the configuration
    config.read(filename)
    print(f"Loaded configuration from {filename}")
    
    return config
