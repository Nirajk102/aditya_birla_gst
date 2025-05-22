import subprocess
import json

# Your curl command (as list of strings)
curl_command = [
    "curl",
    "-s",  # silent mode (no progress bar)
    "-X", "GET",
    "https://api.example.com/data",  # <-- apna URL daal yahan
    "-H", "Accept: application/json"
]

# Run the curl command
result = subprocess.run(curl_command, capture_output=True, text=True)

# Check if command succeeded
if result.returncode == 0:
    try:
        # Parse JSON output to Python dict
        data = json.loads(result.stdout)
        print("Response as dict:", data)
    except json.JSONDecodeError:
        print("Error: Response is not valid JSON.")
        print(result.stdout)
else:
    print("Curl command failed.")
    print("stderr:", result.stderr)

