import os
import sys
import urllib.request

# Check if the system is Windows
if sys.platform.startswith('win'):
    print("Windows detected. Setting up Hadoop compatibility...")
    
    # --- YOUR FRIEND'S CODE GOES HERE ---
    hadoop_home = "C:/hadoop"
    bin_dir = os.path.join(hadoop_home, "bin")
    os.makedirs(bin_dir, exist_ok=True)

    # URL for Hadoop 3.3 winutils.exe
    winutils_url = "https://github.com/kitfactory/winutils/raw/main/hadoop-3.3.1rc3/bin/winutils.exe"
    winutils_path = os.path.join(bin_dir, "winutils.exe")

    if not os.path.exists(winutils_path):
        print("Downloading winutils.exe...")
        # Note: Added simple error handling for the download
        try:
            urllib.request.urlretrieve(winutils_url, winutils_path)
            print(f"Downloaded to {winutils_path}")
        except Exception as e:
            print(f"Failed to download winutils: {e}")
    
    os.environ["HADOOP_HOME"] = hadoop_home
    os.environ["PATH"] += f";{bin_dir}"
    
else:
    print(f"Running on {sys.platform}. Native Hadoop support enabled. No extra setup needed.")