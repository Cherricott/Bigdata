# import os
# import urllib.request

# # 1. Set local folder for Hadoop
# hadoop_home = "C:/hadoop"
# bin_dir = os.path.join(hadoop_home, "bin")
# os.makedirs(bin_dir, exist_ok=True)

# # 2. URL for Hadoop 3.3 winutils.exe
# winutils_url = "https://github.com/kitfactory/winutils/blob/main/hadoop-3.3.1rc3/winutils.exe"
# winutils_path = os.path.join(bin_dir, "winutils.exe")

# # 3. Download winutils.exe if it doesn't exist
# if not os.path.exists(winutils_path):
#     print("Downloading winutils.exe...")
#     urllib.request.urlretrieve(winutils_url, winutils_path)
#     print(f"Downloaded to {winutils_path}")
# else:
#     print("winutils.exe already exists.")

# # 4. Set environment variables for current session
# os.environ["HADOOP_HOME"] = hadoop_home
# os.environ["PATH"] += f";{bin_dir}"

# print("HADOOP_HOME set to:", os.environ["HADOOP_HOME"])
# print("PATH updated with:", bin_dir)

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