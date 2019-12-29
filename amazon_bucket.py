# -*- coding: utf-8 -*-
"""
Created on Sun Dec  8 08:13:04 2019

@author: tjtur
"""

# Brian Blaylock
# Requres `s3fs`
# Website: https://s3fs.readthedocs.io/en/latest/
# In Anaconda, download via conda-forge.

import os
import sys
import s3fs
import numpy as np
from datetime import datetime

try:
    os.listdir('/usr')
    scripts_dir = '/data/scripts'
    sys.path.append(os.path.join(scripts_dir,'resources'))
except:
    scripts_dir = 'C:/data/scripts'
    sys.path.append(os.path.join(scripts_dir,'resources'))

from reference_data import set_paths
data_dir,image_dir,archive_dir,gis_dir,py_call = set_paths()

data_type = 'g16'#'nexrad' # 'g16' 'g17'
YYYY = 2019
mm = 7
dd = 20
day_of_year = datetime(YYYY,mm,dd).timetuple().tm_yday
hr_min = 16 
hr_max = 19
radar = 'KGRR'
ymd_str = f'{YYYY:.0f}{mm:02.0f}{dd:02.0f}'
ymd_str = '20190720'
this_data_dir = os.path.join(data_dir,ymd_str,radar,'raw')
os.makedirs(this_data_dir,exist_ok=True)
#bucket_str = f'noaa-nexrad-level2/{YYYY:.0f}/{mm:02.0f}/{dd:02.0f}/{radar}/'
# Use the anonymous credentials to access public data
fs = s3fs.S3FileSystem(anon=True)

# List contents of GOES-16 bucket.
#https://noaa-nexrad-level2.s3.amazonaws.com/2007/03/15/KDDC/
if data_type == 'nexrad':
    this_data_dir = os.path.join(data_dir,ymd_str,radar,'raw')
    os.makedirs(this_data_dir,exist_ok=True)
    fs.ls('s3://noaa-nexrad-level2/')
    bucket_str = f'noaa-nexrad-level2/{YYYY:.0f}/{mm:02.0f}/{dd:02.0f}/{radar}/'
    files = np.array(fs.ls(bucket_str))
    for f in range(0,len(files)):
        filename = files[f].split('/')[-1]
        file_hour = int(filename.split('_')[1][0:2])
        if file_hour >= hr_min and file_hour <= hr_max and 'MD' not in filename:
            print(file_hour,filename)
            fs.get(files[f], files[f].split('/')[-1])
            print('getting... ' + str(files[f]))
            fs.get(files[f], os.path.join(this_data_dir,files[f].split('/')[-1]))

elif data_type == 'g16':
    this_data_dir = os.path.join(data_dir,ymd_str,'satellite','raw')
    os.makedirs(this_data_dir,exist_ok=True)
    fs.ls('s3://noaa-goes16/ABI-L2-MCMIPC/')
    for h in range(hr_min,hr_max+1,1):
        bucket_str = f'noaa-goes16/ABI-L2-MCMIPC/{YYYY:.0f}/{day_of_year:03.0f}/{h:02.0f}/'
        files = np.array(fs.ls(bucket_str))
        print(bucket_str)#,files)
        for f in range(0,len(files)):
            print('getting ... ' + str(files[f]))#,files)
            fs.get(files[f], os.path.join(this_data_dir,files[f].split('/')[-1]))    
elif data_type == 'g17':
    fs.ls('s3://noaa-goes17/')
else:
    pass

# List specific files of GOES-17 CONUS data (multiband format) on a certain hour
# Note: the `s3://` is not required
#files = np.array(fs.ls('noaa-goes16/ABI-L2-MCMIPM/2019/240/00/'))
#files = np.array(fs.ls('noaa-nexrad-level2/2007/03/15/KGRR/'))
#print(files)

# Download the first file, and rename it the same name (without the directory structure)
#for f in range(0,len(files)):
#    filename = files[f].split('/')[-1]
#    file_hour = filename.split('_')[-1][0:2]
#    if int(file_hour) >= hr_min and int(file_hour) <= hr_max:
#        print(file_hour,filename)
    #fs.get(files[f], files[f].split('/')[-1])
#    print('getting... ' + str(files[f]))