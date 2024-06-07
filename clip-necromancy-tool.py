import pandas as pd
import numpy as np
import os
import time
import subprocess
import requests
from multiprocessing import cpu_count
from multiprocessing.pool import ThreadPool


## Setup things:
#full path to ffprobe because I have a weird setup, could just set it to 'ffprobe' otherwise
ffprobepath = 'D:\\Videos\\ffmpeg-5.1.1-essentials_build\\bin\\ffprobe.exe'

#full path of CSV file with at least the columns "download_url", "Start Offset", and "Output Filename", and should be filtered to already all be on the same VOD.
fpath = 'D:\\Videos\\MCYT\\DSMP\\Necromancy\\2020-09-22 - Tubbo Clip offset format URLs.csv'

#full path to folder where the clips should be saved
outputfolderpath = 'D:\\Videos\\MCYT\\DSMP\\Necromancy\\2020-09-22 - Tubbo Offset Clips'


## Get clip information and organize it before downloading:
clipdf = pd.read_csv(fpath)

#function to get the duration of a clip in seconds from its download url
def getclipduration(clipurl):
  return float(subprocess.check_output(' '.join([ffprobepath, '-i', clipurl, '-show_entries', 'format=duration', '-v', 'quiet', '-of', 'csv="p=0"']), stderr=subprocess.STDOUT))

t0 = time.time(); clipdf['Duration'] = clipdf['download_url'].apply(getclipduration); print(str(time.time() - t0) + 's to get all clip durations')

#subtract half a second because Twitch audio on the last few frames is bad and clips are usually an integer number of seconds plus a frame
clipdf['End Offset'] = clipdf['Start Offset']+clipdf['Duration']-0.5
#
offsetintervals = pd.IntervalIndex.from_arrays(left=clipdf['Start Offset'], right=clipdf['End Offset'], closed='both')

#setup before loop
currenttime = 0.0 #time of the end of the clip set constructed thus far
maxtime = clipdf['End Offset'].max() #maximum time available
clipset = [] #list of indices of clips to download
gaps = [] #list of gaps in overlap

#get a minimal set of clips to cover as much time as possible
t0 = time.time()
while currenttime < maxtime: #go until as much has been covered as possible
  #first look for clips that overlap the current last clip
  overlapclips = clipdf[offsetintervals.contains(currenttime)] #all clips that contain the last time in the clip set constructed
  if (overlapclips.empty) or (overlapclips['End Offset'].max() == currenttime): #There are no clips overlapping the current time, or all clips overlapping end at the current time
    gapstart = currenttime
    currenttime = clipdf[clipdf['Start Offset']>currenttime]['Start Offset'].min() #get the smallest next time available
    print('Gap from ' + str(gapstart) + ' to ' + str(currenttime))
    gaps.append([gapstart, currenttime])
    continue
  
  currenttime = overlapclips['End Offset'].max() #new time is the latest end time available from overlapping clips
  clipset.append(overlapclips[overlapclips['End Offset'] == currenttime].index[-1]) #add the index of the first clip that gives this time

print(str(time.time() - t0) + 's to get clip set')
print('Gaps: ' + str(gaps))
gaps = np.asarray(gaps)
print('Approximate total gap time: ' + str(np.floor(gaps[:,1]-gaps[:,0]).sum().astype(int)) + 's')
print('Total clips to download: ' + str(len(clipset)))

#organize clip set information for downloading
clipstodownload = clipdf.iloc[clipset].copy() #just the clips to download
clipstodownload['Overlap Previous'] = (clipstodownload['End Offset'].shift(1)+0.5-clipstodownload['Start Offset']).fillna(0) #get amount that the start of the new clip should be before the end of the previous clip, negative if there is a gap, to make the manual part faster
clipstodownload['Output Filename'] = clipstodownload['Output Filename'].str.rstrip('.mp4')+'_' + clipstodownload['Overlap Previous'].apply(np.floor).astype(int).astype(str) + '-' + (((clipstodownload['Overlap Previous'])-(clipstodownload['Overlap Previous'].apply(np.floor)))*60).round().astype(int).astype(str) + '.mp4' #add that amount to the filename so it's visible while editing. Note: changed to include 60ths of second / frames after dash.

clipstodownload.to_csv((outputfolderpath+os.path.sep+'clipdownloadinfo.csv'), index=False)

downloadurls = clipstodownload['download_url'].tolist()
downloadfnames = (outputfolderpath+os.path.sep+clipstodownload['Output Filename']).tolist()

## Download the clips
downloadinfo = zip(downloadurls, downloadfnames)

#Function for downloading a clip from a url to a filename, where they are zipped into one argument, args
def download_from_url(args):
  t0 = time.time()
  url, fname = args[0], args[1]
  try:
    r = requests.get(url)
    with open(fname, 'wb') as f:
      f.write(r.content)
    return(url, time.time() - t0)
  except Exception as e:
    print('Exception in download_from_url():', e)

#Function to download multiple clips from a zipped set of urls and filenames in parallel
def download_from_url_parallel(args):
    cpus = cpu_count()
    results = ThreadPool(cpus - 1).imap_unordered(download_from_url, args)
    for result in results:
        print('url:', result[0], 'time (s):', result[1])

#Download clips in parallel
t0 = time.time(); download_from_url_parallel(downloadinfo); print('total download time (s): ' + str(time.time()-t0))

