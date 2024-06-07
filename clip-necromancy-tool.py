import pandas as pd
import numpy as np
import os
import subprocess
import requests
import cv2
from pydub import AudioSegment, silence
from time import time
import requests
import shutil

#Setup:
#full path of originally downloaded full CSV file from https://www.twitchanz.com/clips/ , start with the day of the VOD and go 60 days after then.
#fpath = r"D:\Videos\MCYT\DSMP\Necromancy\spreadsheet work\2020-09-01 to 2020-11-30 - Tubbo Clips.csv" #r"D:\Videos\MCYT\DSMP\Necromancy\spreadsheet work\2021-01-05 to 2021-03-08 - Tubbo Clips.csv" #r"D:\Videos\MCYT\DSMP\Necromancy\spreadsheet work\2021-01-06-to-2021-03-07 DropsByPonk_clips.csv"
fpath = r"D:\Videos\MCYT\DSMP\Necromancy\spreadsheet work\original csv\2020-06-20 to 2020-08-31 DropsByPonk_clips.csv"
#at least one of these two must have a value:
chosenstreamid = 39041624752 #chosenstreamid = 38788861632 #set to None if not using. This will usually match twitchtracker
chosenvodid = 667128237 #set to None if not using. Set this if the vod id is different. I don't know where you find these except by looking at clips.

#full path to folder where the clips should be saved
outputfolderpath = r"D:\Videos\MCYT\DSMP\Necromancy\2020-07-01 Ponk Necromancy"
outputtitle = "2020-07-01 Ponk Restoration - Minecraft but My lemon tree is gone"
maxtime = 7200 # last time in seconds to include in restoration, put None by default
checkbetween = True #whether to have it check seconds between ones listed in CSV, usually default True



#timing functions
_tstart_stack = []

def tic():
  _tstart_stack.append(time())

def toc(txt='', fmt="Elapsed: %s s"):
  print(' '.join(list(filter(None, [fmt,txt]))) % int(time() - _tstart_stack.pop()))

#skipping this for now, might implement later
#nonoffsetclips = fullclipcsv.loc[(~fullclipcsv['filename'].str.contains('offset')) & (fullclipcsv['title']==offsetclips['title'].mode().iloc[0])].sort_values('created_at').reset_index(drop=True) #get non-offset-format clips with the vod's title

#gets the spot of the start of overlap in the first clip, in seconds
#new version can handle
#Not trying to use estimated overlap time because video time seeking is inaccurate. Just seek from beginning
#returns: start time in first clip of overlap in seconds, duration of overlap in seconds, and warning flag (True if something is weird)
#note: ffmpeg export times will be weird if it is to a smaller granularity
def get_clip_overlap_sec(clip1, clip2):
  cap1 = cv2.VideoCapture(clip1)
  cap2 = cv2.VideoCapture(clip2)
  success, frame2 = cap2.read() #get first frame of clip2
  cap2.release()
  success, frame1 = cap1.read() #start at first frame of clip1
  while success: #continue reading frame by frame as long as there are more frames
    if np.array_equal(frame1, frame2): #If a frame of clip1 matches the first frame of clip2:
      overlapsec = round(cap1.get(cv2.CAP_PROP_POS_MSEC)/1000) #second of clip1 it starts to overlap clip2
      success, frame2 = cap1.read(); warningflag = success and np.array_equal(frame1, frame2) #warning for if the next frame of clip1 (if it exists) is the same as this frame
      durationsec = round(float(subprocess.check_output(' '.join(['ffprobe', '-i', '"'+clip1+'"', '-show_entries', 'format=duration', '-v', 'quiet', '-of', 'csv="p=0"']), stderr=subprocess.STDOUT)))-overlapsec #get the duration of the overlap
      cap1.release(); cap2.release() #release the video captures
      return overlapsec, durationsec, warningflag
    
    success, frame1 = cap1.read()
  
  return -1, -1, True #Return -1ms and a warning flag if can't find overlap


def export_clip_audio(clip1, clip2, overlapsec, durationsec):
  subprocess.call(' '.join(['ffmpeg', '-ss', str(overlapsec), '-i', clip1, clip1[:-1]+'3'])) #export mp3 of the first clip starting at the time of first overlap
  subprocess.call(' '.join(['ffmpeg', '-to', str(durationsec), '-i', clip2, clip2[:-1]+'3'])) #export mp3 of the second clip clip through the time of overlap

#gets a quiet spot in the overlap of two clips' audio, to cut with minimal audio disruption. returns timestamp in ms in the middle of the longest quiet section.
def get_audio_quiet_sec_overlap(audiofile1, audiofile2):
  #https://stackoverflow.com/questions/40896370/detecting-the-index-of-silence-from-a-given-audio-file-using-python and look at overlap in pandas or something?
  audio1 = AudioSegment.from_mp3(audiofile1)
  audio2 = AudioSegment.from_mp3(audiofile2)
  #get silences at least 1 second long 
  silences1 = []; silences2 = []; silenceoverlap=[]; threshold=32
  while (not silences1) or (not silences2) or (not silenceoverlap): #get the overlapping silence intervals of at least 0.25 seconds in length and fitting the quietest available silence threshold
    silences1 = silence.detect_silence(audio1, min_silence_len=250, silence_thresh=audio1.dBFS-threshold)
    silences2 = silence.detect_silence(audio2, min_silence_len=250, silence_thresh=audio2.dBFS-threshold)
    threshold = threshold/2
    if (len(silences1) > 0) and (len(silences2) > 0): #get [start, end, duration] of each overlap
      silenceoverlap = [[max(interval1[0],interval2[0]),min(interval1[1],interval2[1]),min(interval1[1],interval2[1])-max(interval1[0],interval2[0])] for interval1 in silences1 for interval2 in silences2 if (interval1[0]<=interval2[1]) and (interval1[1]>=interval2[0])]
  
  silenceoverlap = silenceoverlap[np.where(np.array(silenceoverlap)[:,2]==max(np.array(silenceoverlap)[:,2]))[0][0]]
  return (silenceoverlap[0]+int(silenceoverlap[2]/2))/1000

def get_clip_duration(clipurl):
  try:
    duration = float(subprocess.check_output(' '.join(['ffprobe', '-i', clipurl, '-show_entries', 'format=duration', '-v', 'quiet', '-of', 'csv="p=0"']), stderr=subprocess.STDOUT))
  except subprocess.CalledProcessError:
    duration = -1
  finally:
    return duration

#make offset clips CSV ahead of time, takes about 12 minutes for 1436 clips?
def make_time_offset_clips_csv(offsetclips, offsetclipsfile):
  offsetclips['duration'] = offsetclips['download_url'].apply(get_clip_duration)
  offsetclips = offsetclips.loc[offsetclips['duration']>0].reset_index(drop=True) #drop ones that failed
  offsetclips['end_offset'] = offsetclips['offset']+offsetclips['duration']
  offsetclips.to_csv(offsetclipsfile,index=False)



def download_clips_and_calculate_chains(offsetclipsfile, workingfolder, outfname, starttime=0, maxtime=None, check_unlisted=True):
  offsetclips = pd.read_csv(offsetclipsfile)
  
  if maxtime is None:
    maxtime = offsetclips['end_offset'].max()
  
  currenttime = starttime #point in time to seek from
  currentclip = None
  chain_position = 0 #count of how many clips are in a chain to combine
  clips_df = pd.DataFrame(index=offsetclips.index, columns=['chain_position','filename','start_offset','end_offset','ss','to','note']) #store the chains of clips and how to combine
  df_ind = 0 #track position in clips_df
  audio1 = os.path.join(workingfolder,'audio1.mp3'); audio2 = os.path.join(workingfolder,'audio2.mp3') #constant paths for temporary audio files
  #get prefixes of download urls
  streamdownloadprefix=(None if ((chosenstreamid is None) or (not str(chosenstreamid) in offsetclips['vod'].unique())) else offsetclips.loc[offsetclips['vod']==str(chosenstreamid)].iloc[0].download_url.split('-offset-')[0]+'-offset-')
  voddownloadprefix=(None if ((chosenvodid is None) or (not 'vod-'+str(chosenvodid) in offsetclips['vod'].unique())) else offsetclips.loc[offsetclips['vod']=='vod-'+str(chosenvodid)].iloc[0].download_url.split('-offset-')[0]+'-offset-')
  prefixes = [downloadprefix for downloadprefix in [streamdownloadprefix, voddownloadprefix] if downloadprefix is not None]
  
  while (currenttime < maxtime):
    print('current time: '+str(currenttime)) #tmp
    tic() #tmp
    #print(str(df_ind)+' '+str(chain_position))
    #look for the last clip that overlaps the current time
    overlapclips = offsetclips.loc[(offsetclips['offset']<=currenttime)&(offsetclips['end_offset']>currenttime)]
    if overlapclips.empty: #if there are no clips that overlap with the current time
      if check_unlisted:
        req = None #set req to None for logic
        #first, check for clips between the last known clip's start time and the next clip that aren't listed. note: offset times are always even numbers
        if currentclip is None:
          timestart = int(currenttime)-int(currenttime)%2+2 #start from 2 seconds after the current time if there is no clip to get the start time from
        else:
          timestart = currentclip.offset+2 #otherwise, start from 2 seconds after the start of the current clip
        
        timestop = (maxtime if offsetclips.loc[offsetclips['offset']>currenttime].empty else offsetclips.loc[offsetclips.loc[offsetclips['offset']>currenttime,'offset'].idxmin(),'offset']) #stop at the next known time, or the maxtime if there is none
        print('timestart '+str(timestart)+', timestop '+str(timestop)) #tmp
        for timecheck in range(timestart, timestop, 2): #for each time every 2 seconds between the times
          print('timecheck: '+str(timecheck)) #tmp
          for downloadprefix in prefixes: #for each prefix
            if (req is not None) and (req.ok): #if the previous one worked
              break
            
            req = requests.get(downloadprefix+str(timecheck)+'.mp4') #try to download the clip
            if req.ok: #if it succeeds, break the loop. this will be detected in overlapclips next time
              print('found clip at '+str(timecheck)) #tmp
              #save the download
              with open(os.path.join(workingfolder, downloadprefix.split('/')[-1]+str(timecheck)+'.mp4'), 'wb') as fdl:
                _ = fdl.write(req.content)
              #add an entry for the new clip in the chain
              offsetclips.loc[offsetclips.shape[0]] = offsetclips.iloc[0] #copy structure from first one
              offsetclips.loc[offsetclips.shape[0]-1,['id','creator_name','title','view_count','gamename','created_at','url','thumbnail_url']] = '' #set unknown information to blank
              offsetclips.loc[offsetclips.shape[0]-1,['download_url','filename','offset','duration']] = [downloadprefix+str(timecheck)+'.mp4', downloadprefix.split('/')[-1]+str(timecheck)+'.mp4', timecheck, get_clip_duration(downloadprefix+str(timecheck)+'.mp4')]
              offsetclips.loc[offsetclips.shape[0]-1,'end_offset'] = timecheck+offsetclips.loc[offsetclips.shape[0]-1,'duration']
              offsetclips = offsetclips.loc[~offsetclips.duplicated('download_url')].sort_values('offset')
              offsetclips.to_csv(offsetclipsfile,index=False) #write new clips to file
              if timecheck > currenttime: #if the clip times do not overlap, then there is not a clip between the currentclip and the new clip
                print('clip does not overlap at '+str(timecheck)) #tmp
                currenttime = timecheck #the found clip should be next and does not overlap, so go to its start time
                currentclip = None #no existing clip to check overlap with
                chain_position = 0 #start a new chain of clips
              
              if (offsetclips.loc[offsetclips.offset==timecheck,'end_offset'].iloc[0]) <= currenttime: #if the new clip's end time is not later than the currenttime, it's not progress
                print('clip is not improvement at '+str(timecheck)) #tmp
                req = None #don't use this one
              else:
                break #break this loop of checking times
        
        if (req is None) or (not req.ok): #couldn't find an unlisted time in between
          print('not found') #tmp
          currenttime = timestop #go to the start time of first listed clip after the current time
          currentclip = None #no existing clip to check overlap with
          chain_position = 0 #start a new chain of clips
      
      else: #not check_unlisted
        currenttime = maxtime if offsetclips.loc[offsetclips['offset']>currenttime].empty else (offsetclips.loc[offsetclips.loc[offsetclips['offset']>currenttime,'offset'].idxmin(),'offset']) #go to the start time of first clip after the current time, or the end if there is none
        currentclip = None #no existing clip to check overlap with
        chain_position = 0 #start a new chain of clips
    
    else: #if there are clips that overlap with the current time
      if currentclip is None: #if there is no existing clip to check overlap with,
        currentclip = overlapclips.loc[overlapclips['end_offset'].idxmax()] #take the overlapping clip with the latest end time
        fname = os.path.join(workingfolder, currentclip.filename) #file name the downloaded clip has or will have
        if not os.path.exists(fname): #if the clip is not already downloaded
          req = requests.get(currentclip.download_url) #download the clip
          with open(fname, 'wb') as fdl:
            _ = fdl.write(req.content)
        
        currenttime = currentclip.end_offset #update the current time to the end of the current clip
        clips_df.loc[df_ind,['chain_position', 'filename', 'start_offset', 'end_offset']] = [chain_position, fname, currentclip.offset, currenttime] #add to chain
        clips_df.to_csv(outfname,index=False) #write current progress on chains
        chain_position +=1 #look for another clip to chain with it next
        df_ind += 1 #move to write in the next location
      else: #if there is an existing clip to check overlap with,
        flag_match = False #flag for whether a match was found
        for newclip in overlapclips.sort_values('end_offset',ascending=False).itertuples(index=False, name='clip'): #go through the overlapping clips in decreasing order of end time
          newfname = os.path.join(workingfolder, newclip.filename) #filename the downloaded clip has or will have
          if not os.path.exists(newfname): #if the clip is not already downloaded
            req = requests.get(newclip.download_url) #download the clip
            with open(newfname, 'wb') as fdl:
              _ = fdl.write(req.content)
          
          sec_overlap, sec_duration, flag_warning = get_clip_overlap_sec(fname, newfname) #get time of frames matching
          if sec_overlap == -1: #if the first frame of the new clip doesn't match any frames of the old clip, look for the next overlap
            continue
          else: #if the first frame of the new clip matches a frame of the old clip
            if flag_warning: #if there is an exact match, but the next frame also matches
              currentclip = newclip; fname=newfname #update the current clip to the new clip
              currenttime = currentclip.end_offset 
              chain_position = 0 #start a new chain
              clips_df.loc[df_ind,['chain_position', 'filename', 'start_offset', 'end_offset', 'note']] = [chain_position, newfname, currentclip.offset, currentclip.end_offset, 'duplicated frame match'] #add to new chain
              clips_df.to_csv(outfname,index=False) #write current progress on chains
              chain_position = 1 #look for another clip to chain with it next
              df_ind += 1 #move to write in the next location
              flag_match = True #a frame match was found
              break #exit the for loop for finding a clip from the overlapclips that matches the frame
            else:
              #export the audio from the overlapping times of both clips, to temporary files
              _ = subprocess.run(' '.join(['ffmpeg', '-sseof', str(-1*sec_duration), '-i', '"'+fname+'"', '-vn', '"'+audio1+'"']),capture_output=True) #export from first clip starting from the time overlap starts
              _ = subprocess.run(' '.join(['ffmpeg', '-to', str(sec_duration), '-i', '"'+newfname+'"', '-vn', '"'+audio2+'"']),capture_output=True) #export from second clip until the time overlap ends
              sec_quiet = get_audio_quiet_sec_overlap(audio1, audio2) #get quietest second in both audio (to have the join where it is quietest)
              os.remove(audio1); os.remove(audio2) #delete temporary audio files
              
              clips_df.loc[df_ind-1,'to'] = sec_overlap+sec_quiet #update clip chain to have previous clip end at the quiet spot
              currentclip = newclip; fname=newfname #update the current clip to the new clip
              currenttime = currentclip.end_offset 
              clips_df.loc[df_ind,['chain_position', 'filename', 'start_offset', 'end_offset', 'ss']] = [chain_position, newfname, currentclip.offset, currentclip.end_offset, sec_quiet] #add to chain
              clips_df.to_csv(outfname,index=False) #write current progress on chains
              chain_position +=1 #look for another clip to chain with it next
              df_ind += 1 #move to write in the next location
              flag_match = True #a frame match was found
              break #exit the for loop for finding a clip from the overlapclips that matches the frame
        
        if not flag_match: #if no matching frames were found with the overlapping clips
          currenttime = offsetclips.loc[offsetclips.loc[offsetclips['offset']>currenttime,'offset'].idxmin(),'offset'] #go to the start time of first clip after the current time
          currentclip = None #no existing clip to check overlap with
          chain_position = 0 #start a new chain of clips
    
    toc() #tmp
  
  
  clips_df = clips_df.loc[:df_ind-1] #cut down to only the clips that were actually used
  clips_df.to_csv(outfname,index=False) #write final chains at the end



def make_clip_chains(clips_df, folder_chains):
  chain_starts = clips_df.loc[clips_df['chain_position']==0].index.to_list() + [clips_df.shape[0]]
  for num_chain in range(len(chain_starts)-1):
    chain = clips_df.loc[chain_starts[num_chain]:(chain_starts[num_chain+1]-1)] #get all clips in current chain
    
    command = 'ffmpeg' #start assembling command
    #note: yes I want to reencode even if it's just one clip in the chain because otherwise sometimes it does something weird
    for clip in chain.itertuples(index=False, name='clip'): #add specification of which part of each clip to include
      command += [' -ss '+str(clip.ss),''][pd.isna(clip.ss)]+[' -to '+str(clip.to),''][pd.isna(clip.to)]+ ' '.join(['','-i','"'+clip.filename+'"'])
    
    command += ' -filter_complex "' #start building complex filter
    for clip in range(chain.shape[0]): #choose the streams to send from each clip to the concat filter
      command += '[{}:v:0][{}:a:0]'.format(clip, clip)
    
    command += 'concat=n={}:v=1:a=1[outv][outa]"'.format(chain.shape[0]) #tell it the number of segments, and that there will be one video and audio stream per segment
    command += ' -map "[outv]" -map "[outa]" ' # use the results of the concat filter rather than the streams directly
    command += '"'+os.path.join(folder_chains,'chain_{}_{}-{}.mp4'.format(str(num_chain).zfill(len(str(len(chain_starts)))), chain['start_offset'].min(), int(chain['end_offset'].max())))+'"' #choose output file name
    
    _ = subprocess.run(command,capture_output=True)



def combine_all_clip_chains_1s_gaps(folder_chains, outfname):
  #get chains
  chains = pd.DataFrame(os.listdir(folder_chains),columns=['filename'])
  chains = chains.loc[(chains['filename'].str.endswith('.mp4'))&(chains['filename'].str.startswith('chain_'))].reset_index(drop=True)
  chains[['chain','times']] = chains['filename'].str.lstrip('chain_').str[:-4].str.split('_',expand=True)
  chains[['start_time','end_time']] = chains['times'].str.split('-',expand=True)
  chains['filename'] = chains['filename']
  chains[['start_time','end_time']] = chains[['start_time','end_time']].astype(int)
  chains['gap_before'] = (chains['start_time']-(chains['end_time'].shift(1).fillna(0))).astype(int)
  chains.loc[0,'gap_before']=0 #don't add gap before the first chain
  
  #make black filler video
  path_gap = os.path.join(folder_chains,'gap.mp4')
  #copy the first second of the first chain, with no audio, and set the video to be black. This makes the timebase, framerate, dimensions, etc. match.
  _ = subprocess.run(' '.join(['ffmpeg','-to','1','-i','"'+chains.loc[0,'filename']+'"','-vf','drawbox=color=black:t=fill','-an','"'+path_gap+'"']),capture_output=True,cwd=folder_chains)
  
  #make list of files to concatenate in order
  flist = []
  listpath = os.path.join(folder_chains,'concatlist.txt')
  for chain in chains.itertuples(index=False,name='chain'):
    if chain.gap_before > 0: #add a 1 second gap if there was a gap between this chain and the chain before it
      flist.append("file 'gap.mp4'")
    
    flist.append("file '"+chain.filename+"'")
  
  with open(listpath,'w') as f:
    f.writelines('\n'.join(flist))
  
  #run concatenation with copying
  _ = subprocess.run(' '.join(['ffmpeg','-f','concat','-safe','0','-i','"'+listpath+'"','-c','copy','"'+outfname+'"']), capture_output=True, cwd=folder_chains)
  
  
  #delete black filler video and file list
  os.remove(path_gap)
  os.remove(listpath)



def print_reconstruction_info(chainsfile, outfname, maxtime):
  duration = int(round(float(subprocess.check_output('ffprobe -i "'+outfname+'" -show_entries format=duration -v quiet -of csv="p=0"', stderr=subprocess.STDOUT))))
  clips = pd.read_csv(chainsfile).shape[0]
  print('')
  print('This is a restoration of the original VOD from approximately {clips:,} clips, and is missing some content (approximately {missingtime:,} seconds total).'.format(clips=clips, missingtime=(maxtime-duration+clips-1)))
  print('')



#run code
#read clip info
#note: eventually will need better regexes for more recent clips because the formatting of offset clips changed.
offsetclipsfile = os.path.join(outputfolderpath,('' if chosenstreamid is None else (str(chosenstreamid)+' '))+('' if chosenvodid is None else ('vod-'+str(chosenvodid)+' '))+'Offset Clips.csv')
chainsfile = os.path.join(outputfolderpath,('' if chosenstreamid is None else (str(chosenstreamid)+'_'))+('' if chosenvodid is None else ('vod-'+str(chosenvodid)+'_'))+'clip_chains.csv')

if not os.path.isfile(offsetclipsfile): #if the offset clips csv does not already exist, get material for it
  fullclipcsv = pd.read_csv(fpath)
  offsetclipcsv = fullclipcsv.loc[fullclipcsv['filename'].str.contains('offset')].reset_index(drop=True)
  offsetclipcsv[['vod','offset']] = offsetclipcsv['filename'].str.split('-offset-', expand=True)
  offsetclipcsv['offset'] = offsetclipcsv['offset'].str.removesuffix('.mp4').astype(np.int64)
  
  #get only clips that match the chosenstreamid or chosenvodid
  offsetclips = offsetclipcsv.loc[((False if chosenstreamid is None else offsetclipcsv['vod']==str(chosenstreamid)) | (False if chosenvodid is None else offsetclipcsv['vod']=='vod-'+str(chosenvodid)))].sort_values('offset').reset_index(drop=True)
  offsetclips = offsetclips.loc[~offsetclips.duplicated('download_url',keep='first')].reset_index(drop=True)

os.makedirs(outputfolderpath, exist_ok=True) #make output folder if it doesn't exist
os.makedirs(os.path.join(outputfolderpath,'rawclips'), exist_ok=True) #make raw clips folder in output folder if it doesn't exist
os.makedirs(os.path.join(outputfolderpath,'chains'), exist_ok=True) #make chains folder in output folder if it doesn't exist


tic()
if not os.path.isfile(offsetclipsfile): #if the offset clips csv does not already exist, make it
  tic(); make_time_offset_clips_csv(offsetclips, offsetclipsfile); toc('to create offset clips csv')

tic(); download_clips_and_calculate_chains(offsetclipsfile=offsetclipsfile, workingfolder=os.path.join(outputfolderpath,'rawclips'), outfname=chainsfile, starttime=0, maxtime=maxtime, check_unlisted=checkbetween); toc('to download clips and calculate chains')
tic(); make_clip_chains(clips_df=pd.read_csv(chainsfile), folder_chains=os.path.join(outputfolderpath,'chains')); toc('to make clip chains')
tic(); combine_all_clip_chains_1s_gaps(folder_chains=os.path.join(outputfolderpath,'chains'), outfname=os.path.join(outputfolderpath,outputtitle+'.mp4')); toc('to combine clip chains to a single video')
print_reconstruction_info(chainsfile=chainsfile, outfname=os.path.join(outputfolderpath,outputtitle+'.mp4'), maxtime=maxtime)

toc('total')
