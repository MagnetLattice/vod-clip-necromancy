import cv2
import numpy as np
import os
import time

#Find if a video has a frame identical to the one passed
#Inputs:
# frame: the frame we're looking for
# fname2: filepath to video clip we're looking for the frame in
#Outputs:
# -1 if video does not contain frame
# the frame of overlap in if video does contain frame
def containsoverlap(frame, videopath):
  cap = cv2.VideoCapture(videopath)
  success, image = cap.read()
  while success: #go through all frames of video
    if np.array_equal(image,frame):
      return cap.get(cv2.CAP_PROP_POS_FRAMES)
    success, image = cap.read()
  
  return -1


#Find if a video has a frame identical to any of the ones passed
#Inputs:
# frames: the frames we're looking for
# fname2: filepath to video clip we're looking for the frame in
#Outputs:
# -1 if video does not contain any of the frames
# the index of the frame in the array that matches if it does
def containsoverlapany(frames, videopath):
  cap = cv2.VideoCapture(videopath)
  success, image = cap.read()
  while success: #go through all frames of video
    if any(np.array_equal(image, frame) for frame in frames): #if the frame is equal to any of the frames we're looking for
      return  np.where([np.array_equal(image, frame) for frame in frames])[0][0]
    success, image = cap.read()
  
  return -1


#Take a folder of videos and a set of before and after videos, and look for videos in the folder that overlap the first frame of the after-videos or the last frame of the before-videos
#Inputs:
# knownfolder: folder of the before and after videos (videos with known times)
# fnamesbefore: list of filenames in the known folder whose last frame we're looking for matches on
# fnamesafter: list of filenames in the known folder whose first frame we're looking for matches on
# searchfolder: folder of videos to look for matches in
#Outputs: list of all videos that match on either of these, and what they matched on
def findgapfills(knownfolder, fnamesbefore, fnamesafter, searchfolder):
  frames = [] #list of frames
  
  for fname in fnamesbefore: #for each of the videos whose last frame we're looking for matches on
    cap = cv2.VideoCapture(os.path.join(knownfolder,fname))
    cap.set(cv2.CAP_PROP_POS_FRAMES, cap.get(cv2.CAP_PROP_FRAME_COUNT)-1) #get to right before the last frame
    success, frame = cap.read() #get the last frame
    cap.release()
    frames.append(frame)
  
  for fname in fnamesafter: #for each of the videos whose first frame we're looking for matches on 
    cap = cv2.VideoCapture(knownfolder+fname)
    success, frame = cap.read() #get the first frame
    cap.release()
    frames.append(frame)
  
  knownfiles = fnamesbefore+fnamesafter
  
  matches = [] #list to contain any matches found
  
  for file in os.listdir(searchfolder): #for each file in the searchfolder
    if not file.endswith('.mp4'):
      continue #skip non-mp4s for now
    
    print('checking ' + file)
    t0 = time.time()
    result = containsoverlapany(frames, os.path.join(searchfolder, file)) #check for any matches
    print('check time: ' + str(round(time.time()-t0)) + 's')
    
    if result != -1: #if there was a match
      matches.append(fnames[result], file) #save the file name and what it matched on
      print(file + ' overlaps ' + fnames[result])
    
  return matches


t0 = time.time(); matches = findgapfills(knownfolder = 'D:\\Videos\\Minecraft Videos\\DSMP\\Necromancy\\2020-09-23 - Tubbo Offset Clips', fnamesbefore = ['682424050-00800-RelentlessGeniusDiamondCmonBruh.mp4', '682424050-01740-HedonisticCulturedBubbleteaPanicVis.mp4', '682424050-02958-SpookyUnusualConsoleShadyLulu.mp4', '682424050-03306-SteamyBlatantAlmondJKanStyle.mp4', '682424050-04522-HelpfulLaconicSnakeTwitchRPG.mp4', '682424050-04968-AgreeablePluckyPterodactylDendiFace.mp4', '682424050-05028-InspiringTangiblePorcupineSaltBae.mp4', '682424050-06164-JoyousPunchySalsifyDancingBaby.mp4'], fnamesafter = ['682424050-00832-KitschyAbrasiveGiraffeHoneyBadger.mp4', '682424050-01776-ClumsySolidKuduFeelsBadMan.mp4', '682424050-02994-BlightedDifferentStarThisIsSparta.mp4', '682424050-03356-CrepuscularFineDiamond4Head.mp4', '682424050-04568-CourageousHandsomeMoonHassanChop.mp4', '682424050-05000-ManlyStrangeMooseKeepo.mp4', '682424050-05060-PiliableAbrasiveHawkDogFace.mp4', '682424050-06214-FunnyAbnegateBeaverFunRun.mp4'], searchfolder = 'D:\\Videos\\Minecraft Videos\\DSMP\\Necromancy\\2020-09-23-to-30 Tubbo Nonoffset Clips\\2020-09-23 - The Election FALLOUT'); print('total time: ' + str(round(time.time()-t0)) + ' s')
