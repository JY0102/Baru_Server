import io
import os
import DB
import cv2
import numpy as np
import mediapipe as mp

mp_pose = mp.solutions.pose
base_path = 'C:/Baru/'
base_dir = 'data'  # 최상위 폴더
output_ext = '.npy' # .npy 형태로 저장

def extract_pose_sequence(video_path):
    cap = cv2.VideoCapture(video_path)
    pose = mp_pose.Pose(static_image_mode=False)
    landmarks_seq = []

    while cap.isOpened():
        ret, frame = cap.read()
        if not ret:
            break

        rgb = cv2.cvtColor(frame, cv2.COLOR_BGR2RGB)
        results = pose.process(rgb)

        if results.pose_landmarks:
            landmarks = np.array([[lm.x, lm.y, lm.z] for lm in results.pose_landmarks.landmark])
        else:
            landmarks = np.zeros((33, 3))  # 포즈 인식 실패 시 zero 저장
        landmarks_seq.append(landmarks)

    cap.release()
    pose.close()
    return np.array(landmarks_seq)

def convert_all_videos_to_npy():
    

    for action in os.listdir(base_dir):
        action_dir = os.path.join(base_dir, action)
        if not os.path.isdir(action_dir):
            continue

        for file in os.listdir(action_dir):
            if file.endswith('.mp4'):
                
                #region mp4 -> DB
                video_path = os.path.join(action_dir, file)
                landmarks_seq = extract_pose_sequence(video_path)
                
                buffer = io.BytesIO()
                np.save(buffer, landmarks_seq)
                buffer.seek(0)

                arr_bytes = buffer.read()
                print(f'DB저장 시작 : {os.path.splitext(file)[0]}')
                # DB 저장
                DB.Insert_Exercise(os.path.splitext(file)[0] , arr_bytes)
                #endregion
    
                
# 분할 컴파일 과정에서 호출시에만 실행되게 하기
if __name__ == '__main__':
    convert_all_videos_to_npy()
