
#####################################################
import os
import re
import datetime
#%%
def get_latest_date_for_data_file(data_folder_path, file_name):
    #get list of all files in the data folder
    all_files = os.listdir(data_folder_path)
    #filter for only the files with the correct file extension
    all_files = [file for file in all_files if file_name in file]
    #get the date from the file name
    all_files = [re.search(r'\d{8}', file).group() for file in all_files]
    #convert the dates to datetime objects
    all_files = [datetime.datetime.strptime(date, '%Y%m%d') for date in all_files]
    #get the latest date
    latest_date = max(all_files)
    #convert the latest date to a string
    latest_date = latest_date.strftime('%Y%m%d')
    return latest_date
#%%

def move_archive_to_different_folder(new_archive_folder_path, root_folder_path):
    #move the archive files stored throughout this project to a new folder outside this project

    #get list of all files in an archive folder in this project, including their paths and all subsequent subfolders
    all_files = [os.path.join(root, file) for root, dirs, files in os.walk(root_folder_path) for file in files]
    # grab only the files that are in an archive folder
    all_files = [file for file in all_files if 'archive' in file]

    #move these files to the new_archive_folder_path. Save them with the same folder structure as they had in this project, so if the folder is missing in the new folder, create it
    for file in all_files:
        #get the folder path for the file
        folder_path = os.path.dirname(file)
        #get the new folder path for the file
        new_folder_path = folder_path.replace(root_folder_path, new_archive_folder_path)
        #if the folders for these paths dont exist, create them
        if not os.path.exists(new_folder_path):
            os.makedirs(new_folder_path)
        #move the file to the new folder path
        os.rename(file, file.replace(root_folder_path, new_archive_folder_path))
    return all_files

#%%

# root_folder_path = './'
# archive_folder_path = '../all_archived_files/'
# move_archive_to_different_folder(archive_folder_path, root_folder_path)

# #%%
# #use os.walk to grab all folders and files in the root folder path, including all subsequent subfolders
# from pathlib import Path

# def generate_all_files(root: Path, only_files: bool = True, ignored_folders: list = []):
#     for p in root.rglob("*"):
#         if only_files and not p.is_file():
#             continue
#         if p in ignored_folders:
#             continue
#         yield p


# for p in generate_all_files(Path("."), only_files=False,ignored_folders = ['.vscode', '.git','env_jupyter','.gitattributes', '.gitignore', '.gitkeep', '__pycache__']):
#     print(p)




# #%%
# # all_files = [os.path.join(dirs, file) for root, dirs, files in os.walk(root_folder_path) for file in files]
# #use os.walk to grab all folders and files in the root folder path, including all subsequent subfolders
# all_files = [os.path.join(root, file) for root, dirs, files in os.walk(root_folder_path,topdown=True) for file in files]
# #get subfolders in the root folder path
# # grab only the files that are in an archive folder
# all_files = [file for file in all_files if 'archive' in file]
# #remove any that contain these folders:
# ignored_folders = ['.vscode', '.git','env_jupyter','.gitattributes', '.gitignore', '.gitkeep', '__pycache__']
# all_files = [file for file in all_files if not any(ignored_folder in file for ignored_folder in ignored_folders)]

# #%%
# #move these files to the new_archive_folder_path. Save them with the same folder structure as they had in this project, so if the folder is missing in the new folder, create it
# for file in all_files:
#     #get the folder path for the file
#     folder_path = os.path.dirname(file)
#     #get the new folder path for the file
#     new_folder_path = folder_path.replace(root_folder_path, new_archive_folder_path)
#     #if the folders for these paths dont exist, create them
#     if not os.path.exists(new_folder_path):
#         os.makedirs(new_folder_path)
#     #move the file to the new folder path
#     os.rename(file, file.replace(root_folder_path, new_archive_folder_path))

# # %%


# import os
# import shutil

# # Set the source and destination directories
# src_dir = "/path/to/project/folder"
# dst_dir = "/path/to/other/project/folder"

# # Recursively find all /archive/ directories under src_dir
# for dirpath, dirnames, filenames in os.walk(src_dir):
#     if "archive" in dirnames:
#         archive_dir = os.path.join(dirpath, "archive")
#         # Copy the entire directory structure to dst_dir
#         shutil.copytree(archive_dir, dst_dir + archive_dir.replace(src_dir, ""))
#         dirnames.remove("archive")
