
#####################################################
import os
import re
import datetime
import sys
#%%
def get_latest_date_for_data_file(data_folder_path, file_name):
    #get list of all files in the data folder
    all_files = os.listdir(data_folder_path)
    #filter for only the files with the correct file extension
    all_files = [file for file in all_files if file_name in file]
    #drop any files with no date in the name
    all_files = [file for file in all_files if re.search(r'\d{8}', file)]
    #get the date from the file name
    all_files = [re.search(r'\d{8}', file).group() for file in all_files]
    #convert the dates to datetime objects
    all_files = [datetime.datetime.strptime(date, '%Y%m%d') for date in all_files]
    #get the latest date
    try:
        latest_date = max(all_files)
    except ValueError:
        print('No files found for ' + file_name)
        sys.exit()
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

#%%
#use os.walk to grab all folders and files in the root folder path, including all subsequent subfolders
import os
def move_archived_files_to_different_folder(all_archived_files = 'all_archived_files', root_folder_path = './',ignored_folders_and_files = ['.vscode', '.git','env_jupyter','env_no_jupyter','.gitattributes', '.gitignore', '.gitkeep', '__pycache__']):

    file_list = []
    for root, dirs, files in os.walk(root_folder_path):
        #remove any folders that are in the ignored folders list
        dirs[:] = [d for d in dirs if d not in ignored_folders_and_files]
        #remove any files that are in the ignored files list
        files[:] = [f for f in files if f not in ignored_folders_and_files]
        #remove all files that are
        #connect the dir to the file name with forward slashes
        files = [os.path.join(root, file) for file in files]
        #make  slashes forward slashes
        files = [file.replace('\\', '/') for file in files]
        #add the files to the file list
        file_list.extend(files)

    #remove any files that dont contain the word archive
    file_list = [file for file in file_list if 'archive' in file]

    #add the folder name for this project to the start of the file path
    #get folder name for this project
    project_folder_name = os.path.basename(os.getcwd())

    #now we have a list of all files in the project that are in an archive folder. We will move these to a new folder outside the project, and save them with the same folder structure as they had in this project, so if the folder is missing in the new folder, create it
    for file in file_list:
        #get the folder path for the file
        folder_path = os.path.dirname(file)
        #add the project name to the folder path
        folder_path = folder_path.replace('./', f'./{project_folder_name}/')
        #get the new folder path for the file
        new_folder_path = folder_path.replace('./', f'../{all_archived_files}/')
        #if the folders for these paths dont exist, create them
        if not os.path.exists(new_folder_path):
            os.makedirs(new_folder_path)

        #check if the file is already there, if so then rename that file to '_old', then move the new file to the new folder path. IF there is already a file that has _old in the name, then ask the user if they want to overwrite it
        #create copy of name
        file_name = file.replace('./', f'../{all_archived_files}/{project_folder_name}/')
        #check if the file is already there. If it is, then rename it to _old
        if os.path.exists(file_name):
            #if the file is already there, then rename it to _old
            #get base name 
            new_file_name = os.path.basename(file_name)
            # and remove extension
            extension = os.path.splitext(new_file_name)[1]
            new_file_name = os.path.splitext(new_file_name)[0]
            #  then add _old
            new_file_name = new_file_name + '_old'
            #  then add extension
            new_file_name = new_file_name + extension
            #  then add the folder path
            new_file_name = os.path.dirname(file_name) + '/' + new_file_name
            #if this new file name already exists, then ask the user if they want to overwrite it, otherwise return an error
            if os.path.exists(new_file_name):
                user_input = input(f'The file {new_file_name} already exists. Do you want to overwrite it? (y/n)')
                if user_input == 'y':
                    #rename the file
                    os.rename(file_name, new_file_name)
                else:
                    #error
                    raise Exception(f'The file {new_file_name} already exists. Please fix the issue and try again')
            else:
                #rename the file
                os.rename(file_name, new_file_name)

        #NOW move the ORIGINAL file to the new folder path   
        os.rename(file, file_name)

# move_archived_files_to_different_folder(all_archived_files = 'all_archived_files', root_folder_path = './plotting_output',ignored_folders_and_files = ['.vscode', '.git','env_jupyter','.gitattributes', '.gitignore', '.gitkeep', '__pycache__','env_no_jupyter'])




#%%
def move_all_files_to_archive(folder,files_to_ignore = ['.vscode', '.git','env_jupyter','.gitattributes', '.gitignore', '.gitkeep', '__pycache__', 'Readme.md']):
    #move all files in a folder to an archive folder in that folder. This will walk through all subfolders in the folder and move all files to an archive folder in each subfolder
    #get list of all files in the folder, including their paths and all subsequent subfolders
    all_files = [os.path.join(root, file) for root, dirs, files in os.walk(folder) for file in files]
    #replace double backslashes with forward slashes
    all_files = [file.replace('\\', '/') for file in all_files]
    #ignore any files that are in the archive folder
    all_files = [file for file in all_files if 'archive' not in file]
    #ignore any files that are in the files_to_ignore list
    all_files = [file for file in all_files if os.path.basename(file) not in files_to_ignore]

    #put all files in the archive folder, creating the archive folder if it doesnt exist
    for file in all_files:
        #get the folder path for the file
        folder_path = os.path.dirname(file)
        #get the new folder path for the file
        new_folder_path = os.path.join(folder_path, 'archive')
        #if new_folder_path doesnt exist, create it
        if not os.path.exists(new_folder_path):
            os.makedirs(new_folder_path)
        #replace double backslashes with forward slashes
        new_file_path = os.path.join(new_folder_path, os.path.basename(file))
        #replace double backslashes with forward slashes
        new_file_path = new_file_path.replace('\\', '/')
        #move the file to the new folder path
        os.rename(file,new_file_path )

# move_all_files_to_archive('./plotting_output', files_to_ignore = ['.vscode', '.git','env_jupyter','.gitattributes', '.gitignore', '.gitkeep', '__pycache__', 'Readme.md'])
#%%
#Now put gitkeeps into all folders that arent archive folders
def add_gitkeep_to_all_archive_folders(folder = './',folders_to_ignore = ['.vscode', '.git','env_jupyter','env_no_jupyter','.gitattributes', '.gitignore', '.gitkeep', '__pycache__']):
    #get list of all files in the folder, including their paths and all subsequent subfolders
    all_paths = [os.path.join(root, file) for root, dirs, files in os.walk(folder) for file in files]
    #replace double backslashes with forward slashes
    all_paths = [file.replace('\\', '/') for file in all_paths]
    #get all folders in the folder
    all_folders = [os.path.dirname(path) for path in all_paths]
    #remove any folders that contain words in the folders_to_ignore list
    all_folders = [folder for folder in all_folders if not any(word in folder for word in folders_to_ignore)]
    #remove any folders that are archive folders
    all_folders = [folder for folder in all_folders if 'archive' not in folder]

    #add gitkeeps to all folders if they dont already have a gitkeep
    for folder in all_folders:
        #if the folder doesnt have a gitkeep, add one
        if not os.path.exists(os.path.join(folder, '.gitkeep')):
            #create a gitkeep file in the new folder
            with open(os.path.join(folder, '.gitkeep'), 'w') as f:
                f.write('')

# add_gitkeep_to_all_archive_folders('./')
#%%
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






# all_archived_files = 'all_archived_files' 
# root_folder_path = './'
# ignored_folders_and_files = ['.vscode', '.git','env_jupyter','env_no_jupyter','.gitattributes', '.gitignore', '.gitkeep', '__pycache__']
# file_list = []
# for root, dirs, files in os.walk(root_folder_path):
#     #remove any folders that are in the ignored folders list
#     dirs[:] = [d for d in dirs if d not in ignored_folders_and_files]
#     #remove any files that are in the ignored files list
#     files[:] = [f for f in files if f not in ignored_folders_and_files]
#     #remove all files that are
#     #connect the dir to the file name with forward slashes
#     files = [os.path.join(root, file) for file in files]
#     #make  slashes forward slashes
#     files = [file.replace('\\', '/') for file in files]
#     #add the files to the file list
#     file_list.extend(files)

# #remove any files that dont contain the word archive
# file_list = [file for file in file_list if 'archive' in file]

# #add the folder name for this project to the start of the file path
# #get folder name for this project
# project_folder_name = os.path.basename(os.getcwd())

# #now we have a list of all files in the project that are in an archive folder. We will move these to a new folder outside the project, and save them with the same folder structure as they had in this project, so if the folder is missing in the new folder, create it
# for file in file_list:
#     #get the folder path for the file
#     folder_path = os.path.dirname(file)
#     #add the project name to the folder path
#     folder_path = folder_path.replace('./', f'./{project_folder_name}/')
#     #get the new folder path for the file
#     new_folder_path = folder_path.replace('./', f'../{all_archived_files}/')
#     #if the folders for these paths dont exist, create them
#     if not os.path.exists(new_folder_path):
#         os.makedirs(new_folder_path)
#     #check if the file is already there, if so then rename that file to '_old', then move the new file to the new folder path. IF there is already a file that has _old in the name, then ask the user if they want to overwrite it
#     if os.path.exists(file.replace('./', f'../{all_archived_files}/')):
#         #move the file to the new folder path
#         os.rename(file, file.replace('./', f'../{all_archived_files}/{project_folder_name}/'))
