# !pip install -U -q PyDrive
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
from google.colab import auth
from oauth2client.client import GoogleCredentials

import os
import subprocess
from pathlib import Path

__all__ = [
    'create_archive',
    'extract_archive',
    'GoogleDriveHandler'
]

def create_archive(zip_name, local_file_paths, temp_folder='/tmp', verbose=False):
    zip_name = '{0}/{1}'.format(temp_folder, zip_name) + '.tar.gz' * ('.tar.gz' not in zip_name)
    # Filter out non-existing files and directorys
    zipped_files = []
    for f in local_file_paths:
        if not Path(f).exists():
            print('file {0} does not exist, ignore it'.format(f))
        else:
            zipped_files.append(f)
    # Find common prefix to avoid a too many level folders
    common_prefix = ''
    for chars in zip(*zipped_files):
        if len(set(chars)) == 1:
            common_prefix += chars[0]
        else:
            break
    common_prefix = '/'.join(common_prefix.split('/')[:-1]) + '/'
    # Excuting tar.gz format compression
    L = len(common_prefix)
    zipped_files = ' '.join([f[L:] for f in zipped_files])
    cmd = 'tar -czvf {0} -C {1} {2}'.format(zip_name, common_prefix, zipped_files)
    if verbose: 
        print('ignore the common prefix {0}'.format(common_prefix))
        print('running shell command:','\n'+cmd)
    result = subprocess.check_output(cmd, shell=True).decode('utf-8')
    if verbose: print(result)
    # Return absolute path of the tar.gz file
    return zip_name

def extract_archive(zip_path, target_folder='./', verbose=False):
    cmd = 'tar -xf {0} -C {1}'.format(zip_path, target_folder)
    if verbose: print('running shell command:','\n'+cmd)
    result = subprocess.check_output(cmd, shell=True).decode('utf-8')
    if verbose: print(result)

class GoogleDriveHandler:
    def __init__(self):
        auth.authenticate_user()
        gauth = GoogleAuth()
        gauth.credentials = GoogleCredentials.get_application_default()
        self.drive = GoogleDrive(gauth)

    def path_to_id(self, rel_path, parent_folder_id='root'):
        rel_path = '/'.join(list(filter(len, rel_path.split('/'))))
        if rel_path == '':
            return parent_folder_id
        else:
            first, *rest = list(filter(len, rel_path.split('/')))
            file_dict = {f['title']:f for f in self.list_folder(parent_folder_id)}
            if first not in file_dict:
                raise Exception('{0} not exist'.format(first))
            else:
                return self.path_to_id('/'.join(rest), file_dict[first]['id'])
            
    def list_folder(self, root_folder_id='root', max_depth=0):
        query = "'{0}' in parents and trashed=false".format(root_folder_id)
        file_list, folder_type = [], 'application/vnd.google-apps.folder'
        for f in self.drive.ListFile({'q': query}).GetList():
            if f['mimeType'] == folder_type and max_depth > 0:
                file_list.append(
                    {
                        'title': f['title'], 
                        'id': f['id'], 
                        'link': f['alternateLink'], 
                        'mimeType': f['mimeType'],
                        'children': self.list_folder(f['id'], max_depth-1)
                    }
                )
            else:
                file_list.append(
                    {
                        'title':f['title'], 
                        'id': f['id'], 
                        'link':f['alternateLink'],
                        'mimeType': f['mimeType']
                    }
                )
        return file_list

    def create_folder(self, folder_name, parent_path=''):
        parent_folder_id = self.path_to_id(parent_path)
        folder_type = 'application/vnd.google-apps.folder'
        file_dict = {f['title']:f for f in self.list_folder(parent_folder_id)}
        if folder_name not in file_dict:
            folder_metadata = {
                'title' : folder_name, 
                'mimeType' : folder_type,
                'parents': [{'kind': 'drive#fileLink', 'id': parent_folder_id}]
            }
            folder = self.drive.CreateFile(folder_metadata)
            folder.Upload()
            return folder['id']
        else:
            if file_dict[folder_name]['mimeType'] != folder_type:
                raise Exception('{0} already exists as a file'.format(folder_name))
            else:
                print('{0} already exists'.format(folder_name))
            return file_dict[folder_name]['id']

    def upload(self, local_file_path, parent_path='', overwrite=True):
        parent_folder_id = self.path_to_id(parent_path)
        file_dict = {f['title']:f for f in self.list_folder(parent_folder_id)}
        file_name = local_file_path.split('/')[-1]
        if file_name in file_dict and overwrite:
            file_dict[file_name].Delete()
        file = self.drive.CreateFile(
            {
                'title': file_name, 
                'parents': [{'kind': 'drive#fileLink', 'id': parent_folder_id}]
            }
        )
        file.SetContentFile(local_file_path)
        file.Upload()
        return file['id']

    def download(self, local_file_path, target_path):
        target_id = self.path_to_id(target_path)
        file = self.drive.CreateFile({'id': target_id})
        file.GetContentFile(local_file_path)