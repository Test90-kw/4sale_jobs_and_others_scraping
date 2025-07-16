# Import required libraries
import os  # For file path operations
import json  # For parsing JSON if needed
from google.oauth2.service_account import Credentials  # For service account authentication
from googleapiclient.discovery import build  # To build the Drive API service
from googleapiclient.http import MediaFileUpload  # To upload files to Google Drive
from datetime import datetime, timedelta  # For working with dates

class SavingOnDriveJobs:
    def __init__(self, credentials_dict):
        # Store the credentials dictionary passed during object creation
        self.credentials_dict = credentials_dict
        
        # Define the required scope for accessing Google Drive
        self.scopes = ['https://www.googleapis.com/auth/drive']
        
        # Will hold the authenticated Drive API service instance
        self.service = None
        
        # ID of the parent folder where date-named subfolders will be created
        self.parent_folder_id = '1-aLXkhIP2_DEcpSJ1xw1ukTGsbyf7HQG'  # Your parent folder ID

    def authenticate(self):
        """Authenticate with Google Drive API."""
        try:
            print("Authenticating with Google Drive...")
            # Use the service account credentials to authenticate
            creds = Credentials.from_service_account_info(self.credentials_dict, scopes=self.scopes)
            # Build the Drive API service object
            self.service = build('drive', 'v3', credentials=creds)
            print("Authentication successful.")
        except Exception as e:
            # Print and raise error if authentication fails
            print(f"Authentication error: {e}")
            raise

    def get_folder_id(self, folder_name):
        """Get folder ID by name within the parent folder."""
        try:
            # Create a query to find a folder by name under the specified parent folder
            query = (f"name='{folder_name}' and "
                     f"'{self.parent_folder_id}' in parents and "
                     f"mimeType='application/vnd.google-apps.folder' and "
                     f"trashed=false")
            
            # Execute the query and fetch matching folders
            results = self.service.files().list(
                q=query,
                spaces='drive',
                fields='files(id, name)'
            ).execute()
            
            # Get the folder list from the results
            files = results.get('files', [])
            if files:
                # If found, return the first matching folder's ID
                print(f"Folder '{folder_name}' found with ID: {files[0]['id']}")
                return files[0]['id']
            else:
                # If no folder found, return None
                print(f"Folder '{folder_name}' does not exist.")
                return None
        except Exception as e:
            # Handle and print error
            print(f"Error getting folder ID: {e}")
            return None

    def create_folder(self, folder_name):
        """Create a new folder in the parent folder."""
        try:
            print(f"Creating folder '{folder_name}'...")
            # Metadata for the new folder
            file_metadata = {
                'name': folder_name,
                'mimeType': 'application/vnd.google-apps.folder',
                'parents': [self.parent_folder_id]  # Place under parent folder
            }
            # Create the folder and return its ID
            folder = self.service.files().create(
                body=file_metadata,
                fields='id'
            ).execute()
            print(f"Folder '{folder_name}' created with ID: {folder.get('id')}")
            return folder.get('id')
        except Exception as e:
            # Handle and propagate error
            print(f"Error creating folder: {e}")
            raise

    def upload_file(self, file_name, folder_id):
        """Upload a single file to Google Drive."""
        try:
            print(f"Uploading file: {file_name}")
            # Prepare file metadata including destination folder
            file_metadata = {
                'name': os.path.basename(file_name),
                'parents': [folder_id]
            }
            # Prepare media file upload object
            media = MediaFileUpload(file_name, resumable=True)
            # Upload the file
            file = self.service.files().create(
                body=file_metadata,
                media_body=media,
                fields='id'
            ).execute()
            print(f"File '{file_name}' uploaded with ID: {file.get('id')}")
            return file.get('id')
        except Exception as e:
            # Handle and propagate upload error
            print(f"Error uploading file: {e}")
            raise

    def save_files(self, files):
        """Save files to Google Drive in a folder named after yesterday's date."""
        try:
            # Generate folder name using yesterday's date
            yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
            
            # Try to get existing folder or create a new one
            folder_id = self.get_folder_id(yesterday)
            if not folder_id:
                folder_id = self.create_folder(yesterday)
            
            # Upload all given files to the folder
            for file_name in files:
                self.upload_file(file_name, folder_id)
            
            print(f"All files uploaded successfully to Google Drive folder '{yesterday}'.")
        except Exception as e:
            # Handle any errors during save
            print(f"Error saving files: {e}")
            raise
