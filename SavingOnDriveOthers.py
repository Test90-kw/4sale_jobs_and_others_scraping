# Required imports
import os  # To handle file path operations
import json  # To work with JSON-formatted credentials
from google.oauth2.service_account import Credentials  # For authenticating via a service account
from googleapiclient.discovery import build  # For building Google API clients
from googleapiclient.http import MediaFileUpload  # For uploading files to Google Drive
from datetime import datetime, timedelta  # For generating date-based folder names

class SavingOnDriveOthers:
    def __init__(self, credentials_dict):
        # Store the provided credentials dictionary for later use
        self.credentials_dict = credentials_dict

        # Define the necessary OAuth scope for full access to Google Drive
        self.scopes = ['https://www.googleapis.com/auth/drive']

        # Will hold the authenticated Google Drive service object
        self.service = None

        # ID of the main parent folder in which subfolders will be created
        self.parent_folder_id = '15z0undajkFeOFuAs2xOj93ZnC0p_9y4e'  # Your parent folder ID

    def authenticate(self):
        """Authenticate with Google Drive API."""
        try:
            print("Authenticating with Google Drive...")
            # Create credentials using the provided service account information and defined scopes
            creds = Credentials.from_service_account_info(self.credentials_dict, scopes=self.scopes)
            # Build the Drive v3 API client with those credentials
            self.service = build('drive', 'v3', credentials=creds)
            print("Authentication successful.")
        except Exception as e:
            # Log and re-raise any authentication errors
            print(f"Authentication error: {e}")
            raise

    def get_folder_id(self, folder_name):
        """Get folder ID by name within the parent folder."""
        try:
            # Formulate a query to find a folder with the given name inside the specified parent folder
            query = (f"name='{folder_name}' and "
                     f"'{self.parent_folder_id}' in parents and "
                     f"mimeType='application/vnd.google-apps.folder' and "
                     f"trashed=false")
            
            # Execute the query and fetch the result
            results = self.service.files().list(
                q=query,
                spaces='drive',
                fields='files(id, name)'
            ).execute()
            
            # Extract the list of matching files (folders)
            files = results.get('files', [])
            if files:
                # Folder found, return its ID
                print(f"Folder '{folder_name}' found with ID: {files[0]['id']}")
                return files[0]['id']
            else:
                # Folder not found, return None
                print(f"Folder '{folder_name}' does not exist.")
                return None
        except Exception as e:
            # Log any errors that occur during folder lookup
            print(f"Error getting folder ID: {e}")
            return None

    def create_folder(self, folder_name):
        """Create a new folder in the parent folder."""
        try:
            print(f"Creating folder '{folder_name}'...")
            # Define metadata for the new folder including its parent
            file_metadata = {
                'name': folder_name,
                'mimeType': 'application/vnd.google-apps.folder',
                'parents': [self.parent_folder_id]
            }
            # Call the API to create the folder
            folder = self.service.files().create(
                body=file_metadata,
                fields='id'
            ).execute()
            # Return the ID of the newly created folder
            print(f"Folder '{folder_name}' created with ID: {folder.get('id')}")
            return folder.get('id')
        except Exception as e:
            # Raise any errors that occur during folder creation
            print(f"Error creating folder: {e}")
            raise

    def upload_file(self, file_name, folder_id):
        """Upload a single file to Google Drive."""
        try:
            print(f"Uploading file: {file_name}")
            # Prepare file metadata including name and destination folder
            file_metadata = {
                'name': os.path.basename(file_name),  # Extract just the file name
                'parents': [folder_id]  # Upload to specified folder
            }
            # Create a media object for the file to upload
            media = MediaFileUpload(file_name, resumable=True)
            # Use the Drive API to upload the file
            file = self.service.files().create(
                body=file_metadata,
                media_body=media,
                fields='id'
            ).execute()
            print(f"File '{file_name}' uploaded with ID: {file.get('id')}")
            return file.get('id')
        except Exception as e:
            # Raise any errors during file upload
            print(f"Error uploading file: {e}")
            raise

    def save_files(self, files):
        """Save files to Google Drive in a folder named after yesterday's date."""
        try:
            # Format yesterday's date as folder name (YYYY-MM-DD)
            yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')

            # Try to get the folder for yesterday; create it if it doesn't exist
            folder_id = self.get_folder_id(yesterday)
            if not folder_id:
                folder_id = self.create_folder(yesterday)
            
            # Upload each file to the folder
            for file_name in files:
                self.upload_file(file_name, folder_id)
            
            print(f"All files uploaded successfully to Google Drive folder '{yesterday}'.")
        except Exception as e:
            # Handle and re-raise any error during the upload process
            print(f"Error saving files: {e}")
            raise
