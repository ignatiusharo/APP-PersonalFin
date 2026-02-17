import dropbox
import dropbox.files
import dropbox.exceptions
import os

class DropboxManager:
    def __init__(self, access_token=None, refresh_token=None, app_key=None, app_secret=None):
        if refresh_token and app_key and app_secret:
            # Using refresh token flow for persistent access
            self.dbx = dropbox.Dropbox(
                oauth2_refresh_token=refresh_token,
                app_key=app_key,
                app_secret=app_secret
            )
        else:
            # Fallback to simple access token
            self.dbx = dropbox.Dropbox(access_token)
    
    def check_connection(self):
        try:
            self.dbx.users_get_current_account()
            return True
        except Exception as e:
            print(f"Error connecting to Dropbox: {e}")
            return False

    def download_file(self, dropbox_path, local_path):
        """Downloads a file from Dropbox to local path atomically."""
        try:
            # Metadata check
            try:
                self.dbx.files_get_metadata(dropbox_path)
            except dropbox.exceptions.ApiError as e:
                if e.error.is_path() and e.error.get_path().is_not_found():
                    return False, "File not found in Dropbox"
                raise e

            # Download to memory first
            metadata, res = self.dbx.files_download(path=dropbox_path)
            content = res.content
            
            if not content:
                # If content is empty strings but metadata exists, we might decide to skip
                # for safety, or proceed if 0-byte files are allowed. 
                # For this app, we prefer safety.
                pass 

            # Ensure local directory exists
            os.makedirs(os.path.dirname(local_path), exist_ok=True)
            
            # Write only after full download
            with open(local_path, "wb") as f:
                f.write(content)
                
            return True, f"Downloaded {dropbox_path}"
        except Exception as e:
            return False, f"Error downloading: {str(e)}"

    def upload_file(self, local_path, dropbox_path):
        """Uploads a local file to Dropbox, overwriting if exists."""
        try:
            if not os.path.exists(local_path):
                return False, "Local file does not exist"

            with open(local_path, "rb") as f:
                self.dbx.files_upload(
                    f.read(),
                    dropbox_path,
                    mode=dropbox.files.WriteMode.overwrite
                )
            return True, f"Uploaded {dropbox_path}"
        except Exception as e:
            return False, f"Error uploading: {str(e)}"
