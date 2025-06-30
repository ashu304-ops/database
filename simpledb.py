import json
import os
import shlex
import csv
import threading
import tempfile
import time
import re
import shutil
import subprocess
import gzip
from collections import defaultdict
from threading import Timer
from contextlib import contextmanager
from copy import deepcopy
from pathlib import Path

class BTreeNode:
    def __init__(self, leaf=True):
        self.keys = []
        self.children = []
        self.leaf = leaf

class SimpleDB:
    def __init__(self, db_file='database.json.gz', files_dir='files'):
        self.db_file = Path(db_file)
        self.files_dir = Path(files_dir)
        self.db_file.parent.mkdir(parents=True, exist_ok=True)
        self.files_dir.mkdir(parents=True, exist_ok=True)
        self.store = {}
        self.value_index = defaultdict(list)
        self.inverted_index = defaultdict(list)
        self.btree_root = BTreeNode()
        self.transaction_log = []
        self.transaction_active = False
        self.lock = threading.RLock()
        self.current_user = None
        self.current_role = None
        self.users = {
            "admin": {"password": "admin123", "role": "admin"},
            "user": {"password": "user123", "role": "user"}
        }
        self.load()

    @contextmanager
    def timeout(self, seconds):
        def timeout_handler():
            raise TimeoutError("Operation timed out")
        timer = Timer(seconds, timeout_handler)
        timer.start()
        try:
            yield
        finally:
            timer.cancel()

    def login(self, username, password):
        print("Debug: Attempting to acquire lock for login")
        if not self.lock.acquire(timeout=5):
            print("Error: Timeout acquiring lock for login")
            raise TimeoutError("Timeout acquiring lock for login")
        try:
            print(f"Debug: Processing login for user '{username}'")
            if username in self.users and self.users[username]["password"] == password:
                self.current_user = username
                self.current_role = self.users[username]["role"]
                print(f"Debug: Logged in as {username} with role {self.current_role}")
                return f"Logged in as {username} ({self.current_role})"
            return "Error: Invalid username or password"
        finally:
            print("Debug: Releasing lock for login")
            self.lock.release()

    def logout(self):
        print("Debug: Attempting to acquire lock for logout")
        if not self.lock.acquire(timeout=5):
            print("Error: Timeout acquiring lock for logout")
            raise TimeoutError("Timeout acquiring lock for logout")
        try:
            print("Debug: Processing logout")
            if not self.current_user:
                return "Error: No user is logged in"
            username = self.current_user
            self.current_user = None
            self.current_role = None
            print("Debug: Logged out")
            return f"Logged out user {username}"
        finally:
            print("Debug: Releasing lock for logout")
            self.lock.release()

    def load(self):
        print("Debug: Attempting to acquire lock for load")
        if not self.lock.acquire(timeout=5):
            print("Error: Timeout acquiring lock for load")
            raise TimeoutError("Timeout acquiring lock for load")
        try:
            print("Debug: Entering load method")
            # Check for legacy database.json
            legacy_db_file = self.db_file.with_suffix('.json')
            if legacy_db_file.exists():
                print(f"Debug: Found legacy {legacy_db_file}, converting to {self.db_file}")
                with open(legacy_db_file, 'r') as f:
                    self.store = json.load(f)
                # Convert single file_path to file_paths
                for key, value in self.store.items():
                    if isinstance(value, dict) and "file_path" in value:
                        value["file_paths"] = [{"path": value["file_path"], "original_ext": os.path.splitext(value["file_path"])[1], "size": os.path.getsize(value["file_path"]), "original_size": os.path.getsize(value["file_path"]), "uploaded": time.strftime("%Y-%m-%dT%H:%M:%S"), "mime": self._get_mime_type(value["file_path"])}]
                        del value["file_path"]
                self.save()
                os.remove(legacy_db_file)
                print(f"Debug: Converted and removed {legacy_db_file}")
            if self.db_file.exists():
                try:
                    with self.timeout(5):
                        print("Debug: Opening compressed database file for reading")
                        with gzip.open(self.db_file, 'rt', encoding='utf-8') as f:
                            print("Debug: Reading compressed database file")
                            self.store = json.load(f)
                            if not isinstance(self.store, dict):
                                raise ValueError("Invalid database format")
                    print("Debug: Rebuilding indices")
                    self.rebuild_indices()
                    print(f"Loaded compressed database from {self.db_file}")
                except (json.JSONDecodeError, ValueError):
                    print(f"Error: {self.db_file} is corrupted. Starting with an empty database.")
                    self.store = {}
                    self.save()
                except TimeoutError:
                    print(f"Error: Timeout while reading {self.db_file}")
                except Exception as e:
                    print(f"Error loading database: {e}")
            else:
                print(f"No database file found. Starting with an empty database.")
            print("Debug: Exiting load method")
        finally:
            print("Debug: Releasing lock for load")
            self.lock.release()

    def save(self):
        print("Debug: Attempting to acquire lock for save")
        if not self.lock.acquire(timeout=5):
            print("Error: Timeout acquiring lock for save")
            raise TimeoutError("Timeout acquiring lock for save")
        try:
            print("Debug: Entering save method")
            db_dir = str(self.db_file.parent)
            if not os.access(db_dir, os.W_OK):
                print(f"Error: No write permission for directory {db_dir}")
                raise PermissionError(f"No write permission for directory {db_dir}")
            if self.db_file.exists() and not os.access(self.db_file, os.W_OK):
                print(f"Error: No write permission for {self.db_file}")
                raise PermissionError(f"No write permission for {self.db_file}")
            try:
                if self.db_file.exists() and self.db_file.stat().st_size > 0:
                    backup_file = str(self.db_file) + '.bak'
                    with self.timeout(5):
                        print("Debug: Creating backup")
                        with open(self.db_file, 'rb') as src, open(backup_file, 'wb') as dst:
                            dst.write(src.read())
                            print("Debug: Backup created")
                with self.timeout(5):
                    print("Debug: Creating temporary file")
                    with tempfile.NamedTemporaryFile('wb', delete=False, dir=db_dir, suffix='.gz') as temp_file:
                        print("Debug: Writing to compressed temporary file")
                        with gzip.GzipFile(fileobj=temp_file, mode='wb') as gz:
                            gz.write(json.dumps(self.store, indent=2).encode('utf-8'))
                        temp_file.flush()
                        os.fsync(temp_file.fileno())
                        temp_name = temp_file.name
                    print(f"Debug: Renaming {temp_name} to {self.db_file}")
                    os.replace(temp_name, self.db_file)
                    print(f"Saved compressed database to {self.db_file}")
                print("Debug: File write completed")
            except TimeoutError:
                print(f"Error: Timeout while writing to {self.db_file}")
                raise
            except Exception as e:
                print(f"Error saving database: {e}")
                raise
            print("Debug: Exiting save method")
        finally:
            print("Debug: Releasing lock for save")
            self.lock.release()

    def _get_mime_type(self, file_path):
        ext = os.path.splitext(file_path)[1].lower()
        return {'jpg': 'image/jpeg', 'jpeg': 'image/jpeg', 'png': 'image/png', 'gif': 'image/gif', 'pdf': 'application/pdf', 'doc': 'application/msword', 'docx': 'application/vnd.openxmlformats-officedocument.wordprocessingml.document'}.get(ext[1:], 'application/octet-stream')

    def upload(self, key, file_path):
        print("Debug: Attempting to acquire lock for upload")
        if not self.lock.acquire(timeout=5):
            print("Error: Timeout acquiring lock for upload")
            raise TimeoutError("Timeout acquiring lock for upload")
        try:
            print(f"Debug: Uploading file for key '{key}'")
            if not self.current_user or self.current_role != "admin":
                return "Error: Admin privileges required to upload files"
            if key not in self.store:
                return f"Error: Key '{key}' not found."
            if not os.path.exists(file_path):
                return f"Error: File '{file_path}' not found."
            allowed_extensions = {'.jpg', '.jpeg', '.png', '.gif', '.pdf', '.doc', '.docx'}
            file_ext = os.path.splitext(file_path)[1].lower()
            if file_ext not in allowed_extensions:
                return f"Error: Unsupported file type. Allowed: {', '.join(allowed_extensions)}"
            file_size = os.path.getsize(file_path)
            if file_size > 10 * 1024 * 1024:  # 10MB limit
                return "Error: File size exceeds 10MB limit"
            dest_filename = f"{key}_{os.path.basename(file_path)}.gz"
            dest_path = self.files_dir / dest_filename
            with open(file_path, 'rb') as src, gzip.open(dest_path, 'wb') as dst:
                shutil.copyfileobj(src, dst)
            compressed_size = os.path.getsize(dest_path)
            file_metadata = {
                "path": str(dest_path),
                "original_ext": file_ext,
                "size": compressed_size,
                "original_size": file_size,
                "uploaded": time.strftime("%Y-%m-%dT%H:%M:%S"),
                "mime": self._get_mime_type(file_path)
            }
            if "file_paths" not in self.store[key]:
                self.store[key]["file_paths"] = []
            self.store[key]["file_paths"].append(file_metadata)
            if self.transaction_active:
                self.transaction_log.append(("upload", key, None))
                print(f"Debug: Added upload operation to transaction log for {key}")
            if not self.transaction_active:
                print("Debug: Calling save method")
                self.save()
            print(f"Debug: Uploaded compressed file for {key}: {dest_path}")
            return f"Uploaded compressed file for {key}: {dest_path} (Original size: {file_size} bytes, Compressed size: {compressed_size} bytes)"
        finally:
            print("Debug: Releasing lock for upload")
            self.lock.release()

    def get_file(self, key, index=None):
        print("Debug: Attempting to acquire lock for get_file")
        if not self.lock.acquire(timeout=5):
            print("Error: Timeout acquiring lock for get_file")
            raise TimeoutError("Timeout acquiring lock for get_file")
        try:
            print(f"Debug: Retrieving file for key '{key}'")
            if not self.current_user:
                return "Error: Must be logged in to access files"
            if key not in self.store:
                return f"Error: Key '{key}' not found."
            if "file_paths" not in self.store[key] or not self.store[key]["file_paths"]:
                return f"Error: No files associated with key '{key}'."
            file_paths = self.store[key]["file_paths"]
            if index is not None:
                try:
                    index = int(index)
                    if index < 0 or index >= len(file_paths):
                        return f"Error: Invalid file index {index}. Available: 0 to {len(file_paths)-1}"
                    file_metadata = file_paths[index]
                    file_path = file_metadata["path"]
                    original_ext = file_metadata["original_ext"]
                    metadata_str = f"Size: {file_metadata['size']} bytes, Original size: {file_metadata['original_size']} bytes, Uploaded: {file_metadata['uploaded']}, MIME: {file_metadata['mime']}"
                except ValueError:
                    return "Error: Index must be an integer"
            else:
                return "\n".join(f"[{i}] {f['path']} (Size: {f['size']} bytes, Original size: {f['original_size']} bytes, Uploaded: {f['uploaded']}, MIME: {f['mime']})" for i, f in enumerate(file_paths))
            if not os.path.exists(file_path):
                return f"Error: File '{file_path}' not found on disk."
            temp_dir = tempfile.gettempdir()
            temp_filename = f"{key}_decompressed{original_ext}"
            temp_path = os.path.join(temp_dir, temp_filename)
            try:
                with gzip.open(file_path, 'rb') as src, open(temp_path, 'wb') as dst:
                    shutil.copyfileobj(src, dst)
                if os.name == 'nt':
                    subprocess.run(['start', '', temp_path], shell=True, check=True)
                elif os.name == 'posix':
                    viewers = [('xdg-open', ['xdg-open', temp_path]), ('eog', ['eog', temp_path]), ('firefox', ['firefox', temp_path])]
                    for viewer_name, viewer_cmd in viewers:
                        if shutil.which(viewer_name):
                            subprocess.run(viewer_cmd, check=True)
                            return f"Opened file: {file_path} with {viewer_name} ({metadata_str})"
                    return f"File path: {temp_path} (No viewer installed; install 'eog' or 'firefox' with 'sudo apt install eog firefox' or open manually) ({metadata_str})"
                return f"Opened file: {temp_path} ({metadata_str})"
            except Exception as e:
                return f"File path: {temp_path} (Open manually due to error: {e}) ({metadata_str})"
            finally:
                if os.path.exists(temp_path):
                    os.remove(temp_path)
        finally:
            print("Debug: Releasing lock for get_file")
            self.lock.release()

    def download_file(self, key, index=None, dest_path=None):
        print("Debug: Attempting to acquire lock for download_file")
        if not self.lock.acquire(timeout=5):
            print("Error: Timeout acquiring lock for download_file")
            raise TimeoutError("Timeout acquiring lock for download_file")
        try:
            print(f"Debug: Downloading file for key '{key}'")
            if not self.current_user:
                return "Error: Must be logged in to download files"
            if key not in self.store:
                return f"Error: Key '{key}' not found."
            if "file_paths" not in self.store[key] or not self.store[key]["file_paths"]:
                return f"Error: No files associated with key '{key}'."
            file_paths = self.store[key]["file_paths"]
            if index is not None:
                try:
                    index = int(index)
                    if index < 0 or index >= len(file_paths):
                        return f"Error: Invalid file index {index}. Available: 0 to {len(file_paths)-1}"
                    file_metadata = file_paths[index]
                    file_path = file_metadata["path"]
                    original_ext = file_metadata["original_ext"]
                except ValueError:
                    return "Error: Index must be an integer"
            else:
                if len(file_paths) > 1:
                    return f"Error: Multiple files exist. Specify an index (0 to {len(file_paths)-1}): {', '.join(f['path'] for f in file_paths)}"
                file_metadata = file_paths[0]
                file_path = file_metadata["path"]
                original_ext = file_metadata["original_ext"]
            if not os.path.exists(file_path):
                return f"Error: File '{file_path}' not found on disk."
            if not dest_path:
                dest_path = f"{key}_decompressed{original_ext}"
            try:
                with gzip.open(file_path, 'rb') as src, open(dest_path, 'wb') as dst:
                    shutil.copyfileobj(src, dst)
                return f"Downloaded decompressed file to: {dest_path}"
            except Exception as e:
                return f"Error downloading file: {e}"
        finally:
            print("Debug: Releasing lock for download_file")
            self.lock.release()

    def delete(self, key):
        print("Debug: Attempting to acquire lock for delete")
        if not self.lock.acquire(timeout=5):
            print("Error: Timeout acquiring lock for delete")
            raise TimeoutError("Timeout acquiring lock for delete")
        try:
            print(f"Debug: Attempting to delete key '{key}'")
            if not self.current_user or self.current_role != "admin":
                return "Error: Admin privileges required to delete"
            if key not in self.store:
                return f"Error: Key '{key}' not found."
            value = deepcopy(self.store[key])
            try:
                if self.transaction_active:
                    self.transaction_log.append(("delete", key, value))
                    print(f"Debug: Added delete operation to transaction log for {key}")
                if "file_paths" in value:
                    for file_metadata in value["file_paths"]:
                        if os.path.exists(file_metadata["path"]):
                            os.remove(file_metadata["path"])
                            print(f"Debug: Deleted compressed file {file_metadata['path']}")
                del self.store[key]
                print(f"Debug: Removed key '{key}' from store")
                value_key = json.dumps(value, sort_keys=True)
                if key in self.value_index[value_key]:
                    self.value_index[value_key].remove(key)
                    print(f"Debug: Removed key '{key}' from value_index for value {value_key}")
                    if not self.value_index[value_key]:
                        del self.value_index[value_key]
                        print(f"Debug: Deleted empty value_index entry for {value_key}")
                value_str = str(value).lower()
                words = set(re.findall(r'\b\w+\b', value_str))
                for word in words:
                    if word and key in self.inverted_index[word]:
                        self.inverted_index[word].remove(key)
                        if not self.inverted_index[word]:
                            del self.inverted_index[word]
                            print(f"Debug: Deleted empty inverted_index entry for word '{word}'")
                self.btree_root.keys = [(k, v) for k, v in self.btree_root.keys if k != key]
                print(f"Debug: Removed {key} from B-tree")
                if not self.transaction_active:
                    print("Debug: Calling save method")
                    self.save()
                print(f"Debug: Completed deletion of key '{key}'")
                return f"Deleted key: {key}"
            except Exception as e:
                print(f"Error during delete: {e}")
                return f"Error deleting key '{key}': {e}"
        finally:
            print("Debug: Releasing lock for delete")
            self.lock.release()

    def rollback(self):
        print("Debug: Attempting to acquire lock for rollback")
        if not self.lock.acquire(timeout=5):
            print("Error: Timeout acquiring lock for rollback")
            raise TimeoutError("Timeout acquiring lock for rollback")
        try:
            print("Debug: Rolling back transaction")
            if not self.current_user:
                return "Error: Must be logged in to rollback a transaction"
            if not self.transaction_active:
                return "Error: No transaction in progress."
            try:
                for op, key, old_value in self.transaction_log:
                    if op == "create":
                        if key in self.store:
                            value = self.store[key]
                            if isinstance(value, dict) and "file_paths" in value:
                                for file_metadata in value["file_paths"]:
                                    if os.path.exists(file_metadata["path"]):
                                        os.remove(file_metadata["path"])
                            del self.store[key]
                            value_key = json.dumps(old_value, sort_keys=True) if old_value is not None else None
                            if value_key and key in self.value_index[value_key]:
                                self.value_index[value_key].remove(key)
                                if not self.value_index[value_key]:
                                    del self.value_index[value_key]
                            value_str = str(old_value).lower() if old_value else ""
                            words = set(re.findall(r'\b\w+\b', value_str))
                            for word in words:
                                if word and key in self.inverted_index[word]:
                                    self.inverted_index[word].remove(key)
                                    if not self.inverted_index[word]:
                                        del self.inverted_index[word]
                            self.btree_root.keys = [(k, v) for k, v in self.btree_root.keys if k != key]
                    elif op == "update":
                        self.store[key] = old_value
                        self.rebuild_indices()
                    elif op == "delete":
                        self.store[key] = old_value
                        self.rebuild_indices()
                    elif op == "upload":
                        if key in self.store and isinstance(self.store[key], dict) and "file_paths" in self.store[key]:
                            if self.store[key]["file_paths"]:
                                last_file = self.store[key]["file_paths"].pop()
                                if os.path.exists(last_file["path"]):
                                    os.remove(last_file["path"])
                                if not self.store[key]["file_paths"]:
                                    del self.store[key]["file_paths"]
                                self.rebuild_indices()
                self.transaction_log = []
                self.transaction_active = False
                print("Debug: Transaction rolled back")
                return "Transaction rolled back."
            except Exception as e:
                print(f"Error during rollback: {e}")
                return f"Error rolling back transaction: {e}"
        finally:
            print("Debug: Releasing lock for rollback")
            self.lock.release()

    def parse_value(self, value):
        print(f"Debug: Parsing value '{value}'")
        try:
            parsed = json.loads(value)
            print(f"Debug: Parsed value to {parsed}")
            if isinstance(parsed, dict):
                expected_fields = {'Name', 'Age', 'Grade', 'Class', 'Subjects'}
                if all(field in parsed for field in expected_fields):
                    if not isinstance(parsed['Name'], str):
                        print(f"Debug: Validation failed: Name must be a string")
                        return value
                    if not isinstance(parsed['Age'], int):
                        print(f"Debug: Validation failed: Age must be an integer")
                        return value
                    if not isinstance(parsed['Grade'], (int, str)):
                        print(f"Debug: Validation failed: Grade must be an integer or string")
                        return value
                    if not isinstance(parsed['Class'], str):
                        print(f"Debug: Validation failed: Class must be a string")
                        return value
                    if not isinstance(parsed['Subjects'], list) or not all(isinstance(s, str) for s in parsed['Subjects']):
                        print(f"Debug: Validation failed: Subjects must be a list of strings")
                        return value
                    print("Debug: Student data validation passed")
            return parsed
        except json.JSONDecodeError as e:
            print(f"Debug: Value '{value}' not valid JSON: {e}. Treating as string")
            return value

    def create(self, key, value):
        print("Debug: Attempting to acquire lock for create")
        if not self.lock.acquire(timeout=5):
            print("Error: Timeout acquiring lock for create")
            raise TimeoutError("Timeout acquiring lock for create")
        try:
            print(f"Debug: Creating key '{key}' with value '{value}'")
            if not self.current_user:
                return "Error: Must be logged in to create a key"
            if key in self.store:
                print(f"Debug: Key '{key}' already exists")
                return f"Error: Key '{key}' already exists."
            parsed_value = self.parse_value(value)
            print(f"Debug: Parsed value: {parsed_value}")
            if self.transaction_active:
                self.transaction_log.append(("create", key, None))
                print(f"Debug: Added create operation to transaction log for {key}")
            self.store[key] = parsed_value
            print(f"Debug: Added {key} to store")
            value_key = json.dumps(parsed_value, sort_keys=True)
            self.value_index[value_key].append(key)
            print(f"Debug: Added {key} to value_index with value_key {value_key}")
            value_str = str(parsed_value).lower()
            words = set(re.findall(r'\b\w+\b', value_str))
            print(f"Debug: Extracted words for '{key}': {words}")
            for word in words:
                if word:
                    self.inverted_index[word].append(key)
                    print(f"Debug: Added {key} to inverted_index for word '{word}'")
            if isinstance(parsed_value, (int, float)):
                self.btree_insert(key, parsed_value)
                print(f"Debug: Added {key}:{parsed_value} to B-tree")
            if not self.transaction_active:
                print("Debug: Calling save method")
                self.save()
            print(f"Debug: Completed create for key '{key}'")
            return f"Inserted: {key} -> {json.dumps(parsed_value)}"
        finally:
            print("Debug: Releasing lock for create")
            self.lock.release()

    def read(self, key):
        print("Debug: Attempting to acquire lock for read")
        if not self.lock.acquire(timeout=5):
            print("Error: Timeout acquiring lock for read")
            raise TimeoutError("Timeout acquiring lock for read")
        try:
            print(f"Debug: Reading key '{key}'")
            if not self.current_user:
                return "Error: Must be logged in to read a key"
            if key not in self.store:
                return f"Error: Key '{key}' not found."
            return json.dumps(self.store[key])
        finally:
            print("Debug: Releasing lock for read")
            self.lock.release()

    def update(self, key, value):
        print("Debug: Attempting to acquire lock for update")
        if not self.lock.acquire(timeout=5):
            print("Error: Timeout acquiring lock for update")
            raise TimeoutError("Timeout acquiring lock for update")
        try:
            print(f"Debug: Updating key '{key}'")
            if not self.current_user:
                return "Error: Must be logged in to update a key"
            if key not in self.store:
                return f"Error: Key '{key}' not found."
            old_value = deepcopy(self.store[key])
            parsed_value = self.parse_value(value)
            if isinstance(old_value, dict) and "file_paths" in old_value:
                parsed_value["file_paths"] = old_value["file_paths"]
            if self.transaction_active:
                self.transaction_log.append(("update", key, old_value))
                print(f"Debug: Added update operation to transaction log for {key}")
            self.store[key] = parsed_value
            print(f"Debug: Updated store for {key}")
            old_value_key = json.dumps(old_value, sort_keys=True)
            if key in self.value_index[old_value_key]:
                self.value_index[old_value_key].remove(key)
                print(f"Debug: Removed {key} from value_index for {old_value_key}")
                if not self.value_index[old_value_key]:
                    del self.value_index[old_value_key]
                    print(f"Debug: Deleted empty value_index entry for {old_value_key}")
            value_key = json.dumps(parsed_value, sort_keys=True)
            self.value_index[value_key].append(key)
            print(f"Debug: Added {key} to value_index with value_key {value_key}")
            old_value_str = str(old_value).lower()
            old_words = set(re.findall(r'\b\w+\b', old_value_str))
            for word in old_words:
                if word and key in self.inverted_index[word]:
                    self.inverted_index[word].remove(key)
                    if not self.inverted_index[word]:
                        del self.inverted_index[word]
            new_value_str = str(parsed_value).lower()
            new_words = set(re.findall(r'\b\w+\b', new_value_str))
            print(f"Debug: Extracted words for '{key}': {new_words}")
            for word in new_words:
                if word:
                    self.inverted_index[word].append(key)
                    print(f"Debug: Added {key} to inverted_index for word '{word}'")
            self.btree_root.keys = [(k, v) for k, v in self.btree_root.keys if k != key]
            print(f"Debug: Removed {key} from B-tree")
            if isinstance(parsed_value, (int, float)):
                self.btree_insert(key, parsed_value)
                print(f"Debug: Added {key}:{parsed_value} to B-tree")
            if not self.transaction_active:
                print("Debug: Calling save method")
                self.save()
            print(f"Debug: Completed update for key '{key}'")
            return f"Updated: {key} -> {json.dumps(parsed_value)}"
        finally:
            print("Debug: Releasing lock for update")
            self.lock.release()

    def rebuild_indices(self):
        print("Debug: Attempting to acquire lock for rebuild_indices")
        if not self.lock.acquire(timeout=5):
            print("Error: Timeout acquiring lock for rebuild_indices")
            raise TimeoutError("Timeout acquiring lock for rebuild_indices")
        try:
            print("Debug: Rebuilding indices")
            self.value_index = defaultdict(list)
            self.inverted_index = defaultdict(list)
            self.btree_root = BTreeNode()
            numeric_pairs = []
            try:
                with self.timeout(30):
                    for key, value in self.store.items():
                        print(f"Debug: Processing key '{key}' with value {value}")
                        try:
                            value_key = json.dumps(value, sort_keys=True)
                            self.value_index[value_key].append(key)
                            print(f"Debug: Added {key} to value_index with value_key {value_key}")
                            value_str = str(value).lower()
                            print(f"Debug: Tokenized string for '{key}': {value_str}")
                            words = set(re.findall(r'\b\w+\b', value_str))
                            print(f"Debug: Extracted words for '{key}': {words}")
                            for word in words:
                                if word:
                                    self.inverted_index[word].append(key)
                                    print(f"Debug: Added {key} to inverted_index for word '{word}'")
                            if isinstance(value, (int, float)):
                                numeric_pairs.append((key, value))
                                print(f"Debug: Queued {key}:{value} for B-tree")
                        except Exception as e:
                            print(f"Error processing key '{key}': {e}")
                            continue
                    if numeric_pairs:
                        print("Debug: Performing batch B-tree insertion")
                        self.btree_root.keys.extend(numeric_pairs)
                        self.btree_root.keys.sort(key=lambda x: x[1])
                        print(f"Debug: Inserted {len(numeric_pairs)} numeric pairs into B-tree")
                print("Debug: Indices rebuilt")
            except TimeoutError:
                print("Error: Timeout while rebuilding indices")
                raise
            except Exception as e:
                print(f"Error rebuilding indices: {e}")
                raise
        finally:
            print("Debug: Releasing lock for rebuild_indices")
            self.lock.release()

    def btree_insert(self, key, value):
        print("Debug: Attempting to acquire lock for btree_insert")
        if not self.lock.acquire(timeout=5):
            print("Error: Timeout acquiring lock for btree_insert")
            raise TimeoutError("Timeout acquiring lock for btree_insert")
        try:
            print(f"Debug: Inserting {key}:{value} into B-tree")
            node = self.btree_root
            node.keys.append((key, value))
            node.keys.sort(key=lambda x: x[1])
            print(f"Debug: Inserted {key}:{value} into B-tree")
        finally:
            print("Debug: Releasing lock for btree_insert")
            self.lock.release()

    def btree_range_query(self, operator, query_value):
        print("Debug: Attempting to acquire lock for btree_range_query")
        if not self.lock.acquire(timeout=5):
            print("Error: Timeout acquiring lock for btree_range_query")
            raise TimeoutError("Timeout acquiring lock for btree_range_query")
        try:
            print(f"Debug: Performing B-tree range query with {operator} {query_value}")
            results = []
            for key, value in self.btree_root.keys:
                if (operator == ">" and value > query_value) or \
                   (operator == "<" and value < query_value):
                    results.append(key)
            print(f"Debug: Range query results: {results}")
            return results
        finally:
            print("Debug: Releasing lock for btree_range_query")
            self.lock.release()

    def get_sort_key(self, key, sort_field):
        value = self.store[key]
        if sort_field:
            if isinstance(value, dict) and sort_field in value:
                return value[sort_field]
            return float('-inf')  # Push invalid entries to the end
        return value if isinstance(value, (int, float, str)) else str(value)

    def find(self, query):
        print("Debug: Attempting to acquire lock for find")
        if not self.lock.acquire(timeout=5):
            print("Error: Timeout acquiring lock for find")
            raise TimeoutError("Timeout acquiring lock for find")
        try:
            print(f"Debug: Processing find query: {query}")
            if not self.current_user:
                return "Error: Must be logged in to perform a find query"
            parts = shlex.split(query)
            if len(parts) < 2:
                return "Invalid query. Use: = <value>, > <value>, < <value>, contains <value>, fulltext <value>, <field> = <value> [sortby <field>] [limit <n>]"
            sort_field = None
            limit = None
            main_query = parts[:]
            if "sortby" in parts:
                sort_idx = parts.index("sortby")
                if sort_idx + 1 < len(parts):
                    sort_field = parts[sort_idx + 1]
                    main_query = parts[:sort_idx] + parts[sort_idx + 2:]
            if "limit" in parts:
                limit_idx = parts.index("limit")
                if limit_idx + 1 < len(parts):
                    try:
                        limit = int(parts[limit_idx + 1])
                        if limit <= 0:
                            return "Error: Limit must be a positive integer"
                        main_query = parts[:limit_idx] + parts[limit_idx + 2:]
                    except ValueError:
                        return "Error: Limit must be a valid integer"
            field_or_op = main_query[0]
            query_value = " ".join(main_query[1:]) if len(main_query) > 1 else ""
            if field_or_op == "contains":
                parsed_query_value = query_value.strip('"\'')
                print(f"Debug: Using raw string value '{parsed_query_value}' for contains query")
            elif field_or_op == "fulltext":
                parsed_query_value = query_value.strip('"\'')
                print(f"Debug: Using raw string value '{parsed_query_value}' for fulltext query")
            else:
                parsed_query_value = self.parse_value(query_value)
            if field_or_op in ("=", ">", "<", "contains", "fulltext"):
                operator = field_or_op
                if operator == "=":
                    value_key = json.dumps(parsed_query_value, sort_keys=True)
                    results = self.value_index.get(value_key, [])
                elif operator in (">", "<"):
                    if not isinstance(parsed_query_value, (int, float)):
                        return "Error: Range queries only support numeric values."
                    results = self.btree_range_query(operator, parsed_query_value)
                elif operator == "contains":
                    if not isinstance(parsed_query_value, str):
                        return "Error: Contains queries only support string values."
                    results = []
                    for key, value in self.store.items():
                        value_str = str(value).lower()
                        print(f"Debug: Checking key '{key}' with value_str '{value_str}' for substring '{parsed_query_value.lower()}'")
                        if parsed_query_value.lower() in value_str:
                            results.append(key)
                elif operator == "fulltext":
                    if not isinstance(parsed_query_value, str):
                        return "Error: Fulltext queries only support string values."
                    words = set(parsed_query_value.lower().split())
                    if not words:
                        return "Error: Fulltext query cannot be empty."
                    result_sets = [set(self.inverted_index[word]) for word in words if word in self.inverted_index]
                    print(f"Debug: Inverted index for query terms: {[(word, self.inverted_index[word]) for word in words if word in self.inverted_index]}")
                    if not result_sets:
                        return "No keys found with the specified terms."
                    results = list(set.intersection(*result_sets))
                    print(f"Debug: Fulltext query results: {results}")
                else:
                    return "Invalid operator"
            else:
                field = field_or_op
                if len(main_query) < 3 or main_query[1] != "=":
                    return "Invalid field query. Use: <field> = <value> [sortby <field>] [limit <n>]"
                parsed_query_value = self.parse_value(" ".join(main_query[2:]))
                results = [key for key, value in self.store.items() if isinstance(value, dict) and field in value and value[field] == parsed_query_value]
            if sort_field:
                try:
                    results.sort(key=lambda key: self.get_sort_key(key, sort_field))
                except Exception as e:
                    return f"Error sorting results: {e}"
            if limit is not None:
                results = results[:limit]
            return "Found keys: " + ", ".join(results) if results else "No keys found with the specified condition."
        finally:
            print("Debug: Releasing lock for find")
            self.lock.release()

    def inspect_inverted_index(self, word=None):
        print("Debug: Attempting to acquire lock for inspect_inverted_index")
        if not self.lock.acquire(timeout=5):
            print("Error: Timeout acquiring lock for inspect_inverted_index")
            raise TimeoutError("Timeout acquiring lock for inspect_inverted_index")
        try:
            print("Debug: Inspecting inverted index")
            if not self.current_user:
                return "Error: Must be logged in to inspect inverted index"
            if word:
                return f"Inverted index for '{word}': {self.inverted_index.get(word.lower(), [])}"
            return "\n".join(f"Word '{w}': {keys}" for w, keys in self.inverted_index.items())
        finally:
            print("Debug: Releasing lock for inspect_inverted_index")
            self.lock.release()

    def join(self, key1, key2, field=None):
        print("Debug: Attempting to acquire lock for join")
        if not self.lock.acquire(timeout=5):
            print("Error: Timeout acquiring lock for join")
            raise TimeoutError("Timeout acquiring lock for join")
        try:
            print(f"Debug: Processing join: key1={key1}, key2={key2}, field={field}")
            if not self.current_user:
                return "Error: Must be logged in to perform a join"
            if key1 not in self.store or key2 not in self.store:
                return f"Error: One or both keys not found: {key1}, {key2}"
            value1 = self.store[key1]
            value2 = self.store[key2]
            if field:
                if not (isinstance(value1, dict) and isinstance(value2, dict)):
                    return f"Error: Field-based join requires dictionary values for {key1} and {key2}."
                if field not in value1 or field not in value2:
                    return f"Error: Field '{field}' not found in one or both values."
                if value1[field] == value2[field]:
                    return f"Join result on field '{field}': {key1}={json.dumps(value1)}, {key2}={json.dumps(value2)}"
                return f"No match: {key1} and {key2} have different values for field '{field}'."
            else:
                if value1 == value2:
                    return f"Join result: {key1}={json.dumps(value1)}, {key2}={json.dumps(value2)}"
                return f"No match: {key1} and {key2} have different values."
        finally:
            print("Debug: Releasing lock for join")
            self.lock.release()

    def max(self, key):
        print("Debug: Attempting to acquire lock for max")
        if not self.lock.acquire(timeout=5):
            print("Error: Timeout acquiring lock for max")
            raise TimeoutError("Timeout acquiring lock for max")
        try:
            print(f"Debug: Processing max for key '{key}'")
            if not self.current_user:
                return "Error: Must be logged in to perform max operation"
            if key not in self.store:
                return f"Error: Key '{key}' not found."
            value = self.store[key]
            if isinstance(value, (int, float)):
                return f"Max for {key}: {value}"
            elif isinstance(value, list) and all(isinstance(x, (int, float)) for x in value):
                return f"Max for {key}: {max(value)}"
            return f"Error: Max only supported for numbers or lists of numbers."
        finally:
            print("Debug: Releasing lock for max")
            self.lock.release()

    def min(self, key):
        print("Debug: Attempting to acquire lock for min")
        if not self.lock.acquire(timeout=5):
            print("Error: Timeout acquiring lock for min")
            raise TimeoutError("Timeout acquiring lock for min")
        try:
            print(f"Debug: Processing min for key '{key}'")
            if not self.current_user:
                return "Error: Must be logged in to perform min operation"
            if key not in self.store:
                return f"Error: Key '{key}' not found."
            value = self.store[key]
            if isinstance(value, (int, float)):
                return f"Min for {key}: {value}"
            elif isinstance(value, list) and all(isinstance(x, (int, float)) for x in value):
                return f"Min for {key}: {min(value)}"
            return f"Error: Min only supported for numbers or lists of numbers."
        finally:
            print("Debug: Releasing lock for min")
            self.lock.release()

    def sum(self, key):
        print("Debug: Attempting to acquire lock for sum")
        if not self.lock.acquire(timeout=5):
            print("Error: Timeout acquiring lock for sum")
            raise TimeoutError("Timeout acquiring lock for sum")
        try:
            print(f"Debug: Processing sum for key '{key}'")
            if not self.current_user:
                return "Error: Must be logged in to perform sum operation"
            if key not in self.store:
                return f"Error: Key '{key}' not found."
            value = self.store[key]
            if isinstance(value, (int, float)):
                return f"Sum for {key}: {value}"
            elif isinstance(value, list) and all(isinstance(x, (int, float)) for x in value):
                return f"Sum for {key}: {sum(value)}"
            return f"Error: Sum only supported for numbers or lists of numbers."
        finally:
            print("Debug: Releasing lock for sum")
            self.lock.release()

    def avg(self, key):
        print("Debug: Attempting to acquire lock for avg")
        if not self.lock.acquire(timeout=5):
            print("Error: Timeout acquiring lock for avg")
            raise TimeoutError("Timeout acquiring lock for avg")
        try:
            print(f"Debug: Processing avg for key '{key}'")
            if not self.current_user:
                return "Error: Must be logged in to perform avg operation"
            if key not in self.store:
                return f"Error: Key '{key}' not found."
            value = self.store[key]
            if isinstance(value, (int, float)):
                return f"Average for {key}: {value}"
            elif isinstance(value, list) and all(isinstance(x, (int, float)) for x in value):
                return f"Average for {key}: {sum(value) / len(value)}"
            return f"Error: Average only supported for numbers or lists of numbers."
        finally:
            print("Debug: Releasing lock for avg")
            self.lock.release()

    def list_all(self):
        print("Debug: Attempting to acquire lock for list_all")
        if not self.lock.acquire(timeout=5):
            print("Error: Timeout acquiring lock for list_all")
            raise TimeoutError("Timeout acquiring lock for list_all")
        try:
            print("Debug: Listing all key-value pairs")
            if not self.current_user:
                return "Error: Must be logged in to list all keys"
            if not self.store:
                return "Database is empty."
            return "\n".join(f"{key}: {json.dumps(value)}" for key, value in self.store.items())
        finally:
            print("Debug: Releasing lock for list_all")
            self.lock.release()

    def main(self):
        print("Simple Key-Value Database")
        print("=========================")
        print("Available Commands:")
        print("\nAuthentication:")
        print("  - login <username> <password> : Log in to the database (e.g., login admin admin123)")
        print("  - logout                     : Log out from the database (e.g., logout)")
        print("\nData Operations:")
        print("  - create <key> <value>    : Insert a new key-value pair (e.g., create student001 \"{\\\"Name\\\": \\\"Alice\\\", \\\"Age\\\": 15, \\\"Grade\\\": 10, \\\"Class\\\": \\\"A\\\", \\\"Subjects\\\": [\\\"Math\\\", \\\"Science\\\"]}\")")
        print("  - upload <key> <file_path> : Upload a compressed file for a key (admin only, e.g., upload student001 photo.jpg)")
        print("  - get_file <key> [index]  : Retrieve/open a file for a key (e.g., get_file student001 0)")
        print("  - download_file <key> [index] [dest_path] : Download a decompressed file (e.g., download_file student001 0 ~/Downloads/photo.jpg)")
        print("  - read <key>              : Retrieve the value for a key (e.g., read student001)")
        print("  - update <key> <value>    : Update the value for an existing key (e.g., update student001 \"{\\\"Name\\\": \\\"Alice\\\", \\\"Age\\\": 16, \\\"Grade\\\": 10, \\\"Class\\\": \\\"A\\\", \\\"Subjects\\\": [\\\"Math\\\", \\\"Science\\\"]}\")")
        print("  - delete <key>            : Delete a key-value pair and associated files (admin only, e.g., delete student001)")
        print("  - list                    : List all key-value pairs (e.g., list)")
        print("\nQuery Operations:")
        print("  - find = <value> [sortby <field>] [limit <n>] : Find keys with exact value match (e.g., find = \"{\\\"Name\\\": \\\"Alice\\\"}\" sortby Age limit 5)")
        print("  - find > <value> [sortby <field>] [limit <n>] : Find keys with numeric values greater than specified (e.g., find > 15 sortby Age limit 5)")
        print("  - find < <value> [sortby <field>] [limit <n>] : Find keys with numeric values less than specified (e.g., find < 20 sortby Age limit 5)")
        print("  - find contains <value> [sortby <field>] [limit <n>] : Find keys with values containing substring (e.g., find contains Math sortby Age limit 5)")
        print("  - find fulltext <value> [sortby <field>] [limit <n>] : Find keys with values containing all specified terms (e.g., find fulltext \"Math Science\" sortby Age limit 5)")
        print("  - find <field> = <value> [sortby <field>] [limit <n>] : Find keys with dictionary field matching value (e.g., find Class = A sortby Age limit 5)")
        print("  - join <key1> <key2> [field] : Join two keys by value or field (e.g., join student001 student002 Class)")
        print("\nAggregation Operations:")
        print("  - max <key>               : Get maximum value for a key (number or list, e.g., max student001)")
        print("  - min <key>               : Get minimum value for a key (number or list, e.g., min student001)")
        print("  - sum <key>               : Get sum of values for a key (number or list, e.g., sum student001)")
        print("  - avg <key>               : Get average of values for a key (number or list, e.g., avg student001)")
        print("\nData Import/Export:")
        print("  - import_csv <file>       : Import key-value pairs from CSV (admin only, e.g., import_csv students.csv)")
        print("  - export_csv <file>       : Export key-value pairs to CSV (admin only, e.g., export_csv output.csv)")
        print("\nTransaction Management:")
        print("  - begin                   : Start a transaction (e.g., begin)")
        print("  - commit                  : Commit the current transaction (e.g., commit)")
        print("  - rollback                : Roll back the current transaction (e.g., rollback)")
        print("\nDebugging:")
        print("  - inspect_index [word]    : Inspect the inverted index for a word or all words (e.g., inspect_index math)")
        print("\nOther Commands:")
        print("  - help                    : Display this help message")
        print("  - exit                    : Exit the database CLI")
        print("\nNotes:")
        print("  - Database is stored compressed as database.json.gz")
        print("  - Files are stored compressed in files/ (e.g., student001_photo.jpg.gz)")
        print("  - Must log in to perform operations (default users: admin/admin123, user/user123)")
        print("  - Admin role required for delete, upload, import_csv, and export_csv")
        print("  - Use quotes for values with spaces, e.g., \"{\\\"Name\\\": \\\"Alice\\\", \\\"Age\\\": 15, \\\"Grade\\\": 10, \\\"Class\\\": \\\"A\\\", \\\"Subjects\\\": [\\\"Math\\\", \\\"Science\\\"]}\")")
        print("  - CSV format: key,value (value can be JSON, number, or string)")
        print("  - Type 'help' to display this message again.")
        print("=========================")

        while True:
            try:
                print("Debug: Waiting for input...")
                input_str = input("> ")
                print(f"Debug: Received input: {input_str}")
                parts = shlex.split(input_str)
                print(f"Debug: Parsed parts: {parts}")
            except ValueError as e:
                print(f"Error parsing input: {e}")
                continue
            except EOFError:
                print("EOF received, exiting...")
                break
            except KeyboardInterrupt:
                print("Keyboard interrupt received, exiting...")
                break

            if not parts:
                continue

            command = parts[0].lower()
            try:
                if command == "login" and len(parts) == 3:
                    print(self.login(parts[1], parts[2]))
                elif command == "logout" and len(parts) == 1:
                    print(self.logout())
                elif command == "create" and len(parts) >= 3:
                    key = parts[1]
                    value = " ".join(parts[2:])
                    print(self.create(key, value))
                elif command == "upload" and len(parts) == 3:
                    print(self.upload(parts[1], parts[2]))
                elif command == "get_file" and len(parts) in (2, 3):
                    index = parts[2] if len(parts) == 3 else None
                    print(self.get_file(parts[1], index))
                elif command == "download_file" and len(parts) in (3, 4):
                    index = parts[2] if len(parts) == 4 else None
                    dest_path = parts[3] if len(parts) == 4 else None
                    print(self.download_file(parts[1], index, dest_path))
                elif command == "read" and len(parts) == 2:
                    print(self.read(parts[1]))
                elif command == "update" and len(parts) >= 3:
                    key = parts[1]
                    value = " ".join(parts[2:])
                    print(self.update(key, value))
                elif command == "delete" and len(parts) == 2:
                    print(self.delete(parts[1]))
                elif command == "find" and len(parts) >= 2:
                    print(self.find(" ".join(parts[1:])))
                elif command == "join" and len(parts) in (3, 4):
                    field = parts[3] if len(parts) == 4 else None
                    print(self.join(parts[1], parts[2], field))
                elif command == "max" and len(parts) == 2:
                    print(self.max(parts[1]))
                elif command == "min" and len(parts) == 2:
                    print(self.min(parts[1]))
                elif command == "sum" and len(parts) == 2:
                    print(self.sum(parts[1]))
                elif command == "avg" and len(parts) == 2:
                    print(self.avg(parts[1]))
                elif command == "import_csv" and len(parts) == 2:
                    print(self.import_csv(parts[1]))
                elif command == "export_csv" and len(parts) == 2:
                    print(self.export_csv(parts[1]))
                elif command == "begin" and len(parts) == 1:
                    print(self.begin())
                elif command == "commit" and len(parts) == 1:
                    print(self.commit())
                elif command == "rollback" and len(parts) == 1:
                    print(self.rollback())
                elif command == "list" and len(parts) == 1:
                    print(self.list_all())
                elif command == "inspect_index" and len(parts) in (1, 2):
                    word = parts[1] if len(parts) == 2 else None
                    print(self.inspect_inverted_index(word))
                elif command == "exit" and len(parts) == 1:
                    print("Exiting...")
                    break
                elif command == "help" and len(parts) == 1:
                    print("Simple Key-Value Database")
                    print("=========================")
                    print("Available Commands:")
                    print("\nAuthentication:")
                    print("  - login <username> <password> : Log in to the database (e.g., login admin admin123)")
                    print("  - logout                     : Log out from the database (e.g., logout)")
                    print("\nData Operations:")
                    print("  - create <key> <value>    : Insert a new key-value pair (e.g., create student001 \"{\\\"Name\\\": \\\"Alice\\\", \\\"Age\\\": 15, \\\"Grade\\\": 10, \\\"Class\\\": \\\"A\\\", \\\"Subjects\\\": [\\\"Math\\\", \\\"Science\\\"]}\")")
                    print("  - upload <key> <file_path> : Upload a compressed file for a key (admin only, e.g., upload student001 photo.jpg)")
                    print("  - get_file <key> [index]  : Retrieve/open a file for a key (e.g., get_file student001 0)")
                    print("  - download_file <key> [index] [dest_path] : Download a decompressed file (e.g., download_file student001 0 ~/Downloads/photo.jpg)")
                    print("  - read <key>              : Retrieve the value for a key (e.g., read student001)")
                    print("  - update <key> <value>    : Update the value for an existing key (e.g., update student001 \"{\\\"Name\\\": \\\"Alice\\\", \\\"Age\\\": 16, \\\"Grade\\\": 10, \\\"Class\\\": \\\"A\\\", \\\"Subjects\\\": [\\\"Math\\\", \\\"Science\\\"]}\")")
                    print("  - delete <key>            : Delete a key-value pair and associated files (admin only, e.g., delete student001)")
                    print("  - list                    : List all key-value pairs (e.g., list)")
                    print("\nQuery Operations:")
                    print("  - find = <value> [sortby <field>] [limit <n>] : Find keys with exact value match (e.g., find = \"{\\\"Name\\\": \\\"Alice\\\"}\" sortby Age limit 5)")
                    print("  - find > <value> [sortby <field>] [limit <n>] : Find keys with numeric values greater than specified (e.g., find > 15 sortby Age limit 5)")
                    print("  - find < <value> [sortby <field>] [limit <n>] : Find keys with numeric values less than specified (e.g., find < 20 sortby Age limit 5)")
                    print("  - find contains <value> [sortby <field>] [limit <n>] : Find keys with values containing substring (e.g., find contains Math sortby Age limit 5)")
                    print("  - find fulltext <value> [sortby <field>] [limit <n>] : Find keys with values containing all specified terms (e.g., find fulltext \"Math Science\" sortby Age limit 5)")
                    print("  - find <field> = <value> [sortby <field>] [limit <n>] : Find keys with dictionary field matching value (e.g., find Class = A sortby Age limit 5)")
                    print("  - join <key1> <key2> [field] : Join two keys by value or field (e.g., join student001 student002 Class)")
                    print("\nAggregation Operations:")
                    print("  - max <key>               : Get maximum value for a key (number or list, e.g., max student001)")
                    print("  - min <key>               : Get minimum value for a key (number or list, e.g., min student001)")
                    print("  - sum <key>               : Get sum of values for a key (number or list, e.g., sum student001)")
                    print("  - avg <key>               : Get average of values for a key (number or list, e.g., avg student001)")
                    print("\nData Import/Export:")
                    print("  - import_csv <file>       : Import key-value pairs from CSV (admin only, e.g., import_csv students.csv)")
                    print("  - export_csv <file>       : Export key-value pairs to CSV (admin only, e.g., export_csv output.csv)")
                    print("\nTransaction Management:")
                    print("  - begin                   : Start a transaction (e.g., begin)")
                    print("  - commit                  : Commit the current transaction (e.g., commit)")
                    print("  - rollback                : Roll back the current transaction (e.g., rollback)")
                    print("\nDebugging:")
                    print("  - inspect_index [word]    : Inspect the inverted index for a word or all words (e.g., inspect_index math)")
                    print("\nOther Commands:")
                    print("  - help                    : Display this help message")
                    print("  - exit                    : Exit the database CLI")
                    print("\nNotes:")
                    print("  - Database is stored compressed as database.json.gz")
                    print("  - Files are stored compressed in files/ (e.g., student001_photo.jpg.gz)")
                    print("  - Must log in to perform operations (default users: admin/admin123, user/user123)")
                    print("  - Admin role required for delete, upload, import_csv, and export_csv")
                    print("  - Use quotes for values with spaces, e.g., \"{\\\"Name\\\": \\\"Alice\\\", \\\"Age\\\": 15, \\\"Grade\\\": 10, \\\"Class\\\": \\\"A\\\", \\\"Subjects\\\": [\\\"Math\\\", \\\"Science\\\"]}\")")
                    print("  - CSV format: key,value (value can be JSON, number, or string)")
                    print("  - Type 'help' to display this message again.")
                    print("=========================")
            except Exception as e:
                print(f"Error in command processing: {e}")

if __name__ == "__main__":
    db = SimpleDB()
    db.main()