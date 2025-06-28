import json
import os
import shlex
import csv
import threading
import tempfile
import time
import re
from collections import defaultdict
from threading import Timer
from contextlib import contextmanager
from copy import deepcopy

class BTreeNode:
    def __init__(self, leaf=True):
        self.keys = []
        self.children = []
        self.leaf = leaf

class SimpleDB:
    def __init__(self, db_file='database.json'):
        self.db_file = db_file
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
            if os.path.exists(self.db_file):
                try:
                    with self.timeout(5):
                        print("Debug: Opening database file for reading")
                        with open(self.db_file, 'r') as f:
                            print("Debug: Reading database file")
                            self.store = json.load(f)
                            if not isinstance(self.store, dict):
                                raise ValueError("Invalid database format")
                    print("Debug: Rebuilding indices")
                    self.rebuild_indices()
                    print(f"Loaded database from {self.db_file}")
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
            db_dir = os.path.dirname(self.db_file) or '.'
            if not os.access(db_dir, os.W_OK):
                print(f"Error: No write permission for directory {db_dir}")
                raise PermissionError(f"No write permission for directory {db_dir}")
            if os.path.exists(self.db_file) and not os.access(self.db_file, os.W_OK):
                print(f"Error: No write permission for {self.db_file}")
                raise PermissionError(f"No write permission for {self.db_file}")
            try:
                if os.path.exists(self.db_file) and os.path.getsize(self.db_file) > 0:
                    backup_file = self.db_file + '.bak'
                    with self.timeout(5):
                        print("Debug: Creating backup")
                        with open(self.db_file, 'r') as src, open(backup_file, 'w') as dst:
                            dst.write(src.read())
                            print("Debug: Backup created")
                with self.timeout(5):
                    print("Debug: Creating temporary file")
                    with tempfile.NamedTemporaryFile('w', delete=False, dir=db_dir) as temp_file:
                        print("Debug: Writing to temporary file")
                        json.dump(self.store, temp_file, indent=2)
                        temp_file.flush()
                        print("Debug: Flushing temporary file")
                        os.fsync(temp_file.fileno())
                        temp_name = temp_file.name
                    print(f"Debug: Renaming {temp_name} to {self.db_file}")
                    os.replace(temp_name, self.db_file)
                    print(f"Saved database to {self.db_file}")
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

    def begin(self):
        print("Debug: Attempting to acquire lock for begin")
        if not self.lock.acquire(timeout=5):
            print("Error: Timeout acquiring lock for begin")
            raise TimeoutError("Timeout acquiring lock for begin")
        try:
            print("Debug: Starting transaction")
            if not self.current_user:
                return "Error: Must be logged in to start a transaction"
            if self.transaction_active:
                return "Error: Transaction already in progress."
            self.transaction_active = True
            self.transaction_log = []
            print("Debug: Transaction started")
            return "Transaction started."
        finally:
            print("Debug: Releasing lock for begin")
            self.lock.release()

    def commit(self):
        print("Debug: Attempting to acquire lock for commit")
        if not self.lock.acquire(timeout=5):
            print("Error: Timeout acquiring lock for commit")
            raise TimeoutError("Timeout acquiring lock for commit")
        try:
            print("Debug: Committing transaction")
            if not self.current_user:
                return "Error: Must be logged in to commit a transaction"
            if not self.transaction_active:
                return "Error: No transaction in progress."
            try:
                print("Debug: Calling save method for commit")
                self.save()
                self.transaction_log = []
                self.transaction_active = False
                print("Debug: Transaction committed")
                return "Transaction committed."
            except Exception as e:
                print(f"Error during commit: {e}")
                return f"Error committing transaction: {e}"
        finally:
            print("Debug: Releasing lock for commit")
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

    def import_csv(self, csv_file):
        print("Debug: Attempting to acquire lock for import_csv")
        if not self.lock.acquire(timeout=5):
            print("Error: Timeout acquiring lock for import_csv")
            raise TimeoutError("Timeout acquiring lock for import_csv")
        try:
            print(f"Debug: Importing from CSV file: {csv_file}")
            if not self.current_user:
                return "Error: Must be logged in to import CSV"
            if self.current_role != "admin":
                return "Error: Import CSV operation requires admin privileges"
            start_time = time.time()
            if not os.path.exists(csv_file):
                return f"Error: CSV file '{csv_file}' not found."
            try:
                with self.timeout(5):
                    print("Debug: Opening CSV file for reading")
                    with open(csv_file, 'r', newline='') as f:
                        reader = csv.DictReader(f)
                        if 'key' not in reader.fieldnames or 'value' not in reader.fieldnames:
                            return "Error: CSV must have 'key' and 'value' columns."
                        batch_count = 0
                        for row in reader:
                            key = row['key']
                            value = row['value']
                            print(f"Debug: Importing key '{key}' with value '{value}'")
                            result = self.create(key, value)
                            print(result)
                            batch_count += 1
                            if batch_count % 100 == 0 and not self.transaction_active:
                                print("Debug: Saving batch")
                                self.save()
                        if not self.transaction_active:
                            print("Debug: Saving final batch")
                            self.save()
                print(f"Debug: Completed CSV import from {csv_file} in {time.time() - start_time:.2f} seconds")
                return f"Imported data from {csv_file}"
            except TimeoutError:
                return f"Error: Timeout while reading {csv_file}"
            except Exception as e:
                return f"Error importing CSV: {e}"
        finally:
            print("Debug: Releasing lock for import_csv")
            self.lock.release()

    def export_csv(self, csv_file):
        print("Debug: Attempting to acquire lock for export_csv")
        if not self.lock.acquire(timeout=5):
            print("Error: Timeout acquiring lock for export_csv")
            raise TimeoutError("Timeout acquiring lock for export_csv")
        try:
            print(f"Debug: Exporting to CSV file: {csv_file}")
            if not self.current_user:
                return "Error: Must be logged in to export CSV"
            if self.current_role != "admin":
                return "Error: Export CSV operation requires admin privileges"
            start_time = time.time()
            try:
                with self.timeout(5):
                    print("Debug: Opening CSV file for writing")
                    with open(csv_file, 'w', newline='') as f:
                        writer = csv.writer(f)
                        writer.writerow(['key', 'value'])
                        for key, value in self.store.items():
                            writer.writerow([key, json.dumps(value)])
                print(f"Debug: Completed CSV export to {csv_file} in {time.time() - start_time:.2f} seconds")
                return f"Exported data to {csv_file}"
            except TimeoutError:
                return f"Error: Timeout while writing {csv_file}"
            except Exception as e:
                return f"Error exporting CSV: {e}"
        finally:
            print("Debug: Releasing lock for export_csv")
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

    def delete(self, key):
        print("Debug: Attempting to acquire lock for delete")
        if not self.lock.acquire(timeout=5):
            print("Error: Timeout acquiring lock for delete")
            raise TimeoutError("Timeout acquiring lock for delete")
        try:
            print(f"Debug: Attempting to delete key '{key}'")
            if not self.current_user:
                return "Error: Must be logged in to delete a key"
            if self.current_role != "admin":
                return "Error: Delete operation requires admin privileges"
            if key not in self.store:
                return f"Error: Key '{key}' not found."
            value = deepcopy(self.store[key])
            value_key = json.dumps(value, sort_keys=True)
            try:
                if self.transaction_active:
                    self.transaction_log.append(("delete", key, value))
                    print(f"Debug: Added delete operation to transaction log for {key}")
                del self.store[key]
                print(f"Debug: Removed key '{key}' from store")
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
                    results = list(set.intersection(*result_sets))  # Convert set to list for sorting
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

def main():
    db = SimpleDB()
    print("Simple Key-Value Database")
    print("=========================")
    print("Available Commands:")
    print("\nAuthentication:")
    print("  - login <username> <password> : Log in to the database (e.g., login admin admin123)")
    print("  - logout                     : Log out from the database (e.g., logout)")
    print("\nData Operations:")
    print("  - create <key> <value>    : Insert a new key-value pair (e.g., create student001 \"{\\\"Name\\\": \\\"Alice\\\", \\\"Age\\\": 15, \\\"Grade\\\": 10, \\\"Class\\\": \\\"A\\\", \\\"Subjects\\\": [\\\"Math\\\", \\\"Science\\\"]}\")")
    print("  - read <key>              : Retrieve the value for a key (e.g., read student001)")
    print("  - update <key> <value>    : Update the value for an existing key (e.g., update student001 \"{\\\"Name\\\": \\\"Alice\\\", \\\"Age\\\": 16, \\\"Grade\\\": 10, \\\"Class\\\": \\\"A\\\", \\\"Subjects\\\": [\\\"Math\\\", \\\"Science\\\"]}\")")
    print("  - delete <key>            : Delete a key-value pair (admin only, e.g., delete student001)")
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
    print("  - Must log in to perform operations (default users: admin/admin123, user/user123)")
    print("  - Admin role required for delete, import_csv, and export_csv")
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
                print(db.login(parts[1], parts[2]))
            elif command == "logout" and len(parts) == 1:
                print(db.logout())
            elif command == "create" and len(parts) >= 3:
                key = parts[1]
                value = " ".join(parts[2:])
                print(db.create(key, value))
            elif command == "read" and len(parts) == 2:
                print(db.read(parts[1]))
            elif command == "update" and len(parts) >= 3:
                key = parts[1]
                value = " ".join(parts[2:])
                print(db.update(key, value))
            elif command == "delete" and len(parts) == 2:
                print(db.delete(parts[1]))
            elif command == "find" and len(parts) >= 2:
                print(db.find(" ".join(parts[1:])))
            elif command == "join" and len(parts) in (3, 4):
                field = parts[3] if len(parts) == 4 else None
                print(db.join(parts[1], parts[2], field))
            elif command == "max" and len(parts) == 2:
                print(db.max(parts[1]))
            elif command == "min" and len(parts) == 2:
                print(db.min(parts[1]))
            elif command == "sum" and len(parts) == 2:
                print(db.sum(parts[1]))
            elif command == "avg" and len(parts) == 2:
                print(db.avg(parts[1]))
            elif command == "import_csv" and len(parts) == 2:
                print(db.import_csv(parts[1]))
            elif command == "export_csv" and len(parts) == 2:
                print(db.export_csv(parts[1]))
            elif command == "begin" and len(parts) == 1:
                print(db.begin())
            elif command == "commit" and len(parts) == 1:
                print(db.commit())
            elif command == "rollback" and len(parts) == 1:
                print(db.rollback())
            elif command == "list" and len(parts) == 1:
                print(db.list_all())
            elif command == "inspect_index" and len(parts) in (1, 2):
                word = parts[1] if len(parts) == 2 else None
                print(db.inspect_inverted_index(word))
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
                print("  - read <key>              : Retrieve the value for a key (e.g., read student001)")
                print("  - update <key> <value>    : Update the value for an existing key (e.g., update student001 \"{\\\"Name\\\": \\\"Alice\\\", \\\"Age\\\": 16, \\\"Grade\\\": 10, \\\"Class\\\": \\\"A\\\", \\\"Subjects\\\": [\\\"Math\\\", \\\"Science\\\"]}\")")
                print("  - delete <key>            : Delete a key-value pair (admin only, e.g., delete student001)")
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
                print("  - Must log in to perform operations (default users: admin/admin123, user/user123)")
                print("  - Admin role required for delete, import_csv, and export_csv")
                print("  - Use quotes for values with spaces, e.g., \"{\\\"Name\\\": \\\"Alice\\\", \\\"Age\\\": 15, \\\"Grade\\\": 10, \\\"Class\\\": \\\"A\\\", \\\"Subjects\\\": [\\\"Math\\\", \\\"Science\\\"]}\")")
                print("  - CSV format: key,value (value can be JSON, number, or string)")
                print("  - Type 'help' to display this message again.")
                print("=========================")
        except Exception as e:
            print(f"Error in command processing: {e}")

if __name__ == "__main__":
    main()