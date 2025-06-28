SimpleDB: A Simple Key-Value Database
Overview
SimpleDB is a lightweight, file-based key-value database written in Python. It stores data as key-value pairs, where keys are strings and values can be strings, numbers, or JSON objects (like dictionaries or lists). The database saves data to a JSON file and supports features like searching, transactions, CSV import/export, and user authentication. It's designed for simple applications needing a basic database without complex setup.
Features

Basic Operations: Create, read, update, and delete key-value pairs.
User Authentication: Log in with a username and password; some actions (like delete) require admin privileges.
Search and Query: Find keys by exact value, range (e.g., > or < for numbers), substring, full-text search, or specific fields in JSON objects.
Sorting and Limiting: Sort query results by a field or value and limit the number of results returned.
Transactions: Group operations to ensure they all succeed or fail together.
CSV Import/Export: Import data from or export data to CSV files (admin-only).
Indexing: Uses indexes for fast searches and a B-tree for numeric range queries.
Thread Safety: Handles multiple users safely with locks.

Requirements

Python 3.6 or higher
No external libraries are required (uses standard Python libraries: json, os, shlex, csv, threading, tempfile, time, collections).

Installation

Clone or Download:
Copy the simple_db.py file to your project folder.


Ensure Write Permissions:
Make sure the directory where the database file (/app/data/database.json by default) will be stored has write permissions.


Run the Program:
Open a terminal in the folder containing simple_db.py.
Run: python simple_db.py



Usage
SimpleDB runs as a command-line interface (CLI). When you start it, you'll see a list of available commands. You must log in before performing any operations. Below are the key commands and examples.
Authentication

Log In: Log in with a username and password. Default users are:
admin (password: admin123, role: admin)
user (password: user123, role: user)
Command: login <username> <password>
Example: login admin admin123


Log Out: End your session.
Command: logout
Example: logout



Data Operations

Create: Add a new key-value pair. Values can be strings, numbers, or JSON objects.
Command: create <key> <value>
Example: create student001 "{\"Name\": \"Alice\", \"Age\": 15, \"Grade\": 10, \"Class\": \"A\", \"Subjects\": [\"Math\", \"Science\"]}"


Read: View the value for a key.
Command: read <key>
Example: read student001


Update: Change the value for an existing key.
Command: update <key> <value>
Example: update student001 "{\"Name\": \"Alice\", \"Age\": 16, \"Grade\": 10, \"Class\": \"A\", \"Subjects\": [\"Math\", \"Science\"]}"


Delete: Remove a key-value pair (admin-only).
Command: delete <key>
Example: delete student001


List All: Show all key-value pairs.
Command: list
Example: list



Query Operations

Find Exact Match: Find keys with a specific value.
Command: find = <value> [sortby <field>] [limit <n>]
Example: find = "{\"Name\": \"Alice\"}" sortby Age limit 5


Find Numeric Range: Find keys with numeric values greater or less than a number.
Command: find > <value> [sortby <field>] [limit <n>] or find < <value> [sortby <field>] [limit <n>]
Example: find > 15 sortby Age limit 2


Find Substring: Find keys where values contain a substring.
Command: find contains <value> [sortby <field>] [limit <n>]
Example: find contains Math sortby Name


Find Full-Text: Find keys where values contain all specified words.
Command: find fulltext <value> [sortby <field>] [limit <n>]
Example: find fulltext "Math Science" sortby Age


Find by Field: Find keys where a specific field in a JSON object matches a value.
Command: find <field> = <value> [sortby <field>] [limit <n>]
Example: find Class = A sortby Name limit 2


Join: Compare two keys by their values or a specific field.
Command: join <key1> <key2> [field]
Example: join student001 student002 Class



Aggregation Operations

Max/Min/Sum/Average: Calculate the maximum, minimum, sum, or average for a key's value (works for numbers or lists of numbers).
Commands: max <key>, min <key>, sum <key>, avg <key>
Example: sum student001 (if value is a list of numbers)



Data Import/Export (Admin-Only)

Import CSV: Load key-value pairs from a CSV file (must have key and value columns).
Command: import_csv <file>
Example: import_csv students.csv


Export CSV: Save all key-value pairs to a CSV file.
Command: export_csv <file>
Example: export_csv output.csv



Transaction Management

Begin: Start a transaction to group operations.
Command: begin
Example: begin


Commit: Save all changes in the transaction.
Command: commit
Example: commit


Rollback: Undo all changes in the transaction.
Command: rollback
Example: rollback



Other Commands

Help: Show the list of commands.
Command: help


Exit: Close the program.
Command: exit



Example Session
> login admin admin123
Logged in as admin (admin)
> create student001 "{\"Name\": \"Alice\", \"Age\": 15, \"Grade\": 10, \"Class\": \"A\", \"Subjects\": [\"Math\", \"Science\"]}"
Inserted: student001 -> {"Name": "Alice", "Age": 15, "Grade": 10, "Class": "A", "Subjects": ["Math", "Science"]}
> create student002 "{\"Name\": \"Bob\", \"Age\": 16, \"Grade\": 11, \"Class\": \"A\", \"Subjects\": [\"History\", \"Math\"]}"
Inserted: student002 -> {"Name": "Bob", "Age": 16, "Grade": 11, "Class": "A", "Subjects": ["History", "Math"]}
> find Class = A sortby Age limit 2
Found keys: student001, student002
> find fulltext "Math" sortby Name
Found keys: student001, student002
> delete student001
Deleted key: student001
> logout
Logged out user admin
> login user user123
Logged in as user (user)
> delete student002
Error: Delete operation requires admin privileges
> exit
Exiting...

CSV File Format
For import_csv and export_csv, the CSV file must have two columns: key and value. Example (students.csv):
key,value
student001,"{""Name"": ""Alice"", ""Age"": 15, ""Grade"": 10, ""Class"": ""A"", ""Subjects"": [""Math"", ""Science""]}"
student002,"{""Name"": ""Bob"", ""Age"": 16, ""Grade"": 11, ""Class"": ""A"", ""Subjects"": [""History"", ""Math""]}"

Notes

Login Requirement: You must log in before performing any operations. Use admin/admin123 for full access or user/user123 for limited access.
Admin Privileges: Only the admin role can delete keys or import/export CSV files.
JSON Values: For JSON objects, use escaped quotes (e.g., \" in the CLI). Example: "{\"Name\": \"Alice\"}".
Sorting and Limiting: Use sortby <field> to sort by a field (e.g., Age) or value, and limit <n> to restrict the number of results.
Full-Text Search: Searches for all words in the query (e.g., find fulltext "Math Science" finds keys with both "Math" and "Science").
Data Storage: Data is saved to /app/data/database.json by default. Ensure the directory exists and is writable.
Error Handling: The database provides clear error messages for invalid commands, permissions, or file issues.

Testing
To test SimpleDB:

Run python simple_db.py.
Try the example session above.
Test edge cases:
Invalid login: login wrong password
Non-admin delete: Log in as user and try delete student001.
Invalid query: find Age = invalid sortby InvalidField.
Empty full-text query: find fulltext "".


Create a CSV file and test import_csv and export_csv (as admin).
Use transactions: Run begin, make changes, and test commit or rollback.

Limitations

The user database is in-memory (not saved to a file). For persistent users, you would need to extend the code.
Full-text search splits words by spaces (no advanced tokenization like punctuation handling).
Sorting is ascending only. Add asc/desc options for more flexibility.
The database is single-file and not optimized for very large datasets.

Contributing
To contribute:

Fork or modify the simple_db.py file.
Test your changes thoroughly, especially for thread safety and transaction consistency.
Submit suggestions or improvements via pull requests or issues.

License
Author  :- Ashish babu
