Programming Project: RDBMS Normalizer
by Thaysen Feldbaumer

View Google Docs Layout:
https://docs.google.com/document/d/1xDkqFn4rvYWQfFkGyss2uKA-LBOSgbiu6y9sZIi2e24/edit?tab=t.0


Code Description
This Python script is designed to parse a dataset, normalize it according to specified functional dependencies (FDs), and generate a normalized schema for a database. The script utilizes SQLite for database management and dynamically creates tables and relations based on the input data. The overall workflow and methodology involve three main components: an Input Parser, a Normalizer, and a Final Relation Generator.
Overview
Input Parser: The input parser is responsible for parsing the input dataset and extracting relevant attributes and functional dependencies.
Normalizer: The normalizer ensures that the dataset adheres to the principles of database normalization, moving through various normal forms (1NF, 2NF, 3NF, BCNF, 4NF, and 5NF).
Final Relation Generator: This component creates the actual normalized schema in the database based on the results from the normalization process.

1. Input Parser
The parse_input function is responsible for converting a structured input string into usable data formats for further processing.
Function: parse_input(input_data)
Parameters:
input_data: A string containing the input dataset. The first line should represent the headers, and subsequent lines represent the records.
Returns:
headers: A list of attribute names derived from the input.
records: A list of parsed records, where each record is represented as a list of values.
Logic:
The input string is split into lines, with the first line extracted as headers.
Each subsequent line is processed to identify multi-valued fields (enclosed in curly braces) and split them accordingly, ensuring that values are stored correctly for further normalization.

2. Normalizer
The normalization process is carried out through a series of functions that progressively ensure the dataset meets various normal forms.

Main Functions:
a. ensure_1nf(relation, records)
Purpose: Ensures the relation is in First Normal Form (1NF) by flattening any multi-valued columns.
Parameters:
relation: A dictionary containing the relation metadata (e.g., name, columns).
records: A list of records to be processed.
Returns: A list containing the new relation in 1NF.
Logic: Iterates through records, identifies multi-valued fields, and creates new records for each unique value.

b. ensure_2nf(relation, fds)
Purpose: Ensures the relation is in Second Normal Form (2NF) by removing partial dependencies.
Parameters:
relation: The current relation structure.
fds: A list of functional dependencies applicable to the relation.
Returns: A list of decomposed relations ensuring 2NF.
Logic: Analyzes functional dependencies to decompose relations based on partial dependencies.

c. ensure_3nf(relation, fds)
Purpose: Ensures the relation is in Third Normal Form (3NF) by eliminating transitive dependencies.
Parameters:
relation: The current relation structure.
fds: A list of functional dependencies.
Returns: A list of decomposed relations ensuring 3NF.
Logic: Decomposes the relation whenever non-prime attributes are transitively dependent on the primary key.

d. ensure_bcnf(relation, fds)
Purpose: Ensures the relation adheres to Boyce-Codd Normal Form (BCNF) by addressing violations.
Parameters:
relation: The current relation structure.
fds: A list of functional dependencies.
Returns: A list of relations satisfying BCNF.
Logic: If a functional dependency violates BCNF, the relation is decomposed accordingly.

e. ensure_4nf(relation, mvds, records)
Purpose: Ensures the relation is in Fourth Normal Form (4NF) by handling multi-valued dependencies (MVDs).
Parameters:
relation: The current relation structure.
mvds: A list of multi-valued dependencies.
records: A list of records for validation.
Returns: A list of relations ensuring 4NF.
Logic: Validates each MVD against the records and decomposes the relation if necessary.

f. ensure_5nf(relation, join_dependencies, records)
Parameters:
relation (dict): A dictionary representing the relation to be normalized, including its name and attributes.
join_dependencies (list): A list of join dependencies that need to be validated for the relation.
records (list): A list of records for the relation that will be analyzed against the join dependencies.
Logic:
This function ensures that the relation is in Fifth Normal Form (5NF) by examining any join dependencies among its attributes.
For each join dependency, it checks if the current relation can be decomposed into smaller relations without losing information.
If violations are found, the function decomposes the relation into multiple relations that satisfy the join dependencies, thus ensuring that the original information can be reconstructed through natural joins of the decomposed relations.
Purpose: The main goal of this function is to eliminate redundancy in the database schema by ensuring that all data is stored without unnecessary duplication and that it can be reconstructed efficiently, adhering to 5NF principles.

3. Final Relation Generator
The final relation generator creates tables in the SQLite database based on the normalized relations.
Function: create_normalized_tables(cursor, normalized_relations)
Parameters:
cursor: The SQLite database cursor for executing queries.
normalized_relations: A list of relations that have been normalized.
Logic:
Iterates through each relation and generates a CREATE TABLE SQL command, ensuring that primary keys and their constraints are correctly defined.
Executes the command to create the corresponding tables in the database.
Function: insert_data(cursor, table_name, columns, data, headers)
Purpose: Inserts normalized data into the database tables.
Parameters:
cursor: The SQLite cursor.
table_name: The name of the table where data will be inserted.
columns: The list of columns for insertion.
data: The actual data to be inserted.
headers: The original headers for mapping values correctly.
Logic:
Filters and prepares the data based on the specified columns, ensuring that primary key constraints are not violated before executing the insertion.

Additional Function Parameters and Logic
Below is a brief description of each function, including its parameters and the logic implemented. They are called from the previously mentioned functions, but do not have a direct relationship with the Core Components. 
1. create_normalized_tables(cursor, normalized_relations)
Parameters:
cursor (sqlite3.Cursor): SQLite cursor for executing SQL commands.
normalized_relations (list): A list of dictionaries containing the structure of the tables to be created, including table names and columns.
Logic: Iterates through the normalized relations, constructing SQL CREATE TABLE statements and executing them to create new tables in the database.
2. insert_data(cursor, table_name, columns, data, headers)
Parameters:
cursor (sqlite3.Cursor): SQLite cursor for executing SQL commands.
table_name (str): The name of the table where data will be inserted.
columns (list): The list of column names corresponding to the values being inserted.
data (list): A list containing the actual data to be inserted.
headers (list): The headers from the parsed input, used for mapping data to columns.
Logic:
Filters the data to match the specified columns and prepares an SQL INSERT statement.
Checks for primary key violations before executing the insert, ensuring data integrity.
3. generate_random_table_name(prefix)
Parameters:
prefix (str): The prefix to prepend to the randomly generated table name.
Logic: This function allows for the program to dynamically create and also the number of tables that will be used. Generates a random table name by concatenating the provided prefix with a 10-character random string composed of lowercase letters and digits. This will ensure that the created table does not contain a previously created table name. 
4. is_superkey(lhs, relation)
Parameters:
lhs (list): A list representing the left-hand side of a functional dependency.
relation (dict): The current relation, including its primary key and candidate keys.
Logic:
Checks if the left-hand side of a functional dependency is a superkey of the relation.
5. validate_mvd(records, lhs, rhs, headers)
Parameters:
records (list): A list of records to validate against the multi-valued dependency.
lhs (set): The left-hand side of the multi-valued dependency.
rhs (set): The right-hand side of the multi-valued dependency.
headers (list): The list of headers from the parsed input data.
Logic:
Checks if the given multi-valued dependency is valid by ensuring independent value sets exist in the records.
6. find_join_dependencies(records, columns)
Parameters:
records (list): A list of records to analyze for join dependencies.
columns (list): A list of column names to check for join dependencies.
Logic:
Placeholder function for identifying join dependencies within the data.
7. main()
Parameters:
None
Logic:
The main function serves as the entry point of the script. It orchestrates the entire process of parsing input data, normalizing it through various normal forms, and generating a final database schema.
It defines the input data and functional dependencies. The input data typically consists of the records and attributes that need normalization.
It causes the parse_input function to process the raw input data and extract headers and records.
The normalizer functions (ensure_1nf, ensure_2nf, ensure_3nf, ensure_bcnf, ensure_4nf, and ensure_5nf) are called sequentially to progressively normalize the data, depending on the user’s normalization choice.
After normalization, the create_normalized_tables function is called to create the necessary tables in the SQLite database based on the normalized relations.
Finally, the script includes logic for inserting the normalized data into the database, handling exceptions, and outputting basic relevant information regarding the process.
Purpose: The main function encapsulates the entire normalization workflow, acting as the central hub that coordinates data parsing, normalization, and table creation in the database. It ensures that the entire operation runs smoothly from start to finish, allowing the user to focus on providing the input data while the function manages the underlying complexity of normalization.

User Requirement
This program was created on Python 3.10.12 and ran in Ubuntu. 
Imported libraries include: 
os: For interacting with the operating system, 
sqlite3: For database interactions, 
random: For generating random numbers (used in generating random table names),
string: For string manipulations, 
re: For regular expressions (used in input parsing), and 
json: For handling JSON data (if needed in further developments).

Code Usage and Analysis
Three text files are used with the Python file; these are input_data.txt, fds.txt, and mvds.txt. By default these files contain sample data, in a valid format which will be accepted. While, not necessary, these may be altered, as long as they are in a valid table format. 
Upon inserting the data that is being tested, the user may compile the Python file by running ‘python3 normalization.py’. Assuming the data was inserted correctly, the user will then be prompted to insert an integer, visually representing the normal form they would like to normalize to (1NF-5NF). 
Once the program has compiled, a database will be created called normalization.db. This can be accessed using a database browser or by running ‘sqlite3 normalization.db’ directly into the terminal; this assumes the user has the proper commands installed. This will create a list of normalized tables. The user may view the tables using ‘.tables’ & ‘SELECT * FROM {table_name}’. 
Understanding the Data: The normalized tables will have the corresponding normalization level in the table’s titles. If the database does not include the normalized level that the user was seeking, then the highest normalized level shown in the database represents the corresponding tables used (this implies that there were no normalization steps needed from that point forward). 
Example: ‘CoffeeShop_3NF_hadq842ldl’ corresponds to a 3NF normalization. 

Conclusion
This code provides a robust framework for parsing input datasets, normalizing them according to established database normalization rules, and generating the necessary database schema to ensure data integrity and reduce redundancy. Each component of the system is modular, allowing for flexibility and future enhancements as needed. This comprehensive documentation outlines the flow of the code while providing detailed descriptions of each function and its parameters. 
