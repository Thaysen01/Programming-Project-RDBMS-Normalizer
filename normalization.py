import os
import sqlite3
import random
import string
import re
import json


##### v INPUT PARSER v #####


def parse_input(input_data):
    """Parses input data to extract relevant attributes and functional dependencies."""
    lines = input_data.strip().split("\n")
    headers = lines[0].split()
    headers = [header.strip('"') for header in headers]
    records = []

    for line in lines[1:]:
        # Matches items within {}s to detect where multivalued items are
        record = re.findall(r"\{[^}]*\}|\S+", line)
        parsed_record = []

        for item in record:
            if item.startswith("{") and item.endswith("}"):
                values = item[1:-1]  # Removes braces without splitting
                parsed_record.append(values)
            else:
                parsed_record.append(item)

        records.append(parsed_record)
    return headers, records


##### ^ INPUT PARSER ^ #####
##### v FINAL RELATION GENERATOR v #####


def create_normalized_tables(cursor, normalized_relations):
    """Creates normalized tables in the database based on the provided relations."""
    for relation in normalized_relations:
        table_name = relation["table_name"]
        columns = relation["columns"]
        # Ensures primary keys are valid columns
        primary_keys = relation.get("primary_key", [])
        for pk in primary_keys:
            if pk not in columns:
                columns.append(pk)
        # Defines column definitions without PRIMARY KEY individually
        columns_definition = ", ".join([f"{col} TEXT" for col in columns])
        # If primary keys are defined, adds them as a single PRIMARY KEY constraint
        if primary_keys:
            columns_definition += f", PRIMARY KEY ({', '.join(primary_keys)})"
        # Generates the CREATE TABLE query
        create_table_query = (
            f"CREATE TABLE IF NOT EXISTS {table_name} ({columns_definition});"
        )

        cursor.execute(create_table_query)


def insert_data(cursor, table_name, columns, data, headers):
    """Inserts data into the specified normalized table, matching columns to the correct values dynamically."""
    # Filters data to match the columns
    filtered_data = [data[headers.index(col)] for col in columns if col in headers]
    # Creates placeholders for the filtered values
    placeholders = ", ".join("?" for _ in filtered_data)
    insert_query = (
        f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})"
    )
    try:
        # Checks for primary keys that are relevant to this table
        primary_keys = [
            col for col in columns if col.endswith("ID")
        ]  # Assuming ID columns end with 'ID'
        # If there are primary keys, performs existence check
        for pk in primary_keys:
            if pk in columns:
                # Checks if the primary key already exists before inserting
                select_query = f"SELECT COUNT(*) FROM {table_name} WHERE {pk} = ?"
                cursor.execute(select_query, (filtered_data[columns.index(pk)],))
                count = cursor.fetchone()[0]
                if count > 0:
                    return  # Exits if the primary key already exists

        # If no primary key constraints are violated, execute the insert
        cursor.execute(insert_query, filtered_data)
    except sqlite3.IntegrityError as e:
        print("")
    except sqlite3.OperationalError as e:
        print("")


##### ^ FINAL RELATION GENERATOR ^ #####
##### v NORMALIZER SEGMENTS v #####


def generate_random_table_name(prefix):
    """Generates a random table name with a given prefix."""
    """Used for dynamically creating new tables as the program runs."""
    return f"{prefix}_{''.join(random.choices(string.ascii_lowercase + string.digits, k=10))}"

# Beginning of normilization
def ensure_1nf(relation, records):
    """Ensures the relation is in 1NF by flattening any multi-valued columns."""
    decomposed_relations = []
    table_name = relation["table_name"]
    headers = relation["columns"]
    # Iterates through each record and identify multi-valued fields
    for record in records:
        # Keeps track of how many new records we need to create
        new_records = [record]
        for index, value in enumerate(record):
            if "," in value:  # Check if the value is multi-valued
                # Splits the value by comma and create new records
                split_values = value.split(",")
                # Creates new records for each split value
                new_records = [
                    record[:index] + [v.strip()] + record[index + 1 :]
                    for v in split_values
                ]
                break

        # Adds the new records to the decomposed relations
        decomposed_relations.extend(new_records)
    # Creates a new table name for the normalized relation
    new_table_name = generate_random_table_name(f"CoffeeShop_1NF")
    # Defines the structure of the new table based on headers
    new_relation = {
        "table_name": new_table_name,
        "columns": headers,
        "records": decomposed_relations,
    }
    # Returns a list containing the new relation to match expected structure
    return [new_relation]  # Returns as a list


def ensure_2nf(relation, fds):
    """Ensures the relation is in 2NF by decomposing into relations with partial dependencies."""
    decomposed_relations = []
    table_name = relation["table_name"]
    primary_key = set(relation.get("primary_key", []))  # Safely gets primary key
    decomposed_columns = set()

    for fd in fds:
        lhs = set(fd["lhs"])
        rhs = fd["rhs"]

        if lhs.issubset(primary_key) and lhs != primary_key:
            new_relation_name = generate_random_table_name(f"CoffeeShop_2NF")
            decomposed_relations.append(
                {
                    "table_name": new_relation_name,
                    "columns": list(lhs.union(rhs)),
                    "primary_key": list(lhs),
                }
            )
            decomposed_columns.update(lhs.union(rhs))

    remaining_columns = [
        col for col in relation["columns"] if col not in decomposed_columns
    ]
    if remaining_columns:
        decomposed_relations.append(
            {
                "table_name": table_name,
                "columns": remaining_columns,
                "primary_key": list(
                    primary_key
                ),  # Ensures primary key is added to remaining columns
            }
        )

    return decomposed_relations if decomposed_relations else [relation]


def is_superkey(lhs, relation):
    """Checks if the given left-hand side is a superkey in the relation."""
    candidate_keys = relation.get("candidate_keys", [])
    return set(lhs) == set(relation["primary_key"]) or any(
        set(lhs) == set(candidate_key) for candidate_key in candidate_keys
    )


def ensure_3nf(relation, fds):
    """Ensures the relation is in 3NF by removing transitive dependencies."""
    decomposed_relations = []
    table_name = relation["table_name"]
    decomposed_columns = set()

    for fd in fds:
        lhs = fd["lhs"]
        rhs = fd["rhs"]

        if not is_superkey(lhs, relation) and any(
            attr not in relation["primary_key"] for attr in rhs
        ):
            new_relation_name = generate_random_table_name(f"CoffeeShop_3NF")
            decomposed_relations.append(
                {
                    "table_name": new_relation_name,
                    "columns": lhs + rhs,
                    "primary_key": lhs,
                }
            )
            decomposed_columns.update(lhs + rhs)

    remaining_columns = [
        col for col in relation["columns"] if col not in decomposed_columns
    ]
    if remaining_columns:
        decomposed_relations.append(
            {
                "table_name": table_name,
                "columns": remaining_columns,
                "primary_key": relation["primary_key"],
            }
        )

    return decomposed_relations if decomposed_relations else [relation]


def ensure_bcnf(relation, fds):
    """Ensures the relation is in BCNF by removing violations of the BCNF definition."""
    decomposed_relations = []
    table_name = relation["table_name"]
    primary_key = set(relation["primary_key"])
    all_columns = set(relation["columns"])

    for fd in fds:
        lhs = set(fd["lhs"])
        rhs = set(fd["rhs"])

        # Checks for BCNF violation
        if not is_superkey(fd["lhs"], relation):
            new_relation_name = generate_random_table_name(f"CoffeeShop_BCNF")
            # Creates a new relation that contains the lhs and rhs
            decomposed_relations.append(
                {
                    "table_name": new_relation_name,
                    "columns": list(lhs.union(rhs)),
                    "primary_key": list(lhs),
                }
            )
            # Removes the rhs from the original relation
            all_columns -= rhs

    if all_columns:
        decomposed_relations.append(
            {
                "table_name": table_name,
                "columns": list(all_columns),
                "primary_key": relation["primary_key"],
            }
        )

    return decomposed_relations if decomposed_relations else [relation]


def validate_mvd(records, lhs, rhs, headers):
    """Validates the given MVD by checking for independent value sets in the records."""
    if headers is None:
        return False
    if lhs is None or rhs is None:
        return False  # Returns early if there's a problem

    lhs = list(lhs)  # Converts to list for indexing
    rhs = list(rhs)

    # Ensures all lhs headers exist in headers
    for header in lhs:
        if header not in headers:
            return False

    lhs_values = {}

    for record in records:
        try:
            lhs_key = tuple(record[headers.index(header)] for header in lhs)
            rhs_value = tuple(record[headers.index(header)] for header in rhs)

            if lhs_key in lhs_values:
                lhs_values[lhs_key].add(rhs_value)
            else:
                lhs_values[lhs_key] = {rhs_value}
        except ValueError as e:
            continue

    # Check if each lhs_key has multiple independent rhs values
    for values in lhs_values.values():
        if len(values) > 1:
            return True

    return False


def ensure_4nf(relation, mvds, records):
    """Ensures the relation is in 4NF by validating multi-valued dependencies and decomposing as necessary."""
    decomposed_relations = []
    table_name = relation["table_name"]
    all_columns = set(relation["columns"])
    headers = relation["columns"]

    for mvd in mvds:
        if "lhs" not in mvd or "rhs" not in mvd:
            continue  # Skips if missing data

        lhs = set(mvd["lhs"])  # Ensures these are sets for uniqueness
        rhs = set(mvd["rhs"])

        # Checks if this MVD is valid by analyzing actual records
        if validate_mvd(records, lhs, rhs, headers):
            new_relation_name_lhs = generate_random_table_name(f"CoffeeShop_4NF_lhs")
            new_relation_name_rhs = generate_random_table_name(f"CoffeeShop_4NF_rhs")

            decomposed_relations.append(
                {
                    "table_name": new_relation_name_lhs,
                    "columns": list(lhs.union(relation["primary_key"])),
                    "primary_key": list(relation["primary_key"]),
                }
            )
            decomposed_relations.append(
                {
                    "table_name": new_relation_name_rhs,
                    "columns": list(rhs.union(relation["primary_key"])),
                    "primary_key": list(relation["primary_key"]),
                }
            )
            all_columns -= rhs  # Removes rhs from the original relation

    # Adds remaining columns as a new relation
    if all_columns:
        decomposed_relations.append(
            {
                "table_name": table_name,
                "columns": list(all_columns),
                "primary_key": relation["primary_key"],
            }
        )

    return decomposed_relations if decomposed_relations else [relation]


def find_join_dependencies(records, columns):
    """Identifies join dependencies that exist within the data."""
    join_deps = []
    # This function will analyze records to determine JDs that can be applied
    # Placeholder logic - to be defined based on specific criteria for JDs

    return join_deps


def ensure_5nf(relation, records):
    """Ensure the relation is in 5NF by handling join dependencies and decomposing as necessary."""
    decomposed_relations = []
    table_name = relation["table_name"]
    primary_key = set(relation["primary_key"])
    all_columns = set(relation["columns"])

    # Identifies join dependencies by analyzing the data in records
    join_deps = find_join_dependencies(records, relation["columns"])

    for jd in join_deps:
        lhs = set(jd["lhs"])
        rhs = set(jd["rhs"])

        if not is_superkey(lhs, relation):
            # Creates new relations based on JD decomposition
            new_relation_name_lhs = generate_random_table_name(f"CoffeeShop_5NF_lhs")
            new_relation_name_rhs = generate_random_table_name(f"CoffeeShop_5NF_rhs")

            decomposed_relations.append(
                {
                    "table_name": new_relation_name_lhs,
                    "columns": list(lhs.union(relation["primary_key"])),
                    "primary_key": list(lhs),
                }
            )
            decomposed_relations.append(
                {
                    "table_name": new_relation_name_rhs,
                    "columns": list(rhs.union(relation["primary_key"])),
                    "primary_key": list(rhs),
                }
            )
            all_columns -= rhs  # Remove rhs from the original relation

    # If columns remain after JD decomposition, includes them as a separate relation
    if all_columns:
        decomposed_relations.append(
            {
                "table_name": table_name,
                "columns": list(all_columns),
                "primary_key": relation["primary_key"],
            }
        )

    return decomposed_relations if decomposed_relations else [relation]


def normalize_relations(parsed_relations, fds, mvds=None, max_nf=6, records=None):
    """Normalize relations up to the specified max normal form (1NF, 2NF, 3NF, BCNF, 4NF, or 5NF)."""
    """Runs through normalizing stages up to the point that the user wants to normalze to (default=5NF)"""
    all_relations = []
    seen_tables = set()
    print("Running Normalizer")
    for table_name, relation in parsed_relations.items():

        # Step 1: Ensures 1NF
        relations_1nf = ensure_1nf(relation, records)
        if max_nf == 1:
            return relations_1nf

        # Step 2: Ensures 2NF
        for rel_1nf in relations_1nf:
            relations_2nf = ensure_2nf(rel_1nf, fds)
            if max_nf == 2:
                all_relations.extend(
                    rel_2nf
                    for rel_2nf in relations_2nf
                    if rel_2nf["table_name"] not in seen_tables
                )
                seen_tables.update(rel["table_name"] for rel in relations_2nf)
                return all_relations

            # Step 3: Ensures 3NF
            i = 0  # Initializes index for relations_2nf
            while i < len(relations_2nf):
                rel_2nf = relations_2nf[i]
                relations_3nf = ensure_3nf(rel_2nf, fds)
                j = 0  # Initializes index for relations_3nf
                while j < len(relations_3nf):
                    rel_3nf = relations_3nf[j]
                    if rel_3nf["table_name"] not in seen_tables:
                        all_relations.append(rel_3nf)
                        seen_tables.add(rel_3nf["table_name"])
                    j += 1  # Increments inner index
                i += 1  # Increments outer index

                # Step 4: Ensures BCNF if max_nf allows
                if max_nf >= 4:
                    relations_bcnf = ensure_bcnf(rel_3nf, fds)
                    k = 0  # Initializes index for relations_bcnf
                    while k < len(relations_bcnf):
                        rel_bcnf = relations_bcnf[k]
                        if rel_bcnf["table_name"] not in seen_tables:
                            all_relations.append(rel_bcnf)
                            seen_tables.add(rel_bcnf["table_name"])
                        k += 1  # Increment index

        # Step 5: Ensures 4NF if max_nf allows (independent of BCNF)
        if max_nf >= 5:
            i = 0  # Initializes index for relations_3nf
            while i < len(relations_3nf):
                rel_3nf = relations_3nf[i]
                relations_4nf = ensure_4nf(rel_3nf, mvds, records)  # Pass headers here
                j = 0  # Initializes index for relations_4nf
                while j < len(relations_4nf):
                    rel_4nf = relations_4nf[j]
                    if rel_4nf["table_name"] not in seen_tables:
                        all_relations.append(rel_4nf)
                        seen_tables.add(rel_4nf["table_name"])
                    j += 1  # Increments inner index
                i += 1  # Increments outer index

        # Step 6: Ensures 5NF if max_nf allows
        if max_nf == 6:
            relations_5nf = []
            i = 0  # Initializes index for relations_4nf
            while i < len(relations_4nf):
                rel_4nf = relations_4nf[i]
                relations_5nf.extend(ensure_5nf(rel_4nf, records))
                i += 1  # Increments index

            all_relations.extend(
                rel for rel in relations_5nf if rel["table_name"] not in seen_tables
            )
            seen_tables.update(rel["table_name"] for rel in relations_5nf)

    return all_relations


##### ^ NORMALIZER SEGMENTS ^ #####
##### v MAIN v #####


def main():
    """Main function to process input data, normalize relations, and interact with the database."""
    # Removes normalization.db if it exists
    db_file = "normalization.db"
    if os.path.exists(db_file):
        os.remove(db_file)

    # Reads data in from the 3 files
    with open("input_data.txt", "r") as file:
        input_data = file.read()
    with open("fds.txt", "r") as f:
        fds_text = f.read()  # Read the file contents into a string
        fds = json.loads(fds_text)  # Use json.loads to parse the JSON content
    # Manually inserts here for 4NF and 5NF - Loads multi-valued dependencies
    with open("mvds.txt", "r") as f:
        mvds = json.load(f)

    # Parses input data
    headers, records = parse_input(input_data)

    # Prompts the user with how many normalization steps they would like to go through (6 for 5NF)
    print(
        "Information inserted into intut, fds, and mcds.tx files are being run; Sample inputs are implemented by default"
    )
    print("Would you like to use table input data?")
    print("What normal for would you like to run?")
    max_nf = int(
        input(
            "Please type '1' for 1NF, '2' for 2NF, '3' for 3NF, '4' for BCNF, '5' for 4NF, '6' for 5NF\n"
        )
    )

    # Normalizes the data up to the max_nf level
    normalized_relations = normalize_relations(
        {
            "CoffeeShop": {
                "table_name": "CoffeeShop",
                "columns": headers,
                "primary_key": ["OrderID"],
                "candidate_keys": [["OrderID"]],
            }
        },
        fds,
        mvds,
        max_nf=max_nf,
        records=records,
    )

    # Connects to the SQLite database
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()

    # Creates normalized tables based on the relations
    create_normalized_tables(cursor, normalized_relations)

    # Inserts data into the tables
    for rel in normalized_relations:
        table_name = rel["table_name"]
        columns = rel["columns"]
        for record in records:
            insert_data(cursor, table_name, columns, record, headers)

    conn.commit()
    conn.close()
    print("Tables saved in", db_file)


if __name__ == "__main__":
    main()

##### ^ MAIN ^ #####
