import cx_Oracle
#import os
#import platform
#import sys  

import logging
logging.basicConfig(level=logging.INFO, filename="ora_curr_state_v1.log")

# Add logging statements at appropriate points in your code

# Read the database connection details from standard input
host = input().strip()
port = int(input().strip())
service_name = input().strip()
username = input().strip()
password = input().strip()


# Create the DSN
dsn = cx_Oracle.makedsn(host, port, service_name=service_name)
#print("the DSN being passed to the connection:", dsn)  ##for troubleshooting

# Connect to Oracle Database
connection = cx_Oracle.connect(username, password, dsn)
#print("The actual connection string used:", connection)  ##for troubleshooting

# Create a cursor to execute SQL statements
cursor = connection.cursor()
#######################


###SCHEMA TABLE SECTION


#??? Close the cursor and connection
#logging.debug("Cursor Close")
#cursor.close()
#connection.close()


########
# Print the Oracle information

# Query: Connected Server Datetime Stamp
query_success = False  # Flag to track query success
datetime_stamp = None

try:
    cursor.execute("SELECT SYSDATE FROM dual")  # Query to retrieve the current datetime stamp
    datetime_stamp = cursor.fetchone()[0]
    query_success = True  # Set flag to True if the query succeeds
except cx_Oracle.DatabaseError as e:
    logging.debug("Exception:", str(e))
    datetime_stamp = None

#print("Connected Server Datetime Stamp:", datetime_stamp)
print("----------OUTPUT START @", datetime_stamp)
print("-----")
# Get OS Type and Version  ??? not working
#os_type = platform.system()
#os_version = platform.release()
#print("OS Type:", os_type)
#print("-----")
#print("OS Version:", os_version)
#print("-----")
###

# Get Core Information  ??? not working
#core_count = os.cpu_count()
#print("Core Count:", core_count)
#print("-----")

# Query: Oracle Version
query_success = False  # Flag to track query success
try:
    cursor.execute("SELECT version FROM v$instance")  # for Oracle 11 and earlier
    oracle_version = cursor.fetchone()[0]
    query_success = True  # Set flag to True if first query succeeds
except cx_Oracle.DatabaseError as e:
    if not query_success:  # Execute second query only if first query fails
        try:
            cursor.execute("SELECT version_full FROM v$instance")  # for Oracle 19+
            oracle_version = cursor.fetchone()[0]
            query_success = True  # Set flag to True if second query succeeds
        except cx_Oracle.DatabaseError as e:
            logging.debug("Exception:", str(e))
            oracle_version = None
print("Oracle Version:", oracle_version)
print("-----")

# OS Qeury: Get Oracle Home Location from environment variable  ????
#oracle_home = os.environ.get('ORACLE_HOME')
#print("Oracle Home Location:", oracle_home)
#print("-----")


# Query: Oracle SID
cursor.execute("SELECT sys_context('userenv', 'db_name') FROM dual")
oracle_sid = cursor.fetchone()[0]
print("Oracle SID:", oracle_sid)
print("-----")
###

# Query: Check if ASM is used and get the location of ASM logs
# Check if ASM is used
try:
    cursor.execute("SELECT count(*) FROM v$asm_diskgroup")
    asm_diskgroup_count = cursor.fetchone()[0]
except cx_Oracle.DatabaseError as e:
    logging.debug("DBerror", str(e))

if asm_diskgroup_count > 0:
    # ASM is used
    print("ASM Yes")
    print("-----")
    # Get the location of ASM logs
    cursor.execute("SELECT value FROM v$parameter WHERE name = 'asm_diskstring'")
    asm_logs_location = cursor.fetchone()[0]
    print("Location of ASM logs:", asm_logs_location)
    print("-----")
else:
    # ASM is not used
    print("ASM:", "No")
    print("-----")



# Query: Check if RAC is enabled
try:
    cursor.execute("SELECT value FROM v$option WHERE parameter = 'Real Application Clusters'")
    rac_enabled = cursor.fetchone()[0] == 'TRUE'
    print("RAC Enabled:", rac_enabled)
except cx_Oracle.DatabaseError as e:
    logging.debug("DBerror", str(e))
print("-----")


# Query: Is Data Guard enabled and its mode?
try:
    cursor.execute("SELECT value FROM v$parameter WHERE name = 'log_archive_dest_state_2'")
    data_guard_enabled = cursor.fetchone()[0] == 'ENABLED'

    data_guard_mode = None
    if data_guard_enabled:
        cursor.execute("SELECT protection_mode FROM v$database")
        data_guard_mode = cursor.fetchone()[0]
except cx_Oracle.DatabaseError as e:
    logging.debug("DBerror", str(e))
    print("Data Guard Enabled:", data_guard_enabled)
    print("-----")
if data_guard_enabled:
    print("Data Guard Mode:", data_guard_mode)
    print("-----")
    
    
# Query: Is Archivelog enabled?
cursor.execute("SELECT log_mode FROM v$database")
archivelog_enabled = cursor.fetchone()[0] == 'ARCHIVELOG'
logging.debug("End of query to get archive log enabled")
print("Archivelog Enabled:", archivelog_enabled)
print("-----")

# Print the archive log details
print("Archive Logs:")
# Query: List of Archive Logs with Timestamps and File Size
try:
    archive_logs = []
    cursor.execute("""
        SELECT sequence#, first_time, next_time, block_size
        FROM v$archived_log
        ORDER BY sequence#
    """)
    logging.debug("End of query list of Archive logs with timestamps and file size")

    # Fetch all rows returned by the query
    archive_logs = cursor.fetchall()
    logging.debug("End of query to get list of arch logs")

    for log in archive_logs:
        sequence_number = log[0]
        first_time = log[1]
        next_time = log[2]
        block_size = log[3]
        print(f"Sequence #: {sequence_number}, First Time: {first_time}, Next Time: {next_time}, Block Size: {block_size}")

    logging.debug("End of archive log detail")
    print("-----")

    # Print the total disk space in gigabytes
    try:
        total_disk_space = 0

        for log in archive_logs:
            block_size = log[3]
            total_disk_space += (block_size * 1.2)  # Add 20% cushion

        # Multiply by the number of archive logs
        total_disk_space *= len(archive_logs)

        # Convert the disk space to a suitable unit (e.g., gigabytes)
        total_disk_space_gb = total_disk_space / (1024 ** 3)

        print("Total Disk Space Required for Archive Logs: {:.2f} GB".format(total_disk_space_gb))
        logging.debug("End print of total disk space in gigabytes")
        print("-----")

    except cx_Oracle.DatabaseError as e:
        logging.debug("DBerror", str(e))

except cx_Oracle.DatabaseError as e:
    logging.debug("DBerror", str(e))


# Query: Check if DBA_AUDIT_TRAIL table exists
try:
    cursor.execute("""
        SELECT count(*)
        FROM all_tables
        WHERE owner = 'SYS'
        AND table_name = 'DBA_AUDIT_TRAIL'
    """)
    table_exists = cursor.fetchone()[0] == 1
    logging.debug("End of query to see if dba_audit_trail is enabled")
except cx_Oracle.DatabaseError as e:
    logging.debug("DBerror", str(e))
if table_exists:
    print("DBA_AUDIT_TRAIL:", "is enabled")
    print("-----")
else:
    print("DBA_AUDIT_TRAIL:", "is disabled")
print("-----")

###?? print("TDE:", tde_enabled)
# Query: Check if TDE is enabled
try:
    cursor.execute("SELECT wrl_type FROM v$encryption_wallet")
    row = cursor.fetchone()
    logging.debug("End of Query: check if TDE is enabled")
except cx_Oracle.DatabaseError as e:
    logging.debug("DBerror", str(e))
if row is not None:
    try:
        tde_enabled = int(row[0]) > 0
        print("TDE: is enabled")
        print("-----")
    except ValueError:
        tde_enabled = False
        print("TDE: is disabled")
        print("-----")
else:
    tde_enabled = False
    print("TDE: is disabled")
    print("-----")
logging.debug("End of print TDE info")


    
## Total db size
#Query: Get total disc space used
cursor.execute("SELECT SUM(bytes) FROM dba_segments")
total_size_bytes = cursor.fetchone()[0]
total_size_gb = total_size_bytes / (1024 ** 3)  # Convert bytes to GB
logging.debug("End of query to get total disc space")
print("Total Database Size:", total_size_gb, "GB")
print("-----")
logging.debug("End of Print of total db size")



# Query: CDB/PDB Name
logging.debug("Before CDB/PDB Name query - a")
try:
    cdb_pdb_name = []
    cursor.execute("SELECT name FROM v$pdbs")
    cdb_pdb_name = cursor.fetchall()
    cdb_pdb_name = [row[0] for row in cdb_pdb_name[1:]]  # Fetch everything after the first element
    for row in cursor:
        cdb_pdb_name.append(row[0])    
    print("CDB/PDB:", cdb_pdb_name)
    print("-----")
except cx_Oracle.DatabaseError as e:
    logging.debug("Error querying CDB/PDB Name:", str(e))
    print("CDB/PDB: None")
    print("Altered session to cdb/pdb name 1:", cdb_pdb_name)
    
    
######## started here
if not cdb_pdb_name:
    # No PDB found 
    ## print("No PDB Found")    ###???
    print("-----")
    # Query: Get a list of all schemas in the database
    schema_list = []
    try:
        cursor.execute("SELECT username FROM dba_users WHERE username NOT IN ('SYS', 'SYSTEM', 'GGSADMIN_INTERNAL')")
        for row in cursor:
            schema_list.append(row[0])
        logging.debug("End of query to get a list of all schemas in the db")
    except cx_Oracle.DatabaseError as e:
        logging.debug("Error executing the query to get a list of all schemas:", str(e))
    print("List of All Schemas:")
    for schema in schema_list:
        print(schema)
    logging.debug("End print of the list of schemas")
    print("-----")


    # Print the list of tables in each application schema
    print("List of Application Tables in Each Application Schema:")
    # Query: Get a list of all tables in each application schema
    try:
        table_list1 = []
        cursor.execute("""
            SELECT OWNER, TABLE_NAME
            FROM ALL_TABLES
            WHERE OWNER NOT LIKE '%SYS%' ESCAPE '/'
            AND OWNER NOT LIKE 'APEX%'
            AND OWNER NOT LIKE 'ORDDATA%'
            AND OWNER NOT LIKE 'XDB'
            AND OWNER NOT LIKE 'DBS%'
            AND OWNER NOT LIKE 'OUTLN'
            AND OWNER NOT LIKE 'FLOWS_FILES'
            AND OWNER NOT LIKE 'GSMADMIN_INTERNAL'
        """)
        for row in cursor:
            owner = row[0]
            table_name = row[1]
            table_list1.append({'owner': owner, 'table_name': table_name})
            logging.debug("End of query to get list of all tables in each app schema")
    except cx_Oracle.DatabaseError as e:
        logging.debug("end of list of all tables in each schema", str(e))
    for table_info in table_list1:
        print("  Owner:", table_info['owner'], "- Table Name:", table_info['table_name'])
    logging.debug("End of the print the list of tables in the each application schema")
    print("-----")


    # Query: Is Database Supplemental Logging enabled?
    cursor.execute("SELECT supplemental_log_data_min FROM v$database")
    supplemental_logging_enabled = cursor.fetchone()[0] == 'YES'
    logging.debug("End of query to get db supp logging info")
    print("A CDC REQUIREMENT- Database Supplemental Logging Enabled:", supplemental_logging_enabled)
    logging.debug("End print of supplemental logging")
    print("-----")


    # Query and print: Check if forcing logging is enabled
    try:
        cursor.execute("SELECT force_logging FROM v$database")
        force_logging_enabled = cursor.fetchone()[0] == 'YES'
        logging.debug("End of query to check if force logging is enabled")
    except cx_Oracle.DatabaseError as e:
        logging.debug("DBerror", str(e))
    #cursor.execute("SELECT force_logging FROM v$database")
    #forcing_logging_enabled = cursor.fetchone()[0] == 'YES'
    logging.debug("End of query to get force logging on or off")
    print("Force Logging Enabled:", force_logging_enabled)
    logging.debug("End of print of force logging")
    print("-----")


    # Query: List all application tables without Table-level Supplemental Logging
    print("Application Tables without Table-level Supplemental Logging:")
    try:
        application_tables_no_supplemental_logging = []
        cursor.execute("""
        SELECT OWNER, TABLE_NAME
        FROM ALL_TABLES
        WHERE OWNER NOT LIKE '%SYS%' ESCAPE '/'
        AND OWNER NOT LIKE 'APEX%'
        AND OWNER NOT LIKE 'ORDDATA%'
        AND OWNER NOT LIKE 'XDB'
        AND OWNER NOT LIKE 'DBS%'
        AND OWNER NOT LIKE 'OUTLN'
        AND OWNER NOT LIKE 'FLOWS_FILES'
        AND OWNER NOT LIKE 'GSMADMIN_INTERNAL'
        AND OWNER || '.' || TABLE_NAME NOT IN (
            SELECT OWNER || '.' || TABLE_NAME
            FROM DBA_LOGSTDBY_UNSUPPORTED)
    """)
        for row in cursor:
                owner = row[0]
                table_name = row[1]
                application_tables_no_supplemental_logging.append({'owner': owner, 'table_name': table_name})
                logging.debug("End of query to get tables without supp logging")
    except cx_Oracle.DatabaseError as e:
        logging.debug("List all application tables without Table-level Supplemental Logging", str(e))
        
    for table_info in application_tables_no_supplemental_logging:
        print("Owner:", table_info['owner'], "- Table Name:", table_info['table_name'])
    logging.debug("End of print table level supp logging 1")
    print("-----")
        

    # Query: Is Table-level Supplemental Logging enabled for application databases and tables?
    print("Application Tables with Table-level Supplemental Logging:")
    table_supplemental_logging_enabled = []
    try: 
        cursor.execute("""
        SELECT OWNER, TABLE_NAME
        FROM ALL_TABLES
        WHERE OWNER NOT LIKE '%SYS%' ESCAPE '/'
        AND OWNER NOT LIKE 'APEX%'
        AND OWNER NOT LIKE 'ORDDATA%'
        AND OWNER NOT LIKE 'XDB'
        AND OWNER NOT LIKE 'DBS%'
        AND OWNER NOT LIKE 'OUTLN'
        AND OWNER NOT LIKE 'FLOWS_FILES'
        AND OWNER NOT LIKE 'GSMADMIN_INTERNAL'
        AND OWNER || '.' || TABLE_NAME IN (
            SELECT OWNER || '.' || TABLE_NAME
            FROM DBA_LOGSTDBY_UNSUPPORTED)
    """)
        for row in cursor:
            owner = row[0]
            table_name = row[1]
            table_supplemental_logging_enabled.append({'owner': owner, 'table_name': table_name})
            logging.debug("End of query to get table level supp logging")
    except cx_Oracle.DatabaseError as e:
        logging.debug("Error executing the query to get a list of all schemas:", str(e))
    
    for table_info in table_supplemental_logging_enabled:
        print("Owner:", table_info['owner'], "- Table Name:", table_info['table_name'])
    logging.debug("End of print of table lebel supp logging 2")
    print("-----")
    
    # Print the row counts
    print("Row Count by current Date and Time for all Application Tables:")
    # Query: Row count by date and time for the tables
    try:
        table_row_counts = []
        for table_info in application_tables_no_supplemental_logging:
            owner = table_info['owner']
            table_name = table_info['table_name']

            query = f"SELECT TRUNC(COUNT(*)) AS row_count, TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS') AS current_datetime FROM {owner}.{table_name}"

            cursor.execute(query)
            row_count, current_datetime = cursor.fetchone()

            table_row_counts.append({'owner': owner, 'table_name': table_name, 'row_count': row_count, 'current_datetime': current_datetime})
            logging.debug("End of query to get row count by date and time for the app tables")
    except cx_Oracle.DatabaseError as e:
        logging.debug("dberror", str(e))
    for row_count_info in table_row_counts:
        owner = row_count_info['owner']
        table_name = row_count_info['table_name']
        row_count = row_count_info['row_count']
        current_datetime = row_count_info['current_datetime']
        print(f"Owner: {owner}, Table Name: {table_name}, Row Count: {row_count}, Current DateTime: {current_datetime}")
    logging.debug("End print of row count by current data and time for all application tables")
    print("-----")


    # Query and Print: List application tables without primary keys
    try:
        tables_without_primary_keys = []
        cursor.execute("""
            SELECT OWNER, TABLE_NAME
            FROM ALL_TABLES
            WHERE OWNER NOT LIKE '%SYS%' ESCAPE '/'
            AND OWNER NOT LIKE 'APEX%'
            AND OWNER NOT LIKE 'ORDDATA%'
            AND OWNER NOT LIKE 'XDB'
            AND OWNER NOT LIKE 'DBS%'
            AND OWNER NOT LIKE 'OUTLN'
            AND OWNER NOT LIKE 'FLOWS_FILES'
            AND OWNER NOT LIKE 'GSMADMIN_INTERNAL'
            AND TABLE_NAME NOT IN (
                SELECT TABLE_NAME
                FROM ALL_CONSTRAINTS
                WHERE CONSTRAINT_TYPE = 'P'
                AND OWNER NOT LIKE '%SYS%' ESCAPE '/'
                AND OWNER NOT LIKE 'APEX%'
                AND OWNER NOT LIKE 'ORDDATA%'
                AND OWNER NOT LIKE 'XDB'
                AND OWNER NOT LIKE 'DBS%'
                AND OWNER NOT LIKE 'OUTLN'
                AND OWNER NOT LIKE 'FLOWS_FILES'
                AND OWNER NOT LIKE 'GSMADMIN_INTERNAL'
            )
        """)
        for row in cursor:
            owner = row[0]
            table_name = row[1]
            tables_without_primary_keys.append({'owner': owner, 'table_name': table_name})
        logging.debug("End of Query: List application tables without primary keys")
    except cx_Oracle.DatabaseError as e:
        logging.debug("DBerror", str(e))
    print("Application Tables without Primary Keys:")
    for table_info in tables_without_primary_keys:
        print("Owner:", table_info['owner'], "- Table Name:", table_info['table_name'])
    logging.debug("End of print of tables without primary keys")
    print("-----")


    # Print the tables without unique indexes
    # Query: List application tables without unique indexes
    try:
        tables_without_unique_indexes = []
        cursor.execute("""
            SELECT OWNER, TABLE_NAME
            FROM ALL_TABLES
            WHERE OWNER NOT LIKE '%SYS%' ESCAPE '/'
            AND OWNER NOT LIKE 'APEX%'
            AND OWNER NOT LIKE 'ORDDATA%'
            AND OWNER NOT LIKE 'XDB'
            AND OWNER NOT LIKE 'DBS%'
            AND OWNER NOT LIKE 'OUTLN'
            AND OWNER NOT LIKE 'FLOWS_FILES'
            AND OWNER NOT LIKE 'GSMADMIN_INTERNAL'
            AND TABLE_NAME NOT IN (
                SELECT TABLE_NAME
                FROM ALL_INDEXES
                WHERE UNIQUENESS = 'UNIQUE'
                AND OWNER NOT LIKE '%SYS%' ESCAPE '/'
                AND OWNER NOT LIKE 'APEX%'
                AND OWNER NOT LIKE 'ORDDATA%'
                AND OWNER NOT LIKE 'XDB'
                AND OWNER NOT LIKE 'DBS%'
                AND OWNER NOT LIKE 'OUTLN'
                AND OWNER NOT LIKE 'FLOWS_FILES'
                AND OWNER NOT LIKE 'GSMADMIN_INTERNAL'
            )
        """)
        for row in cursor:
            owner = row[0]
            table_name = row[1]
            tables_without_unique_indexes.append({'owner': owner, 'table_name': table_name})
        logging.debug("End of Query: List application tables without unique indexes")
    except cx_Oracle.DatabaseError as e:
        logging.debug("DBerror", str(e))
    print("Application Tables without Unique Indexes:")
    for table_info in tables_without_unique_indexes:
        print("Owner:", table_info['owner'], "- Table Name:", table_info['table_name'])
    logging.debug("End of print tables without unique indexes")
    print("-----")


    # Print the application tables with binary object blobs, clobs, and others
    print("Application Tables with Binary Object Blobs, Clobs, and Others:")
    # Query: List application tables with binary object blobs, clobs, and others
    try:
        application_tables_with_binary_objects = []
        cursor.execute("""
            SELECT OWNER, TABLE_NAME, COLUMN_NAME, DATA_TYPE
            FROM ALL_TAB_COLUMNS
            WHERE OWNER NOT LIKE '%SYS%' ESCAPE '/'
            AND OWNER NOT LIKE 'APEX%'
            AND OWNER NOT LIKE 'ORDDATA%'
            AND OWNER NOT LIKE 'XDB'
            AND OWNER NOT LIKE 'DBS%'
            AND OWNER NOT LIKE 'OUTLN'
            AND OWNER NOT LIKE 'FLOWS_FILES'
            AND OWNER NOT LIKE 'GSMADMIN_INTERNAL'
            AND (DATA_TYPE = 'BLOB' OR DATA_TYPE = 'CLOB' OR DATA_TYPE LIKE '%BINARY%')
        """)
        for row in cursor:
            owner = row[0]
            table_name = row[1]
            column_name = row[2]
            data_type = row[3]
            application_tables_with_binary_objects.append({'owner': owner, 'table_name': table_name, 'column_name': column_name, 'data_type': data_type})
        logging.debug("End of Query: to List application tables with binary object blobs, clobs, and others")
    except cx_Oracle.DatabaseError as e:
        logging.debug("DBerror", str(e))
    if len(application_tables_with_binary_objects) > 0:
        for table_info in application_tables_with_binary_objects:
            print("Owner:", table_info['owner'], "- Table Name:", table_info['table_name'], "- Column Name:", table_info['column_name'], "- Data Type:", table_info['data_type'])
    else:
        print("No Application Tables found with Binary Object Blobs, Clobs, and Others.")
    logging.debug("End of print application tables with binary object")
    print("-----")

        

    # Query: Check for RDBMS jobs
    try:
        cursor.execute("SELECT job_name, schedule_name FROM dba_scheduler_jobs")
        jobs = cursor.fetchall()
        for job in jobs:
            job_name = job[0]
            schedule_name = job[1]
        logging.debug("End of Query: check for RDBMS jobs")
    except cx_Oracle.DatabaseError as e:
        logging.debug("DBerror", str(e))
    if jobs:
        # RDBMS jobs exist
        print("RDBMS Jobs:")
        for job in jobs:
            print("Job Name:", job_name)
            print("Schedule Name:", schedule_name)
            
    else:
        # No RDBMS jobs found
        print("No RDBMS jobs found")
    logging.debug("End print of RDBMS jobs")
    print("-----")





    # Query and print: List of users with creation time and last access time
    try:
        logging.debug("Start of query to get list of users with creation time and last acess time")
        cursor.execute("""
            SELECT u.username, u.created, a.timestamp
            FROM dba_users u
            LEFT JOIN dba_audit_trail a ON u.username = a.username
            WHERE u.username NOT IN ('SYS', 'SYSTEM')
        """)
        user_list = cursor.fetchall()
        logging.debug("End of query to get list of users with creation time and last acess time")
    except cx_Oracle.DatabaseError as e:
        logging.debug("Error", str(e))
    
    print("List of Users with Creation Time and Last Access Time:")
    for user_info in user_list:
        username = user_info[0]
        creation_time = user_info[1]
        last_access_time = user_info[2]

        print(f"Username: {username}, Creation Time: {creation_time}, Last Access Time: {last_access_time}")
    print("-----")
        
    logging.debug("End of print for list of users with creation time and last access times")


    # Print the list of tables in each application schema along with their views, indexes, constraints, triggers, and stored procedures
    # Query: Get a list of all tables in each application schema along with their views, indexes, constraints, triggers, and stored procedures
    try:
        cursor.execute(f"SELECT VIEW_NAME FROM ALL_VIEWS WHERE OWNER = '{owner}' AND VIEW_NAME LIKE '{table_name}%'")
        views = [view[0] for view in cursor]
        logging.debug("End of Query: Get views for the current table ")
    except cx_Oracle.DatabaseError as e:
        logging.debug("DBerror", str(e))
            
    # Query: Get indexes for the current table
    try:
        cursor.execute(f"SELECT INDEX_NAME FROM ALL_INDEXES WHERE OWNER = '{owner}' AND TABLE_NAME = '{table_name}'")
        indexes = [index[0] for index in cursor]
        logging.debug("End of Query: Get indexes for current table ")
    except cx_Oracle.DatabaseError as e:
        logging.debug("DBerror", str(e))
            
    # Query: Get constraints for the current table
    try:
        cursor.execute(f"SELECT CONSTRAINT_NAME FROM ALL_CONSTRAINTS WHERE OWNER = '{owner}' AND TABLE_NAME = '{table_name}'")
        constraints = [constraint[0] for constraint in cursor]
        logging.debug("End of Query: get constraints for the current table")
    except cx_Oracle.DatabaseError as e:
        logging.debug("DBerror", str(e))
            
    # Query: Get triggers for the current table
    try:
        cursor.execute(f"SELECT TRIGGER_NAME FROM ALL_TRIGGERS WHERE OWNER = '{owner}' AND TABLE_NAME = '{table_name}'")
        triggers = [trigger[0] for trigger in cursor]
        logging.debug("End of Query: get triggers for the current table")
    except cx_Oracle.DatabaseError as e:
        logging.debug("DBerror", str(e))
            ###^^^^
        print("Dberror with at end of get triggers for the current table:", str(e))  ##used for troubleshooting
    
    # Query: Get views for the current table
    
    # Query: Get stored procedures for the current table
    try:
        table_list2 = []
        cursor.execute(f"SELECT OBJECT_NAME FROM ALL_OBJECTS WHERE OWNER = '{owner}' AND OBJECT_TYPE = 'PROCEDURE' AND OBJECT_NAME LIKE '{table_name}%'")
        stored_procedures = [procedure[0] for procedure in cursor]
        table_list2.append({
            'owner': owner,
            'table_name': table_name,
            'views': views,
            'indexes': indexes,
            'constraints': constraints,
            'triggers': triggers,
            'stored_procedures': stored_procedures
        })
        logging.debug("End of Query: get sotred procedures for the current table")

        table_list2 = []
        cursor.execute("""
            SELECT OWNER, TABLE_NAME
            FROM ALL_TABLES
            WHERE OWNER NOT LIKE '%SYS%' ESCAPE '/'
            AND OWNER NOT LIKE 'APEX%'
            AND OWNER NOT LIKE 'ORDDATA%'
            AND OWNER NOT LIKE 'XDB'
            AND OWNER NOT LIKE 'DBS%'
            AND OWNER NOT LIKE 'OUTLN'
            AND OWNER NOT LIKE 'FLOWS_FILES'
            AND OWNER NOT LIKE 'GSMADMIN_INTERNAL'
        """)
        for row in cursor:
            owner = row[0]
            table_name = row[1]
    except cx_Oracle.DatabaseError as e:
        logging.debug("DBerror", str(e))
        
        for table_info in table_list2:
            owner = table_info['owner']
            table_name = table_info['table_name']
            views = table_info['views']
            indexes = table_info['indexes']
            constraints = table_info['constraints']
            triggers = table_info['triggers']
            stored_procedures = table_info['stored_procedures']
            
            print("-----")
            print("List of Tables and its Owner with Views, Indexes, Consraints, Triggers and Stored Procs:")
            print("Owner:", owner)
            print("Table Name:", table_name)
            print("Views:", views)
            print("Indexes:", indexes)
            print("Constraints:", constraints)
            print("Triggers:", triggers)
            print("Stored Procedures:", stored_procedures)
            print("-----")
        logging.debug("End of print of list of tables and its owner with views, indexes, constraints, triggers...")
        
        
            #Query: Get the top CPU-consuming SQL statements
    try:
        cursor.execute("""
            SELECT sql_id, sql_text, cpu_time
            FROM (
                SELECT sql_id, sql_text, cpu_time
                FROM v$sql
                ORDER BY cpu_time DESC
            ) WHERE ROWNUM <= 5
        """)
        top_sql_statements = cursor.fetchall()
        for sql_statement in top_sql_statements:
            sql_id = sql_statement[0]
            sql_text = sql_statement[1]
            cpu_time = sql_statement[2]
            print("##TOP CPU SQL ID:", sql_id)
            print("##TOP CPU SQL Text:", sql_text)
            print("##TOP CPU Time:", cpu_time)
            print("-----")
        logging.debug("End of Query: Get the top cpu-consuming sql statements")
    except cx_Oracle.DatabaseError as e:
        logging.debug("DBerror", str(e))    
        

        # Close the cursor and connection
    logging.debug("Cursor Close")
    cursor.close()
    connection.close()
    
    
    
    
    
    
    


for pdb in cdb_pdb_name:
    
    ##print("Altered session to PDB 1:", pdb)  ##??    
    # Alter the session to the current PDB
    cursor.execute("ALTER SESSION SET CONTAINER = {0}".format(pdb))
    print("Altered session to PDB:", pdb)
    logging.debug("Altered session to PDB 2: {0}".format(pdb))

    # Query: Get a list of all schemas in the database
    schema_list = []
    try:
        cursor.execute("SELECT username FROM dba_users WHERE username NOT IN ('SYS', 'SYSTEM', 'GSADMIN_INTERNAL')")
        for row in cursor:
            schema_list.append(row[0])
        logging.debug("End of query to get a list of all schemas in the db")
    except cx_Oracle.DatabaseError as e:
        logging.debug("Error executing the query to get a list of all schemas:", str(e))
    print("List of All Schemas:")
    for schema in schema_list:
        print(schema)
    logging.debug("End print of the list of schemas")
    print("-----")


    # Print the list of tables in each application schema
    print("List of Application Tables in Each Application Schema:")
    # Query: Get a list of all tables in each application schema
    try:
        table_list1 = []
        cursor.execute("""
            SELECT OWNER, TABLE_NAME
            FROM ALL_TABLES
            WHERE OWNER NOT LIKE '%SYS%' ESCAPE '/'
            AND OWNER NOT LIKE 'APEX%'
            AND OWNER NOT LIKE 'ORDDATA%'
            AND OWNER NOT LIKE 'XDB'
            AND OWNER NOT LIKE 'DBS%'
            AND OWNER NOT LIKE 'OUTLN'
            AND OWNER NOT LIKE 'FLOWS_FILES'
            AND OWNER NOT LIKE 'GSMADMIN_INTERNAL'
        """)
        for row in cursor:
            owner = row[0]
            table_name = row[1]
            table_list1.append({'owner': owner, 'table_name': table_name})
            logging.debug("End of query to get list of all tables in each app schema")
    except cx_Oracle.DatabaseError as e:
        logging.debug("end of list of all tables in each schema", str(e))
    for table_info in table_list1:
        print("  Owner:", table_info['owner'], "- Table Name:", table_info['table_name'])
    logging.debug("End of the print the list of tables in the each application schema")
    print("-----")


    # Query: Is Database Supplemental Logging enabled?
    cursor.execute("SELECT supplemental_log_data_min FROM v$database")
    supplemental_logging_enabled = cursor.fetchone()[0] == 'YES'
    logging.debug("End of query to get db supp logging info")
    print("A CDC REQUIREMENT- Database Supplemental Logging Enabled:", supplemental_logging_enabled)
    logging.debug("End print of supplemental logging")
    print("-----")


    # Query and print: Check if forcing logging is enabled
    try:
        cursor.execute("SELECT force_logging FROM v$database")
        force_logging_enabled = cursor.fetchone()[0] == 'YES'
        logging.debug("End of query to check if force logging is enabled")
    except cx_Oracle.DatabaseError as e:
        logging.debug("DBerror", str(e))
    #cursor.execute("SELECT force_logging FROM v$database")
    #forcing_logging_enabled = cursor.fetchone()[0] == 'YES'
    logging.debug("End of query to get force logging on or off")
    print("Force Logging Enabled:", force_logging_enabled)
    logging.debug("End of print of force logging")
    print("-----")


    # Query: List all application tables without Table-level Supplemental Logging
    print("Application Tables without Table-level Supplemental Logging:")
    try:
        application_tables_no_supplemental_logging = []
        cursor.execute("""
        SELECT OWNER, TABLE_NAME
        FROM ALL_TABLES
        WHERE OWNER NOT LIKE '%SYS%' ESCAPE '/'
        AND OWNER NOT LIKE 'APEX%'
        AND OWNER NOT LIKE 'ORDDATA%'
        AND OWNER NOT LIKE 'XDB'
        AND OWNER NOT LIKE 'DBS%'
        AND OWNER NOT LIKE 'OUTLN'
        AND OWNER NOT LIKE 'FLOWS_FILES'
        AND OWNER NOT LIKE 'GSMADMIN_INTERNAL'
        AND OWNER || '.' || TABLE_NAME NOT IN (
            SELECT OWNER || '.' || TABLE_NAME
            FROM DBA_LOGSTDBY_UNSUPPORTED)
    """)
        for row in cursor:
                owner = row[0]
                table_name = row[1]
                application_tables_no_supplemental_logging.append({'owner': owner, 'table_name': table_name})
                logging.debug("End of query to get tables without supp logging")
    except cx_Oracle.DatabaseError as e:
        logging.debug("List all application tables without Table-level Supplemental Logging", str(e))
        
    for table_info in application_tables_no_supplemental_logging:
        print("Owner:", table_info['owner'], "- Table Name:", table_info['table_name'])
    logging.debug("End of print table level supp logging 1")
    print("-----")
        

    # Query: Is Table-level Supplemental Logging enabled for application databases and tables?
    print("Application Tables with Table-level Supplemental Logging:")
    table_supplemental_logging_enabled = []
    try: 
        cursor.execute("""
        SELECT OWNER, TABLE_NAME
        FROM ALL_TABLES
        WHERE OWNER NOT LIKE '%SYS%' ESCAPE '/'
        AND OWNER NOT LIKE 'APEX%'
        AND OWNER NOT LIKE 'ORDDATA%'
        AND OWNER NOT LIKE 'XDB'
        AND OWNER NOT LIKE 'DBS%'
        AND OWNER NOT LIKE 'OUTLN'
        AND OWNER NOT LIKE 'FLOWS_FILES'
        AND OWNER NOT LIKE 'GSMADMIN_INTERNAL'
        AND OWNER || '.' || TABLE_NAME IN (
            SELECT OWNER || '.' || TABLE_NAME
            FROM DBA_LOGSTDBY_UNSUPPORTED)
    """)
        for row in cursor:
            owner = row[0]
            table_name = row[1]
            table_supplemental_logging_enabled.append({'owner': owner, 'table_name': table_name})
            logging.debug("End of query to get table level supp logging")
    except cx_Oracle.DatabaseError as e:
        logging.debug("Error executing the query to get a list of all schemas:", str(e))
    for table_info in table_supplemental_logging_enabled:
        print("Owner:", table_info['owner'], "- Table Name:", table_info['table_name'])
    logging.debug("End of print of table lebel supp logging 2")
    print("-----")
    
    
    
    
    
    # Print the row counts
    print("Row Count by current Date and Time for all Application Tables:")
    # Query: Row count by date and time for the tables
    try:
        table_row_counts = []
        for table_info in application_tables_no_supplemental_logging:
            owner = table_info['owner']
            table_name = table_info['table_name']

            query = f"SELECT TRUNC(COUNT(*)) AS row_count, TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS') AS current_datetime FROM {owner}.{table_name}"

            cursor.execute(query)
            row_count, current_datetime = cursor.fetchone()

            table_row_counts.append({'owner': owner, 'table_name': table_name, 'row_count': row_count, 'current_datetime': current_datetime})
            logging.debug("End of query to get row count by date and time for the app tables")
    except cx_Oracle.DatabaseError as e:
        logging.debug("dberror", str(e))
    for row_count_info in table_row_counts:
        owner = row_count_info['owner']
        table_name = row_count_info['table_name']
        row_count = row_count_info['row_count']
        current_datetime = row_count_info['current_datetime']
        print(f"Owner: {owner}, Table Name: {table_name}, Row Count: {row_count}, Current DateTime: {current_datetime}")
    logging.debug("End print of row count by current data and time for all application tables")
    print("-----")


    # Query and Print: List application tables without primary keys
    try:
        tables_without_primary_keys = []
        cursor.execute("""
            SELECT OWNER, TABLE_NAME
            FROM ALL_TABLES
            WHERE OWNER NOT LIKE '%SYS%' ESCAPE '/'
            AND OWNER NOT LIKE 'APEX%'
            AND OWNER NOT LIKE 'ORDDATA%'
            AND OWNER NOT LIKE 'XDB'
            AND OWNER NOT LIKE 'DBS%'
            AND OWNER NOT LIKE 'OUTLN'
            AND OWNER NOT LIKE 'FLOWS_FILES'
            AND OWNER NOT LIKE 'GSMADMIN_INTERNAL'
            AND TABLE_NAME NOT IN (
                SELECT TABLE_NAME
                FROM ALL_CONSTRAINTS
                WHERE CONSTRAINT_TYPE = 'P'
                AND OWNER NOT LIKE '%SYS%' ESCAPE '/'
                AND OWNER NOT LIKE 'APEX%'
                AND OWNER NOT LIKE 'ORDDATA%'
                AND OWNER NOT LIKE 'XDB'
                AND OWNER NOT LIKE 'DBS%'
                AND OWNER NOT LIKE 'OUTLN'
                AND OWNER NOT LIKE 'FLOWS_FILES'
                AND OWNER NOT LIKE 'GSMADMIN_INTERNAL'
            )
        """)
        for row in cursor:
            owner = row[0]
            table_name = row[1]
            tables_without_primary_keys.append({'owner': owner, 'table_name': table_name})
        logging.debug("End of Query: List application tables without primary keys")
    except cx_Oracle.DatabaseError as e:
        logging.debug("DBerror", str(e))
    print("Application Tables without Primary Keys:")
    for table_info in tables_without_primary_keys:
        print("Owner:", table_info['owner'], "- Table Name:", table_info['table_name'])
    logging.debug("End of print of tables without primary keys")
    print("-----")


    # Print the tables without unique indexes
    # Query: List application tables without unique indexes
    try:
        tables_without_unique_indexes = []
        cursor.execute("""
            SELECT OWNER, TABLE_NAME
            FROM ALL_TABLES
            WHERE OWNER NOT LIKE '%SYS%' ESCAPE '/'
            AND OWNER NOT LIKE 'APEX%'
            AND OWNER NOT LIKE 'ORDDATA%'
            AND OWNER NOT LIKE 'XDB'
            AND OWNER NOT LIKE 'DBS%'
            AND OWNER NOT LIKE 'OUTLN'
            AND OWNER NOT LIKE 'FLOWS_FILES'
            AND OWNER NOT LIKE 'GSMADMIN_INTERNAL'
            AND TABLE_NAME NOT IN (
                SELECT TABLE_NAME
                FROM ALL_INDEXES
                WHERE UNIQUENESS = 'UNIQUE'
                AND OWNER NOT LIKE '%SYS%' ESCAPE '/'
                AND OWNER NOT LIKE 'APEX%'
                AND OWNER NOT LIKE 'ORDDATA%'
                AND OWNER NOT LIKE 'XDB'
                AND OWNER NOT LIKE 'DBS%'
                AND OWNER NOT LIKE 'OUTLN'
                AND OWNER NOT LIKE 'FLOWS_FILES'
                AND OWNER NOT LIKE 'GSMADMIN_INTERNAL'
            )
        """)
        for row in cursor:
            owner = row[0]
            table_name = row[1]
            tables_without_unique_indexes.append({'owner': owner, 'table_name': table_name})
        logging.debug("End of Query: List application tables without unique indexes")
    except cx_Oracle.DatabaseError as e:
        logging.debug("DBerror", str(e))
    print("Application Tables without Unique Indexes:")
    for table_info in tables_without_unique_indexes:
        print("Owner:", table_info['owner'], "- Table Name:", table_info['table_name'])
    logging.debug("End of print tables without unique indexes")
    print("-----")


    # Print the application tables with binary object blobs, clobs, and others
    print("Application Tables with Binary Object Blobs, Clobs, and Others:")
    # Query: List application tables with binary object blobs, clobs, and others
    try:
        application_tables_with_binary_objects = []
        cursor.execute("""
            SELECT OWNER, TABLE_NAME, COLUMN_NAME, DATA_TYPE
            FROM ALL_TAB_COLUMNS
            WHERE OWNER NOT LIKE '%SYS%' ESCAPE '/'
            AND OWNER NOT LIKE 'APEX%'
            AND OWNER NOT LIKE 'ORDDATA%'
            AND OWNER NOT LIKE 'XDB'
            AND OWNER NOT LIKE 'DBS%'
            AND OWNER NOT LIKE 'OUTLN'
            AND OWNER NOT LIKE 'FLOWS_FILES'
            AND OWNER NOT LIKE 'GSMADMIN_INTERNAL'
            AND (DATA_TYPE = 'BLOB' OR DATA_TYPE = 'CLOB' OR DATA_TYPE LIKE '%BINARY%')
        """)
        for row in cursor:
            owner = row[0]
            table_name = row[1]
            column_name = row[2]
            data_type = row[3]
            application_tables_with_binary_objects.append({'owner': owner, 'table_name': table_name, 'column_name': column_name, 'data_type': data_type})
        logging.debug("End of Query: to List application tables with binary object blobs, clobs, and others")
    except cx_Oracle.DatabaseError as e:
        logging.debug("DBerror", str(e))
    if len(application_tables_with_binary_objects) > 0:
        for table_info in application_tables_with_binary_objects:
            print("Owner:", table_info['owner'], "- Table Name:", table_info['table_name'], "- Column Name:", table_info['column_name'], "- Data Type:", table_info['data_type'])
    else:
        print("No Application Tables found with Binary Object Blobs, Clobs, and Others.")
    logging.debug("End of print application tables with binary object")
    print("-----")

        

    # Query: Check for RDBMS jobs
    try:
        cursor.execute("SELECT job_name, schedule_name FROM dba_scheduler_jobs")
        jobs = cursor.fetchall()
        for job in jobs:
            job_name = job[0]
            schedule_name = job[1]
        logging.debug("End of Query: check for RDBMS jobs")
    except cx_Oracle.DatabaseError as e:
        logging.debug("DBerror", str(e))
    if jobs:
        # RDBMS jobs exist
        print("RDBMS Jobs:")
        for job in jobs:
            print("Job Name:", job_name)
            print("Schedule Name:", schedule_name)
            
    else:
        # No RDBMS jobs found
        print("No RDBMS jobs found")
    logging.debug("End print of RDBMS jobs")
    print("-----")





    # Query and print: List of users with creation time and last access time
    try:
        logging.debug("Start of query to get list of users with creation time and last acess time")
        cursor.execute("""
            SELECT u.username, u.created, a.timestamp
            FROM dba_users u
            LEFT JOIN dba_audit_trail a ON u.username = a.username
            WHERE u.username NOT IN ('SYS', 'SYSTEM')
        """)
        user_list = cursor.fetchall()
        logging.debug("End of query to get list of users with creation time and last acess time")
    except cx_Oracle.DatabaseError as e:
        logging.debug("Error", str(e))
    print("List of Users with Creation Time and Last Access Time:")
    for user_info in user_list:
        username = user_info[0]
        creation_time = user_info[1]
        last_access_time = user_info[2]

        print(f"Username: {username}, Creation Time: {creation_time}, Last Access Time: {last_access_time}")
    print("-----")
    logging.debug("End of print for list of users with creation time and last access times")


        # Print the list of tables in each application schema along with their views, indexes, constraints, triggers, and stored procedures
    # Query: Get a list of all tables in each application schema along with their views, indexes, constraints, triggers, and stored procedures
    try:
        cursor.execute(f"SELECT VIEW_NAME FROM ALL_VIEWS WHERE OWNER = '{owner}' AND VIEW_NAME LIKE '{table_name}%'")
        views = [view[0] for view in cursor]
        logging.debug("End of Query: Get views for the current table ")
    except cx_Oracle.DatabaseError as e:
        logging.debug("DBerror", str(e))
            
    # Query: Get indexes for the current table
    try:
        cursor.execute(f"SELECT INDEX_NAME FROM ALL_INDEXES WHERE OWNER = '{owner}' AND TABLE_NAME = '{table_name}'")
        indexes = [index[0] for index in cursor]
        logging.debug("End of Query: Get indexes for current table ")
    except cx_Oracle.DatabaseError as e:
        logging.debug("DBerror", str(e))
            
    # Query: Get constraints for the current table
    try:
        cursor.execute(f"SELECT CONSTRAINT_NAME FROM ALL_CONSTRAINTS WHERE OWNER = '{owner}' AND TABLE_NAME = '{table_name}'")
        constraints = [constraint[0] for constraint in cursor]
        logging.debug("End of Query: get constraints for the current table")
    except cx_Oracle.DatabaseError as e:
        logging.debug("DBerror", str(e))
            
    # Query: Get triggers for the current table
    try:
        cursor.execute(f"SELECT TRIGGER_NAME FROM ALL_TRIGGERS WHERE OWNER = '{owner}' AND TABLE_NAME = '{table_name}'")
        triggers = [trigger[0] for trigger in cursor]
        logging.debug("End of Query: get triggers for the current table")
    except cx_Oracle.DatabaseError as e:
        logging.debug("DBerror", str(e))
            ###^^^^
        print("Dberror with at end of get triggers for the current table:", str(e))  ##used for troubleshooting
    
    # Query: Get views for the current table
    
    # Query: Get stored procedures for the current table
    try:
        table_list2 = []
        cursor.execute(f"SELECT OBJECT_NAME FROM ALL_OBJECTS WHERE OWNER = '{owner}' AND OBJECT_TYPE = 'PROCEDURE' AND OBJECT_NAME LIKE '{table_name}%'")
        stored_procedures = [procedure[0] for procedure in cursor]
        table_list2.append({
            'owner': owner,
            'table_name': table_name,
            'views': views,
            'indexes': indexes,
            'constraints': constraints,
            'triggers': triggers,
            'stored_procedures': stored_procedures
        })
        logging.debug("End of Query: get sotred procedures for the current table")

        table_list2 = []
        cursor.execute("""
            SELECT OWNER, TABLE_NAME
            FROM ALL_TABLES
            WHERE OWNER NOT LIKE '%SYS%' ESCAPE '/'
            AND OWNER NOT LIKE 'APEX%'
            AND OWNER NOT LIKE 'ORDDATA%'
            AND OWNER NOT LIKE 'XDB'
            AND OWNER NOT LIKE 'DBS%'
            AND OWNER NOT LIKE 'OUTLN'
            AND OWNER NOT LIKE 'FLOWS_FILES'
            AND OWNER NOT LIKE 'GSMADMIN_INTERNAL'
        """)
        for row in cursor:
            owner = row[0]
            table_name = row[1]
    except cx_Oracle.DatabaseError as e:
        logging.debug("DBerror", str(e))
        
        for table_info in table_list2:
            owner = table_info['owner']
            table_name = table_info['table_name']
            views = table_info['views']
            indexes = table_info['indexes']
            constraints = table_info['constraints']
            triggers = table_info['triggers']
            stored_procedures = table_info['stored_procedures']
            
            print("-----")
            print("List of Tables and its Owner with Views, Indexes, Consraints, Triggers and Stored Procs:")
            print("Owner:", owner)
            print("Table Name:", table_name)
            print("Views:", views)
            print("Indexes:", indexes)
            print("Constraints:", constraints)
            print("Triggers:", triggers)
            print("Stored Procedures:", stored_procedures)
            print("-----")
        logging.debug("End of print of list of tables and its owner with views, indexes, constraints, triggers...")
        
        
            #Query: Get the top CPU-consuming SQL statements
    try:
        cursor.execute("""
            SELECT sql_id, sql_text, cpu_time
            FROM (
                SELECT sql_id, sql_text, cpu_time
                FROM v$sql
                ORDER BY cpu_time DESC
            ) WHERE ROWNUM <= 5
        """)
        top_sql_statements = cursor.fetchall()
        for sql_statement in top_sql_statements:
            sql_id = sql_statement[0]
            sql_text = sql_statement[1]
            cpu_time = sql_statement[2]
            print("##TOP CPU SQL ID:", sql_id)
            print("##TOP CPU SQL Text:", sql_text)
            print("##TOP CPU Time:", cpu_time)
            print("-----")
            
        logging.debug("End of Query: Get the top cpu-consuming sql statements")
    except cx_Oracle.DatabaseError as e:
        logging.debug("DBerror", str(e))   

        # Close the cursor and connection
        logging.debug("Cursor Close")
        cursor.close()
        connection.close()
    
    
