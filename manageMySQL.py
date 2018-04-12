import warnings
import mysql.connector
from mysql.connector import errorcode
import os
import json


def get_login_info():
    try:
        user = os.environ['SQL_USER']
        password = os.environ['SQL_PASSWORD']
        host = os.environ['SQL_HOST']
    except KeyError:
        return "", "", ""
    else:
        return user, password, host


def update_server_config():
        return {
            'user': get_login_info()[0],
            'password': get_login_info()[1],
            'host': get_login_info()[2],
            'raise_on_warnings': True,
        }


def connect():
    config = update_server_config()
    try:
        cnx = mysql.connector.connect(**config)
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print "%s: %s" % (err.errno, err.msg)
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            print "%s: %s" % (err.errno, err.msg)
        else:
            print "%s: %s" % (err.errno, err.msg)
    else:
        return cnx


def create_database(db):
    connection = connect()
    c = connection.cursor()
    print "Creating database '{}'".format(db)
    try:
        c.execute("CREATE DATABASE {} DEFAULT CHARACTER SET 'utf8'".format(db))
    except mysql.connector.Error as err:
        print "%s: %s" % (err.errno, err.msg)
    c.close()
    connection.close()


def show_all_tables(db):
    connection = connect()
    c = connection.cursor()
    try:
        connection.database = db
    except mysql.connector.Error as err:
        print "%s: %s" % (err.errno, err.msg)
    else:
        c.execute("SHOW TABLES")
        tables = c.fetchall()
        print "Fetching all tables in database: %s" % db
        print str(len(tables)) + " tables found"
        return tables
    c.close()
    connection.close()


def get_column_names(db, table):
    query = ("SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = '%s'"
             " AND TABLE_NAME = '%s';") % (db, table)
    connection = connect()
    c = connection.cursor()
    try:
        connection.database = db
    except mysql.connector.Error as err:
        print err
        exit(err.errno)
    c.execute(query)

    column_names = []
    for COLUMN_NAME in c:
        column_names.append(COLUMN_NAME[0])

    c.close()
    connection.close()
    return column_names


def select_column_from_table(db, table, column="*", limit="1000"):
    query = "select %s FROM %s LIMIT %s" % (column, table, limit)
    connection = connect()
    c = connection.cursor()
    try:
        connection.database = db
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_BAD_DB_ERROR:
            print "Database '%s' not found" % db
            print "%s: %s" % (err.errno, err.msg)
        else:
            print "%s: %s" % (err.errno, err.msg)
    items = []
    try:
        print query
        c.execute(query)
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_NO_SUCH_TABLE:
            print "table: '%s' does not exist..." % table
            print "%s: %s" % (err.errno, err.msg)
        else:
            print "%s: %s" % (err.errno, err.msg)
    else:
        for item in c:
            items.append(item)

    c.close()
    connection.close()
    return items


def select_item_from_table(db, table, where):
    query = "select * FROM %s WHERE %s" % (table, where)
    connection = connect()
    c = connection.cursor()
    try:
        connection.database = db
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_BAD_DB_ERROR:
            print "Database '%s' not found" % db
            print "%s: %s" % (err.errno, err.msg)
        else:
            print "%s: %s" % (err.errno, err.msg)
    items = []
    try:
        print query
        c.execute(query)
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_NO_SUCH_TABLE:
            print "table: '%s' does not exist..." % table
            print "%s: %s" % (err.errno, err.msg)
        else:
            print "%s: %s" % (err.errno, err.msg)
    else:
        for item in c:
            items.append(item)

    c.close()
    connection.close()
    return items


def create_table(db, table_name, table_info, overwrite=False):
    connection = connect()
    c = connection.cursor()
    query = "CREATE TABLE `%s` (%s)" % (table_name, table_info)
    try_again = True

    try:
        connection.database = db
    except mysql.connector.Error as err:
        print "%s: %s" % (err.errno, err.msg)
    else:
        while try_again:
            try:
                try_again = False
                print "Creating table '{}': ".format(table_name)
                c.execute(query)
            except mysql.connector.Error as err:
                if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
                    if overwrite is True:
                        print "Table '%s' exists." % table_name
                        drop_table(db, table_name)
                        try_again = True
                    else:
                        print "%s: %s" % (err.errno, err.msg)
                        print ("Table: '%s' already exists. If you wish to overwrite this anyway, "
                               "specify 'overwrite=True' \n"
                               "WARNING!!! - Doing so will cause all data in this table to be deleted"
                               " and cannot be recovered!!!" % table_name)
                else:
                    print "%s: %s" % (err.errno, err.msg)
            else:
                print "OK"
    c.close()
    connection.close()


def drop_table(db, table_name):
    connection = connect()
    c = connection.cursor()
    query = "DROP TABLE IF EXISTS `%s`" % table_name

    try:
        connection.database = db
    except mysql.connector.Error as err:
        print "%s: %s" % (err.errno, err.msg)
    else:
        try:
            print "Dropping table '{}': ".format(table_name)
            c.execute(query)
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_BAD_TABLE_ERROR:
                print ("Table: '%s' not found." % table_name)
                print "%s: %s" % (err.errno, err.msg)
            else:
                print "%s: %s" % (err.errno, err.msg)
        else:
                print "OK"
        c.close()
        connection.close()


def truncate_table(db, table, safe=True):
    connection = connect()
    c = connection.cursor()
    if safe:
        query = "TRUNCATE TABLE `%s`" % table
    else:
        query = "SET FOREIGN_KEY_CHECKS = 0; \n TRUNCATE TABLE `%s`; \n SET FOREIGN_KEY_CHECKS = 1;" % table
    try:
        connection.database = db
    except mysql.connector.Error as err:
        print "%s: %s" % (err.errno, err.msg)
    try:
        print "Truncating '{}' table ".format(table)
        c.execute(query)
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_NO_SUCH_TABLE:
            print "table: '%s' does not exist..." % table
            print "%s: %s" % (err.errno, err.msg)
        else:
            print "%s: %s" % (err.errno, err.msg)
    else:
        print "OK"
    print "."
    c.close()
    connection.close()


def get_policies(db):
    connection = connect()
    c = connection.cursor()

    try:
        connection.database = db
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_BAD_DB_ERROR:
            print "%s: %s" % (err.errno, err.msg)
    print "Retrieving Policies..."
    policies = []

    try:
        for item in policies:
            c.execute("INSERT INTO Policies (PolicyId, LastModifiedUtc, PolicyObject) VALUES (%s,%s,%s)",
                      (item['PolicyId'], item['LastModifiedUtc'], item['PolicyObject']))
        connection.commit()
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_DUP_ENTRY:
            print "POLICIES NOT RESTORED - Duplicate entry found. Verify truncate operation was successful"
        else:
            print err.msg
    else:
        print "OK"
    c.close()
    connection.close()
    # Close DB and connection


def drop_db(db):
    connection = connect()
    c = connection.cursor()
    print "Dropping database '{}'...".format(db)
    try:
        c.execute("DROP DATABASE `{}` ".format(db))
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_DB_DROP_EXISTS:
            print "%s: %s" % (err.errno, err.msg)
        else:
            print "%s: %s" % (err.errno, err.msg)
    else:
        print "OK"
    c.close()
    connection.close()


def delete_entry(db, table, column, value):
    connection = connect()
    c = connection.cursor()
    print "Deleting record from database '{}'...".format(db)
    try:
        connection.database = db
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_BAD_DB_ERROR:
            create_database(db)
            connection.database = db
        else:
            print "%s: %s" % (err.errno, err.msg)
    try:
        c.execute("DELETE FROM `" + table + "` WHERE `" + column + "` = '" + value + "'")
        connection.commit()
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_EVENT_CANNOT_DELETE:
            print "Failed to Delete record - Not found"
            print "%s: %s" % (err.errno, err.msg)
        else:
            print "%s: %s" % (err.errno, err.msg)
    else:
        print "OK"
    c.close()
    connection.close()


# Everything below this line is for testing/debugging

table_information = ("`Id` varchar(767) NOT NULL,"
                     "`Name` text,"
                     "PRIMARY KEY (`Id`)")
#drop_db("cjsm", "seantest")
create_database("seantest")
# drop_table("seantest", "sean")
#create_table("seantest", "sean", table_information, overwrite=True)
# INSERT into table
#print select_item_from_table("mxstore", "Submissions", where="Id='004b1e2dd254413da787b164f41b492a'")
#truncate_table("seanstore", "Submissions")
#print show_all_tables("seanstore")
#print get_column_names("seanstore", "Files")
#print select_column_from_table("seanstore", "Submissions", "Id")
