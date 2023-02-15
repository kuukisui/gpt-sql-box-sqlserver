"""Generate SQL Schema from PostgreSQL"""
import os
import sys
from dotenv import load_dotenv
import psycopg2

#libraries for SQL connections
import urllib
from sqlalchemy import create_engine

# import all ADL module functions required to perform all steps required for ADL
from modules.autodataloader import create_server_connection, sql_query_execute

# Read .env file
load_dotenv()

DATABASE_URL = os.getenv('DATABASE_URL')




#Testing obtaining database schema info and parsing into GPT-3 Inspired from: https://medium.com/@hormold/make-gpt-3-work-for-you-17a3bf744234
class Schema:
    """Generate SQL Schema from PostgreSQL"""
    
    def __init__(self, 
                 db_server,
                 db_name,
                 schema = None,
                ):
        """Connect to database"""
        self.schema = schema
        self.db_server = db_server
        self.db_name = db_name
    
        ########################################################
        # step 1) 	Connect to target SQL server database
        ########################################################

        engine, error_message  = create_server_connection(db_server, db_name)


        # if an error has occured, abort by returning from this function
        if error_message:
            print("Error encountered whilst attempting to connect to target DB...ABORTING...")
            sys.exit(1)
        else:
            print("Connecting to target DB complete...")


        self.conn = engine
        
        self.comments = []
        self.tables = []
        self.columns = []
        
        
    def get_tables(self):
        """Get list of tables"""
        
        query = f"SELECT table_name FROM information_schema.tables"

        if self.schema is not None:
            query = query + f" WHERE table_schema = '{self.schema}'"

        tables_cur = self.conn.execute(query)
        tables = tables_cur.fetchall()
        self.tables = tables
        return tables

    def get_all_comments(self):
        """Get list of all comments"""
        # https://stackoverflow.com/questions/59082755/accessing-table-comments-in-sql-server
        
        query = """
        select   --t.id                        as  [object_id]
                 CAST(schema_name(t2.schema_id)  AS VARCHAR(2000)) as  table_schema
                ,CAST(t.name                     AS VARCHAR(2000)) as  table_name
                ,CAST(t3.name                    AS VARCHAR(2000)) as  column_name
                ,CAST(t4.value                   AS VARCHAR(2000)) as  column_description
                ,CAST(t5.value                   AS VARCHAR(2000)) as  table_description
        from    sysobjects t
        inner join sys.tables t2 on t2.object_id = t.id
        inner join sys.columns t3 on t3.object_id = t.id
        left join sys.extended_properties t4 on t4.major_id = t.id
                                                and t4.name = 'MS_Description'
                                                and t4.minor_id = t3.column_id
        left join sys.extended_properties t5 on t5.major_id = t.id
                                                and t5.name = 'MS_Description'
                                                and t5.minor_id = 0
        """
        
        if self.schema is not None:
            query = query + f" WHERE   t2.schema_id = schema_id('{self.schema}')"
        
        
        comments_cur = self.conn.execute(query)
        comments = comments_cur.fetchall()
        self.comments = comments
        return comments
    
    
    def get_columns(self, table):
        """Get list of columns for a table"""
                
        query = f"SELECT column_name, data_type FROM information_schema.columns"
        query = query + f" WHERE table_name = '{table}'"

        if self.schema is not None:
            query = query + f" AND table_schema = '{self.schema}'"
        
        columns_cur = self.conn.execute(query)
        columns = columns_cur.fetchall()
        return columns
    

    def regen(self, selected):
        """Regenerate SQL Schema only for selected tables"""
        if len(selected) == 0:
            return 'No tables selected.'
        prompt = ''
        tables = self.tables
        comments = self.comments
        for table in tables:
            if table[0] in selected:
                columns = self.get_columns(table[0])
                prompt += f'The "{self.schema}'+'.'+f'{table[0]}" table has columns: '
                for column in columns:
                    cmnt = ''
                    for comment in comments:
                        if comment[0] == self.schema and comment[1] == table[0] and comment[2] == column[0]:
                            cmnt = comment[3]
                            break
                    if cmnt == '':
                        prompt += f'{column[0]} ({column[1]}), '
                    else:
                        prompt += f'{column[0]} ({column[1]} - {cmnt}), '
                prompt = prompt[:-2] + '. '
        return prompt

    def index(self):
        """Generate SQL Schema"""
        prompt = ''
        json_data = {}
        tables = self.get_tables()        
        comments = self.get_all_comments()
        for table in tables:
            columns = self.get_columns(table[0])
            prompt += f'The "{self.schema}'+'.'+f'{table[0]}" table has columns: '
            json_data[table[0]] = []
            for column in columns:
                cmnt = ''
                for comment in comments:
                    if comment[0] == self.schema and comment[1] == table[0] and comment[2] == column[0]:
                        cmnt = comment[3]
                        break
                if cmnt == '':
                    prompt += f'{column[0]} ({column[1]}), '
                else:
                    prompt += f'{column[0]} ({column[1]} - {cmnt}), '
                json_data[table[0]].append({
                    'name': column[0],
                    'type': column[1],
                    'comment': cmnt,
                    "seleted": True
                })
            prompt = prompt[:-2] + '. '
        return prompt, json_data