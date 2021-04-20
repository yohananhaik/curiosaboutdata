import configparser
import os
import subprocess
import re
import psycopg2
from subprocess import PIPE,Popen
import pgpasslib


config = configparser.ConfigParser()
config.read('C:\Git\curiosaboutdata\scripts\python\settings.ini')
host = config.get('postgresql', 'host')
database = config.get('postgresql', 'database')
user = config.get('postgresql', 'user')
password = config.get('postgresql', 'password')
root_dir = config.get('postgresql', 'backup_dir')
mysqldump_dir = config.get('postgresql', 'mysqldump_dir')
get_routines = int(config.get('postgresql', 'get_routines'))
get_schema = int(config.get('postgresql', 'get_schema'))
get_data = int(config.get('postgresql', 'get_data'))
pg_dump='"' +mysqldump_dir + "pg_dump.exe"+'"'
limit_data=1


conn = psycopg2.connect(host=host,database=database,user=user,password=password)
cur = conn.cursor()
os.environ['PGPASSWORD'] = password 

pg_dump_base = pg_dump+ " -U {} ".format(user)

qry_db_list = "SELECT datname FROM pg_database;"
except_dbs = ["postgres", "template1", "template0", "sys"]
types_to_dump = ["EXTENSION","FUNCTION","INDEX","SEQUENCE","TABLE","VIEW"]
extensions=[]

def main():
    db_list = []
    db_list = run_qury(qry_db_list)
    for d in db_list:
        db_name = d[0]
        if not os.path.exists(root_dir):
            os.mkdir(root_dir)
        if db_name not in except_dbs and db_name == database:
                work_on_db(db_name)

def run_qury(qry_text):
    cur.execute(qry_text)
    return cur.fetchall()

def work_on_db(dbname):
    db_dir = os.path.join(root_dir, dbname)
    if not os.path.exists(db_dir):
        os.mkdir(db_dir)

    dump_file_dir = db_dir+"\\"+dbname+"_dump.sql"
    
    create_dump_file(dump_file_dir, dbname)

    dump_file = read_dump_file(dump_file_dir, dbname)

    dump = dump_file.replace("--\n","").split("-- ")

    for line in dump:
        if line.find("Name")==0:
            process_chunk(line)

    write_file(root_dir+"\\{}\\extensions.sql".format(database),"".join(extensions))

def process_chunk(line):
    create_index = line.find("CREATE")
    if create_index >= 0:
        ends = line.find(";")
        name = line[6:ends]
        type = line[ends+8:line.find("Schema")-2]
        if type in types_to_dump:
            chunk = line[create_index:]
            if type == "EXTENSION":
                extensions.append(chunk)
            elif type=="VIEW":
                save_chunk(type.lower()+"s",name,chunk)
            elif type =="FUNCTION":
                bracket = name.find("(")
                if bracket >0:
                    name = name[:bracket]
                save_chunk(type.lower()+"s",name,chunk)
            elif type in ["INDEX","SEQUENCE","TABLE"]:
                save_chunk("tables", "{}_{}".format(type.lower(),name),chunk)


def save_chunk(folder_type,file_name, chunk):
    # Generate path
    dir_check(root_dir+"\\{}\\{}\\".format(database,folder_type))
    # Write to file
    write_file(root_dir+"\\{}\\{}\\{}.sql".format(database,folder_type,file_name),chunk) 

  
def dir_check(dir_name):
    if not os.path.exists(dir_name):
        os.mkdir(dir_name)

def write_file(file_path, file_text):
    with open(file_path, "w", encoding="utf-8") as file_to_write:
        file_to_write.write(file_text)
        file_to_write.close()

def create_dump_file(dump_file_dir, dbname):
    dump_string = pg_dump_base + " -d {} -s > {}".format(database,dump_file_dir)
    res = subprocess.Popen(dump_string, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    out, err = res.communicate()

def read_dump_file(dump_file_dir, dbname):
    with open(dump_file_dir, 'r') as dump_file:
        return dump_file.read()


main()