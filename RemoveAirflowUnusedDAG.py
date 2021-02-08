import sys
import MySQLdb

dag_id = sys.argv[1]

query = {'delete from xcom where dag_id = "' + dag_id + '"',
        'delete from task_instance where dag_id = "' + dag_id + '"',
        'delete from sla_miss where dag_id = "' + dag_id + '"',
        'delete from log where dag_id = "' + dag_id + '"',
        'delete from job where dag_id = "' + dag_id + '"',
        'delete from dag_run where dag_id = "' + dag_id + '"',
        'delete from dag where dag_id = "' + dag_id + '"'}

def connect(query):
        db = MySQLdb.connect(host="localhost", user="root", passwd="test1", db="airflow")
        cur = db.cursor()
        cur.execute(query)
        db.commit()
        db.close()
        return

for value in query:
        connect(value)
