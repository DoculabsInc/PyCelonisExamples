from pycelonis import get_celonis,pql,celonis_api

## Login to Celonis Environment
## returns celonis object
def login(url, key):
    l={
            "url": url,
            "api_token": key,
            "key_type": "USER_KEY"
            }

    return get_celonis(**l)

## Query audit table to get size of table in bytes
## return size of table in bytes
def get_audit_results(trans, table):
    y = []
    try:
        x = trans.execute_from_workbench(statement="SELECT size_bytes, object_name from user_audits where object_name = '{}' order by audit_start_timestamp desc".format(table))
        y = x[0]['result']['tableContent']
    except:
        pass 
    z = {}
    for row in y:
        if float(row[0]) > 0:
            z[row[1]] = row[0]
    if len(z) == 0:
        z[table] = 0
    return z

## Get the Columns from a table
## returns list of columns in table
def get_columns(trans, table_name):
    x=trans.execute_from_workbench(statement="SELECT * from columns where table_name='{}'".format(table_name))
    y=x[0]['result']['tableContent']
    z=[]
    for row in y:
        z.append([row[5],row[6]])
    return z


