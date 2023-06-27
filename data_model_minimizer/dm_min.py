from pycelonis import get_celonis,pql,celonis_api
import json,sys,getopt

def minimize_table(trans, table_name):
    p=pql.PQL()
    p.add(pql.PQLColumn(query='"{}"."{}"'.format(activity_table,activity_col), name="activity"))
    p.add(pql.PQLColumn(query='LEN("{}"."{}")'.format(activity_table,activity_col),name="len"))
    p.add(pql.PQLColumn(query="COUNT_TABLE({})".format(activity_table),name="num"))

    x=dm.get_data_frame(p)
    activity_code='CASE '
    translations={}
    # check number of activities
    if len(x.index) > len(codes):
        print("Error: Too Many Activities, {}".format(len(x.index)))
        sys.exit()
    for y, c in zip(x.iterrows(), codes):
        index, row = y
        activity_code += "WHEN {} = '{}' THEN '{}' ".format(activity_col, row['activity'], c)
        translations[row['activity']] = c
    activity_code += 'END'
    create_table(trans, activity_table, new_table, activity_col, activity_code, translations, trans_table)



def get_columns(trans, table_name):
    x=trans.execute_from_workbench(statement="SELECT * from columns where table_name='{}'".format(table_name))
    y=x[0]['result']['tableContent']
    z=[]
    for row in y:
        z.append([row[5],row[6]])
    return z

def create_translation_table(trans, translations, trans_table):
    print(translations)
    statement='drop table if exists {}; create table {} ( Activity varchar(100), code varchar(1) ); '.format(trans_table, trans_table)
    for t in translations:
        statement += "insert into {} ( Activity, code ) values ( '{}', '{}' ); ".format(trans_table, t, translations[t])
    print(statement)
    trans.execute_from_workbench(statement=statement)
    return

def create_table(trans, source_table, new_table, activity_col, activity_code, translations, trans_table):
    cols=get_columns(trans, source_table)
    statement='drop table if exists {}; create table {} ( '.format(new_table, new_table)
    insert='insert into {} ( '.format(new_table)
    sel='select '
    for r in cols:
        statement += '{} {},'.format(r[0],r[1])
        insert += '{},'.format(r[0])
        if r[0] == activity_col:
            sel += '{} as {},'.format(activity_code, activity_col)
        else:
            sel += '{},'.format(r[0])
    statement=statement[:-1]
    statement+= ' );'
    insert=insert[:-1]
    insert+= ' )'
    sel=sel[:-1]
    sel += ' from {}'.format(source_table)
    statement += insert
    statement += sel
    trans.execute_from_workbench(statement=statement)
    create_translation_table(trans,translations,trans_table)
    return

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



def login(url, key):
    l={
            "url": url,
            "api_token": key,
            "key_type": "USER_KEY"
            }

    return get_celonis(**l)

def check_global(job,source):
    y=''
    if source == 'global':
        try:
            y=job.data_connection
        except:
            y='global'
    else:
        try:
            if source == job.data_connection[0].name:
                y='global'
        except:
            y=''
    return y

def usage():
    print("Data Model Minimization")
    print("    -h: show this text")
    print("    -e: celonis environment profile name (if in ~/.celonis-content-cli-profiles/")
    print("    -p: name of celonis data pool")
    print("    -m: mode, either create or audit")
    print("    -n: name of the new table created")
    print("    -c: name of data connection of data job scope")

def main():

    try:
        opts, args = getopt.getopt(sys.argv[1:], "he:p:d:m:n:c:", ["help","env=","pool=","dm=","mode=","new=","conn="])
    except getopt.GetoptError:
        usage()
        sys.exit(2)

    new_table='new_table'
    data_connection='global'

    for o, a in opts:
        if o in ("-h","--help"):
            usage()
            sys.exit()
        elif o in ("-e","--env"):
            profile=a
        elif o in ("-p","--pool"):
            data_pool=a
        elif o in ("-d","--dm"):
            data_model=a
        elif o in ("-m","--mode"):
            mode=a
        elif o in ("-n","--new"):
            new_table=a
        elif o in ("-c","--conn"):
            data_connection=a
        elif o in ("-t", "--table"):
            table=a

    codes='abcdefghijklmnopqrstuvwxyz1234567890'
    trans_table=new_table+'_TRANSLATIONS'

    with open("/home/{}/.celonis-content-cli-profiles/{}.json".format("james",profile),'r') as f:
        x=json.loads(f.read())

    source=login(x['team'],x['apiToken'])

    pools=source.pools
    pool=[x for x in pools if x.name == data_pool][0]
    dm=[x for x in pool.datamodels if x.name == data_model][0]
    jobs=pool.data_jobs
    trans=[x for x in jobs if check_global(x,data_connection)=='global'][0].transformations[0]
    activity_table=dm.process_configurations[0].activity_table.name
    try:
        case_table=dm.process_configurations[0].case_table.name
    except:
        case_table=activity_table + "_CASES"
    activity_col=dm.process_configurations[0].activity_column
    time_col=dm.process_configurations[0].timestamp_column
    case_col=dm.process_configurations[0].case_column

    if mode == 'create':

        p=pql.PQL()
        p.add(pql.PQLColumn(query='"{}"."{}"'.format(activity_table,activity_col), name="activity"))
        p.add(pql.PQLColumn(query='LEN("{}"."{}")'.format(activity_table,activity_col),name="len"))
        p.add(pql.PQLColumn(query="COUNT_TABLE({})".format(activity_table),name="num"))

        x=dm.get_data_frame(p)
        activity_code='CASE '
        translations={}
        # check number of activities
        if len(x.index) > len(codes):
            print("Error: Too Many Activities, {}".format(len(x.index)))
            sys.exit()
        for y, c in zip(x.iterrows(), codes):
            index, row = y
            activity_code += "WHEN {} = '{}' THEN '{}' ".format(activity_col, row['activity'], c)
            translations[row['activity']] = c
        activity_code += 'END'
        create_table(trans, activity_table, new_table, activity_col, activity_code, translations, trans_table)

    elif mode == 'audit':

        res=get_audit_results(trans, activity_table)

        old_gbs=float(res[activity_table])/1024/1024/1024

        new_gbs=(float(get_audit_results(trans, new_table)[new_table])+float(get_audit_results(trans, trans_table)[trans_table]))/1024/1024/1024
        # new_gbs=float(res[new_table])/1024/1024/1024

        print("Performing audit on {}:".format(profile))
        if new_gbs == 0:
            print('New table created, check back for size savings')
        else:
            print('Old GBs: {}'.format(old_gbs))
            print('New GBs: {}'.format(new_gbs))
            print('Saved {} GBs'.format(old_gbs-new_gbs))
            print('    or {}%'.format(((old_gbs-new_gbs)/old_gbs)*100))

main()
