'''
Created on Mar 3, 2015
@author: edmundwong
'''
import pymysql, json, keyring

def db_connect(db):
    with open('./config.json') as f: 
        cfg = json.load(f)["mysql"]
    host = cfg["host"]
    port = cfg["port"]
    user = cfg["user"]
    pw = keyring.get_password(cfg["pw_key_name"],cfg["pw_acct_name"])
    if db == "calpendo":
        db = cfg["calpendo_db"]
    elif db == "rmc":
        db = cfg["rmc_db"]
    conn = pymysql.connect(host=host, port=port, user=user, passwd=pw, db=db)
    cur = conn.cursor()
    return cur

def insertIntoRMCTable1(result):
    cur = db_connect("rmc")
    
    for row in result:
        query_block = """INSERT INTO ris (gco, project, MRN, PatientsName, BirthDate, target_organ, target_abbr, accession_no, ScanDate, referring_md, status) VALUES ('""" + row[0] + "','" + row[1] + "','" + row[2] + "','" + row[3] + "','" + row[4] + "','" + row[5] + "','" + row[6] + "','" + row[7] + "','" + row[8] + "','" + row[9] + "','" + row[10] + "')" 
        cur.execute(query_block)
        
def insertIntoRMCTable2(result):
    cur = db_connect("rmc")    
    query_block = """CREATE TABLE temp_rsrch_dump (
                    Accession INT(12) NULL,
                    LastName VARCHAR(50) NULL,
                    FirstName VARCHAR(50) NULL,
                    ScheduledDTTM DATETIME NULL,
                    CompletedDTTM DATETIME NULL,
                    PerformingResourceDesc VARCHAR(120) NULL,
                    DurationMinutes INT(11) NULL,
                    UNIQUE INDEX Accession (Accession)
                    )
                    COLLATE='latin1_swedish_ci'
                    ENGINE=MyISAM;"""
    cur.execute(query_block)
    for row in result:
        query_block = """INSERT INTO temp_rsrch_dump (Accession, LastName, FirstName, ScheduledDTTM, CompletedDTTM, PerformingResourceDesc, DurationMinutes) VALUES ('""" + row[0] + "','" + row[2] + "','" + row[3] + "','" + row[5] + "','" + row[6] + "','" + row[11] + "','" + row[13] + "')"
        cur.execute(query_block)
    cur.execute("""UPDATE ris
                INNER JOIN temp_rsrch_dump
                ON ris.accession_no=temp_rsrch_dump.Accession
                SET ris.LastName = temp_rsrch_dump.LastName,
                ris.FirstName = temp_rsrch_dump.FirstName,
                ris.Duration = temp_rsrch_dump.DurationMinutes,
                ris.ScanDTTM = temp_rsrch_dump.ScheduledDTTM,
                ris.CompletedDTTM = temp_rsrch_dump.CompletedDTTM,
                ris.Resource = temp_rsrch_dump.PerformingResourceDesc""")    
    cur.execute("DROP TABLE temp_rsrch_dump")
    
def insertIntoRMCTable3(result):
    cur = db_connect("rmc")
    for row in result:
        row = list(row)
        for idx, val in enumerate(row):
            try:
                row[idx] = pymysql.escape_string(unicode(val))
            except UnicodeDecodeError:
                row[idx] = pymysql.escape_string(val.decode('iso-8859-1'))
        
            
        query_block = """INSERT INTO ris (gco, project, MRN, PatientsName, BirthDate, target_organ, target_abbr, ScanDate, referring_md, LastName, FirstName, Duration, ScanDTTM, CompletedDTTM, Resource, Investigator, fund_no) VALUES ('""" + row[0] + "','" + row[1] + "','" + row[2] + "','" + row[3] + "','" + row[4] + "','" + row[5] + "','" + row[6] + "','" + row[7] + "','" + row[8] + "','" + row[9] + "','" + row[10] + "','" + row[11] + "','" + row[12] + "','" + row[13] + "','" + row[14] + "','" + row[15] + "','" + row[16] + "')"
        cur.execute(query_block)
        
def RMCPostImportSql(monthYear):
    cur = db_connect("rmc")
    
    #Find new rates and import to database
    query_block = """SELECT DISTINCT
                    ris.gco, ris.Investigator, ris.project, ris.target_organ, ris.target_abbr,
                    case when ris.Resource like 'TMII HESS%' then 'Calpendo' else 'RIS' end as 'system'
                    FROM ris LEFT JOIN rates on rates.gco=ris.gco and rates.target_abbr=ris.target_abbr
                    WHERE ris.gco not like '%spec%' and
                    rates.gco is null and
                    rates.target_abbr is null"""
    cur.execute(query_block)
    newRates = cur.fetchall()
    cur.execute("""INSERT INTO rates (gco, investigator, title, target_organ, target_abbr, system) """ + query_block)
    
    #Find new RIS projects and import to database
    query_block = """select distinct ris.gco from ris
                  left join project_Basics on project_basics.gco=ris.gco 
                  where project_Basics.gco is null and ris.gco not like '%spec%' and
                   ris.gco not like '' and (ris.Resource not like 'TMII HESS%' or ris.Resource is null);"""
    cur.execute(query_block)
    newRisProjects = cur.fetchall()
    cur.execute("""insert into project_basics (gco) """ + query_block)
    
    cur.execute("""update ris
                left join rates on
                ris.gco = rates.gco and ris.target_abbr = rates.target_abbr
                SET invoice_method = "TMII Central v3.0",
                Invoice_Date = curdate(),
                ris.charge = case when ris.duration is null then rates.basecharge
                else round((ris.Duration/60-1)*(2*rates.addhalfhourcharge)+rates.basecharge,2) end
                WHERE ris.scandate like '""" + monthYear + """%' and
                ris.charge is null and
                ris.no_charge_comment is null and
                ris.charge_or_not = 1;""")

    return [newRates, newRisProjects]

def return_HumanScans_billing(monthYear):
    
    cur = db_connect("rmc")
    cur.execute("""select
                case
                when ris.Resource like 'TMII HESS%' then ris.Investigator
                else
                project_basics.investigator
                end as 'Investigator',
                case
                when ris.Resource like 'TMII HESS%' then ris.fund_no
                else
                project_basics.fund_no
                end as 'Fund Number',
                ris.target_abbr as "Catalog",
                ris.target_organ as "Description",
                ris.gco as "GCO Number",
                ris.patientsname as "Subject ID",
                ris.scandate as "Date of Service",
                ris.charge as "Unit Price","1" as Quantity,
                ris.charge as "Total",
                ris.Resource as "Resource",
                ris.Duration as "Duration"
                from ris
                left join project_basics on ris.gco = project_basics.gco
                where
                ris.scandate like '""" + monthYear + """%' and
                 (ris.charge > '0' or ris.charge is null) and ris.GCO not like '%D'""")
    
    humanBillableScanList = cur.fetchall()
    return humanBillableScanList

def return_SmallAnimal_billing(monthYear):
    
    cur = db_connect("calpendo")
    cur.execute("""select
                concat(projects.pi_firstname,' ',projects.principal_investigator) as 'Investigator',
                projects.fund_number as 'Fund Number',
                projects.project_code as 'GCO Number', date_format(bookings.start_date,'%Y-%m-%d') as 'Date of Service (based on start time)',
                case
                when dayofweek(bookings.start_date)=1 then 'Sun'
                when dayofweek(bookings.start_date)=2 then 'Mon'
                when dayofweek(bookings.start_date)=3 then 'Tue'
                when dayofweek(bookings.start_date)=4 then 'Wed'
                when dayofweek(bookings.start_date)=5 then 'Thur'
                when dayofweek(bookings.start_date)=6 then 'Fri'
                when dayofweek(bookings.start_date)=7 then 'Sat'
                else
                'noday'
                end as 'Day (based on start time)',
                
                date_format(bookings.start_date,'%H:%i') as 'Scheduled Start Time',
                date_format(bookings.finish_date,'%H:%i') as 'Scheduled Finish Time',
                round(bookings.duration/60,2) as 'Scheduled Duration (Hours)', 
                
                resources.name as 'Resource'
                from bookings
                left join projects on bookings.project_id=projects.id
                left join resources on bookings.resource_id=resources.id
                where
                projects.project_code NOT LIKE '%-%D%'and
                (bookings.development LIKE '%NO%' or bookings.development IS NULL) and
                bookings.start_date like '""" + monthYear + """%' and
                (bookings.status = 'APPROVED' or bookings.status = 'REQUESTED') and
                (resources.id='4' or resources.id='5') and
                projects.project_code NOT LIKE 'BRUKER_DEV-0001'
                order by start_date""")
    
    smallAnimalBillableScanList = cur.fetchall()
    return smallAnimalBillableScanList

def return_NTR_billing(monthYear):
    
    cur = db_connect("calpendo")
    cur.execute("""select
                    projects.project_code as 'gco',
                    concat(projects.pi_firstname,' ',projects.principal_investigator) as 'Investigator',
                    bookings.MRN as 'MRN',
                    
                    case
                    when bookings.last_name = '' and bookings.first_name = ''
                    then NULL
                    else
                    concat(bookings.last_name,', ', bookings.first_name) 
                    end as'ParientsName',
                    
                    #null as 'accession_no',
                    date_format(bookings.start_date,'%Y-%m-%d') as 'Date',
                    #null as 'status',
                    
                    
                    round(bookings.duration/60,2) as 'Duration(hours)',
                    concat('TMII HESS ', resources.name) as 'Resource',
                    projects.fund_number as 'fund_no'
                    
                    from bookings
                    left join projects on bookings.project_id=projects.id
                    left join resources on bookings.resource_id=resources.id
                    left join project_types on projects.type_id=project_types.id
                    left join users on projects.owner_id=users.id 
                    where
                    bookings.status = 'APPROVED' and 
                    bookings.start_date like '""" + monthYear + """%' and
                    (bookings.resource_id = 18) and
                    project_types.id in (2,3,9,10,11) and
                    projects.project_code NOT LIKE '%-%D'and
                    (bookings.development LIKE '%NO%' or bookings.development IS NULL) and
                    (projects.funding_source != 'GCRC' or projects.funding_source IS NULL) and
                    projects.fund_number like '%-%'
                    order by bookings.start_date""")
    ntrBillableScanList = cur.fetchall()
    return ntrBillableScanList

def return_SRF_billing(monthYear):
    cur = db_connect("calpendo")
    cur.execute("""select 
                projects.project_code as 'GCO#',
                resources.name as 'Resource Name',
                bookings.start_date,
                bookings.finish_date,
                round(bookings.duration/60,2) as 'Duration (Hours)',
                case
                when (bookings.resource_id=15 or bookings.resource_id=16) then concat(round(bookings.duration/60 * 100,2)) 
                when (bookings.resource_id=37 or bookings.resource_id=38) then concat(round(bookings.duration/60 * 40,2))
                end as 'Price',
                bookings.description as 'User and Notes',
                concat(projects.pi_firstname,' ',projects.principal_investigator) as 'Investigator',
                projects.department,
                projects.fund_number
                from bookings 
                left join resources on bookings.resource_id=resources.id 
                left join projects on bookings.project_id=projects.id
                where
                bookings.resource_id in (15,16,37,38) and
                start_date like '""" + monthYear + """%' and
                bookings.status = 'APPROVED'and
                projects.project_code not like '%SRF_Testing%'
                order by start_date""")
    srfBillableScanList = cur.fetchall()
    return srfBillableScanList

def updateRates(idx, changeBase, changeHalf):
    cur = db_connect("rmc")
    cur.execute(""" UPDATE rates
    SET basecharge=""" + str(changeBase) + ", addhalfhourcharge=" + str(changeHalf) + """
    WHERE id = """ + str(idx))