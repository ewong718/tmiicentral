'''
Created on Mar 3, 2015
@author: edmundwong
'''
import json
import keyring
import pymysql
import datetime
from sqlalchemy import create_engine, MetaData, Table, and_, or_, not_
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from searchQuery import run_query

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
    
    # sqlalchemy
    global metadata
    global engine
    engine = create_engine('mysql+pymysql://{user}:{pw}@{host}:{port}/{db}'.format(host = host, user = user,
                                                                                   pw = pw, db = db, port = port), echo = False)
    metadata = MetaData(bind=engine)
    session = sessionmaker(bind=engine)
    s = session()    
    return s


def insertIntoRMCTable1(result):
    s = db_connect("rmc")
    class Ris(declarative_base()):
        __table__ = Table('ris', metadata, autoload=True)
    for row in result:
        entry = Ris(gco = row[0], project = row[1], MRN = row[2], PatientsName = row[3],
                        BirthDate = row[4], target_organ = row[5], target_abbr = row[6],
                        accession_no = row[7], ScanDate = row[8], referring_md = row[9],
                        status = row[10])
        s.add(entry)
    s.commit()

        
def insertIntoRMCTable2(result):
    s = db_connect("rmc")
    class Ris(declarative_base()):
        __table__ = Table('ris', metadata, autoload=True)
    for row in result:       
        s.query(Ris).filter(Ris.accession_no == row[0]).update({Ris.ScanDTTM: row[5],
                                                                Ris.CompletedDTTM: row[6],
                                                                Ris.Resource: row[11],
                                                                Ris.Duration: row[13]})      


def insertIntoRMCTable3(result):
    s = db_connect("rmc")
    class Ris(declarative_base()):
        __table__ = Table('ris', metadata, autoload=True)
    for row in result:
        row = list(row)
        for idx, val in enumerate(row):
            try:
                row[idx] = pymysql.escape_string(unicode(val))
            except UnicodeDecodeError:
                row[idx] = pymysql.escape_string(val.decode('iso-8859-1'))
        entry = Ris(gco = row[0], project = row[1], MRN = row[2], PatientsName = row[3],
                    BirthDate = row[4], target_organ = row[5], target_abbr = row[6],
                    ScanDate = row[7], referring_md = row[8], Duration = row[9], ScanDTTM = row[10],
                    CompletedDTTM = row[11], Resource = row[12], Investigator = row[13],
                    fund_no = row[14])
        s.add(entry)
    s.commit()
        
        
def RMCPostImportSql(monthYear):
    #cur = db_connect("rmc")
    
    s = db_connect("rmc")
    class Ris(declarative_base()):
        __table__ = Table('ris', metadata, autoload=True)
    class Rates(declarative_base()):
        __table__ = Table('rates', metadata, autoload=True)
      
    #Find new rates and import to database
    q1 = s.query(Ris.gco, Ris.Investigator, Ris.project, Ris.target_organ, Ris.target_abbr, Ris.Resource)
    q2 = q1.outerjoin(Rates, and_(Rates.gco == Ris.gco, Rates.target_abbr == Ris.target_abbr)).distinct()
    newRates = q2.filter(and_(not_(Ris.gco.contains('spec')), Rates.gco == None, Rates.target_abbr == None)).all()
    for row in newRates:
        if row[5].startswith('TMII HESS'):
            system = 'Calpendo'
        else:
            system = 'RIS'
        entry = Rates(gco = row[0], investigator = row[1], title = row[2],
                      target_organ = row[3], target_abbr = row[4], system = system)
        s.add(entry)
    s.commit()
    
    class Project_basics(declarative_base()):
        __table__ = Table('project_basics', metadata, autoload=True)
      
    #Find new RIS projects and import to database
    q1 = s.query(Ris.gco).distinct().outerjoin(Project_basics, Project_basics.gco == Ris.gco)
    q2 = q1.filter(and_(Project_basics.gco == None, not_(Ris.gco.contains('spec'))),
                   or_(not_(Ris.Resource.startswith('TMII HESS')), Ris.Resource == None))
    newRisProjects = q2.all()
    for row in newRisProjects:
        entry = Project_basics(gco = row[0])
        s.add(entry)
    s.commit()
    
    q1 = s.query(Ris).outerjoin(Rates, and_(Rates.gco == Ris.gco, Rates.target_abbr == Ris.target_abbr))
    q2 = q1.filter(and_(Ris.ScanDate.startswith(monthYear), Ris.charge == None,
                        Ris.no_charge_comment == None))
    q2.update({Ris.invoice_method: 'TMII Central v3.2', Ris.invoice_date: datetime.date.today()}, synchronize_session = False)
    return [newRates, newRisProjects]

def return_HumanScans_billing(monthYear):

    humanBillableScanList = run_query("call GenerateInvoiceByMonth('{monthYear}%')".format(monthYear = monthYear), "rmc")
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