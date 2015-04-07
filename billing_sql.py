'''
billing_sql
Functions that interface with the databases for billing
Created on Mar 3, 2015
@author: edmundwong
'''

import datetime
import json
import keyring
import pymysql
from sqlalchemy import create_engine, MetaData, Table, and_, or_, not_
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.declarative import declarative_base
from searchQuery import run_query


def db_connect(db):
    with open('./config.json') as f:
        cfg = json.load(f)["mysql"]
    host = cfg["host"]
    port = cfg["port"]
    user = cfg["user"]
    pw = keyring.get_password(cfg["pw_key_name"], cfg["pw_acct_name"])
    if db == "calpendo":
        db = cfg["calpendo_db"]
    elif db == "rmc":
        db = cfg["rmc_db"]

    # sqlalchemy
    global metadata
    global engine
    engine = create_engine('mysql+pymysql://{user}:{pw}@{host}:{port}/{db}'.format(host=host, user=user,
                                                                                   pw=pw, db=db, port=port), echo=False)
    metadata = MetaData(bind=engine)
    session = sessionmaker(bind=engine)
    s = session()
    return s


# Import parsed data from first excel file into rmc database
def insertIntoRMCTable1(result):
    s = db_connect("rmc")

    class Ris(declarative_base()):
        __table__ = Table('ris', metadata, autoload=True)
    for row in result:
        entry = Ris(gco=row[0], project=row[1], MRN=row[2], PatientsName=row[3],
                    BirthDate=row[4], target_organ=row[5], target_abbr=row[6],
                    accession_no=row[7], ScanDate=row[8], referring_md=row[9],
                    status=row[10])
        s.add(entry)
        try:
            s.commit()
        except IntegrityError:
            print "Warning: Duplicate row detected in ris table."
            s.rollback()


# Import parsed data from second excel file and updates rmc rows
def insertIntoRMCTable2(result):
    s = db_connect("rmc")

    class Ris(declarative_base()):
        __table__ = Table('ris', metadata, autoload=True)
    for row in result:
        s.query(Ris).filter(Ris.accession_no == row[0]).update({Ris.ScanDTTM: row[5],
                                                                Ris.CompletedDTTM: row[6],
                                                                Ris.Resource: row[11],
                                                                Ris.Duration: row[13]})


# Import calpendo into rmc
def importCalpendoIntoRMC_3(monthYear):
    result = run_query("call billingCalpendoByMonth('{monthYear}%')".format(monthYear=monthYear), "calpendo")
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
        entry = Ris(accession_no=row[0], gco=row[1], project=row[2], MRN=row[3], PatientsName=row[4],
                    BirthDate=row[5], target_organ=row[6], target_abbr=row[7],
                    ScanDate=row[8], referring_md=row[9], Duration=row[10], ScanDTTM=row[11],
                    CompletedDTTM=row[12], Resource=row[13], Investigator=row[14],
                    fund_no=row[15])
        s.add(entry)
        try:
            s.commit()
        except IntegrityError:
            print "Warning: Duplicate row detected in ris table."
            s.rollback()
    return result


def RMCPostImportSql(monthYear):
    s = db_connect("rmc")

    class Ris(declarative_base()):
        __table__ = Table('ris', metadata, autoload=True)

    class Rates(declarative_base()):
        __table__ = Table('rates', metadata, autoload=True)

    # Find new rates and import to database
    q1 = s.query(Ris.gco, Ris.Investigator, Ris.project,
                 Ris.target_organ, Ris.target_abbr, Ris.Resource)
    q2 = q1.outerjoin(Rates, and_(Rates.gco == Ris.gco,
                                  Rates.target_abbr == Ris.target_abbr)).distinct()
    newRates = q2.filter(and_(not_(Ris.gco.contains('spec')),
                              Rates.gco is None, Rates.target_abbr is None)).all()
    for row in newRates:
        if row[5].startswith('TMII HESS'):
            system = 'Calpendo'
        else:
            system = 'RIS'
        entry = Rates(gco=row[0], investigator=row[1], title=row[2],
                      target_organ=row[3], target_abbr=row[4], system=system)
        s.add(entry)
    s.commit()

    class Project_basics(declarative_base()):
        __table__ = Table('project_basics', metadata, autoload=True)

    # Find new RIS projects and import to database
    q1 = s.query(Ris.gco).distinct().outerjoin(Project_basics, Project_basics.gco == Ris.gco)
    q2 = q1.filter(and_(Project_basics.gco is None, not_(Ris.gco.contains('spec'))),
                   or_(not_(Ris.Resource.startswith('TMII HESS')), Ris.Resource is None))
    newRisProjects = q2.all()
    for row in newRisProjects:
        entry = Project_basics(gco=row[0])
        s.add(entry)
    s.commit()

    # Stamp new charges (may not be necessary)
    q = s.query(Ris).filter(and_(Ris.ScanDate.startswith(monthYear), Ris.charge is None,
                                 Ris.no_charge_comment is None))
    q.update({Ris.invoice_method: 'TMII Central v3.2', Ris.invoice_date: datetime.date.today()}, synchronize_session=False)
    return [newRates, newRisProjects]


def generateInvoiceData(moYr):
    # Generate finalized invoice results for various resources
    a1 = run_query("call generateInvoiceByMonth('{moYr}%')".format(moYr=moYr), "rmc")
    a2 = run_query("call brukerBillingByMonth('{moYr}%')".format(moYr=moYr), "calpendo")
    a3 = run_query("call ntrBillingByMonth('{moYr}%')".format(moYr=moYr), "calpendo")
    a4 = run_query("call srfBillingByMonth('{moYr}%')".format(moYr=moYr), "calpendo")
    return [a1, a2, a3, a4]


def updateRates(idx, changeBase, changeHalf):
    s = db_connect("rmc")

    class Rates(declarative_base()):
        __table__ = Table('rates', metadata, autoload=True)
    s.query(Rates).filter(Rates.id == idx).update({Rates.basecharge: changeBase, Rates.addhalfhourcharge: changeHalf})
