'''
billing_sql
Functions that interface with the databases for billing
Created on Mar 3, 2015
@author: edmundwong
'''

import datetime
import pymysql
from sqlalchemy import and_, or_, not_
from sqlalchemy.exc import IntegrityError
from searchQuery import run_query
from models import db_connect, Ris, Rates, Project_basics, Examcodes


# Import parsed data from RSRCH excel file and updates rmc rows
def insertIntoRMCTable(result):
    s = db_connect("rmc")
    session = []
    for row in result:
        if row[12] != 'X' and not row[10].startswith('RE '):
            org = row[10][:2]
            if org == 'PE':  # PET exam descriptions are in part of NM
                org = 'NM'
            examcode = '(' + org + ') ' + row[9]
            if row[6] == '':  #Check if CompletedDTTM is blank
                row[6] = None
            entry = Ris(gco=row[7][2:],
                        project=row[8],
                        MRN=row[1],
                        PatientsName=row[2] + ', ' + row[3],
                        BirthDate=row[4],
                        target_organ=row[10],
                        target_abbr=examcode,
                        accession_no=row[0],
                        ScanDate=row[5],
                        referring_md=row[14],
                        status=row[12],
                        ScanDTTM=row[5],
                        CompletedDTTM=row[6],
                        Resource=row[11],
                        Duration=row[13])
            s.add(entry)
            try:
                s.commit()
            except IntegrityError:
                print "Warning: Duplicate row detected in ris table."
                s.rollback()
            else:
                examEntry = Examcodes(target_abbr=examcode, target_organ=row[10])
                s.add(examEntry)
                try:
                    s.commit()
                except IntegrityError:
                    print "Warning: Examcode already exists."
                    s.rollback()
            disp_entry = [row[7][2:], row[8], row[1], row[2] + ', ' + row[3], row[4][:10],
                          examcode, row[10], row[0], row[5][:10], row[14], row[12]]
            session.append(disp_entry)
    return session


# Import calpendo into rmc
def importCalpendoIntoRMC(monthYear):
    result = run_query("call billingCalpendoByMonth('{monthYear}%')".format(monthYear=monthYear), "calpendo")
    s = db_connect("rmc")

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
                    CompletedDTTM=row[12], Resource=row[13])
        s.add(entry)
        try:
            s.commit()
        except IntegrityError:
            print "Warning: Duplicate row detected in ris table."
            s.rollback()
        else:
            examEntry = Examcodes(target_abbr=row[7], target_organ=row[6])
            s.add(examEntry)
            try:
                s.commit()
            except IntegrityError:
                print "Warning: Examcode already exists."
                s.rollback()
    return result


def RMCPostImportSql(monthYear):
    s = db_connect("rmc")

    newRates = run_query("call tc_show_new_ratesb4add", "rmc")
    run_query("call tc_addNewRates", "rmc")

    # Find new RIS projects and import to database
    q1 = s.query(Ris.gco).distinct().outerjoin(Project_basics, Project_basics.gco == Ris.gco)
    q2 = q1.filter(and_(Project_basics.gco.is_(None), not_(Ris.gco.contains('spec'))),
                   or_(not_(Ris.Resource.startswith('TMII HESS')), Ris.Resource.is_(None)))
    newRisProjects = q2.all()
    for row in newRisProjects:
        entry = Project_basics(gco=row[0])
        s.add(entry)
    s.commit()

    # Stamp new charges (may not be necessary)
    q = s.query(Ris).filter(and_(Ris.ScanDate.startswith(monthYear), Ris.charge.is_(None)))
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
    s.query(Rates).filter(Rates.id == idx).update({Rates.basecharge: changeBase, Rates.addhalfhourcharge: changeHalf})
