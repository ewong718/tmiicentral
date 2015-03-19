'''
Created on Mar 3, 2015
Parsers

@author: Edmund Wong
'''

import xlrd
import pymysql

###Parses Centricity Research File 1 into Mysql importable list###
def ris_parse_file1_file(filename):

    wb = xlrd.open_workbook(filename)
    
    sheet = wb.sheet_by_index(0)
    
    myCol = sheet.col_values(0)
    myCol2 = sheet.col_values(1)
    target_abbrCol = sheet.col_values(2)
    target_organCol = sheet.col_values(3)
    referringCol = sheet.col_values(4)
    dobCol = sheet.col_values(5)
    status = sheet.col_values(6)
    GCOcol = sheet.col_values(7)
    
    sessions = []
    
    #Get GCO index
    gco_idx = []
    for idx, row in enumerate(myCol[:-1]):
        if "Loc" in str(row):
            gco_idx.append(idx)    
               
    # Loop through each GCO except last
    for idx, val in enumerate(gco_idx[:-1]):
        for val2 in range(gco_idx[idx], gco_idx[idx+1]):
            if "MRN" in str(myCol2[val2]):
                val3=val2
                while GCOcol[val3+1] != '':
                    gco             = GCOcol[val3+1][2:]
                    project         = myCol2[val]
                    mrn             = myCol2[val2][4:]
                    patientsname    = myCol[val2]
                    dob             = unicode(xlrd.xldate.xldate_as_datetime(dobCol[val3+1],0).date())
                    target_abbr     = target_abbrCol[val3+1]
                    target_organ    = target_organCol[val3+1]
                    status_val      = status[val3+1]
                    scandate        = unicode(xlrd.xldate.xldate_as_datetime(myCol[val3+1],0).date())
                    referring       = referringCol[val3+1]
                    accession_no    = myCol2[val3+1]
                    
                    mri_session = [ gco, project, mrn, patientsname, dob, target_organ, target_abbr, accession_no, scandate, referring, status_val ]
                    sessions.append(mri_session)
                    val3 += 1
                
    # For last GCO
    
    for val2 in range(gco_idx[-1], len(myCol)):
        if "MRN" in str(myCol2[val2]):
            val3=val2
            while GCOcol[val3+1] != '':
                gco         = GCOcol[val3+1][2:]
                project     = myCol2[val]
                mrn          = myCol2[val2][4:]
                patientsname = myCol[val2]
                dob         = unicode(xlrd.xldate.xldate_as_datetime(dobCol[val3+1],0).date())
                target_abbr = target_abbrCol[val3+1]
                target_organ = target_organCol[val3+1]
                status_val = status[val3+1]
                scandate     = unicode(xlrd.xldate.xldate_as_datetime(myCol[val3+1],0).date())
                referring   = referringCol[val3+1]
                accession_no = myCol2[val3+1]
                              
                mri_session = [ gco, project, mrn, patientsname, dob, target_organ, target_abbr, accession_no, scandate, referring, status_val ]
                sessions.append(mri_session)
                val3 += 1

    return sessions

def ris_parse_file2_file(filename):
    
    wb = xlrd.open_workbook(filename)    
    sheet = wb.sheet_by_index(0)
    
    sessions = []
    i=1
    while True:
        try:
            row = sheet.row_values(i)
        except IndexError:
            break
        try:
            row[4] =  unicode(xlrd.xldate.xldate_as_datetime(row[4],0))
        except ValueError:
            row[4] = ''
        try:
            row[5] =  unicode(xlrd.xldate.xldate_as_datetime(row[5],0))
        except ValueError:
            row[5] = ''
        try:
            row[6] =  unicode(xlrd.xldate.xldate_as_datetime(row[6],0))
        except ValueError:
            row[6] = ''
        row[13] = unicode(row[13])
        sessions.append(row)
        i += 1
    return sessions


def insertIntoRMCTable3(monthYear):
    mysql_hostname = 'anvilmacmini.1470mad.mssm.edu'
    mysql_user = 'edmund'
    mysql_pw = 'jeff1'
    mysql_db = 'calpendo_tmii'
    conn = pymysql.connect(host=mysql_hostname, port=3306, user=mysql_user, passwd=mysql_pw, db=mysql_db)
    cur = conn.cursor()
    query_block = """#This query covers ONLY the 3 TMII research scanners.
                    #For research projects that have a VALID fund number in the correct format
                    #Must be updated in the future to account for terminated GCOs in the current month
                    #Does not query -D projects
                    
                    #Version History:
                    # v2.0 - Accounts for all projects now, not just only approved projects
                    #       - Uses string_enum_values examtype lookup table
                    #       - Code refactor
                    #       - Accounts for Force CT
                    
                    
                    SELECT
                    
                    projects.project_code AS 'gco',
                    projects.name AS 'project',
                    bookings.MRN AS 'MRN',
                    CASE
                        WHEN bookings.last_name = '' AND bookings.first_name = ''
                        THEN NULL
                        ELSE
                        concat(bookings.last_name,', ', bookings.first_name) 
                        END AS 'PatientsName',
                    bookings.date_of_birth AS 'BirthDate',
                    CASE
                        project_types.id
                        WHEN '2' THEN 'Animal'
                        ELSE
                        bookings.ExamType1 
                        END
                        AS 'target_organ',
                    CASE
                        project_types.id
                        WHEN '2' THEN 'Animal'
                        ELSE
                        concat('(RE) ', string_enum_values.ris_examcode)
                        END AS target_abbr,
                    date_format(bookings.start_date,'%Y-%m-%d') AS 'ScanDate',
                    projects.referring_physician AS 'referring_md',
                    bookings.last_name AS 'LastName',
                    bookings.first_name AS 'FirstName',
                    bookings.duration AS 'Duration',
                    bookings.start_date AS 'ScanDTTM',
                    bookings.finish_date AS 'CompletedDTTM',
                    concat('TMII HESS ', resources.name) AS 'Resource',
                    concat(projects.pi_firstname, ' ', projects.principal_investigator) AS 'Investigator',
                    projects.fund_number AS 'fund_no'
                    
                    FROM bookings
                    LEFT JOIN projects ON bookings.project_id=projects.id
                    LEFT JOIN resources ON bookings.resource_id=resources.id
                    LEFT JOIN project_types ON projects.type_id=project_types.id
                    LEFT JOIN users ON projects.owner_id=users.id
                    LEFT JOIN string_enum_values ON string_enum_values.value = bookings.Examtype1
                    WHERE
                    bookings.start_date LIKE '""" + monthYear + """%' AND
                    bookings.status IN ('APPROVED', 'REQUESTED') AND 
                    bookings.resource_id IN (1, 2, 3, 40) AND
                    project_types.id IN (2, 3, 9, 10, 11) AND
                    projects.project_code NOT LIKE '%-%D' AND
                    (bookings.development LIKE '%NO%' or bookings.development IS NULL) AND
                    (projects.funding_source != 'GCRC' or projects.funding_source IS NULL) AND
                    projects.fund_number LIKE '%-%' #Debatable Filter
                    ORDER BY bookings.start_date"""
    cur.execute(query_block)
    sessions = cur.fetchall()
    return sessions
        