'''
Parsers
Loads native research ris reports into python

Created on Mar 3, 2015

@author: Edmund Wong
'''

import xlrd


# Parses Centricity Research File 1 into Mysql importable list
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

    # Get GCO index
    gco_idx = []
    for idx, row in enumerate(myCol[:-1]):
        if "Loc" in str(row):
            gco_idx.append(idx)

    # Loop through each GCO except last
    for idx, val in enumerate(gco_idx[:-1]):
        for val2 in range(gco_idx[idx], gco_idx[idx+1]):
            if "MRN" in str(myCol2[val2]):
                val3 = val2
                while GCOcol[val3+1] != '':
                    gco             = GCOcol[val3+1][2:]
                    project         = myCol2[val]
                    mrn             = myCol2[val2][4:]
                    patientsname    = myCol[val2]
                    dob             = unicode(xlrd.xldate.xldate_as_datetime(dobCol[val3+1], 0).date())
                    target_abbr     = target_abbrCol[val3+1]
                    target_organ    = target_organCol[val3+1]
                    status_val      = status[val3+1]
                    scandate        = unicode(xlrd.xldate.xldate_as_datetime(myCol[val3+1], 0).date())
                    referring       = referringCol[val3+1]
                    accession_no    = myCol2[val3+1]

                    mri_session = [gco, project, mrn, patientsname, dob, target_organ, target_abbr,
                                   accession_no, scandate, referring, status_val]
                    sessions.append(mri_session)
                    val3 += 1

    # For last GCO

    for val2 in range(gco_idx[-1], len(myCol)):
        if "MRN" in str(myCol2[val2]):
            val3 = val2
            while GCOcol[val3+1] != '':
                gco         = GCOcol[val3+1][2:]
                project     = myCol2[val]
                mrn          = myCol2[val2][4:]
                patientsname = myCol[val2]
                dob         = unicode(xlrd.xldate.xldate_as_datetime(dobCol[val3+1], 0).date())
                target_abbr = target_abbrCol[val3+1]
                target_organ = target_organCol[val3+1]
                status_val = status[val3+1]
                scandate     = unicode(xlrd.xldate.xldate_as_datetime(myCol[val3+1], 0).date())
                referring   = referringCol[val3+1]
                accession_no = myCol2[val3+1]

                mri_session = [gco, project, mrn, patientsname, dob, target_organ,
                               target_abbr, accession_no, scandate, referring, status_val]
                sessions.append(mri_session)
                val3 += 1

    return sessions


def ris_parse_file2_file(filename):

    wb = xlrd.open_workbook(filename)
    sheet = wb.sheet_by_index(0)

    sessions = []
    i = 1
    while True:
        try:
            row = sheet.row_values(i)
        except IndexError:
            break
        try:
            row[4] = unicode(xlrd.xldate.xldate_as_datetime(row[4], 0))
        except ValueError:
            row[4] = ''
        try:
            row[5] = unicode(xlrd.xldate.xldate_as_datetime(row[5], 0))
        except ValueError:
            row[5] = ''
        try:
            row[6] = unicode(xlrd.xldate.xldate_as_datetime(row[6], 0))
        except ValueError:
            row[6] = ''
        row[13] = unicode(row[13])
        sessions.append(row)
        i += 1
    return sessions
