'''
Created on Jan 30, 2015

@author: edmundwong
'''

import json
import keyring
import pymysql


def searchBookings(startRange, endRange, resources):

    startRange = pymysql.escape_string(startRange)
    endRange = pymysql.escape_string(endRange)

    if resources:
        resource_id_condition = "in (" + ",".join(resources) + "))"
    else:
        resource_id_condition = "like '%' or bookings.resource_id is null)"

    query_block = """
        SELECT projects.project_code,
        concat(projects.pi_firstname, ' ', projects.principal_investigator),
        projects.fund_number,
        projects.funding_source,
        resources.name,
        date(bookings.start_date),
        round(bookings.duration/60,2),
        bookings.development,
        bookings.examtype1,
        bookings.examtype2,
        bookings.examtype3,
        bookings.isotope
        FROM bookings
        LEFT JOIN projects ON bookings.project_id = projects.id
        LEFT JOIN resources ON bookings.resource_id = resources.id
        WHERE
        (bookings.status = 'APPROVED' or bookings.status = 'REQUESTED') and
        projects.project_code != 'BLOCKER'
        and (bookings.start_date >= '""" + startRange + """'
        and bookings.start_date <= '""" + endRange + """')
        and (bookings.resource_id """ + resource_id_condition

    print query_block
    result = (run_query(query_block, "calpendo"),
              ('GCO', 'PI', 'FundNumber', 'Funding Source',
               'Resource', 'Date', 'Duration', 'Development',
               'ExamType1', 'ExamType2', 'ExamType3', 'Isotope'))
    return result


def searchProjects(projectType, projectGroup):

    if projectType:
        projectType_condition = "in (" + ",".join(projectType) + ")"
    else:
        projectType_condition = "like '%'"

    if projectGroup[0] != '':
        projectGroup_condition = "= '" + projectGroup[0] + "')"
    else:
        projectGroup_condition = "like '%' or project_groups.name is null)"

    query_block = """
        SELECT DISTINCT projects.project_code,
        projects.name,
        project_types.name,
        concat(projects.pi_firstname, ' ', projects.principal_investigator),
        projects.pi_email,
        projects.pi_phone_number,
        projects.fund_number,
        projects.funding_source
        FROM projects
        LEFT JOIN project_types ON projects.type_id = project_types.id
        LEFT JOIN project_group_projects
        ON projects.id = project_group_projects.project_id
        LEFT JOIN project_groups
        ON project_groups.id = project_group_projects.project_group_id
        WHERE projects.status = 'Approved' and
        projects.type_id """ + projectType_condition + """ and
        (project_groups.name """ + projectGroup_condition

    print query_block
    result = (run_query(query_block, "calpendo"),
              ('GCO', 'Name', 'Type', 'PI', 'Pi Email',
               'PI Phone Number', 'FundNumber', 'Funding Source'))
    return result


def getUserActivity():
    query_block = "call userActivity"
    result = (run_query(query_block, "calpendo"),
              ('Name', 'UserName', 'Last Activity',
               'Activity Count', 'Created', 'User Status'))
    return result


def searchRescheduledBookings():
    query_block = "call rescheduledBookings"
    result = (run_query(query_block, "calpendo"),
              ('Booking ID', 'Created', 'Init_sched_time1',
               'Init_sched_time2', 'Updated_sched_time1',
               'Updated_sched_time2', 'Update_log_date',
               'Status', 'Development', 'gco', 'PI',
               'Resource', 'Rescheduler'))
    return result


def getGcoInfo(gco):
    result = (run_query("CALL getGcoInfo('{gco}')".format(gco=gco),
              "calpendo"),
              run_query("CALL getGcoInfo_resourcesUsed('{gco}')".format(gco=gco),
              "calpendo"),
              run_query("CALL getGcoInfo_users('{gco}')".format(gco=gco),
              "calpendo"),
              ('Title', 'GCO', 'PI', 'Status', 'Type', 'PI email',
               'PI Phone Number', 'Coordinator E-mail', 'Coordinator Name',
               'Primary Resource', 'Other investigators', 'Department',
               'Description', 'Fund#', 'Duration', 'Proposed Start Date',
               'Total Dollar Budget', '# of Scans (Year 1)',
               '# of Scans (Year 2)', '# of Scans (Year 3)',
               '# of Scans (Year 4)', '# of Scans (Year 5)',
               'Session Duration'))
    return result


def searchFinances(start, end, resources):
    start = pymysql.escape_string(start)
    end = pymysql.escape_string(end)

    # MySQL Stored Procedure
    query_block = "call revenueByProject('{start}%','{end}%')".format(start=start, end=end)
    result = (run_query(query_block, "calpendo"),
              ('GCO', 'FundNumber', 'Investigator',
               'Revenue', 'totalApprovedHours', 'DevelopmentHours',
               'NonDevelopmentHours', 'risHours', 'CalpendoHours'))
    return result


def getRates():
    query_block = """select id, gco, investigator, target_organ, target_abbr,
                    system, basecharge, addhalfhourcharge from rates"""
    result = (run_query(query_block, "rmc"),
              ('GCO', 'investigator', 'exam desc', 'examcode', 'system',
               'basecharge', 'addhalfhourcharge',
               'UPDATE basecharge/addhalfhourcharge'))
    return result


def run_query(query_block, db):
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
    cur.execute(query_block)
    result = cur.fetchall()
    return result
