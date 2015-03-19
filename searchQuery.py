'''
Created on Jan 30, 2015

@author: edmundwong
'''

import pymysql
import json

def searchBookings(startRange, endRange, resources):

    startRange = pymysql.escape_string(startRange)
    endRange = pymysql.escape_string(endRange)
    
    if resources:
        resource_id_condition = "in (" + ",".join(resources) + "))"
    else:
        resource_id_condition = "like '%' or bookings.resource_id is null)"

    query_block= """
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
        (bookings.status = 'APPROVED' or bookings.status = 'REQUESTED') and projects.project_code != 'BLOCKER'
        and (bookings.start_date >= '""" + startRange + """' and bookings.start_date <= '""" + endRange + """')
        and (bookings.resource_id """ + resource_id_condition    
    
    print query_block
    result = (execute_query(query_block), ('GCO','PI','FundNumber','Funding Source','Resource','Date','Duration','Development','ExamType1','ExamType2','ExamType3','Isotope'))
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
        
    query_block= """
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
        LEFT JOIN project_group_projects ON projects.id = project_group_projects.project_id
        LEFT JOIN project_groups ON project_groups.id = project_group_projects.project_group_id        
        WHERE projects.status = 'Approved' and
        projects.type_id """ + projectType_condition + """ and
        (project_groups.name """ + projectGroup_condition
    
        
    print query_block
    result = (execute_query(query_block), ('GCO','Name','Type','PI','Pi Email','PI Phone Number','FundNumber', 'Funding Source'))
    return result

def getUserActivity():
    query_block = """SELECT 
        concat(users.given_name,' ',users.family_name) as 'Name',
        users.login_name,
        max(audit_log.log_date) as 'Last Activity', 
        count(audit_log.affected_type) as 'Activity Count',
        users.created,
        users.status
        
        FROM audit_log
        RIGHT JOIN users ON users.id = audit_log.user_id
        WHERE 
        users.id != 1
        GROUP BY users.id
        order by concat(users.given_name,' ',users.family_name)"""
        
    result = (execute_query(query_block), ('Name', 'UserName', 'Last Activity','Activity Count','Created','User Status'))
    return result

def searchRescheduledBookings():
    query_block = """select bookings_with_updated_data.affected_pk as 'Booking ID',
        bookings.created,
        bookings_with_updated_data.init_sched_time1,
        bookings_with_updated_data.init_sched_time2,
        bookings_with_updated_data.updated_sched_time1,
        bookings_with_updated_data.updated_sched_time2,
        bookings_with_updated_data.update_log_date,
        bookings.status,
        bookings.development, 
        projects.project_code as 'gco',
        projects.principal_investigator, 
        resources.name as 'Resource',
        concat(users.login_name, ' (', users.given_name, ' ', users.family_name, ')') as 'Rescheduler'
        from
        
        (select 
        created_bookings.create_log_date,
        created_bookings.affected_pk,
        created_bookings.init_sched_time1,
        created_bookings.init_sched_time2,
        updated_daterange_info.updated_sched_time1,
        updated_daterange_info.updated_sched_time2,
        updated_daterange_info.update_log_date,
        updated_daterange_info.user_id,
        updated_daterange_info.ip_address
        
        from
        
        (select updated_action.id, 
            updated_action.log_date as 'create_log_date',
            updated_action.affected_pk,
            audit_log_properties.tsval1 as 'init_sched_time1',
            audit_log_properties.tsval2 as 'init_sched_time2'
            from    
                (select * 
                from audit_log
                where 
                audit_log.affected_type = 'booking' and audit_log.crud = 'CREATE') AS updated_action ###
            left join audit_log_properties
            on
                audit_log_properties.audit_log_id=updated_action.id and
                audit_log_properties.name='dateRange'
            order by affected_pk) as created_bookings
        
        inner join
        
            (select 
            updated_action2.log_date as 'update_log_date',
            updated_action2.user_id,
            updated_action2.affected_pk,
            updated_action2.ip_address,
            audit_log_properties.tsval1 as 'updated_sched_time1',
            audit_log_properties.tsval2 as 'updated_sched_time2'
            from
                (select *
                from audit_log
                 where 
                audit_log.affected_type = 'booking' and audit_log.crud = 'UPDATE') AS updated_action2 ###
            left join audit_log_properties
            on
            audit_log_properties.audit_log_id=updated_action2.id and
            audit_log_properties.name='dateRange'
            group by `tsval1`, `tsval2`, `affected_pk`
            order by affected_pk) as updated_daterange_info
        
        on    
            created_bookings.affected_pk=updated_daterange_info.affected_pk
        where
            (init_sched_time1 != updated_sched_time1 or init_sched_time2 != updated_sched_time2)
        ) as bookings_with_updated_data
        
        inner join bookings
        on bookings.id = bookings_with_updated_data.affected_pk
        left join resources
        on resources.id = bookings.resource_id
        left join projects
        on projects.id = bookings.project_id
        left join users
        on users.id = bookings_with_updated_data.user_id
        
        where
        resources.id in (1, 2, 3, 4)
        order by bookings_with_updated_data.affected_pk, bookings_with_updated_data.update_log_date
    """
    result = (execute_query(query_block), ('Booking ID','Created','Init_sched_time1','Init_sched_time2','Updated_sched_time1','Updated_sched_time2','Update_log_date', 'Status', 'Development','gco','PI','Resource','Rescheduler'))
    return result

def getGcoInfo(gco):
    query_block = """SELECT
                        projects.name,
                        projects.project_code,
                        concat(projects.pi_firstname, ' ', projects.principal_investigator),
                        projects.status,
                        project_types.name,
                        projects.pi_email,                        
                        projects.pi_phone_number,
                        projects.coordinator_email,
                        projects.coordinator_name,                        
                        projects.primary_resource,                                                
                        projects.other_investigators,
                        projects.department,
                        projects.project_description,
                        projects.fund_number,
                        projects.duration_of_project,                        
                        projects.proposed_start_date,
                        projects.total_dollar_budget,
                        projects.y1_no_of_scans,
                        projects.y2_no_of_scans,
                        projects.y3_no_of_scans,
                        projects.y4_no_of_scans,
                        projects.y5_no_of_scans,
                        projects.session_duration
                        
                        FROM projects
                        left join project_types on project_types.id = projects.type_id
                        WHERE projects.project_code='""" + gco + "'"
    
    query_block2 = """SELECT resources.name 
                        FROM project_resources
                        RIGHT JOIN projects ON projects.id = project_resources.project_id
                        LEFT JOIN resources ON resources.id = project_resources.resource_id
                        WHERE projects.project_code='""" + gco + "'"
                        
    query_block3 = """SELECT 
                        concat(users.given_name,' ',users.family_name),
                        users.login_name
                        FROM project_users
                        RIGHT JOIN projects ON projects.id = project_users.project_id
                        LEFT JOIN users ON users.id = project_users.user_id
                        WHERE projects.project_code='""" + gco + "'"
                        
    result = (execute_query(query_block), execute_query(query_block2), execute_query(query_block3), ('Title','GCO','PI','Status','Type','PI email','PI Phone Number','Coordinator E-mail', 'Coordinator Name','Primary Resource','Other investigators', 'Department', 'Description', 'Fund#', 'Duration', 'Proposed Start Date','Total Dollar Budget','# of Scans (Year 1)','# of Scans (Year 2)','# of Scans (Year 3)','# of Scans (Year 4)','# of Scans (Year 5)','Session Duration'))
   
    return result



def execute_query(query_block):
    with open('./config.json') as f: 
        cfg = json.load(f)["mysql"]
    
    host = cfg["host"]
    port = int(cfg["port"])
    user = cfg["user"]
    pw = cfg["pw"]
    db = cfg["calpendo_db"]
    
    conn = pymysql.connect(host=host, port=port, user=user, passwd=pw, db=db)
    cur = conn.cursor()
    cur.execute(query_block)
    result = cur.fetchall()
    return result
