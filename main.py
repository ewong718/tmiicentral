'''
Core function to be run for TMII Central
Created on Jan 28, 2015

@author: edmundwong
'''

from bottle import route, run, template, static_file, error, request, view, post
import searchQuery
import bottle
from cork import Cork
from beaker.middleware import SessionMiddleware
import parsers
import billing_sql
import os
import json

#Load configuration settings
with open('./config.json') as f: 
        cfg = json.load(f)["main"]

###### Authentication ######

# Use users.json and roles.json in auth_conf
aaa = Cork('auth_conf', email_sender=cfg["email_sender"], smtp_url=cfg["smtp_url"])

app = bottle.app()
session_opts = {
    'session.cookie_expires': True,
    'session.encrypt_key': 'please use a random key and keep it secret!',
    'session.httponly': True,
    'session.timeout': int(cfg["timeout_secs"]),
    'session.type': 'cookie',
    'session.validate_key': True,
    }

app = SessionMiddleware(app, session_opts)


@route('/login')
@view('login_form')
def login_form():
    """Serve login form"""
    return {}

@route('/signup')
@view('signup_form')
def signup_form():
    """Serve signup form"""
    return {}


@post('/login')
def login():
    """Authenticate users"""
    username = post_get('username')
    password = post_get('password')
    if username == '': 
        print 'BlankUserName, but user still logs in...'
        username = 'BlankUserNamePw$nFhA52$s'
    aaa.login(username, password, success_redirect='/tmiicentral', fail_redirect='/login')
    
@bottle.route('/logout')
def logout():
    aaa.logout(success_redirect='/login')
    
@route('/pw_reset')
@view('pw_reset_form')
def reset_form():
    """Reset form"""
    return {}

@post('/change_password')
def change_password():
    """Change password"""
    aaa.reset_password(post_get('reset_code'), post_get('password'))
    return 'Thanks. <a href="/login">Go to login</a>'


@post('/reset_password')
def send_password_reset_email():
    """Send out password reset email"""
    aaa.send_password_reset_email(
        username=post_get('username'),
        email_addr=post_get('email_address')
    )
    return 'Please check your mailbox.'

@bottle.route('/change_password/:reset_code')
@bottle.view('pw_change_form')
def change_password2(reset_code):
    """Show password change form"""
    return dict(reset_code=reset_code)

    
def post_get(name, default=''):
    return request.POST.get(name, default).strip()

@post('/register')
def register():
    """Send out registration email"""
    aaa.register(post_get('username'), post_get('password'), post_get('email_address'))
    return 'Please check your mailbox.'

@route('/validate_registration/:registration_code')
def validate_registration(registration_code):
    """Validate registration, create user account"""
    aaa.validate_registration(registration_code)
    return 'Thanks. <a href="/login">Go to login</a>'

###### Custom Queries ######

@route('/customQuery')
def do_login3():
    result = searchQuery.searchRescheduledBookings()
    return template('base.tpl', result=result)

@route('/customQuery2') #user activity count
def do_login5():
    result = searchQuery.getUserActivity()
    return template('base.tpl', result=result)


###### Core ######

@route('/static/<filename>')
def server_static(filename):
    return static_file(filename, root='./static')

@route('/static/img/<filename>')
def server_static2(filename):
    return static_file(filename, root='./static/img')


@route('/tmiicentral')
def index():
    aaa.require(fail_redirect='/login')
    user = aaa.current_user.username
    return template('main.tpl', user=user)


@route('/searchProjects')
def initSearchProjects():
    aaa.require(fail_redirect='/login')
    return template('searchProjects.tpl', result=None)


@route('/searchProjects', method='POST')
def do_login2():
    aaa.require(fail_redirect='/login')
    projectType = request.forms.getall('projectType')
    projectGroup = request.forms.getall('projectGroup')
    result = searchQuery.searchProjects(projectType, projectGroup)
    return template('searchProjects.tpl', result=result)


@route('/searchBookings')
def initSearchBookings():
    aaa.require(fail_redirect='/login')
    return template('searchBookings.tpl', result=None)


@route('/searchBookings', method='POST')
def do_login():
    aaa.require(fail_redirect='/login')
    startRange = request.forms.get('startRange')
    endRange = request.forms.get('endRange')
    resources = request.forms.getall('resource')
    for val in request.forms:
        print val, request.forms.getall(val)
    result = searchQuery.searchBookings(startRange, endRange, resources)
    return template('searchBookings.tpl', result=result)

@route('/billing')
def do_billing():
    result = None
    return template('billing.tpl', result=result)

@route('/billing', method='POST')
def do_billing_post():
    upload     = request.files.get('upload')
    upload2     = request.files.get('upload2')
    monthYear = request.forms.get('monthYear')
    for upload_file in [upload, upload2]:
        _unused, ext = os.path.splitext(upload_file.filename)
        if ext not in ('.xls','.xlsx','.csv'):
            return 'File extension not allowed.'
    try:    
        upload.save(cfg["upload_path"]) # appends upload.filename automatically
        upload2.save(cfg["upload_path"])
    except IOError:
        print 'IOError: One or more of the files exist.'
    sessions = parsers.ris_parse_file1_file(cfg["upload_path"] + upload.filename)
    sessions2 = parsers.ris_parse_file2_file(cfg["upload_path"] + upload2.filename)

    sessions3 = parsers.insertIntoRMCTable3(monthYear)
    
    billing_sql.insertIntoRMCTable1(sessions)
    billing_sql.insertIntoRMCTable2(sessions2)
    billing_sql.insertIntoRMCTable3(sessions3)
    newRates = billing_sql.RMCPostImportSql(monthYear)
    
    infoView = [sessions, sessions3, newRates, monthYear]
    return template('billing_import_verify.tpl', result=infoView)

@route('/processedBillingData/<monthYear>')
def do_processedbillingdata(monthYear):
    humanBillableScanList = billing_sql.return_HumanScans_billing(monthYear)
    saBillableScanList = billing_sql.return_SmallAnimal_billing(monthYear)
    ntrBillableScanList = billing_sql.return_NTR_billing(monthYear)
    srfBillableScanList = billing_sql.return_SRF_billing(monthYear)
    return template('processedBillingData.tpl', result=[humanBillableScanList, saBillableScanList, ntrBillableScanList, srfBillableScanList])

###### PROJECTS Profile ######


@route('/projects/<gco>')
def cb(gco):
    aaa.require(fail_redirect='/login')
    result = list(searchQuery.getGcoInfo(gco))
    result.append(gco)
    return template('gcoProfile.tpl', result=result)

@error(404)
def error404(error):
    return 'This page does not exist. Nothing here.'

run(app=app, host=cfg["bottle_host"], port=cfg["bottle_port"])