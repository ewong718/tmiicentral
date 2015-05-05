#!/usr/bin/env python
'''
TMIICentral is a web application for TMII's departmental financial analysis,
billing, resource usage, and reporting using the Python Bottle web framework.
Dependencies include Bottle and Cork (It's authentication library).

Bottle: http://bottlepy.org/docs/dev/index.html
Cork: http://cork.firelet.net/

v2.1
Created on Jan 28, 2015 

@author: Edmund Wong
'''

import os
import json
import bottle
import parsers
import billing_sql
import searchQuery
from bottle import route, run, template, static_file, error, request, view, post
from cork import Cork
from beaker.middleware import SessionMiddleware
from pymysql import OperationalError

# Load configuration settings
with open(os.path.dirname(__file__) + '/config.json') as f:
    cfg = json.load(f)["main"]

# Initialization
session_opts = {
    'session.cookie_expires': True,
    'session.encrypt_key': 'please use a random key and keep it secret!',
    'session.httponly': True,
    'session.timeout': int(cfg["timeout_secs"]),
    'session.type': 'cookie',
    'session.validate_key': True,
    }

app = SessionMiddleware(bottle.app(), session_opts)
aaa = Cork('auth_conf', email_sender=cfg["email_sender"],
           smtp_url=cfg["smtp_url"])


# Routing
@post('/change_password')
def change_password():
    """Change password"""
    aaa.reset_password(post_get('reset_code'), post_get('password'))
    return 'Thanks. <a href="/login">Go to login</a>'


@bottle.route('/change_password/:reset_code')
@bottle.view('pw_change_form')
def change_password2(reset_code):
    """Show password change form"""
    return dict(reset_code=reset_code)


@route('/login')
@view('login_form')
def login_form():
    """Serve login form"""
    return {}


@post('/login')
def login():
    """Authenticate users"""
    username = post_get('username')
    password = post_get('password')
    if username == '':
        print 'BlankUserName, but user still logs in...'
        username = 'BlankUserNamePw$nFhA52$s'
    aaa.login(username, password,
              success_redirect='/tmiicentral', fail_redirect='/login')


@bottle.route('/logout')
def logout():
    aaa.logout(success_redirect='/login')


@route('/pw_reset')
@view('pw_reset_form')
def reset_form():
    """Reset form"""
    return {}


@post('/register')
def register():
    """Send out registration email"""
    aaa.register(post_get('username'), post_get('password'),
                 post_get('email_address'))
    return 'Please check your mailbox.'


@post('/reset_password')
def send_password_reset_email():
    """Send out password reset email"""
    aaa.send_password_reset_email(
        username=post_get('username'),
        email_addr=post_get('email_address')
    )
    return 'Please check your mailbox.'


@route('/signup')
@view('signup_form')
def signup_form():
    """Serve signup form"""
    return {}


@route('/validate_registration/:registration_code')
def validate_registration(registration_code):
    """Validate registration, create user account"""
    aaa.validate_registration(registration_code)
    return 'Thanks. <a href="/login">Go to login</a>'


# Core pages

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
    result = searchQuery.searchBookings(startRange, endRange, resources)
    return template('searchBookings.tpl',
                    result=result,
                    startRange=startRange,
                    endRange=endRange)


@route('/searchFinances')
def initSearchFinances():
    aaa.require(fail_redirect='/login')
    return template('searchFinances.tpl', result=None)


@route('/searchFinances', method='POST')
def postSearchFinances():
    aaa.require(fail_redirect='/login')
    startRange = request.forms.get('startRange')
    endRange = request.forms.get('endRange')
    resources = request.forms.getall('resource')
    result = searchQuery.searchFinances(startRange, endRange, resources)
    return template('searchFinances.tpl',
                    result=result,
                    startRange=startRange,
                    endRange=endRange)


@route('/rates', method='POST')
def ratesPost():
    aaa.require(fail_redirect='/login')
    idx = request.forms.get('idx')
    changeBase = request.forms.get('changeBase')
    changeHalf = request.forms.get('changeHalf')
    billing_sql.updateRates(idx, changeBase, changeHalf)
    result = searchQuery.getRates()
    return template('rates.tpl', result=result)


@route('/rates')
def rates():
    aaa.require(fail_redirect='/login')
    result = searchQuery.getRates()
    return template('rates.tpl', result=result)


@route('/billing')
def do_billing():
    aaa.require(fail_redirect='/login')
    result = None
    return template('billing.tpl', result=result)


@route('/billing', method='POST')
def do_billing_post():
    aaa.require(fail_redirect='/login')
    upload = request.files.get('upload')
    monthYear = request.forms.get('monthYear')
    _unused, ext = os.path.splitext(upload.filename)
    if ext not in ('.xls', '.xlsx', '.csv'):
        return 'File extension not allowed.'
    try:
        upload.save(cfg["upload_path"]) # appends upload.filename automatically
    except IOError:
        print 'IOError: One or more of the files exist.'
    rsrch_allSessions = parsers.ris_parse_file2_file(cfg["upload_path"] + upload.filename)
    try:
        r = billing_sql.insertIntoRMCTable(rsrch_allSessions)
        c = billing_sql.importCalpendoIntoRMC(monthYear)
        newRatesProjects = billing_sql.RMCPostImportSql(monthYear)
    except OperationalError:
        return 'OperationalError (pymysql). Please return to the previous page and retry.'
    infoView = {'ris_bill_tbl':r,
                'cal_bill_tbl':c,
                'newRatesProjects':newRatesProjects,
                'monthYear':monthYear}
    return template('billing_import_verify.tpl', i=infoView)


@route('/processedBillingData/<monthYear>')
def do_processedbillingdata(monthYear):
    aaa.require(fail_redirect='/login')
    iData = billing_sql.generateInvoiceData(monthYear)
    return template('processedBillingData.tpl', result=iData)


# PROJECTS Profile

@route('/projects/<gco>')
def cb(gco):
    aaa.require(fail_redirect='/login')
    r = searchQuery.getGcoInfo(gco)
    if not r['gcoInfo']:
        return 'GCO {gco} does not exist in calpendo_tmii database.'.format(gco=gco)
    return template('gcoProfile.tpl',
                    r=r,
                    gco=gco)


# Custom Queries

@route('/customQuery')
def do_login3():
    result = searchQuery.searchRescheduledBookings()
    return template('base.tpl', result=result)


@route('/customQuery2')  # user activity count
def do_login5():
    result = searchQuery.getUserActivity()
    return template('base.tpl', result=result)


@error(404)
def error404(error):
    return 'This page does not exist. Nothing here.'


def post_get(name, default=''):
    return request.POST.get(name, default).strip()

run(app=app, host=cfg["bottle_host"], port=cfg["bottle_port"])
