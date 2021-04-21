import xmlrpclib
import psycopg2 as psql
import sys
#import numpy as np
from datetime import datetime

def get_conexion(host='10.0.1.17', port=5432, dbname='GMM_OUv7', user= 'openerp', password='op3n3rp'):
	return psql.connect("host='%s' port=%s dbname='%s' user='%s' password='%s'" % (host, port, dbname, user, password))


url = 'http://localhost:7069'
db = 'GMM_OUv7'
username = 'admin'
password = 'victoria'
common = xmlrpclib.ServerProxy('{}/xmlrpc/2/common'.format(url))
uid = common.authenticate(db, username, password, {})
models = xmlrpclib.ServerProxy('{}/xmlrpc/2/object'.format(url))


conexion = get_conexion()


def valida_conexion():

	cn = conexion
	if cn:
		return cn
	else:
		cn = get_conexion()

	return cn

def execute_query(sql,tofetch=0,tocommit=False):

	records = ''
	cn = valida_conexion()
	crs = cn.cursor()
	crs.execute(sql)
	if tocommit:
		cn.commit()
		return records

	if tofetch == 1:
		records = crs.fetchone()
	elif tofetch > 1:
		records = crs.fetchmany(tofetch)
	else:
		records = crs.fetchall()
	crs.close()

	return records

def get_payments():

	sql = """
			select id from account_voucher av 
			where state = 'draft' and "type" = 'receipt' and "date" >= '2019-07-31';
		"""

	#print  "** Las pagos son: %s ***" % sql

	return execute_query(sql)


def apply_voucher(voucherid):


	print "Pago ID %s, " % (voucherid),

	try:
		afieldsdpayment = ['id', 'number']
		fields = {'fields': afieldsdpayment}
		domain = [[['id', '=', voucherid]]]
		voucher = models.execute_kw(db, uid, password, 'account.voucher', 'search_read', domain, fields)
		balanced = (voucher[0]['writeoff_amount'] == 0.00)
		print "Aplicando %s, %s" %	(voucherid, voucher['number'])
		#if balanced:
		#	models.execute_kw(db, uid, password, 'account.voucher', 'proforma_voucher', [int(voucherid)])
	except:
		pass



	return id


def dopayments(start=startdate_arg, stop=stopdate_arg,ous=ou_arg):

	try:
		payments = get_payments()

		for payment in payments:
			apply_voucher(payment[0])

	except Exception as err:
		print repr(err)
	finally:
		if (conexion):
			conexion.close()


if __name__ == "__main__":
    dopayments()


