import xmlrpclib
import psycopg2 as psql
import sys
#import numpy as np
from datetime import datetime

url = 'http://localhost:7069'
db = 'GMM_OUv7'
username = 'admin'
password = 'victoria'
common = xmlrpclib.ServerProxy('{}/xmlrpc/2/common'.format(url))
uid = common.authenticate(db, username, password, {})
models = xmlrpclib.ServerProxy('{}/xmlrpc/2/object'.format(url))


def get_payments():

	afieldsdpayment = ['id', 'number']
	fields = {'fields': afieldsdpayment}
	domain = [[['type', '=', 'receipt'],
			   ['voucher_operating_unit_id', '=', 570],
			   ['state', '=', 'posted']
			   ]]
	vouchers = models.execute_kw(db, uid, password, 'account.voucher', 'search_read', domain, fields)

	return vouchers


def apply_voucher(voucher):

	try:
		print "Desaplicando %s, %s" % (voucher[0], voucher[1])
		models.execute_kw(db, uid, password, 'account.voucher', 'cancel_voucher', [int(voucher[0])])
	except:
		pass

	try:
		print "Aplicando %s, %s" % (voucher[0], voucher[1])
		models.execute_kw(db, uid, password, 'account.voucher', 'proforma_voucher', [int(voucher[0])])
	except:
		pass

	return voucher[0]


def re_apply():

	try:
		payments = get_payments()

		for payment in payments:
			apply_voucher(payment)

	except Exception as err:
		print repr(err)


if __name__ == "__main__":
    re_apply()


