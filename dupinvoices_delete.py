import xmlrpclib
import psycopg2 as psql
import sys
#import numpy as np
from datetime import datetime

def get_conexion(host='192.168.1.75', port=5432, dbname='GMM', user= 'openerp', password='0p3n3rp'):
	return psql.connect("host='%s' port=%s dbname='%s' user='%s' password='%s'" % (host, port, dbname, user, password))


url = 'http://localhost:8069'
db = 'GMM'
username = 'admin'
password = 'victoria'
common = xmlrpclib.ServerProxy('{}/xmlrpc/2/common'.format(url), allow_none=True)
uid = common.authenticate(db, username, password, {})
models = xmlrpclib.ServerProxy('{}/xmlrpc/2/object'.format(url), allow_none=True)

conexion = get_conexion()


def valida_conexion():

	cn = conexion
	if cn:
		return cn
	else:
		cn = get_conexion()

	return cn


def execute_query(sql, tofetch=0, tocommit=False):

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


def get_dupinvoices(ou_id):

	sql = """
	with dupinv as 
	(
		select internal_number, count(*)
		from account_invoice ai 
		where type = 'out_invoice' and ai.operating_unit_id = %s and state = 'open'
		group by 1 having count(*) > 1
	)
	select unnest(array[ai.id])
	FROM account_invoice ai inner join dupinv di1 on (ai.internal_number  = di1.internal_number)
	WHERE  ai.id IN (SELECT id
			  FROM (SELECT id, ROW_NUMBER() 
				   OVER (partition BY ai2.internal_number ORDER BY ai2.id) AS rnum
				   FROM account_invoice ai2 inner join dupinv di on (ai2.internal_number = di.internal_number)
				   ) t 
			  WHERE t.rnum > 1)
	order by 1;
	""" % ou_id
	#ids = execute_query(sql)
	ids = [4016428]
	afields = ['id', 'state', 'internal_number', 'number', 'move_id', 'cfdi_id', 'account_id']
	fields = {'fields': afields}
	domain = [[['id', 'in', ids]]]
	invoices = models.execute_kw(db, uid, password, 'account.invoice', 'search_read', domain, fields)

	return invoices


def get_ous():
	sql = """
		with dupinv as 
		(
			select operating_unit_id, internal_number, count(*)
			from account_invoice ai 
			where type = 'out_invoice' and state = 'open'
			group by 1, internal_number having count(*) > 1
		)
		select unnest(array[operating_unit_id]) 
		from dupinv 
		group by 1
		order by 1;	
	"""

	ids = execute_query(sql)

	afields = ['id', 'code', 'name']
	fields = {'fields': afields}
	domain = [[['id', 'in', ids]]]
	ous = models.execute_kw(db, uid, password, 'operating.unit', 'search_read', domain, fields)

	return ous


def cancel_payment(move_id, account_id):

	afields = ['id', 'reconcile_id', 'reconcile_partial_id', 'reconcile_ref']
	fields = {'fields': afields}
	domain = [[['move_id', '=', move_id],
				['account_id', '=', account_id]
			]]
	mlines = models.execute_kw(db, uid, password, 'account.move.line', 'search_read', domain, fields)
	for mline in mlines:
		if mline['reconcile_ref'] or mline['reconcile_id'] or mline['reconcile_partial_id']:
			afields = ['id', 'voucher_id', 'move_line_id']
			fields = {'fields': afields}
			domain = [[['move_line_id', '=', mline['id']]]]
			vouchers = models.execute_kw(db, uid, password, 'account.voucher.line', 'search_read', domain, fields)
			for voucher in vouchers:
				models.execute_kw(db, uid, password, 'account.voucher', 'cancel_voucher', [int(voucher['voucher_id'][0])])



def dup_invoice():

	try:
		ous = get_ous()
		processtot = 0
		for ou in ous:

			invoices = get_dupinvoices(int(ou['id']))
			totinvoices = len(invoices)
			processou = 0
			for invoice in invoices:
				if invoice['state'] in ['open', 'paid']:
					cancel_payment(invoice['move_id'][0], invoice['account_id'][0])
					models.execute_kw(db, uid, password, 'account.move', 'button_cancel', [[int(invoice['move_id'][0])]])
					models.execute_kw(db, uid, password, 'account.invoice', 'invoice_cancel', [[int(invoice['id'])]])
					models.execute_kw(db, uid, password, 'account.invoice', 'action_cancel_draft', [[int(invoice['id'])]])
				elif invoice['state'] == 'cancel':
					models.execute_kw(db, uid, password, 'account.invoice', 'action_cancel_draft', [[int(invoice['id'])]])

				if invoice['cfdi_id']:
					afields = ['id', 'file_xml_sign']
					fields = {'fields': afields}
					domain = [[['id', '=', [int(invoice['cfdi_id'][0])]]]]
					cfdi = models.execute_kw(db, uid, password, 'ir.attachment.facturae.mx', 'search_read', domain, fields)
					sql = """
						delete from ir_attachment_facturae_mx where id = %s;
						delete from ir_attachment where id = %s;
					""" % (int(cfdi[0]['id']), int(cfdi[0]['file_xml_sign'][0]))
					execute_query(sql, 0, True)
				#models.execute_kw(db, uid, password, 'account.invoice', 'unlink', [[int(invoice['id'])]])
				processou += 1
				print 'Processando %s/%s, ID: %s, Factura: %s' % \
					  (processou, totinvoices, invoice['id'], invoice['internal_number'])

			processtot += processou

		print 'Total procesadas:  %s:', processtot

	except Exception as err:
		print repr(err)


if __name__ == "__main__":
    dup_invoice()


