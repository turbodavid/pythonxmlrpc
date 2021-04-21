import xmlrpclib
import psycopg2 as psql
import sys
from datetime import datetime

first_arg = sys.argv[1]
conexion = get_conexion()

def get_conexion(host='192.168.0.12', port=5432, dbname='GMM_OUv7', user= 'openerp', password='0p3n3rp'):
    return psql.connect("host='%s' port=%s dbname='%s' user='%s' password='%s'" % (host, port, dbname, user, password))

def get_sql_vouchers_tofix(ou):

	sql = """
		select  av.id, coalesce(vtof.actionto, 'nofix') actionto
		from account_voucher av 
			inner join operating_unit ou on (av.voucher_operating_unit_id = ou.id)
			left join vouchers_tofix vtof on (vtof.voucher_id = av.id)
		where av.id >= 51877 and av.type = 'receipt' and av.state = 'draft' and ou.code = '%s'
		order by av.id
	""" % ou
	return sql

def get_sql_voucher_fix(voucherid):
	sql = """ 
		select ai.id id_factura, ai.move_id, aml.id move_line_id, ai.account_id, 
			invoice_id, ai.internal_number,smr.id smr_id, smr.voucher_id,
			smr.invoice_type, smr.num_mov, smr.amount_total, aml.amount_residual, 
			ai.residual, ai.amount_total, 
			aml.name, aml.date, aml.date_maturity,
			coalesce( (select sum(amount_total)
						from sync_morsa_refunds smrcredits 
						where smrcredits.invoice_type = 'out_invoice' and smrcredits.num_mov >= 610
								and smrcredits.partner_id_internal = smr.partner_id_internal
								and trim(smrcredits.invoice_number_serie) = trim(smr.invoice_number_serie)
								and trim(smrcredits.invoice_number) = trim(smr.invoice_number)
								and smrcredits.state != 'draft' ), 0) credits,
			coalesce( smr.acc_relative_partner_id,
						(	select substring(value_reference from 17 )::int4 
							from ir_property 
							where "name" = 'property_account_receivable' and
									res_id = 'res.partner,'||smr.partner_id::varchar
						), 0 ) acc_receivable
		from sync_morsa_refunds smr 
			left join account_invoice  ai on 
				(concat(trim(smr.invoice_number_serie),'-',trim(smr.invoice_number)) = ai.internal_number)
			left join account_move_line aml on (aml.move_id = ai.move_id and aml.account_id = ai.account_id)
		where voucher_id = %s and num_mov = 500
		order by ai.id, smr.num_mov, smr.id
	""" % voucherid
	return sql

def fix_voucher(voucherid, cn):

	crsinfo = cn.cursor()
	crsinfo.execute(get_sql_voucher_fix(voucherid))
	records = crsinfo.fetchall()
	crsinfo.close()

	return records

def get_aml(cn, account_id, partner_id, amount):

	sql = """
			select aml.id, aml.name, aml.debit, aml.amount_residual, aml."date", aml.date_maturity 
			from account_move_line aml
			where state = 'valid' and account_id = %s and partner_id = %s and 
					debit > 0 and amount_residual >= %s and  "date" <= '2018-12-31'
			order by "date" desc limit 1;	
		""" % (account_id, partner_id, amount)

	crsaml = cn.cursor()
	crsaml.execute(sql)
	record = crsaml.fetchone()

	return record

def dopayments(ou=first_arg):


	if not ou:
		ou = "04"
	url = 'http://localhost:8069'
	db = 'GMM_OUv7'
	username = 'admin'
	password = 'victoria'

	common = xmlrpclib.ServerProxy('{}/xmlrpc/2/common'.format(url))
	uid = common.authenticate(db, username, password, {})
	models = xmlrpclib.ServerProxy('{}/xmlrpc/2/object'.format(url))

	try:

		conexion = get_conexion()
		crsvouchers = conexion.cursor()
		crsexec = conexion.cursor()
		crsvouchers.execute(get_sql_vouchers_tofix(ou))
		vouchers_tofix = crsvouchers.fetchmany(7)
		counter = 0
		nfetched = len(vouchers_tofix)

		for vtofix in vouchers_tofix:

			voucherid = vtofix[0]
			domain = [[['id', '=', voucherid]]]
			afieldsdpayment = ['id', 'partner_id', 'account_id', 'line_cr_ids', 'voucher_operating_unit_id',
								'state', 'paid_amount_in_company_currency', 'name', 'number']
			fields = {'fields': afieldsdpayment}

			if vtofix[1] == 'to_fix':

				voucher = models.execute_kw(db, uid, password, 'account.voucher', 'search_read', domain, fields)
				vlines = fix_voucher(voucherid, conexion)

				lcrdis = voucher[0]['line_cr_ids']
				domain = [[['id', 'in', lcrdis]]]
				detpagos = models.execute_kw(db, uid, password,
								'account.voucher.line', 'search_read', domain, {'fields': ['move_line_id']})
				detpagos = [{amlid['move_line_id'][0]: amlid['id']} for amlid in detpagos]
				rs = {}
				for d in detpagos:
					rs.update(d)
				amlsafeccted  = list(set([x.keys()[0] for x in detpagos]) - set([v[2] for v in vlines]))
				linestodelete = []
				for k in amlsafeccted:
					if k in rs.keys():
						linestodelete.append(rs[k])

				if len(linestodelete) > 0:
					models.execute_kw(db, uid, password, 'account.voucher.line', 'unlink', [linestodelete])

				#linestodelete = [val for val in amlsafeccted for val in rs.values()]
				counter += 1

				rs = []
				invoiceid = ''
				amountpaid = 0
				amountpaidni = 0
				amountcredit = 0
				amountunreconciled = 0
				avl = {}

				for vl in vlines: #primero los que si tienen invoice id

					if vl[4] != 0:
						continue

					if not vl[0]:
						amountpaidni += (vl[10] + vl[17])
						rs.append([[vl[6]], {'state': 'paid'}])
						continue

					if invoiceid != vl[0]:
						if invoiceid:
							avl.update({'amount': float(amountpaid+amountcredit),
										'reconcile': abs((amountpaid+amountcredit)-amountunreconciled) < 1})
							models.execute_kw(db, uid, password, 'account.voucher.line', 'create', [avl])
						invoiceid = vl[0] if vl[0] else '-1'
						amountpaid = 0
						amountcredit = vl[17]
						amountunreconciled = vl[11] or 0
						avl = {}

					amountpaid += vl[10]
					rs.append([[vl[6]], {'invoice_id': vl[0], 'state': 'paid'}])

					if not avl:
						avl = {
							'name': vl[14],
							'type': 'cr',
							'move_line_id': vl[2],
							'account_id': vl[3],
							'amount_original': float( vl[13] ),
							'amount': 0,
							'date_original': vl[15].strftime('%Y-%m-%d'),
							'date_due': vl[16].strftime('%Y-%m-%d'),
							'amount_unreconciled': float(vl[11]),
							'reconcile': abs((vl[10] + vl[17])-vl[11]) < 1,
							'voucher_id': voucherid,
						}

				else:
					if invoiceid:
						avl.update({'amount': float(amountpaid + amountcredit),
										'reconcile': abs((amountpaid + amountcredit) - amountunreconciled) < 1})
						models.execute_kw(db, uid, password, 'account.voucher.line', 'create', [avl])

				if amountpaidni > 0:

					aml = get_aml(conexion, vl[18], voucher[0]['partner_id'][0], amountpaidni)
					avl = {
						'name': aml[1],
						'type': 'cr',
						'move_line_id': aml[0],
						'account_id': vl[18],
						'amount_original': float(aml[2]),
						'amount': float(amountpaidni),
						'date_original': aml[4].strftime('%Y-%m-%d'),
						'date_due': aml[5].strftime('%Y-%m-%d'),
						'amount_unreconciled': float(aml[3]),
						'reconcile': abs(amountpaidni - aml[3]) < 1,
						'voucher_id': voucherid,
					}
					models.execute_kw(db, uid, password, 'account.voucher.line', 'create', [avl])

				for s in rs:
					models.execute_kw(db, uid, password, 'sync.morsa.refunds', 'write', s)

			balanced = True
			try:
				afieldsdpayment.append('writeoff_amount')
				fields = {'fields': afieldsdpayment}
				domain = [[['id', '=', voucherid]]]
				voucher = models.execute_kw(db, uid, password, 'account.voucher', 'search_read', domain, fields)
				balanced = (voucher[0]['writeoff_amount'] == 0.00)
				if balanced:
					models.execute_kw(db, uid, password, 'account.voucher', 'proforma_voucher', [int(voucherid)])
			except:
				pass

			if vtofix[1] == 'to_fix':
				sql = "update vouchers_tofix set actionto = 'fixed', balanced = %s where voucher_id = %s;" % (balanced, voucherid)

				if not conexion or crsexec.closed != 0:
					conexion = get_conexion()
					crsexec  = conexion.cursor()

				crsexec.execute(sql)
				conexion.commit()

	except Exception as err:
		print repr(err)
	finally:
		if (conexion):
			crsvouchers.close()
			crsexec.close()
			conexion.close()


if __name__ == "__main__":
    dopayments()

	




