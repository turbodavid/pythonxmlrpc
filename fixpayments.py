import xmlrpclib
import psycopg2 as psql
import sys
from datetime import datetime

ou_arg = sys.argv[1]

def get_conexion(host='192.168.0.12', port=5432, dbname='GMM_OUv7', user= 'openerp', password='0p3n3rp'):
    return psql.connect("host='%s' port=%s dbname='%s' user='%s' password='%s'" % (host, port, dbname, user, password))


conexion = get_conexion()
url = 'http://localhost:8069'
db = 'GMM_OUv7'
username = 'admin'
password = 'victoria'

common = xmlrpclib.ServerProxy('{}/xmlrpc/2/common'.format(url))
uid = common.authenticate(db, username, password, {})
models = xmlrpclib.ServerProxy('{}/xmlrpc/2/object'.format(url))


def valida_conexion():

	cn = conexion
	if not cn:
		conxion = get_conexion()
		cn = conexion

	return cn

def ejecuta_sql(sql, tofetch=0, afectar=False):

	records = ''
	crs = valida_conexion().cursor()
	crs.execute(sql)

	if afectar:
		conexion.commit()
		return records

	if tofetch == 1:
		records = crs.fetchone()
	elif tofetch > 1:
		records = crs.fetchmany(tofetch)
	else:
		records = crs.fetchall()

	crs.close()
	return records

def get_sql_vouchers_tofix(ou):
	sql = """
			select distinct av.id, av.state, voucher_operating_unit_id, vtofix.actionto
			from account_voucher av inner join account_voucher_line avl on (avl.voucher_id = av.id)
				inner join account_move_line aml on (avl.move_line_id = aml.id)
				inner join operating_unit ou on (av.voucher_operating_unit_id = ou.id)
				left join vouchers_tofix vtofix on (vtofix.voucher_id = av.id)
			where (vtofix.actionto is not null and ou.code = '%s' and av."type" = 'receipt' and 
					avl."type" = 'dr' and aml."date" > av."date") 
					or av.id in 
			(
				with pagodet as 
				(
					select av.id, sum(case when avl."type" = 'cr' then avl.amount else 0 end)  pagos,
								  sum(case when avl."type" = 'dr' then avl.amount else 0 end) creditos
					from account_voucher av inner join account_voucher_line avl on (avl.voucher_id = av.id)
							inner join operating_unit ou on (av.voucher_operating_unit_id = ou.id)
					where av."type" = 'receipt' 
					group by av.id
				)
				select pagodet.id 
				from pagodet inner join account_voucher av on (pagodet.id = av.id)
				where av.amount + creditos != pagos
			)
			order by voucher_operating_unit_id, av.id;	
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
								and smrcredits.date_refund <= smr.date_refund
								and smrcredits.partner_id_internal = smr.partner_id_internal
								and trim(smrcredits.invoice_number_serie) = trim(smr.invoice_number_serie)
								and trim(smrcredits.invoice_number) = trim(smr.invoice_number)
								and smrcredits.state != 'draft' ), 0) credits,
			coalesce( smr.acc_relative_partner_id, ai.account_id, 
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

def fix_voucher_credits(voucherid, fecha):
	sql = """
		with payments as 
		(
			select distinct smr.invoice_number, smr.invoice_number_serie, smr.date_refund fechapago
			from sync_morsa_refunds smr 
			where  voucher_id = %s and smr.num_mov = 500 and smr.invoice_type = 'in_payment'
		)
		select aml.id, refund_id, refund_number, refund_number_serie, aml.account_id, aml."name", 
				aml."date", aml.date_maturity, aml.amount_residual, abs(balance) balance, sum(smr.amount_total) notcre  
		from sync_morsa_refunds smr 
			inner join payments on (trim(smr.invoice_number_serie) = trim(payments.invoice_number_serie) and 
										trim(smr.invoice_number) = trim(payments.invoice_number) )
			inner join account_invoice nc on (nc.id = smr.refund_id)
			inner join account_move_line aml on (aml.move_id = nc.move_id and aml.account_id = nc.account_id)
		where  smr.num_mov >= 610 and smr.date_refund <= '%s'
		group by aml.id, smr.refund_id, refund_number, refund_number_serie;	
	""" % (voucherid, fecha)

	return ejecuta_sql(sql)

def fix_voucher(voucherid):
	return ejecuta_sql(get_sql_voucher_fix(voucherid))

def get_aml(account_id, partner_id, amount):

	sql = """
			select aml.id, aml.name, aml.debit, aml.amount_residual, aml."date", aml.date_maturity 
			from account_move_line aml
			where state = 'valid' and account_id = %s and partner_id = %s and 
					debit > 0 and amount_residual >= %s and  "date" <= '2018-12-31'
			order by "date" desc limit 1;	
		""" % (account_id, partner_id, amount)

	return ejecuta_sql(sql,1)

def afecta_smr(domain, rsupdate ):

	smrs = models.execute_kw(db, uid, password, 'sync.morsa.refunds', 'search', domain)
	for s in smrs:
		models.execute_kw(db, uid, password, 'sync.morsa.refunds', 'write', [[s], rsupdate])

def delete_voucherlines(ltodelete):

	if len(ltodelete) > 0:
		models.execute_kw(db, uid, password, 'account.voucher.line', 'unlink', [ltodelete])


def fixpayments(ou=ou_arg):

	if not ou or ou == '99':
		ou = '01'

	ous = ['01', '02', '03', '04', '05', '06', '07', '09', '10']
	try:
		for ou in ous:
			vouchers_tofix = ejecuta_sql(get_sql_vouchers_tofix(ou), 0)
			counter = 0
			nfetched = len(vouchers_tofix)

			for vtofix in vouchers_tofix:

				voucherid = vtofix[0]
				domain = [[['id', '=', voucherid]]]
				afieldsdpayment = ['id', 'partner_id', 'account_id', 'line_cr_ids', 'line_dr_ids',
									'voucher_operating_unit_id',
									'state', 'paid_amount_in_company_currency', 'name', 'number', 'date']
				fields = {'fields': afieldsdpayment}

				lposted = vtofix[1] == 'posted'
				if lposted:
					models.execute_kw(db, uid, password, 'account.voucher', 'cancel_voucher', [int(voucherid)])

				voucher = models.execute_kw(db, uid, password, 'account.voucher', 'search_read', domain, fields)
				lcrids = voucher[0]['line_cr_ids']
				ldrids = voucher[0]['line_dr_ids']

				domain = [[['id', 'in', lcrids]]]
				pagosfac = models.execute_kw(db, uid, password,
								'account.voucher.line', 'search_read', domain, {'fields': ['id', 'move_line_id', 'amount']})
				pagosfac = [{amlid['move_line_id'][0]: [amlid['id'], amlid['amount']]} for amlid in pagosfac]
				rsfac = {}
				for p in pagosfac:
					rsfac.update(p)

				domain = [[['id', 'in', ldrids]]]
				pagosnc = models.execute_kw(db, uid, password,
								'account.voucher.line', 'search_read', domain, {'fields': ['id', 'move_line_id', 'amount']})
				pagosnc = [{amlid['move_line_id'][0]: [amlid['id'], amlid['amount']]} for amlid in pagosnc]
				rsnc = {}
				for nc in pagosnc:
					rsnc.update(nc)


		#			models.execute_kw(db, uid, password, 'account.voucher.line', 'unlink', [linestodelete])
				counter += 1
				print "Corrigiendo %s/%s VOUCHER ID %s en sucursal %s, con %s Facturas y %s  Creditos" \
					  % (counter, len(vouchers_tofix), voucherid, ou, len(lcrids), len(ldrids)),

				rs = []
				invoiceid = ''
				amountpaid = 0
				amountpaidni = 0
				amountcredit = 0
				amountregistered = 0
				mlid = ''
				mlidni = ''
				amlidsok = []
				avlids = []

				vlines = fix_voucher_credits(voucherid, voucher[0]['date'])
				for vl in vlines:
					mlid = vl[0]
					if not mlid in rsnc.keys():
						avl = {
							'name': vl[5],
							'type': 'dr',
							'move_line_id': mlid,
							'account_id': vl[4],
							'date_original': vl[6].strftime('%Y-%m-%d'),
							'date_due': vl[7].strftime('%Y-%m-%d'),
							'amount': float(vl[10]),
							'amount_unreconciled': float(vl[8]),
							'amount_original': float(vl[9]),
							'reconcile': abs(float(vl[10])- float(vl[8])) < 1,
							'voucher_id': voucherid,
						}
						vlid = models.execute_kw(db, uid, password, 'account.voucher.line', 'create', [avl])
					else:
						vlid = rsnc[mlid][0]
						if float(vl[10]) != float(rsnc[mlid][1]):
							vlid = rsnc[mlid][0]
							models.execute_kw(db, uid, password, 'account.voucher.line', 'write', [[vlid], {'amount': float(vl[10])}])

					avlids.append(vlid)
					domain = [[['invoice_type', '=', 'out_invoice'],
							   ['num_mov', '>=', 610],
							   ['refund_number', '=', vl[2]],
							   ['refund_number_serie', '=', vl[3]],
							   ['voucher_id', '=', False],
							   ['date_refund', '<=', voucher[0]['date']]
							   ]]

					afecta_smr(domain, {'state': 'paid', 'voucher_id': voucherid})
					#smrs = models.execute_kw(db, uid, password, 'sync.morsa.refunds', 'search', domain)
					#for s in smrs:
					#	models.execute_kw(db, uid, password, 'sync.morsa.refunds', 'write',
					#					  [[s], {'state': 'paid', 'voucher_id': voucherid}])

				delete_voucherlines(list(set(ldrids) - set(avlids)))
				#if len(linestodelete) > 0:
				#	models.execute_kw(db, uid, password, 'account.voucher.line', 'unlink', [linestodelete])

				domain = [[['invoice_type', '=', 'out_invoice'],
						   ['num_mov', '>=', 610],
						   ['voucher_id', '=', voucherid],
						   ['date_refund', '>', voucher[0]['date']]
						   ]]

				afecta_smr(domain, {'state': 'open', 'vocher_id': False})
				#smrs = models.execute_kw(db, uid, password, 'sync.morsa.refunds', 'search', domain)
				#for s in smrs:
				#	models.execute_kw(db, uid, password, 'sync.morsa.refunds', 'write', [[s], {'state': 'open', 'voucher_id': False}])


				amlidsok = []
				invoiceid = ''
				amountpaid = 0
				amountpaidni = 0
				amountcredit = 0
				amountregistered = 0
				amountunreconciled = 0
				accid = ''
				vloperation = 'write'

				vlines = fix_voucher(voucherid)
				for vl in vlines: #primero los que si tienen invoice id

					accid = vl[18] if not accid else accid
					if not vl[0]:
						amountpaidni += (vl[10] + vl[17])
						continue

					if invoiceid != vl[0]:
						if invoiceid:
							lreconcile = abs(float(amountpaid) + float(amountcredit) - float(amountunreconciled)) < 1
							if vloperation == 'write':
								vlid = rsfac[mlid][0]
								amountregistered = rsfac[mlid][1]
								if float(amountpaid) + float(amountcredit) != float(amountregistered):
									models.execute_kw(db, uid, password,
										'account.voucher.line', 'write', [[vlid], {'amount': float(amountpaid) + float(amountcredit),
											'reconcile': lreconcile}])
							else:
								avl.update({'amount': float(amountpaid + amountcredit),'reconcile': lreconcile})
								models.execute_kw(db, uid, password, 'account.voucher.line', 'create', [avl])

						invoiceid = vl[0] if vl[0] else '-1'
						amountpaid = 0
						amountunreconciled = vl[11] or 0
						amountcredit = vl[17]
						vloperation = 'write'
						mlid = vl[2]
						amlidsok.append(mlid)

					amountpaid += vl[10]
					if vl[4] == 0:
						vloperation = 'create'
						avl = {
							'name': vl[14],
							'type': 'cr',
							'move_line_id': vl[2],
							'account_id': vl[3],
							'amount_original': float(vl[13]),
							'amount': 0,
							'date_original': vl[15].strftime('%Y-%m-%d'),
							'date_due': vl[16].strftime('%Y-%m-%d'),
							'amount_unreconciled': float(vl[11]),
							'reconcile': abs((vl[10] + vl[17]) - vl[11]) < 1,
							'voucher_id': voucherid,
						}
						rs.append([[vl[6]], {'invoice_id': vl[0], 'state': 'paid'}])
				else:
					if invoiceid:
						lreconcile = abs(float(amountpaid) + float(amountcredit) - float(amountunreconciled)) < 1
						if vloperation == 'write':
							vlid = rsfac[mlid][0]
							amountregistered = rsfac[mlid][1]
							if float(amountpaid) + float(amountcredit) != float(amountregistered):
								models.execute_kw(db, uid, password,
												  'account.voucher.line', 'write',
												  [[vlid], {'amount': float(amountpaid) + float(amountcredit),
															'reconcile': lreconcile}])
						else:
							avl.update({'amount': float(amountpaid + amountcredit), 'reconcile': lreconcile})
							models.execute_kw(db, uid, password, 'account.voucher.line', 'create', [avl])

				for s in rs:
					models.execute_kw(db, uid, password, 'sync.morsa.refunds', 'write', s)

				linestodelete = []
				amlidsok = list(set([x for x in rsfac]) - set(amlidsok))
				if amountpaidni > 0:
					mlidni = ''
					vlid = ''
					for i in xrange(0, len(amlidsok)):
						if float(rsfac[amlidsok[i]][1]) >= float(amountpaidni) and not vlid:
							mlidni = amlidsok[i]
							vlid = rsfac[mlidni][0]
						else:
							linestodelete.append(rsfac[amlidsok[i]][0])

					if mlidni:
						amountregistered = rsfac[mlidni][1]
						if float(amountpaidni) != float(amountregistered):
							models.execute_kw(db, uid, password, 'account.voucher.line', 'write',
											  [[vlid], {'amount': float(amountpaidni)}])
					else:
						aml = get_aml(accid, voucher[0]['partner_id'][0], amountpaidni)
						avl = {
							'name': aml[1],
							'type': 'cr',
							'move_line_id': aml[0],
							'account_id': accid,
							'amount_original': float(aml[2]),
							'amount': float(amountpaidni),
							'date_original': aml[4].strftime('%Y-%m-%d'),
							'date_due': aml[5].strftime('%Y-%m-%d'),
							'amount_unreconciled': float(aml[3]),
							'reconcile': abs(amountpaidni - aml[3]) < 1,
							'voucher_id': voucherid,
						}
						models.execute_kw(db, uid, password, 'account.voucher.line', 'create', [avl])
				else:
	#				for i in range(0, len(amlidsok)):
	#					linestodelete.append(rsfac[amlidsok[i]][0])
					linestodelete = [rsfac[x][0] for x in amlidsok]

				delete_voucherlines(linestodelete)
					#if len(linestodelete) > 0:
					#	models.execute_kw(db, uid, password, 'account.voucher.line', 'unlink', [linestodelete])

				balanced = True
				try:
					afieldsdpayment.append('writeoff_amount')
					fields = {'fields': afieldsdpayment}
					domain = [[['id', '=', voucherid]]]
					voucher = models.execute_kw(db, uid, password, 'account.voucher', 'search_read', domain, fields)
					balanced = (voucher[0]['writeoff_amount'] == 0.00)
					lcrids = voucher[0]['line_cr_ids']
					ldrids = voucher[0]['line_dr_ids']
					print ", quedo con %s Facturas y %s Creditos %s balance" \
						  % (len(lcrids), len(ldrids), 'CON' if balanced else 'SIN')
					if balanced:
						models.execute_kw(db, uid, password, 'account.voucher', 'proforma_voucher', [int(voucherid)])
				except:
					pass

				if vtofix[3]:
					sql = "update vouchers_tofix set actionto = 'fixed', balanced = %s where voucher_id = %s" % \
						  (balanced, voucherid)
					ejecuta_sql(sql, 0, True)


	except Exception as err:
		print repr(err)
	finally:
		if (conexion):
			conexion.close()


if __name__ == "__main__":
    fixpayments()

	




