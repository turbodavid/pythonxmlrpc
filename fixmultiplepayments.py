import xmlrpclib
import psycopg2 as psql
import sys
from datetime import datetime

def get_conexion(host='192.168.0.14', port=5432, dbname='GMM_OUv7', user= 'openerp', password='0p3n3rp'):
	return psql.connect("host='%s' port=%s dbname='%s' user='%s' password='%s'" % (host, port, dbname, user, password))


url = 'http://localhost:8069'
db = 'GMM_OUv7'
username = 'admin'
password = 'victoria'
common = xmlrpclib.ServerProxy('{}/xmlrpc/2/common'.format(url))
uid = common.authenticate(db, username, password, {})
models = xmlrpclib.ServerProxy('{}/xmlrpc/2/object'.format(url))

DOMAIN_PAYMENT = "invoice_type = 'in_payment' and num_mov = 500"
DOMAIN_CREDIT = "smr.date_refund <= pagos.date_refund and invoice_type = 'out_invoice' and num_mov >= 610"
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


def udpate_smr_ids(voucherid, paiddate, newvoucher, isinvoice=True):

	if isinvoice:
		xfield = 'invoice_id'
		sql = """	
				select  smr.id, coalesce( ai.id, 0 ) factura_id 
				from sync_morsa_refunds smr 
						left join account_invoice  ai on 
									(concat(trim(smr.invoice_number_serie),'-',trim(smr.invoice_number)) = ai.internal_number)
				where smr.voucher_id = %s and smr.date_refund = '%s' and %s;
			""" % (voucherid, paiddate, DOMAIN_PAYMENT)
	else:
		xfield = 'refund_id'
		sql = """ 
			with pagos as
			(
				select distinct smr.invoice_number_serie, smr.invoice_number, smr.date_refund
				from sync_morsa_refunds smr 
				where  voucher_id = %s and date_refund = '%s' and %s 
			)
			select smr.id, coalesce( ai.id, 0 ) notcre_id
			from pagos left join sync_morsa_refunds smr 
				on ( trim(pagos.invoice_number_serie) = trim(smr.invoice_number_serie) 
						and trim(pagos.invoice_number) = trim(smr.invoice_number)
					)
				left join account_invoice  ai on 
					(concat(trim(smr.refund_number),'/',trim(smr.refund_number_serie),'-',trim(smr.refund_number)) = 
								ai.internal_number
					)
			where (smr.voucher_id = %s or voucher_id is null) and  %s
		""" % (voucherid, paiddate, DOMAIN_PAYMENT, voucherid, DOMAIN_CREDIT)

	smrs = execute_query(sql)
	for smr in smrs:
		asmr = [[smr[0]], {xfield: smr[1], 'state': 'paid', 'voucher_id': newvoucher}]
		models.execute_kw(db, uid, password, 'sync.morsa.refunds', 'write', asmr)

	return

def get_facturaspagadas(voucherid, paiddate):

	sql = """	
			select  ai.id factura_id, smr.invoice_id, smr.invoice_number_serie, smr.invoice_number, 
				aml.id move_line_id, aml.amount_residual, coalesce( smr.acc_relative_partner_id, 
				ai.account_id) acc_receivable, aml.name, smr.date_refund,
				sum(smr.amount_total) + 
				coalesce( ( select sum( coalesce(smr2.amount_total, 0))
							from sync_morsa_refunds smr2 
							where trim(smr2.invoice_number_serie) =  trim(smr.invoice_number_serie)
								and trim(smr2.invoice_number) = trim(smr.invoice_number)
								and smr2.date_refund <=  smr.date_refund 
								and smr2.invoice_type = 'out_invoice' and smr2.num_mov >= 610
								and (smr2.voucher_id = %s or voucher_id is null)
						), 0) invoicepayment
			from sync_morsa_refunds smr 
					left join account_invoice  ai on 
								(concat(trim(smr.invoice_number_serie),'-',trim(smr.invoice_number)) = ai.internal_number)
					left join account_move_line aml on 
								(aml.move_id = ai.move_id and aml.account_id = ai.account_id)
			where smr.voucher_id = %s and smr.date_refund = '%s' and %s
			group by ai.id, smr.invoice_id, smr.invoice_number_serie, smr.invoice_number, 
					aml.id, aml.amount_residual, coalesce(smr.acc_relative_partner_id, ai.account_id), aml.name, smr.date_refund
			order by smr.invoice_number_serie, smr.invoice_number; 
		""" % (voucherid, voucherid, paiddate, DOMAIN_PAYMENT)

	return execute_query(sql)


def get_creditosfactura(voucherid, paiddate):

	sql = """ 
		with pagos as
		(
			select distinct smr.invoice_number_serie, smr.invoice_number, smr.date_refund
			from sync_morsa_refunds smr 
			where  voucher_id = %s and date_refund = '%s' and %s 
		)
		select ai.id notcre_id, smr.refund_id, smr.refund_number_serie, smr.refund_number, 
					aml.id move_line_id, aml.amount_residual, ai.amount_total, aml.name, sum(smr.amount_total) creditapplied
		from pagos left join sync_morsa_refunds smr 
			on ( trim(pagos.invoice_number_serie) = trim(smr.invoice_number_serie) 
					and trim(pagos.invoice_number) = trim(smr.invoice_number)
				)
			left join account_invoice  ai on 
				(concat(trim(smr.refund_number),'/',trim(smr.refund_number_serie),'-',trim(smr.refund_number)) = 
							ai.internal_number
				)
			left join account_move_line aml on 
				(aml.move_id = ai.move_id and aml.account_id = ai.account_id)
		where (smr.voucher_id = %s or voucher_id is null) and  %s
		group by ai.id, smr.refund_id, smr.refund_number_serie, smr.refund_number, 
					aml.id, aml.amount_residual
		order by smr.refund_number_serie, smr.refund_number;
	""" % (voucherid, paiddate, DOMAIN_PAYMENT, voucherid, DOMAIN_CREDIT)

	return execute_query(sql)


def get_vouchers():

	sql = """
			with vdate as 
			(
				select distinct voucher_id, date_refund
				from sync_morsa_refunds smr
				where invoice_type = 'in_payment' and num_mov = 500
			)
			select voucher_id, count(*)
			from vdate 
			where voucher_id is not null 
			group by voucher_id having count(*) > 1
			order by voucher_id;		
		"""
	return execute_query(sql)


def get_voucher_dates(voucherid, partnerid):

	sql = """
			select  date_refund, partner_id_internal, partner_id, sum(amount_total) voucher_payment 
			from sync_morsa_refunds smr 
			where  partner_id_internal = %s
				and smr.voucher_id = %s and %s
			group by date_refund, partner_id_internal, partner_id;
		""" % (partnerid, voucherid, DOMAIN_PAYMENT)

	return execute_query(sql)


def get_aml(account_id, partner_id, amount):

	sql = """
			select aml.id, aml.name, aml.debit, aml.amount_residual, aml."date", aml.date_maturity 
			from account_move_line aml inner join account_period ap on (aml.period_id = ap.id)
			where aml.state = 'valid' and aml.account_id = %s and aml.partner_id = %s and 
					aml.debit > 0 and aml.amount_residual >= %s and  aml."date" <= '2018-12-31' and not ap.special
			order by aml."date" desc limit 1;	
		""" % (account_id, partner_id, amount)

	return execute_query(sql, 1)


def get_acc_cxc_id(partnerid):

	sql = """
			select replace(value_reference,'account.account,','')::int4 
			from ir_property where "name" = 'property_account_receivable' and res_id = concat('res.partner,',%s)
		""" % partnerid
	data = execute_query(sql, 1)

	return data[0]


def create_voucher_lines(voucherid, newvoucher, paiddate, partnerid, acc_cxc):

	vlines = get_facturaspagadas(voucherid, paiddate)
	amountpaidni = 0
	print "Facturas: %s, " % len(vlines),
	for vl in vlines:
		# factura_id				01
		# smr.invoice_id			02
		# smr.invoice_number_serie	03
		# smr.invoice_number		04
		# move_line_id				05
		# aml.amount_residual		06
		# aml.name					07
		# ai.amount_total			08
		# sum(smr.amount_total)     09
		if vl[0]:
			avl = {
				'name': vl[7],
				'type': 'cr',
				'move_line_id': vl[4],
				'account_id': acc_cxc,
				'amount_original': float(vl[6]),
				'amount': float(vl[9]),
				'amount_unreconciled': float(vl[5]),
				'reconcile': abs(float(vl[9]) - float(vl[5])) < 1,
				'voucher_id': newvoucher
			}
			models.execute_kw(db, uid, password, 'account.voucher.line', 'create', [avl])
		else:
			amountpaidni += float(vl[9])

	#para los pagos sin factura en el ERP
	if amountpaidni > 0:
		aml = get_aml(acc_cxc, partnerid, amountpaidni)
		avl = {
			'name': aml[1],
			'type': 'cr',
			'move_line_id': aml[0],
			'account_id': acc_cxc,
			'amount_original': float(aml[2]),
			'amount': float(amountpaidni),
			'date_original': aml[4].strftime('%Y-%m-%d'),
			'date_due': aml[5].strftime('%Y-%m-%d'),
			'amount_unreconciled': float(aml[3]),
			'reconcile': abs(float(amountpaidni) - float(aml[3])) < 1,
			'voucher_id': newvoucher,
		}
		models.execute_kw(db, uid, password, 'account.voucher.line', 'create', [avl])

	vlines = get_creditosfactura(voucherid, paiddate)
	print "Creditos: %s, " % len(vlines),
	for vl in vlines:
		avl = {
			'name': vl[7],
			'type': 'dr',
			'move_line_id': vl[4],
			'account_id': acc_cxc,
			'amount_original': float(vl[6]),
			'amount': float(vl[8]),
			'amount_unreconciled': float(vl[5]),
			'reconcile': abs(float(vl[8]) - float(vl[5])) < 1,
			'voucher_id': newvoucher
		}
		models.execute_kw(db, uid, password, 'account.voucher.line', 'create', [avl])

	if len(vlines) > 0:
		udpate_smr_ids(voucherid, paiddate, newvoucher, False)
	udpate_smr_ids(voucherid, paiddate, newvoucher)

	return


def create_voucher(voucher, pago):

	av = {
		'partner_id': pago[2],
		'amount': float(pago[3]),
		'payment_type_id': voucher[0]['payment_type_id'][0],
		'date': pago[0].strftime('%Y-%m-%d'),
		'name': voucher[0]['name'],
		'journal_id': voucher[0]['journal_id'][0],
		'account_id': voucher[0]['account_id'][0],
		'period_id': voucher[0]['period_id'][0],
		'type': 'receipt',
		'voucher_operating_unit_id': voucher[0]['voucher_operating_unit_id'][0],
		'company_id': 1,
		'pre_line': True,
	}

	id = models.execute_kw(db, uid, password, 'account.voucher', 'create', [av])

	return id


def fix_vouchers(voucherid):

	domain = [[['id', '=', voucherid]]]
	afieldsdpayment = ['id', 'partner_id', 'state', 'account_id', 'period_id',
						'payment_type_id', 'name', 'journal_id', 'voucher_operating_unit_id', 'line_ids']
	fields = {'fields': afieldsdpayment}
	voucher = models.execute_kw(db, uid, password, 'account.voucher', 'search_read', domain, fields)
	if voucher[0]['state'] != 'draft':
		return

	partnerid = voucher[0]['partner_id'][0]
	partnerinternal = voucher[0]['name'][voucher[0]['name'].find('[')+1:]
	partnerinternal = partnerinternal[:partnerinternal.find('-')]
	lids = voucher[0]['line_ids']
	if len(lids):
		domain = domain = [[['id', '=', lids[0]]]]
		vl = models.execute_kw(db, uid, password, 'account.voucher.line', 'search_read', domain, {'fields': ['account_id']})
		acc_cxc = vl[0]['account_id'][0]
	else:
		acc_cxc = get_acc_cxc_id(partnerid)

	pagos = get_voucher_dates(voucherid, partnerinternal)
	for pago in pagos:
		newvoucherid = create_voucher(voucher, pago)
		print "Voucher ID: %s, New voucher: %s, Dia: %s" % (voucherid, newvoucherid, pago[0]),
		create_voucher_lines(voucherid, newvoucherid, pago[0], partnerid, acc_cxc)
		#models.execute_kw(db, uid, password, 'account.voucher.line', 'unlink', [lids])

		try:
			afieldsdpayment = ['id', 'partner_id', 'account_id', 'writeoff_amount']
			fields = {'fields': afieldsdpayment}
			domain = [[['id', '=', newvoucherid]]]
			newvoucher = models.execute_kw(db, uid, password, 'account.voucher', 'search_read', domain, fields)
			balanced = (newvoucher[0]['writeoff_amount'] == 0.00)
			print 'Aplicando %s' % 'Cuadrado' if balanced else 'SIN Cuadrar'
			if balanced:
				models.execute_kw(db, uid, password, 'account.voucher', 'proforma_voucher', [int(newvoucherid)])
		except:
			pass

	return id

def dopayments():

	try:
		pagos = get_vouchers()
		ncounter = 0
		for pago in pagos:
			ncounter += 1
			print "Corrigiendo %s/%s, " % (ncounter, len(pagos)),
			fix_vouchers(pago[0])
			domain = [[['voucher_id', '=', pago[0]]]]
			smrs = models.execute_kw(db, uid, password, 'sync.morsa.refunds', 'search', domain)
			if len(smrs) > 0:
				models.execute_kw(db, uid, password, 'sync.morsa.refunds', 'write',
								[smrs, {'state': 'open', 'voucher_id': False}])
	except Exception as err:
		print repr(err)
	finally:
		if (conexion):
			conexion.close()


if __name__ == "__main__":
    dopayments()

	




