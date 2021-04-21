import xmlrpclib
import psycopg2 as psql
import sys
#import numpy as np
from datetime import datetime

def get_conexion(host='192.168.0.13', port=5432, dbname='GMM_OUv7', user= 'openerp', password='0p3n3rp'):
	return psql.connect("host='%s' port=%s dbname='%s' user='%s' password='%s'" % (host, port, dbname, user, password))


url = 'http://localhost:8069'
db = 'GMM_OUv7'
username = 'admin'
password = 'victoria'
common = xmlrpclib.ServerProxy('{}/xmlrpc/2/common'.format(url))
uid = common.authenticate(db, username, password, {})
models = xmlrpclib.ServerProxy('{}/xmlrpc/2/object'.format(url))

DOMAIN_PAYMENT = "invoice_type = 'in_payment' and num_mov between 500 and 599 and smr.state != 'paid' and voucher_id is null"
DOMAIN_CREDIT = "invoice_type = 'out_invoice' and num_mov >= 610 and smr.state != 'paid' and voucher_id is null"
PAYMENT_TYPE_CODE = '03'

def get_paymenttype():
	domain = [[['code', '=', PAYMENT_TYPE_CODE]]]
	paymentobj = models.execute_kw(db, uid, password, 'payment.type', 'search_read', domain)
	return paymentobj[0]['id']

startdate_arg = sys.argv[1]
stopdate_arg = sys.argv[2]
ou_arg = sys.argv[3]

conexion = get_conexion()
paymenttype = get_paymenttype()
acc_receivable_id = ''


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

def get_where_facturas(ou, pago):
	#partner_id 				0
	#date_refund				1
	#partner_id_internal 		2
	#partner_id_internal_socio 	3
	#refund_ref					4
	#refund_number				5
	#refund_number_serie		6
	#refund_id					7
	#refund_uuid				8


	swhere = """
		where  partner_id_internal = %s and date_refund = '%s'
			and refund_ref =  %s 
			and (refund_number = '%s' or refund_number is null)
			and (refund_number_serie = '%s' or refund_number_serie is null) 
			and (refund_uuid = '%s' or refund_uuid is null)
			and smr.folio_ingreso = '%s'
			and smr.journal_id = %s
			and smr.operating_unit_id = %s and %s	
		""" % (pago[2], pago[1], pago[4], pago[5], pago[6], pago[8], pago[3], pago[10], ou, DOMAIN_PAYMENT)

	swhere = swhere.replace('None', '')
	return swhere

def update_invoiceids(ou):

	sql = """
		with facturas as
		(
			select smr.id smr_id, ai.id id_factura
			from sync_morsa_refunds smr 
				left join account_invoice  ai on 
					(concat(trim(smr.invoice_number_serie),'-',trim(smr.invoice_number)) = ai.internal_number)
			where invoice_type in ('in_payment') and num_mov = 500 and smr.operating_unit_id = %s
				(smr.invoice_id = 0 or smr.invoice_id is null) and ai.id > 0 and voucher_id is null 
		)
		update sync_morsa_refunds set  invoice_id = facturas.id_factura 
		from facturas 
		where sync_morsa_refunds.id = facturas.smr_id;
	""" % ou
	return execute_query(sql, 0, True)

def get_facturasid(ou, pago):

	swhere = get_where_facturas(ou, pago)

	sql = """
		select distinct smr.id  
		from sync_morsa_refunds smr %s  
		union
		(
			with pagos as
			(
				select distinct smr.id smr_id, smr.invoice_number_serie, smr.invoice_number 
				from sync_morsa_refunds smr %s  
			)
			select smr.id 
			from pagos left join sync_morsa_refunds smr 
				on ( trim(pagos.invoice_number_serie) = trim(smr.invoice_number_serie) 
						and trim(pagos.invoice_number) = trim(smr.invoice_number)
					)
			where smr.date_refund <= '%s' and %s
		)
	""" % (swhere,  swhere, pago[1], DOMAIN_CREDIT)

	#print "IDs a actualizar: %s" % sql
	ids = execute_query(sql)

	#npids = np.array(ids)
	#idsinvoice = list(npids[:, 0])
	#idsrefund = list(filter(lambda idrefund: idrefund != 0, npids[:, 1]))

	smrids = [int(x[0]) for x in ids]

	return smrids

def get_facturaspagadas(ou, pago):

	sql = """	
		select  ai.id factura_id, smr.invoice_id, smr.invoice_number_serie, smr.invoice_number, 
				aml.id move_line_id, ai.residual, ai.amount_total, aml.name,
				sum(smr.amount_total) invoicepayment, 
				coalesce( ( select sum( coalesce(smr2.amount_total, 0))
							from sync_morsa_refunds smr2 
							where trim(smr2.invoice_number_serie) =  trim(smr.invoice_number_serie)
								and trim(smr2.invoice_number) = trim(smr.invoice_number)
								and smr2.date_refund <=  '%s'
								and smr2.invoice_type = 'out_invoice' and smr2.num_mov >= 610
								and smr2.state != 'paid' and smr2.voucher_id is null
						), 0) invoicecredits
		from sync_morsa_refunds smr 
				left join account_invoice  ai on 
							(concat(trim(smr.invoice_number_serie),'-',trim(smr.invoice_number)) = ai.internal_number)
				left join account_move_line aml on 
							(aml.move_id = ai.move_id and aml.account_id = ai.account_id)
		%s 
		group by ai.id, smr.invoice_id, smr.invoice_number_serie, smr.invoice_number, 
				aml.id, aml.amount_residual, ai.amount_total, aml.name
		order by smr.invoice_number_serie, smr.invoice_number;
	""" % (pago[1], get_where_facturas(ou, pago))

	# with pagos as
	# (
	#
	# 	select  ai.id factura_id, smr.invoice_id, smr.invoice_number_serie, smr.invoice_number,
	# 			aml.id move_line_id, aml.amount_residual, ai.amount_total, aml.name,
	# 			sum(smr.amount_total) invoicepayment,
	# 	from sync_morsa_refunds smr
	# 			left join account_invoice  ai on
	# 						(concat(trim(smr.invoice_number_serie),'-',trim(smr.invoice_number)) = ai.internal_number)
	# 			left join account_move_line aml on
	# 						(aml.move_id = ai.move_id and aml.account_id = ai.account_id)
	# 	%s
	# 	group by ai.id, smr.invoice_id, smr.invoice_number_serie, smr.invoice_number,
	# 			aml.id, aml.amount_residual, ai.amount_total, aml.name
	# )
	# select pagos.*, coalesce( sum(smr.amount_total), 0) invoicecredits
	# from pagos left join sync_morsa_refunds smr
	# 	on ( trim(pagos.invoice_number_serie) = trim(smr.invoice_number_serie)
	# 			and trim(pagos.invoice_number) = trim(smr.invoice_number)
	# 		)
	# where smr.date_refund <= '%s' and %s
	# group by pagos.factura_id, pagos.invoice_id, pagos.invoice_number_serie, pagos.invoice_number,
	# 		pagos.move_line_id, pagos.amount_residual, pagos.name, pagos.amount_total, pagos.invoicepayment
	# order by pagos.invoice_number_serie, pagos.invoice_number
	# """ % (get_where_facturas(ou, pago), pago[1], DOMAIN_CREDIT)

	#print "Facturas pagadas: %s" % sql
	return execute_query(sql)

def get_creditosfactura(ou, pago):

	sql = """ 
		with pagos as
		(
			select distinct smr.invoice_number_serie, smr.invoice_number
			from sync_morsa_refunds smr
			%s
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
		where ai.state = 'open' and smr.date_refund <= '%s' and %s
		group by ai.id, smr.refund_id, smr.refund_number_serie, smr.refund_number, 
					aml.id, aml.amount_residual
		order by smr.refund_number_serie, smr.refund_number;
	""" % (get_where_facturas(ou, pago),  pago[1], DOMAIN_CREDIT)

	#print "Creditos de factura: %s" % sql
	return execute_query(sql)

def get_dates(start, stop):

	#whereou = ''
	#if ou:
	#	if ou == 535:
	#		ou = 318
	#	whereou = "ou.id = %s and " % ou

	#sql = """
	#		select distinct smr.date_refund, ou.code, operating_unit_id 
	#		from sync_morsa_refunds smr inner join operating_unit ou on (smr.operating_unit_id = ou.id)
	#		where %s date_refund between '%s' and '%s' and %s 
	#		order by smr.date_refund, operating_unit_id;
	#	""" % (whereou, start, stop, DOMAIN_PAYMENT)

	sql = """
		select distinct smr.date_refund
		from sync_morsa_refunds smr 
		where date_refund between '%s' and '%s' and %s 
		order by smr.date_refund;
	""" % (start, stop, DOMAIN_PAYMENT)

	#print  "** Las fechas son: %s ***" % sql
	return execute_query(sql)

def get_partners(ou, date):
	sql = """
			select distinct partner_id_internal, partner_id
			from sync_morsa_refunds smr
			where operating_unit_id = %s and date_refund = '%s' and %s 
			order by partner_id_internal;
		""" % (ou, date, DOMAIN_PAYMENT)

	#print  "** Las partner son: %s ***" % sql

	return execute_query(sql)

def get_partner_payments(ou, date, partner):

	sql = """
		select smr.partner_id, date_refund, partner_id_internal, smr.folio_ingreso, smr.refund_ref, 
				refund_number, refund_number_serie, refund_id, refund_uuid, 
				regexp_replace(smr.xmlfile, '[^\x20-\x7f\x0d\x1b]', ' ', 'g') xmlfile,
				smr.journal_id, partner_vat, 
				coalesce( smr.acc_relative_partner_id, 
							replace(ip.value_reference, 'account.account,', '')::int4 
						) acc_receivable,
				sum(amount_total) paymenttotal
		from sync_morsa_refunds smr inner join sync_morsa_incomes smi on (smr.sync_morsa_incomes_id = smi.id)
				inner join account_journal aj on (aj.id = smi.journal_income_id)
				inner join ir_property ip on 
							(ip."name" = 'property_account_receivable' and 
								ip.res_id = concat('res.partner,', smr.partner_id::text)
							)
		where smr.operating_unit_id = %s and date_refund = '%s' and partner_id_internal = %s 
			  and %s 
		group by smr.partner_id, date_refund, partner_id_internal, smr.folio_ingreso, smr.refund_ref, 
					refund_number, refund_number_serie, refund_id, refund_uuid, xmlfile,
					smi.journal_id, partner_vat, acc_relative_partner_id, ip.value_reference		
		order by refund_number_serie, refund_number; """ % (ou, date, partner, DOMAIN_PAYMENT)

	#print  "** Las pagos son: %s ***" % sql

	return execute_query(sql)

def get_aml(account_id, partner_id, amount):

	sql = """
			select aml.id, aml.name, aml.debit, aml.amount_residual, aml."date", aml.date_maturity 
			from account_move_line aml inner join account_period ap on (aml.period_id = ap.id)
			where aml.state = 'valid' and aml.account_id = %s and aml.partner_id = %s and not ap.special 
					and aml.debit > 0 and aml.amount_residual >= %s and  "date" <= '2018-12-31'
			order by "date" desc limit 1;	
		""" % (account_id, partner_id, amount)

	return execute_query(sql, 1)

def create_voucher(pago, ou, period_id):

	id = ''
	id_fe_mx = ''
	vouchernumber = ''

	if pago[8]:
		vouchernumber = pago[6].strip() + "-" + pago[5].strip()

	name = pago[11].strip() + "[" + str(pago[2]) + "]"
	av = {
		'partner_id': pago[0],
		'amount': float(pago[13]),
		'payment_type_id': paymenttype,
		'date': pago[1].strftime('%Y-%m-%d'),
		'number': vouchernumber,
		'name': name,
		'reference': pago[3] or name,
		'journal_id': pago[10],
		#'account_id': 1649,
		'period_id': period_id,
		'type': 'receipt',
		'voucher_operating_unit_id': ou,
		'company_id': 1,
		'pre_line': True,
	}

	id = models.execute_kw(db, uid, password, 'account.voucher', 'create', [av])

	print "Pago %s en folio %s, " % (id, vouchernumber),
	if pago[8]:
		vat = av['name'][0:av['name'].find("[")]
		datos = {
			'name': vat + '_' + vouchernumber + '.xml',
			'type': 'binary',
			'datas': pago[9].encode('base64'),
			'res_name': vouchernumber,
			'res_model': 'account.voucher',
			'company_id': 1,
			'datas_fname': vat + '_' + vouchernumber + '.xml',
			'res_id': id
		}
		id_attach = models.execute_kw(db, uid, password, 'ir.attachment', 'create', [datos])

		"""LOGICA PARA ASIGNAR EL UUID AL VOUCHER CREADO """
		if id_attach:
			datos = {
				'name': vat + '_' + vouchernumber,
				'uuid': pago[8],
				'state': 'done',
				'company_id': 1,
				'cfdi_type': 'incoming',
				'file_xml_sign': id_attach,
				'type_attachment': 'account.voucher',
				'res_id': id
			}
			id_fe_mx = models.execute_kw(db, uid, password, 'ir.attachment.facturae.mx', 'create', [datos])
			models.execute_kw(db, uid, password, 'account.voucher', 'write', [[id], {'cfdi_id': id_fe_mx}])

	create_voucher_lines(id, pago, ou)

	return id

def create_voucher_lines(voucherid, pago, ou):

	vlines = get_facturaspagadas(ou, pago)
	print "con %s facturas " % len(vlines),
	amountpaidni = 0
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
				'account_id': pago[12],
				'amount_original': float(vl[6]),
				'amount': float(vl[8]) + float(vl[9]),
				'amount_unreconciled': float(vl[5]),
				'reconcile': abs(float(vl[8]) + float(vl[9]) - float(vl[5])) < 1,
				'voucher_id': voucherid
			}
			models.execute_kw(db, uid, password, 'account.voucher.line', 'create', [avl])
		else:
			amountpaidni += float(vl[8]) + float(vl[9])

	#para los pagos sin factura en el ERP
	if amountpaidni > 0:
		aml = get_aml(pago[12], pago[0], amountpaidni)
		avl = {
			'name': aml[1],
			'type': 'cr',
			'move_line_id': aml[0],
			'account_id': pago[12],
			'amount_original': float(aml[2]),
			'amount': float(amountpaidni),
			'date_original': aml[4].strftime('%Y-%m-%d'),
			'date_due': aml[5].strftime('%Y-%m-%d'),
			'amount_unreconciled': float(aml[3]),
			'reconcile': abs(float(amountpaidni) - float(aml[3])) < 1,
			'voucher_id': voucherid,
		}
		models.execute_kw(db, uid, password, 'account.voucher.line', 'create', [avl])

	vlines = get_creditosfactura(ou, pago)
	print "y %s nc, " % len(vlines),
	for vl in vlines:
		avl = {
			'name': vl[7],
			'type': 'dr',
			'move_line_id': vl[4],
			'account_id': pago[12],
			'amount_original': float(vl[6]),
			'amount': float(vl[8]),
			'amount_unreconciled': float(vl[5]),
			'reconcile': abs(float(vl[8]) - float(vl[5])) < 1,
			'voucher_id': voucherid
		}
		models.execute_kw(db, uid, password, 'account.voucher.line', 'create', [avl])

	models.execute_kw(db, uid, password, 'sync.morsa.refunds', 'write',
							[get_facturasid(ou, pago), {'state': 'paid', 'voucher_id': voucherid}])

	try:
		afieldsdpayment = ['id', 'partner_id', 'account_id', 'writeoff_amount']
		fields = {'fields': afieldsdpayment}
		domain = [[['id', '=', voucherid]]]
		voucher = models.execute_kw(db, uid, password, 'account.voucher', 'search_read', domain, fields)
		balanced = (voucher[0]['writeoff_amount'] == 0.00)
		print '%s' % ' Contabilizado' if balanced else ' SIN Cuadrar'
		if balanced:
			models.execute_kw(db, uid, password, 'account.voucher', 'proforma_voucher', [int(voucherid)])
	except:
		pass

	return

def dopayments(start=startdate_arg, stop=stopdate_arg,ous=ou_arg):

	try:
		if ous == '99':
			ous = '01'

		if ous in ['0', '']:
			domain = [[['code', 'not ilike', '-']]]
			ouids = models.execute_kw(db, uid, password, 'operating.unit', 'search_read', domain, {'order': 'code', 'fields':['id', 'name']})
		else:
			domain = [[['code', 'in', ous.split(",")]]]
			ouids = models.execute_kw(db, uid, password, 'operating.unit', 'search_read', domain, {'order': 'code', 'fields': ['id', 'name']})
		#print "OU IDS: %s" % ouids

		cperiod = ''
		period_id = ''
		dates = get_dates(start, stop)

		for date in dates:
			if cperiod != date[0].strftime('%m/%Y'):
				cperiod = date[0].strftime('%m/%Y')
				period_id = models.execute_kw(db, uid, password, 'account.period', 'search', [[['code', '=', cperiod]]])
			sdate = date[0].strftime('%Y-%m-%d')

			for ou in ouids:

				update_invoiceids(ou['id'])
				partners = get_partners(ou['id'], sdate)

				for partner in partners:
					print "En Sucursal %s[%s], Fecha: %s, Cliente: %s" % (ou['id'], ou['name'], sdate, partner[0]),
					domain = [[int(1)], 'out_invoice', 'partner_id_internal, refund_uuid', ou['id'], [['partner_id_internal', '=', partner[0]]]]
					#print "el dominio es: %s" % domain
					try:
						models.execute_kw(db, uid, password, 'sync.morsa.refunds', 'create_refunds', domain)
					except:
						pass
					#print "**ejecute bien el create_refunds****"
					pagos = get_partner_payments(ou['id'], sdate, partner[0])

					for pago in pagos:
						create_voucher(pago, ou['id'], period_id[0])
	except Exception as err:
		print repr(err)
	finally:
		if (conexion):
			conexion.close()


if __name__ == "__main__":
    dopayments()


