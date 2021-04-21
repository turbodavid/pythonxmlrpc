from lxml import etree as ET
import xmlrpclib
import psycopg2 as psql
import psycopg2.extras
import sys
import time
from datetime import datetime

JOURNAL_ID = 262


def get_conexion(host='192.168.0.17', port=5432, dbname='GMM', user= 'openerp', password='0p3n3rp'):
	return psql.connect("host='%s' port=%s dbname='%s' user='%s' password='%s'" % (host, port, dbname, user, password))
#

# def get_conexion(host='10.0.1.181', port=5432, dbname='GMM_OUv7', user= 'openerp_rc14', password='op3n3rp'):
# 	return psql.connect("host='%s' port=%s dbname='%s' user='%s' password='%s'" % (host, port, dbname, user, password))


def get_conexion_direct(host, dbname, port=5432, user='kerberox', password='204N1tN3L@V19'):
	return psql.connect("host='%s' port=%s dbname='%s' user='%s' password='%s'" % (host, port, dbname, user, password))


# url = 'http://localhost:7069'
# db = 'GMM_OUv7'
# username = 'admin'
# password = 'victoria'
# common = xmlrpclib.ServerProxy('{}/xmlrpc/2/common'.format(url))
# uid = common.authenticate(db, username, password, {})
# models = xmlrpclib.ServerProxy('{}/xmlrpc/2/object'.format(url))

url = 'http://localhost:8069'
db = 'GMM'
username = 'admin'
password = 'victoria'
common = xmlrpclib.ServerProxy('{}/xmlrpc/2/common'.format(url))
uid = common.authenticate(db, username, password, {})
models = xmlrpclib.ServerProxy('{}/xmlrpc/2/object'.format(url))

period_arg = sys.argv[1]

conexion = get_conexion()
conexion_direct = get_conexion_direct('culiacan.morsa.com.mx', 'culiacan')

def valida_conexion(cnaux):

	if not cnaux:
		cn = conexion
		if cn:
			return cn
		else:
			cn = get_conexion()
	else:
		cn = cnaux

	return cn


def execute_query(cn, sql, tofetch=0, tocommit=False):

	records = ''
	if not cn:
		cn = valida_conexion(cn)
	crs = cn.cursor(cursor_factory=psycopg2.extras.DictCursor)
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


def create_log(ou, err_description):

	if ou:
		sales_log = {
			'tipo': 'Nota Credito Ventas',
			'operating_unit': ou['name'],
			'message': err_description,
			'ip_addres': ou['ip_address'],
			'data_base': ou['data_base'],
		}
	else:
		sales_log = {
			'tipo': 'Nota Credito Ventas',
			'message': err_description,
		}

	logid = models.execute_kw(db, uid, password, 'sync.sales.log', 'create', [sales_log])

	return logid


def get_sql_company_folios_paid(moveid, datepaid, onlypayments=False, pagouuid=None):

	fields_ = ['aml."name"', 'ou.code']
	condition_ = ['', '']
	sql = ''
	for i in range(0, 2):
		sql = """
			select distinct %s
			from account_move_line aml inner join operating_unit ou on (aml.operating_unit_id = ou.id)
				inner join account_account aa on (aml.account_id = aa.id)
			where aml.move_id = %s and aml.debit > 0 and aa."type" = 'payable';       
		""" % (fields_[i], moveid)
		result = execute_query(None, sql)
		condition_[i] = str(tuple([int(x[0]) for x in result])).replace(',)', ')')
		if condition_[i] == "()":
			condition_[i] = "(0)"

	swhereuuid = ''
	if onlypayments:
		sql = """
            select distinct pago.foliofiscal pagouuid, pago.serie seriepago, pago.folio foliopago,  
                    pago.fecha::date fecha_cfdi,
                   cpago.monto::numeric / 100 total_cfdi
        """
	else:
		sql = """
            select cfdi.emirfc, f.num_prov, f.num_suc, fg.folio, f.serie, f.foliofact, f.invoice_id, f.importe, 
                   cfdi.foliofiscal, pago.foliofiscal pagouuid, 
                   pago.serie seriepago, pago.folio foliopago,  pago.fecha::date fecha_cfdi,
                   cpago.monto::numeric / 100 total_cfdi,
                   dpago.imppagado / 100::numeric pagado, dpago.impsaldoinsoluto / 100::numeric pendiente,
                   dpago.impsaldoant / 100::numeric anterior, 
                   ROUND(importe-(saldo_notas+p_promocion+n_cargo+anticipo+saldo_desctos+devolucion+faltante),02) ImpPagado	
            """
		if pagouuid:
			swhereuuid = "pago.foliofiscal = '%s' and " % pagouuid

	sql += """from cxpfacturas f 
                INNER JOIN cxpproveedores p ON f.num_prov = p.num_prov
                INNER JOIN foliosgen fg ON (f.num_prov = fg.num_prov AND f.fec_venc = fg.fecha_ven )
                inner join cat_sucursales suc on (f.num_suc=suc.num_suc)
                left join cfdi_cfdcomprobanteproveedor cfdi 
                        on (f.foliofact=cfdi.folio and trim(f.serie)=trim(cfdi.serie) 
                             and replace(trim(p.rfc),'-','') = cfdi.emirfc )
                left join cfdi_cfdcomprobantepagodoctos dpago on (cfdi.foliofiscal = dpago.iddocumento)
                left join cfdi_cfdcomprobanteproveedor pago on (dpago.foliofiscal = pago.foliofiscal)
                left join cfdi_cfdcomprobantepago cpago on (cpago.foliofiscal = pago.foliofiscal)            
         where %s
            fg.folio in %s
            and f.num_suc in %s
            and f.fecha_pago = %s""" % (swhereuuid, condition_[0], condition_[1], datepaid)

	payments = execute_query(conexion_direct, sql)

	return payments


def process_payments(partner_id, period_id, startdate, stopdate):

	mls = get_supplier_payments(partner_id['partner_id'], period_id)
	for ml in mls:
		first = True
		ainvoices_ = []
		ainvuuids_ = []
		resinv = {}
		rfc = partner_id['vat'][2:].strip()
		sumcredits = 0
		datefrom = ml['date']
		payments = get_sql_company_folios_paid(ml['move_id'], str(ml['date']).replace('-', ''), True)
		status = 'draft'
		for payment in payments:
			paymentid = False
			withuuid = False
			pagouuid = payment['pagouuid']
			if pagouuid:
				withuuid = True
				status = 'process'
			else:
				pagouuid = str(ml['id'])

			print "Pagando RFC: %s, Pago: %s" % (rfc, pagouuid)
			paymentid = False
			domain = [[['uuid', '=', pagouuid]]]
			paymentobj = models.execute_kw(db, uid, password, 'gmm.cfdi.supplier.payment', 'search_read', domain)
			if paymentobj:
				paymentid = paymentobj[0]['id']
			ainvoices_ = []
			ainvuuids_ = []
			resinv = {}
			sumcredits = 0
			cdate = False
			if payment['fecha_cfdi']:
				cdate = str(payment['fecha_cfdi'])

			if not paymentid:
				pay_info = {
					'partner_id': partner_id['partner_id'],
					'serie': payment['seriepago'] or False,
					'number': payment['foliopago'] or False,
					'uuid': pagouuid or False,
					'date':  cdate,
					'amount': float(payment['total_cfdi']) if payment['total_cfdi'] else False,
					'company_paid_date': str(ml['date']) or False,
					'state': status or False,
					'name': 'Pago Emitido el ' + cdate if cdate else 'SIN PAGO'
				}

				paymentid = models.execute_kw(db, uid, password, 'gmm.cfdi.supplier.payment', 'create', [pay_info])
				models.execute_kw(db, uid, password, 'gmm.cfdi.supplier.payment', 'add_payment_lines', [int(paymentid), ml['id']])

			models.execute_kw(db, uid, password, 'gmm.cfdi.supplier.payment', 'add_invoices_paid', [int(paymentid)])
			models.execute_kw(db, uid, password, 'gmm.cfdi.supplier.payment', 'action_confirm', [int(paymentid)])

			# paymentid.add_credits_used(cn, datefrom, dateto)

	return True


def look_for_credits(payment_id):

	return

def get_supplier_payments(partner_id, period_id):

	sql = """
		select aml.id, aml."date", aml."ref", aml."name", move_id, debit, credit
		from account_move_line aml inner join account_account aa on (aml.account_id = aa.id)
		where aml.period_id = %s and aml.partner_id = %s
			and (aa.id in (2017,2022,2860) or aa."type" = 'liquidity')
		order by 2, 5;
	""" % (period_id, partner_id)

	supplier_payments = execute_query(None, sql)

	return supplier_payments


def get_suppliers_paid(period_id):
	# and am.partner_id = 794
	# and
	# rp.vat not in ('MXAFR020201DX3', 'MXBBT680215S78', 'MXBME511128MZ2', 'MXBSI0703301I3', 'MXBCO041112CHA',
	# 			   'MXCAH870321S97', 'MXCDN020723FD6', 'MXCRE9006209Q6', 'MXCCO830526FE3', 'MXCDM040330828',
	# 			   'MXCRO820927AH3', 'MXDAC021220RF8', 'MXAOM160209M68', 'MXAEJA7508316W8', 'MXAME960704IW8',
	# 			   'MXBMT110509SC4', 'MXCVE930225KZ1', 'MXAEAA750124EF0', 'MXCPT7605187A0', 'MXDAC1705107P5',
	# 			   'MXDAI8205246A2', 'MXDBA131001UN4', 'MXDDA870519Q49')
	sql = """
		select partner_id, rp.vat, count(*)
		from account_move am inner join res_partner rp on (am.partner_id = rp.id)
		where am.period_id = %s and am.journal_id = 262
		group by 1,2
		order by 2,1;
	""" % period_id

	suppliers_paid = execute_query(None, sql)

	return suppliers_paid


def dosupplierpayments(period_code=period_arg):

	try:
		domain = [[['code', '=', period_code]]]
		period = models.execute_kw(db, uid, password, 'account.period', 'search_read', domain,
									  {'order': 'code', 'fields': ['id','name', 'code', 'date_start', 'date_stop']})

		period_id = period[0]['id']
		suppliers = get_suppliers_paid(period_id)

		for supplier in suppliers:
			try:
				process_payments(supplier, period_id, period[0]['date_start'], period[0]['date_stop'])
			except Exception as err2:
				print "Error con %s: %s" % (supplier['vat'], repr(err2))
				pass

	except Exception as err:
		print repr(err)
	finally:
		if (conexion):
			conexion.close()


if __name__ == "__main__":
	dosupplierpayments()

	




