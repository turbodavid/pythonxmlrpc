from lxml import etree as ET
import xmlrpclib
import psycopg2 as psql
import psycopg2.extras
import sys
import time
from datetime import datetime


def get_conexion(host='10.0.1.181', port=5432, dbname='GMM_OUv7', user= 'openerp_rc14', password='op3n3rp'):
        return psql.connect("host='%s' port=%s dbname='%s' user='%s' password='%s'" % (host, port, dbname, user, password))

def get_conexion_direct(host, dbname, port=5432, user='kerberox', password='204N1tN3L@V19'):
        return psql.connect("host='%s' port=%s dbname='%s' user='%s' password='%s'" % (
                                        host, port, dbname, user, password))


url = 'http://localhost:7069'
db = 'GMM_OUv7'
username = 'admin'
password = 'victoria'
common = xmlrpclib.ServerProxy('{}/xmlrpc/2/common'.format(url))
uid = common.authenticate(db, username, password, {})
models = xmlrpclib.ServerProxy('{}/xmlrpc/2/object'.format(url))


# def get_conexion(host='192.168.1.75', port=5432, dbname='GMM', user= 'openerp', password='0p3n3rp'):
# 	return psql.connect("host='%s' port=%s dbname='%s' user='%s' password='%s'" % (host, port, dbname, user, password))
#
#
# def get_conexion_direct(host, dbname, port=5432, user='kerberox', password='204N1tN3L@V19'):
# 	return psql.connect("host='%s' port=%s dbname='%s' user='%s' password='%s'" % (
# 					host, port, dbname, user, password))
#
# url = 'http://localhost:8069'
# db = 'GMM'
# username = 'admin'
# password = 'victoria'
# common = xmlrpclib.ServerProxy('{}/xmlrpc/2/common'.format(url))
# uid = common.authenticate(db, username, password, {})
# models = xmlrpclib.ServerProxy('{}/xmlrpc/2/object'.format(url))

ou_arg = sys.argv[1]

conexion = get_conexion()


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

def read_credits_from_xml(data):

	data = data.encode("UTF-8")
	root = ET.fromstring(data)
	etaxes = root.findall('cfdi:Conceptos/cfdi:Concepto/cfdi:Impuestos/cfdi:Traslados/cfdi:Traslado', root.nsmap)
	base = 0
	base0 = 0
	taxes = 0
	for tax in etaxes:
		if tax.attrib['TasaOCuota'] == "0.160000":
			taxes += float(tax.attrib['Importe'])
			base += float(tax.attrib['Base'])
		else:
			base0 += float(tax.attrib['Base'])

	subtotal = root.attrib['SubTotal']
	total = root.attrib['Total']

	return float(subtotal), float(total), taxes, base, base0


def get_vouchers(ou):

	sql = """
			select distinct avl.voucher_id, av."number", av."name", av.state
			from invamounts i inner join account_invoice ai on (i."invoiceid" = ai.id)
					inner join operating_unit ou on (ai.operating_unit_id = ou.id)
					left join account_move_line aml on (aml.move_id = ai.move_id and aml.account_id = ai.account_id)
					left join account_voucher_line avl on (aml.id = avl.move_line_id)
					left join account_voucher av on (av.id = avl.voucher_id)
			where ai.operating_unit_id = %s
			order by voucher_id ;
		""" % ou

	return execute_query(conexion, sql)


def delete_vouchers(ou):

	vouchers = get_vouchers(ou)
	for voucher in vouchers:
		msgdocto = ''
		try:
			voucherid = int(voucher['voucher_id'])
			msgdocto =  "En %s, eliminando pago: %s(%s-%s)" % (ou['name'], voucherid, voucher['number'], voucher['name'])
			print msgdocto
			if voucher['state'] == 'posted':
				models.execute_kw(db, uid, password, 'account.voucher', 'cancel_voucher', [[voucherid]])
			if voucher['state'] != 'draft':
				models.execute_kw(db, uid, password, 'account.voucher', 'action_cancel_draft', [[voucherid]])
			domain = [[['voucher_id', '=', voucherid]]]
			smrs = models.execute_kw(db, uid, password, 'sync.morsa.refunds', 'search_read', domain, {'fields': ['id']})
			smrs = [smr['id'] for smr in smrs]
			datos = {'state': 'open', 'voucher_id': False }
			models.execute_kw(db, uid, password, 'sync.morsa.refunds', 'write', [smrs, datos])
			models.execute_kw(db, uid, password, 'account.voucher', 'unlink', [[voucherid]])
		except Exception as err:
			msgdocto += "\n" + repr(err)
			create_log(ou, msgdocto)
			pass

	return


def get_sales(cn, ou):

	sql = """
			select ai.id, ai.internal_number, ai.state, ai.move_id, am.state statemove, ia.index_content
			from invamounts i inner join account_invoice ai on (ai.id = i.invoiceid)
					inner join ir_attachment ia on (ia.res_id = ai.id and res_model = 'account.invoice')
					left join account_move am on (am.id = ai.move_id)
			where ai.operating_unit_id = %s;	
		""" % (ou['id'])

	sales = execute_query(cn, sql)

	return sales


# def get_sales(cn, ou, datestart, datestop):
#
# 	sql = """
# 		with invamounts as
# 		(
# 			select ai.id, ai.internal_number, ai.amount_untaxed, ai.amount_tax, move_id, ait.id taxid,
# 				unnest( xpath('//cfdi:Comprobante/@Serie',ir.index_content::xml,
# 							ARRAY[ARRAY['cfdi', 'http://www.sat.gob.mx/cfd/3']]) )::text SerieDoc,
# 				unnest( xpath('//cfdi:Comprobante/@Folio',ir.index_content::xml,
# 					ARRAY[ARRAY['cfdi', 'http://www.sat.gob.mx/cfd/3']]) )::text DoctoDoc,
# 				unnest( xpath('//cfdi:Comprobante/@SubTotal',ir.index_content::xml,
# 					ARRAY[ARRAY['cfdi', 'http://www.sat.gob.mx/cfd/3']]) )::text::numeric SubTotal,
# 				unnest( xpath('//cfdi:Comprobante/@Descuento',ir.index_content::xml,
# 					ARRAY[ARRAY['cfdi', 'http://www.sat.gob.mx/cfd/3']]) )::text::numeric Descuento,
# 				unnest( xpath('//cfdi:Comprobante/@Total',ir.index_content::xml,
# 					ARRAY[ARRAY['cfdi', 'http://www.sat.gob.mx/cfd/3']]) )::text::numeric Total,
# 				unnest( xpath('//cfdi:Impuestos/@TotalImpuestosTrasladados',ir.index_content::xml,
# 					ARRAY[ARRAY['cfdi', 'http://www.sat.gob.mx/cfd/3']]) )::text::numeric Impuestos,
# 				ir.index_content
# 		from account_invoice ai
# 			inner join ir_attachment ir on (ir.res_id=ai.id and ir.res_model = 'account.invoice')
# 			inner join account_invoice_tax ait on (ait.invoice_id = ai.id)
# 		where ai.operating_unit_id = %s and ai."type" = 'out_invoice' and ai.state = 'open'
# 			and ai.date_invoice between '%s' and '%s'
# 		)
# 		select * from invamounts
# 		where impuestos != amount_tax or  (subtotal - descuento) != amount_untaxed;
# 	""" % (ou['id'], datestart, datestop)
#
# 	sales = execute_query(cn, sql)
#
# 	return sales


def create_log(ou, err_description):

	sales_log = {
		'tipo': 'Venta Correccion',
		'operating_unit': ou['name'],
		'message': err_description,
		'ip_addres': ou['ip_address'],
		'data_base': ou['data_base'],
	}

	logid = models.execute_kw(db, uid, password, 'sync.sales.log', 'create', [sales_log])

	return logid


def corrige_detalle(invid, base, base0):

	domain = [[['invoice_id', '=', invid]]]
	invl = models.execute_kw(db, uid, password, 'account.invoice.line', 'search_read', domain,
							{'fields': ['id', 'name', 'account_id', 'price_unit']})
	detalle = { 'price_unit': base }
	models.execute_kw(db, uid, password, 'account.invoice.line', 'write', [[int(invl[0]['id'])], detalle])

	if base0 > 0:
		detalle = {
					'name': invl[0]['name'],
					'account_id': invl[0]['account_id'][0],
					'quantity': 1,
					'price_unit': base0,
					'uos_id': 1,
					'company_id': 1,
					'invoice_id': invid,
				}
		models.execute_kw(db, uid, password, 'account.invoice.line', 'create', [detalle])

def corrige_invoice(ou, sale):

	ok = 0
	state = ''
	msgdocto = "Corrigiendo Factura: %s" % sale['internal_number']
	print msgdocto,
	importes = read_credits_from_xml(sale['index_content'])
	try:
		moveid = sale['move_id']
		invid = sale['id']
		subtotal = importes[0]
		impuestos = importes[2]
		total = importes[1]
		base = importes[3]
		base0 = importes[4]
		taxid = int(sale['taxid'])
		state = '. Cancela Movto'

		if sale['movestate'] == 'posted':
			models.execute_kw(db, uid, password, 'account.move', 'button_cancel', [[moveid]])

		if sale['state'] == 'open':
			state = '. Cancela Factura'
			models.execute_kw(db, uid, password, 'account.invoice', 'action_cancel', [[invid]])

		state = '. Actualiza Montos Factura'
		amounts = {
			'amount_untaxed': subtotal,
			'amount_tax': impuestos,
			'amount_total': total ,
			'state': 'draft',
			}
		models.execute_kw(db, uid, password, 'account.invoice', 'write', [[invid], amounts])

		state = '. Analizo Detalle'
		corrige_detalle(invid, base, base0)

		amounts = {
				'base_amount': base,
				'amount': impuestos,
				'base': base,
		}
		state = '. Actualiza Montos Impuestos'
		models.execute_kw(db, uid, password, 'account.invoice.tax', 'write', [[taxid], amounts])

		state = '. Aplicando Factura'
		models.execute_kw(db, uid, password, 'account.invoice', 'invoice_open', [int(invid)])

		state = '. Factura Actualizada'
		ok = 1
		print state

	except Exception as err:
		msgdocto = msgdocto + "\n" + state + "\n" + repr(err)
		create_log(ou, msgdocto)
		print msgdocto
		pass

	return ok


def dosales(ous=ou_arg):

	try:
		if ous in ['0', '']:
			domain = [[['code', 'not ilike', '-']]]
			ouids = models.execute_kw(db, uid, password, 'operating.unit', 'search_read', domain,
									  {'order': 'code', 'fields': ['id','name', 'partner_id', 'code', 'ip_address', 'data_base']})
		else:
			domain = [[['code', 'in', ous.split(",")]]]
			ouids = models.execute_kw(db, uid, password, 'operating.unit', 'search_read', domain,
									  {'order': 'code', 'fields': ['id', 'name', 'partner_id', 'code', 'ip_address', 'data_base']})

		for ou in ouids:

			# try:
			# 	cn = get_conexion_direct(ou['ip_address'], ou['data_base'])
			# except Exception as e:
			# 	create_log(ou, "Error en conexion en la sucursal \n")
			# 	pass
			# 	continue

			start_time = time.time()
			sales = get_sales(conexion, ou)

			delete_vouchers(ou)
			applied = 0
			counter = 0
			totinv = len(sales)
			for sale in sales:
				counter += 1
				print "En Sucursal %s, (%s de %s), " % (ou['name'], counter, totinv),
				applied += corrige_invoice(ou, sale)

			create_log(ou, "Corregidas: %s. No Corregidas %s. Tiempo en minutos: %s" %
								(applied, counter - applied, (time.time() - start_time)/60))


	except Exception as err:
		print repr(err)
	finally:
		if (conexion):
			conexion.close()


if __name__ == "__main__":
	dosales()

	




