from lxml import etree as ET
import xmlrpclib
import psycopg2 as psql
import psycopg2.extras
import sys
import time
from datetime import datetime

CXC_PR = 1869
INGRESO_PR = 2159
INGRESO_CONTADO = 2157
INGRESO_CREDITO = 2158
CHEQUE_BOTADO = {220: [1649, 'NF-'], 310: [2063, 'NFDD-']}
IMPUESTO_IVA = 50
IACC = 0
SERIENF = 1
DEFAULT_UNIT_ID = 318
DEFAULT_UNIT_CODE = '01'
CTA_VTAS_SERIE_ME = 2168
ANALYTIC_JOURNAL_ID = 1
GMM_RFC = 'GMM991105IS8'


# def get_conexion(host='10.0.1.181', port=5432, dbname='GMM_OUv7', user= 'openerp_rc14', password='op3n3rp'):
#         return psql.connect("host='%s' port=%s dbname='%s' user='%s' password='%s'" % (host, port, dbname, user, password))
#
#
# def get_conexion_direct(host, dbname, port=5432, user='kerberox', password='204N1tN3L@V19'):
#         return psql.connect("host='%s' port=%s dbname='%s' user='%s' password='%s'" % (
#                                         host, port, dbname, user, password))
#
#
# url = 'http://localhost:7069'
# db = 'GMM_OUv7'
# username = 'admin'
# password = 'victoria'
# common = xmlrpclib.ServerProxy('{}/xmlrpc/2/common'.format(url))
# uid = common.authenticate(db, username, password, {})
# models = xmlrpclib.ServerProxy('{}/xmlrpc/2/object'.format(url))


def get_conexion(host='192.168.0.8', port=5432, dbname='GMM', user= 'openerp', password='0p3n3rp'):
	return psql.connect("host='%s' port=%s dbname='%s' user='%s' password='%s'" % (host, port, dbname, user, password))

def get_conexion_direct(host, dbname, port=5432, user='kerberox', password='204N1tN3L@V19'):
	return psql.connect("host='%s' port=%s dbname='%s' user='%s' password='%s'" % (
					host, port, dbname, user, password))

url = 'http://localhost:8069'
db = 'GMM'
username = 'admin'
password = 'victoria'
common = xmlrpclib.ServerProxy('{}/xmlrpc/2/common'.format(url))
uid = common.authenticate(db, username, password, {})
models = xmlrpclib.ServerProxy('{}/xmlrpc/2/object'.format(url))

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


def get_journal(unit=None, serie=None):
	if serie:
		if len(serie) >= 7:
			cCode = 'VT'
		else:
			cCode = 'VTA'
		cCode += serie
	else:
		cCode = 'VTANF'
	# cCode = 'VTA' + serie if serie else 'NF'
	domain = [[['code', '=', cCode]]]
	journal = models.execute_kw(db, uid, password, 'account.journal', 'search_read', domain,
									{'fields': ['id', 'code', 'name']})

	datos = {}
	if not journal:
		datos = {
			'name': 'Ventas ' + unit['name'] + ' ' + serie,
			'code': cCode,
			'type': 'sale',
			'analytic_journal_id': ANALYTIC_JOURNAL_ID,
			'default_debit_account_id': INGRESO_CONTADO,
			'default_credit_account_id': INGRESO_CONTADO,
			'active': True,
			'update_posted': True,
		}
		idjournal = models.execute_kw(db, uid, password, 'account.journal', 'create', [datos])
	else:
		idjournal = journal[0]['id']

	return idjournal, cCode.strip()[-1:] == 'E'


def get_invoice_data(ou):

	invoice = """f.id_factura, f.numdocto,
				 '['|| f.numcte::text || '-' || f.numsocio::text || ']' refcli,
				importe, iva, round(importe / (1+(iva/100)), 2) subtotal, 
				round(importe - (importe / (1+(iva/100))), 2) importe_iva, f.importe_descuento, 
			   """
	if ou == '99':
		invoice = """f.numdocto id_factura,  f.numdocto,
					'['|| f.numcte::text || '-' || f.numsocio::text || ']' refcli,
					importe, iva, f.subtotal, f.importe_iva, f.importe_descuento, 
				"""
	return invoice


def get_sales(cn, ou, estatus_operator):

	sql = """select %s 
				trim(cast(substr(fecha_gen::Text,1,10)||'T'||substr(fecha_gen::Text,12,8) as char(19))) fecha,
				f.fechaven, trim(f.serie) serie, parte_rel, tipo_venta, upper(folio_fiscal) folio_fiscal, 
				regexp_replace(c.nombre, '[^\x20-\x7f\x0d\x1b]', ' ', 'g') nombre,
				replace(c.rfc, '-','') rfc, regexp_replace(s.cfdi_xml, '[^\x20-\x7f\x0d\x1b]', ' ', 'g') cfdi_xml,
				invoice_id, tipomov
			 from cxcfacturas f inner join cxcclientes c on f.numcte=c.numcte
				 left join cfdi_sellado s on f.serie=s.serie and f.numdocto=s.numdocto
			 where %s and fechadoc >= 20201201 and tipomov in (110,120,210,220,230,240,250,310)
					and ((folio_fiscal is not null and trim(folio_fiscal) <> '') or tipomov in (220,310)) 
			 order by id_factura;""" % (get_invoice_data(ou), estatus_operator)

	sales = execute_query(cn, sql)

	return sales

def create_log(ou, err_description):

	sales_log = {
		'tipo': 'Venta',
		'operating_unit': ou['name'],
		'message': err_description,
		'ip_addres': ou['ip_address'],
		'data_base': ou['data_base'],
	}

	logid = models.execute_kw(db, uid, password, 'sync.sales.log', 'create', [sales_log])

	return logid


def cancel_invoice(invid):

	canceled = False
	domain = [[['id', '=', invid]]]
	invoice = models.execute_kw(db, uid, password, 'account.invoice', 'search_read', domain,
									{'fields': ['id', 'state', 'move_id']})

	if invoice and invoice[0]['state'] == 'open':
		canceled = models.execute_kw(db, uid, password, 'account.move', 'button_cancel', [[invoice[0]['move_id'][0]]])
		if canceled:
			models.execute_kw(db, uid, password, 'account.invoice', 'action_cancel', [[invid]])

	return canceled


def read_from_xml(data):

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

	subtotal = float(root.attrib['SubTotal'])
	descuento = float(root.attrib['Descuento'])
	subtotal -= descuento
	total = float(root.attrib['Total'])

	return [base, taxes, base0]


def create_invoice(sale, ou, partneracc):

	invid = False
	ai_acc = partneracc
	if sale['parte_rel'] == 'S':
		ai_acc = CXC_PR
		ail_acc = INGRESO_PR
	elif sale['tipo_venta'] == 'C':
		ail_acc = INGRESO_CONTADO
	else:
		ail_acc = INGRESO_CREDITO

	docto_cliente = sale['serie'] + "-" + str(sale['numdocto'])
	taxid = [[6, 0, [IMPUESTO_IVA]], ]
	importes = [[0, '', 0], [0, '', 0]]
	if sale['tipomov'] in CHEQUE_BOTADO.keys():
		docto_cliente = CHEQUE_BOTADO[sale['tipomov']][1] + ou['code'] + '-' + str(sale['numdocto'])
		ail_acc = CHEQUE_BOTADO[sale['tipomov']][IACC]
		importes = [[0, '', 0], [float(sale['importe']), '', 0]]

	msgdocto = 'RFC: %s. Serie:%s. Factura: %s. UUID: %s' % \
			   (sale['rfc'], sale['serie'], sale['numdocto'], sale['folio_fiscal'] if sale['folio_fiscal'] else '')

	date_inv = sale['fecha']
	date_invoice = date_inv[0:10]
	date_due = datetime.strptime(str(sale['fechaven']), '%Y%m%d')
	fecha_ven = datetime.strftime(date_due, '%Y-%m-%d')

	try:
		journalid, isserie_me = get_journal(ou, sale['serie'])
		invoice_header = {
			'partner_id': ou['partner_id'][0],
			'company_id': 1,
			'account_id': ai_acc,
			'journal_id': journalid,  # preguntar que diario usar
			'operating_unit_id': ou['id'] if ou['code'] != '99' else DEFAULT_UNIT_ID,
			'type': 'out_invoice',
			'origin': str(sale['id_factura']),
			'date_invoice': date_invoice,
			'date_due': fecha_ven,
			'number': docto_cliente,
			'internal_number': docto_cliente,
			'reference': sale['refcli'] + ' ' + sale['nombre'].strip(),
			'name': sale['rfc'] + sale['refcli'],
			'comment': sale['tipomov'],
		}
		invid  = models.execute_kw(db, uid, password, 'account.invoice', 'create', [invoice_header])

		"""LOGICA PARA CREAR EL ATTACHMENT CON EL XML DE LA FACTURA"""
		if sale['cfdi_xml']:
			xml_file = sale['cfdi_xml']
			importes = read_from_xml(xml_file)
			importes = [[importes[0], taxid, importes[1]], [importes[2], '', 0]]
			invoice_attachment = {
				'name': GMM_RFC + '_' + sale['serie'] + '-' + str(sale['numdocto']) + '.xml',
				'type': 'binary',
				'datas': xml_file.encode('base64'),
				'res_model': 'account.invoice',
				'company_id': 1,
				'datas_fname': GMM_RFC + '_' + sale['serie'] + '-' + str(sale['numdocto']),
				'res_id': invid,
			}
			try:
				attch = models.execute_kw(db, uid, password, 'ir.attachment', 'create', [invoice_attachment])
			except Exception as e:
				msgdocto = 'En: Enlazando archivo XML. \n' + msgdocto + "\n" + repr(e)
				create_log(ou, msgdocto)
				pass

		"""LOGICA PARA ASIGNAR EL UUID A LA FACTURA CREADA EL UUID SE SACA DEL REGISTRO DONDE SE CREA EL ATTACHMENT"""
		if sale['folio_fiscal']:
			attachment_facturae = {
				'name': sale['serie'] + '-' + str(sale['numdocto']),
				'uuid': sale['folio_fiscal'],
				'state': 'done',
				'company_id': 1,
				'cfdi_type': 'incoming',
				'file_xml_sign': attch,
				'type_attachment': 'account.invoice',
				'res_id': invid,
			}
			try:
				attch = models.execute_kw(db, uid, password, 'ir.attachment.facturae.mx', 'create', [attachment_facturae])
				if attch:
					models.execute_kw(db, uid, password, 'account.invoice', 'write', [[invid], {'cfdi_id': attch}])
			except Exception as e:
				msgdocto = 'En: Asignando UUID. \n' + msgdocto + "\n" + repr(e)
				create_log(ou, msgdocto)
				pass
		"""LINEAS DE LA FACTURA"""
		invoice_line = {
			'name': (ou['code'].rstrip() if ou['code'] != '99' else DEFAULT_UNIT_CODE) +
					'|' + sale['refcli'] + ' ' + sale['nombre'].strip(),
			'account_id': CTA_VTAS_SERIE_ME if isserie_me else ail_acc,
			'quantity': 1,
			'uos_id': 1,
			'company_id': 1,
			'invoice_id': invid,
		}
		# SE CREAN LAS LINEAS DE LA FACTURA
		try:
			for line in importes:
				if line[0]:
					invoice_line.update({'price_unit': line[0], 'invoice_line_tax_id': line[1]})
					models.execute_kw(db, uid, password, 'account.invoice.line', 'create', [invoice_line])
		except Exception as e:
			msgdocto = 'En: Creando detalle. \n' + msgdocto + "\n" + repr(e)
			create_log(ou, msgdocto)

		"""SE ACTUALIZA LA LINEA DE IMPUESTOS PARA DESPUES ASIGNARLE DE MANERA MANUAL EL IMPORTE CORRECTO DEL IMPUESTO"""
		if importes[0][0]:
			models.execute_kw(db, uid, password, 'account.invoice', 'button_reset_taxes', [[int(invid)]])
			domain = [[['invoice_id', '=', invid]]]
			invoice_tax = models.execute_kw(db, uid, password, 'account.invoice.tax', 'search_read', domain,
									  {'fields': ['id', 'invoice_id']})
			if invoice_tax:
				invtaxid = invoice_tax[0]['id']
				update_amount = {
					'amount': importes[0][2],
				}
				models.execute_kw(db, uid, password, 'account.invoice.tax', 'write', [[invtaxid], update_amount])
		"""SE VALIDA LA FACTURA"""
		models.execute_kw(db, uid, password, 'account.invoice', 'invoice_open', [int(invid)])

	except Exception as err:
		msgdocto = 'En: Creando Factur. \n' + msgdocto + "\n" + repr(err)
		create_log(ou, msgdocto)
		pass

	return invid


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

			if not ou['ip_address'] or not  ou['data_base']:
				continue

			
			field_to_update = 'id_factura'
			try:
				cn = get_conexion_direct(ou['ip_address'], ou['data_base'])
			except Exception as e:
				create_log(ou, "Error en conexion en la sucursal \n")
				pass
				continue

			start_time = time.time()
			sales = get_sales(cn, ou['code'], "f.invoice_id = 0 and f.estatus = 'V'")
			domain = [[['id', '=', ou['partner_id'][0]]]]
			partner = models.execute_kw(db, uid, password, 'res.partner', 'search_read', domain,
										{'fields': ['id', 'name', 'property_account_receivable']})
			applied = 0
			canceled = 0
			notapplied = 0
			notcanceled = 0
			totrec =  len(sales)
			if ou['code'] == '99':
				field_to_update =  'numdocto'

			for sale in sales:
				print "En Sucursal %s, Cliente: %s, Docto %s-%s, ID Factura: %s" % \
					  (ou['name'], sale['refcli'], sale['serie'],sale['numdocto'], sale['id_factura']),

				invid = create_invoice(sale, ou, partner[0]['property_account_receivable'][0])
				if invid:
					applied += 1
					print ". ID en ERP: %s (%s/%s)" % (invid, applied, totrec)
					sql = "update cxcfacturas set invoice_id = %s where %s = %s " % (invid, field_to_update, sale['id_factura'])
					execute_query(cn, sql, 0, True)
				else:
					notapplied += 1
					print ". NO Sincronizado"

			sales = get_sales(cn, ou['code'], "f.invoice_id > 0 and f.estatus = 'C'")
			totrec = len(sales)
			for sale in sales:
				invid = sale['invoice_id']
				print "En Sucursal %s, Cliente: %s, Docto %s-%s, ID Factura ERP: %s" % \
					  (ou['name'], sale['refcli'], sale['serie'],sale['numdocto'], sale['invoice_id']),


				if cancel_invoice(invid):
					canceled += 1
					print ". Cancelada (%s/%s)" % (canceled, totrec)
					sql = "update cxcfacturas set invoice_id = -%s where %s  = %s " % (invid, field_to_update,  sale['id_factura'])
					execute_query(cn, sql, 0, True)
				else:
					notcanceled += 1
					print ". NO Cancelada"

			if applied > 0 or notapplied > 0 or canceled > 0 or notcanceled > 0:
				create_log(ou, "Aplicadas: %s. No Aplicdas %s. \nCanceladas %s. No Canceladas %s \nTiempo en minutos: %s" % 
						(applied, notapplied, canceled, notcanceled, (time.time() - start_time)/60))


	except Exception as err:
		print repr(err)
	finally:
		if (conexion):
			conexion.close()


if __name__ == "__main__":
	dosales()

	




