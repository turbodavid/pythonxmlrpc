from lxml import etree as ET
import xmlrpclib
import psycopg2 as psql
import psycopg2.extras
import sys
import time
from datetime import datetime

COMPANY_ID_MOR = 1
TAXCODESALE = {'16': 50, '8': 69}
PAYMENT_TYPE_CODE = '03'
ID_CTA_IVA_POR_PAGAR = 2071
SPECIALS_ACCOUNTS = {'500': '', '510': 2299, '520': 2305, '530': 2291}
DEFAULT_UNIT_ID = 318
DEFAULT_UNIT_CODE = '01'
CXC_PR = 1869
CHEQUE_BOTADO = {220: [1649, 'NF-'], 310: [2063, 'NFDD-']}
ANALYTIC_JOURNAL_ID = 1
CC_BONIFICACION = 2163
CC_DEVOLUCION = 2162
IMPUESTO_IVA = 50


# def get_conexion(host='192.168.0.9', port=5432, dbname='GMM', user= 'openerp', password='0p3n3rp'):
# 	return psql.connect("host='%s' port=%s dbname='%s' user='%s' password='%s'" % (host, port, dbname, user, password))
#

def get_conexion(host='10.0.1.181', port=5432, dbname='GMM_OUv7', user= 'openerp_rc14', password='op3n3rp'):
	return psql.connect("host='%s' port=%s dbname='%s' user='%s' password='%s'" % (host, port, dbname, user, password))


def get_conexion_direct(host, dbname, port=5432, user='kerberox', password='204N1tN3L@V19'):
	return psql.connect("host='%s' port=%s dbname='%s' user='%s' password='%s'" % (host, port, dbname, user, password))


url = 'http://localhost:8069'
db = 'GMM_OUv7'
username = 'admin'
password = 'victoria'
common = xmlrpclib.ServerProxy('{}/xmlrpc/2/common'.format(url))
uid = common.authenticate(db, username, password, {})
models = xmlrpclib.ServerProxy('{}/xmlrpc/2/object'.format(url))

#url = 'http://localhost:8069'
# db = 'GMM'
# username = 'admin'
# password = 'victoria'
# common = xmlrpclib.ServerProxy('{}/xmlrpc/2/common'.format(url))
# uid = common.authenticate(db, username, password, {})
# models = xmlrpclib.ServerProxy('{}/xmlrpc/2/object'.format(url))

startdate_arg = sys.argv[1]
stopdate_arg = sys.argv[2]
ou_arg = sys.argv[3]

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


def get_journal(unit, serie, fiscal, tipomov):

	ou_name = unit['name']
	accid = CC_BONIFICACION
	if tipomov in (620, 720):
		accid = CC_DEVOLUCION

	if fiscal:
		if not serie:
			serie = ''
		if len(serie) >= 7:
			journalcode = 'NC'
		else:
			journalcode = 'NCVT'
		journalcode += serie
	else:
		journalcode = 'NCVTANF'

	domain = [[['code', '=', journalcode]]]
	journal = models.execute_kw(db, uid, password, 'account.journal', 'search_read', domain, {'fields': ['id', 'code', 'name']})

	if not journal:
		datos = {
			'name': 'Nota Credito Ventas ' + ('' if fiscal else 'No Fiscal ') + \
					ou_name + ' ' + (serie if serie else ''),
			'code': journalcode,
			'type': 'sale_refund',
			'analytic_journal_id': ANALYTIC_JOURNAL_ID,
			'default_debit_account_id': accid,
			'default_credit_account_id': accid,
			'active': True,
			'update_posted': True,
		}
		journalid = models.execute_kw(db, uid, password, 'account.journal', 'create', [datos])
	else:
		journalid = journal[0]['id']

	return journalid


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

	subtotal = float(root.attrib['SubTotal'])
	descuento = float(root.attrib['Descuento'])
	subtotal -= descuento
	taxrate = round(float(taxes) / float(base), 2)

	return base, base0, taxes, taxrate * 100

def get_credits_in_smr(ou, date):
	sql = """
		select ou.code sucursal, smr.partner_id_internal numcte, smr.partner_id_internal_socio numsocio, 
				smr.partner_vat rfc, smr.date_refund fechadoc, smr.refund_ref referencia, 
				smr.refund_number_serie serie, 
				upper(smr.refund_uuid) folio_fiscal, 
				regexp_replace(smr.xmlfile, '[^\x20-\x7f\x0d\x1b]', ' ', 'g') xmlfile,
				regexp_replace(smr."comment", '[^\x20-\x7f\x0d\x1b]', ' ', 'g') concepto, 
				case when smr.acc_relative_partner_id is not null then 'S' else 'N' end parte_rel, 
				smr.journal_id diario, smr.num_mov tipomov,
				round( sum(smr.amount_total), 2) importe, round( sum(smr.amount_untaxed), 2) impsiniva 
		from sync_morsa_refunds smr inner join operating_unit ou on (smr.operating_unit_id = ou.id)
		where smr.operating_unit_id = %s and smr.date_refund = '%s' 
				and smr.state = 'draft' and smr.invoice_type = 'out_invoice' and smr.num_mov >= 610
		group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13
		order by 2, smr.refund_number_serie, smr.refund_ref;
	""" % (ou['id'], date)

	credits = execute_query(None, sql)

	return credits

def get_credits_by_uuid(cn, ou, date):

	sql = """
			select k.sucursal, k.numcte, k.numsocio, cli.rfc, 
					to_date( k.fechadoc::text, 'YYYYMMDD' ) fechadoc,  
					k.referencia, trim(k.serie) serie, 
					upper(cfdi.folio_fiscal) folio_fiscal, 
					regexp_replace(cfdi.cfdi_xml, '[^\x20-\x7f\x0d\x1b]', ' ', 'g') xmlfile,
					regexp_replace(k.concepto, '[^\x20-\x7f\x0d\x1b]', ' ', 'g') concepto, 
					cli.parte_rel, coalesce( dep.diario, 621) diario, k.tipomov,
					round( sum(k.importe), 2) importe, round( sum(k.importe / (1+ (k.iva/100))), 2) impsiniva 
			from cxckardex k
					left join cxcfacturas f on (trim(k.seriedoc)=trim(f.serie) and k.numdocto = f.numdocto and k.tipomov2=f.tipomov)
					left join cxcclientes cli on (k.numcte = cli.numcte)
					left join cfdi_sellado cfdi on (trim(k.serie) = trim(cfdi.serie) and k.referencia=cfdi.numdocto)
					left join cfdi_sellado cfdif on (trim(f.serie) = trim(cfdif.serie) and f.numdocto=cfdif.numdocto)
					left join cxcctasdep dep on (k.cta_ingreso = dep.numero)           
			where k.importe > 0.04 and k.estatus = 'V' and k.fechadoc = %s 
					and k.tipomov in (605, 610, 618, 619, 620, 630, 640, 650, 660, 710, 720, 730, 740, 750)
					and (cfdi.folio_fiscal is not null and trim(cfdi.folio_fiscal) != '')
					and k.id_kar not in (select id from erpid where tabla = 'cxckardex')
			group by 1, 2, 3, 4, k.fechadoc, 6, k.serie, cfdi.folio_fiscal, cfdi.cfdi_xml, 
					10, 11, dep.diario, 13
			order by k.numcte, k.serie, k.referencia, cfdi.folio_fiscal;
			""" % date

	credits = execute_query(cn, sql)

	return credits


def crea_notcre(ou, uuid, partneracc):

	invid = False
	ai_acc = partneracc
	if uuid['parte_rel'] == 'S':
		ai_acc = CXC_PR

	ail_acc = CC_BONIFICACION
	if uuid['tipomov'] in [620, 720]:
		ail_acc = CC_DEVOLUCION

	taxid = [[6, 0, [IMPUESTO_IVA]], ]
	base, base0, taxes, taxrate = read_credits_from_xml(uuid['xmlfile'])
	importes = [[base, taxid, taxes], [base0, '', 0]]

	docto_cliente = uuid['serie']
	docto_cliente += ('-' if docto_cliente else '') + str(uuid['referencia'])
	docto_cliente = str(uuid['referencia']) + "/" + docto_cliente

	msgdocto = 'RFC: %s. Serie:%s. Not Cre: %s. UUID: %s' % \
			   (uuid['rfc'], uuid['serie'], uuid['referencia'], uuid['folio_fiscal'] if uuid['folio_fiscal'] else '')

	date_invoice = datetime.strftime(uuid['fechadoc'], '%Y-%m-%d')
	print msgdocto,
	try:
		journalid = get_journal(ou, uuid['serie'], True, uuid['tipomov'])
		invoice_header = {
			'partner_id': ou['partner_id'][0],
			'company_id': 1,
			'account_id': ai_acc,
			'journal_id': journalid,  # preguntar que diario usar
			'operating_unit_id': ou['id'] if ou['code'] != '99' else DEFAULT_UNIT_ID,
			'type': 'out_refund',
			'origin': str(uuid['referencia']),
			'date_invoice': date_invoice,
			'number': docto_cliente,
			'internal_number': docto_cliente,
			'reference': uuid['referencia'],
			'name': uuid['rfc'] + "[" + str(uuid['numcte']) + "-" + str(uuid['numsocio']) + "]",
			'comment': str(uuid['tipomov']) + "-" + uuid['concepto'],
		}
		invid = models.execute_kw(db, uid, password, 'account.invoice', 'create', [invoice_header])

		"""LOGICA PARA CREAR EL ATTACHMENT CON EL XML DE LA FACTURA"""
		if uuid['xmlfile']:
			xml_file = uuid['xmlfile']
			invoice_attachment = {
				'name': uuid['rfc'] + '_' + uuid['serie'] + '-' + str(uuid['referencia']) + '.xml',
				'res_name': 'NC Clientes ' + docto_cliente,
				'type': 'binary',
				'datas': xml_file.encode('base64'),
				'res_model': 'account.invoice',
				'company_id': 1,
				'datas_fname': uuid['rfc'] + '_' + uuid['serie'] + '-' + str(uuid['referencia']),
				'res_id': invid,
			}
			try:
				attch = models.execute_kw(db, uid, password, 'ir.attachment', 'create', [invoice_attachment])
			except Exception as e:
				msgdocto = 'En: Enlazando archivo XML. \n' + msgdocto + "\n" + repr(e)
				create_log(ou, msgdocto)
				pass

		"""LOGICA PARA ASIGNAR EL UUID A LA FACTURA CREADA EL UUID SE SACA DEL REGISTRO DONDE SE CREA EL ATTACHMENT"""
		if uuid['folio_fiscal']:
			attachment_facturae = {
				'name': uuid['serie'] + '-' + str(uuid['referencia']),
				'uuid': uuid['folio_fiscal'],
				'state': 'done',
				'company_id': 1,
				'cfdi_type': 'outgoing',
				'file_xml_sign': attch,
				'type_attachment': 'account.invoice',
				'res_id': invid,
			}
			try:
				attch = models.execute_kw(db, uid, password, 'ir.attachment.facturae.mx', 'create',
										  [attachment_facturae])
				if attch:
					models.execute_kw(db, uid, password, 'account.invoice', 'write', [[invid], {'cfdi_id': attch}])
			except Exception as e:
				msgdocto = 'En: Asignando UUID. \n' + msgdocto + "\n" + repr(e)
				create_log(ou, msgdocto)
				pass
		"""LINEAS DE LA FACTURA"""
		docto_cliente = uuid['rfc'] + "[" + str(uuid['numcte']) + "-" + str(uuid['numsocio']) + "]"
		invoice_line = {
			'name': (ou['code'].rstrip() if ou['code'] != '99' else DEFAULT_UNIT_CODE) + '|' + docto_cliente,
			'account_id': ail_acc,
			'quantity': 1,
			'uos_id': 1,
			'company_id': 1,
			'invoice_id': invid,
		}
		# SE CREAN LAS LINEAS DE LA FACTURA
		try:
			for line in importes:
				if line[0]:
					if line[0]:
						invoice_line.update({'price_unit': line[0], 'invoice_line_tax_id': line[1]})
						models.execute_kw(db, uid, password, 'account.invoice.line', 'create', [invoice_line])

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

		except Exception as e:
			msgdocto = 'En: Creando detalle. \n' + msgdocto + "\n" + repr(e)
			create_log(ou, msgdocto)

		"""SE VALIDA LA FACTURA"""
		msgdocto = "En validando documento: \n" + msgdocto + "\n"
		models.execute_kw(db, uid, password, 'account.invoice', 'invoice_open', [int(invid)])
		print ". ID en ERP: %s" % invid

	except Exception as err:
		print ". NO Sincronizada."
		msgdocto += repr(err)
		create_log(ou, msgdocto)
		print repr(err)
		pass

	return invid

def get_credits(cn, ou, date, partneracc):

	applied = 0
	updated = 0
	movtos = 0

	credits_uuid = get_credits_in_smr(ou, date)
	for uuid in credits_uuid:
		crea_notcre(ou, uuid, partneracc)
		applied += 1

	credits_uuid = get_credits_by_uuid(cn, ou, date)
	for uuid in credits_uuid:
		invid = get_invoiceid_by_uuid(uuid['folio_fiscal'])
		if not invid:
			invid = crea_notcre(ou, uuid, partneracc)
			applied += 1
		else:
			updated += 1
		smrs = get_smr(uuid['folio_fiscal'])
		credits = get_incomes(cn, ou, '', True, False, [uuid['serie'], uuid['referencia']])
		i = 0
		amount_paid = 0
		movtos += len(credits)
		for credit in credits:

			if len(smrs) > 0 and i < len(smrs):
				create_smr(credit, ou, invid, smrs[i]['id'])
				amount_paid += smrs[i]['amount_total'] if smrs[i]['voucher_id'] else 0
				i += 1
			else:
				smrid = create_smr(credit, ou, invid)
				if smrid:
					sql = "insert into erpid values (%s, %s, 'cxckardex', 'sync_morsa_refunds'); " \
									% (credit['id_kar'], smrid)
					try:
						execute_query(cn, sql, 0, True)
					except Exception as err:
						create_log( ou, repr(err) )
						pass
				else:
					msgdoc = "Fallo la  actualizando de la NC %s-%s, %s"  \
						% ( uuid['serie'], uuid['referencia'], uuid['folio_fiscal'])
					create_log(ou,  msgdoc)


		if amount_paid > 0:
			models.execute_kw(db, uid, password, 'account.invoice', 'write', [[invid], {'residual': amount_paid}])

	return applied, updated, movtos


def get_dates_smr(ou, startdate, stopdate):

	sql = """
		select distinct to_char(smr.date_refund, 'YYYYMMDD') fechadoc 
		from sync_morsa_refunds smr inner join operating_unit ou on (smr.operating_unit_id = ou.id)
		where smr.operating_unit_id = %s and smr.date_refund between '%s' and '%s' 
				and smr.state = 'draft' and smr.invoice_type = 'out_invoice' and smr.num_mov >= 610
		order by 1;
	""" % (ou['id'], startdate, stopdate)

	datesnc = execute_query(None, sql)

	return datesnc


def get_incomes(cn, ou, date='', credits=False, onlydate=False, uuid=''):

	if onlydate:
		movtos = '(500, 510, 520, 530, 605, 610, 618, 619, 620, 630, 640, 650, 660, 710, 720, 730, 740, 750)'
	else:
		movtos = '(500, 510, 520, 530)'
	order = 'k.tipomov, k.numcte, k.serie, k.referencia, cfdi.folio_fiscal, k.seriedoc, k.numdocto'
	if credits:
		movtos = '(605, 610, 618, 619, 620, 630, 640, 650, 660, 710, 720, 730, 740, 750)'
		if uuid:
			order = 'k.seriedoc, k.numdocto'
		else:
			order = 'k.numcte, k.serie, k.referencia, cfdi.folio_fiscal, k.seriedoc, k.numdocto'

	where = """k.importe > 0.04 and k.tipomov in %s and k.estatus = 'V'
				%s
				and k.id_kar not in (select id from erpid where tabla = 'cxckardex')
			""" % (movtos, date)

	if ou['code'] != '99':
		where += " and k.sucursal = %s " % int(ou['code'])

	if onlydate:
		sql = "select distinct k.fechadoc from cxckardex k where %s order by 1;" % where
	else:
		if uuid:
			where = " k.serie = '%s' and k.referencia = %s " % (uuid[0], uuid[1])
		sql = """
			select k.sucursal, k.numcte, k.numsocio, cli.rfc, k.fecha_gen, 
					to_date( k.fechadoc::text, 'YYYYMMDD' ) fechadoc, 
					to_date( f.fechadoc::text, 'YYYYMMDD' )invoice_date, 
					k.tipomov, k.id_kar, f.invoice_id, k.numdocto invoice_number, trim(k.seriedoc) invoice_serie, 
					upper(cfdif.folio_fiscal) invoice_uuid, k.referencia, trim(k.serie) serie,
					upper(cfdi.folio_fiscal) folio_fiscal, regexp_replace(cfdi.cfdi_xml, '[^\x20-\x7f\x0d\x1b]', ' ', 'g') xmlfile, 
					regexp_replace(k.concepto, '[^\x20-\x7f\x0d\x1b]', ' ', 'g') concepto, cli.parte_rel, 
					coalesce( dep.diario, 621) diario, 
					regexp_replace(k.folio_ingreso, '[^\x20-\x7f\x0d\x1b]', ' ', 'g') folio_ingreso,
					k.importe, round( k.importe / (1+ (k.iva/100)), 2) impsiniva, k.tipomov2
			from cxckardex k
					left join cxcfacturas f on (trim(k.seriedoc)=trim(f.serie) and k.numdocto = f.numdocto and k.tipomov2=f.tipomov)
					left join cxcclientes cli on (k.numcte = cli.numcte)
					left join cfdi_sellado cfdi on (trim(k.serie) = trim(cfdi.serie) and k.referencia=cfdi.numdocto)
					left join cfdi_sellado cfdif on (trim(f.serie) = trim(cfdif.serie) and f.numdocto=cfdif.numdocto)
					left join cxcctasdep dep on (k.cta_ingreso = dep.numero)           
			where %s 
			order by %s;
			""" % (where, order)

	cxcmovtos = execute_query(cn, sql)

	return cxcmovtos


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


def get_invoiceid(invid, invnumber):

	invoiceid = False
	if invid:
		invoiceid = invid
	else:
		#invnumber = serie + '-' + str(number)
		domain = [[['number', '=', invnumber], ['type', 'in', ['out_invoice', 'out_refund']]]]
		inv = models.execute_kw(db, uid, password, 'account.invoice', 'search_read', domain,
								  {'fields': ['id', 'name']})
		if inv:
			invoiceid = inv[0]['id']

	return invoiceid


def get_smr(uuid):

	domain = [[['refund_uuid', '=', uuid]]]
	smrs = models.execute_kw(db, uid, password, 'sync.morsa.refunds', 'search_read', domain,
								  {'fields': ['id', 'name', 'state', 'voucher_id', 'amount_total']})
	return smrs


def get_invoiceid_by_uuid(uuid):

	invoiceid = False

	domain = [[['uuid', '=', uuid]]]
	inv = models.execute_kw(db, uid, password, 'ir.attachment.facturae.mx', 'search_read', domain,
							  {'fields': ['id', 'name', 'res_id']})
	if inv:
		invoiceid = inv[0]['res_id']

	return invoiceid


def create_smr(income, ou, refundid=False, smridtoupdate=0):

	tipomov = income['tipomov']
	tipomov2 = income['tipomov2']
	serie = income['invoice_serie']
	number = income['invoice_number']
	if tipomov2 in CHEQUE_BOTADO.keys():
		serie = CHEQUE_BOTADO[tipomov2][1] + ou['code']
	invnumber = serie + "-" if serie else ""
	invnumber += str(number)

	dateconverted = datetime.strftime(income['fechadoc'], '%Y-%m-%d')
	dictincome = {
		'invoice_type': 'in_payment' if 500 <= tipomov <= 599 else 'out_invoice',
		'partner_id_internal': income['numcte'],
		'partner_id_internal_socio': income['numsocio'],
		'partner_vat': income['rfc'],
		'partner_id': ou['partner_id'][0],
		'state': 'open' if refundid else 'draft',
		'invoice_id': get_invoiceid(income['invoice_id'], invnumber),
		'invoice_uuid': income['invoice_uuid'] or False,
		'date_refund': dateconverted,
		'amount_untaxed': float(income['impsiniva']),
		'amount_total': float(income['importe']),
		'amount_taxes': float(income['importe'] - income['impsiniva']),
		'refund_ref': income['referencia'] or False, 
		'refund_id': refundid or False,
		'invoice_number': number,
		'invoice_number_serie': serie,
		'refund_number': income['referencia'] or False,
		'refund_number_serie': income['serie'] or False,
		'refund_uuid': income['folio_fiscal'] or False,
		'xmlfile': income['xmlfile'] or False,
		'comment': income['concepto'].strip(),
		'operating_unit_id': ou['id'] if ou['code'] != '99' else DEFAULT_UNIT_ID,  
		'num_mov': tipomov,
		'id_kardex': income['id_kar'],
		'acc_relative_partner_id': CXC_PR if income['parte_rel'] == 'S' else '',
		'journal_id': income['diario'],
		'folio_ingreso': income['folio_ingreso']
	}

	smrid = False
	try:
		if smridtoupdate:
			smrid = smridtoupdate
			models.execute_kw(db, uid, password, 'sync.morsa.refunds', 'write', [[smridtoupdate], dictincome])
		else:
			smrid = models.execute_kw(db, uid, password, 'sync.morsa.refunds', 'create', [dictincome])
	except Exception as e:
		msg = "Al crear operacion: \n"
		msg += "Cliente: %s, RFC: %s, TipoMov: %s, Serie: %s, Referencia: %s, Serie-Factura: %s-%s \n" % \
			   (income['numcte'], income['rfc'], income['tipomov'], income['serie'], income['referencia'],
				income['invoice_serie'], income['invoice_number'])
		msg += repr(e)
		create_log(ou, msg)

	return smrid


def doincomes(startdate=startdate_arg, stopdate=stopdate_arg, ous=ou_arg):

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
			
			if not ou['ip_address'] or not ou['data_base']:
				continue

			try:
				cn = get_conexion_direct(ou['ip_address'], ou['data_base'])
			except Exception as e:
				create_log(ou, "Error en conexion en la sucursal \n")
				pass
				continue

			start_time = time.time()
			ncdates = get_dates_smr( ou, startdate, stopdate)
			startdate = int(startdate.replace('-', ''))
			stopdate = int(stopdate.replace('-', ''))
			cdate = 'and k.fechadoc between {startdate} and {stopdate} '.format(startdate=startdate, stopdate=stopdate)
			dates = get_incomes(cn, ou, cdate, False, True)
			noincomes = (len(dates) == 0)

			for date in ncdates:
				if date not in dates:
					dates.append(date)

			domain = [[['id', '=', ou['partner_id'][0]]]]
			partner = models.execute_kw(db, uid, password, 'res.partner', 'search_read', domain,
										{'fields': ['id', 'name', 'property_account_receivable']})
			for date in dates:

				incomes = []
				if not noincomes:
					incomes = get_incomes(cn, ou, "and k.fechadoc = %s " % date[0])

				applied = 0
				notapplied = 0
				for income in incomes:
					print "Pagos En Sucursal %s, Tipo Mov: %s, Cliente: %s, Docto %s-%s, Factura: %s-%s" % \
						  (ou['name'], income['tipomov'], income['numcte'], income['serie'], income['referencia'],
							income['invoice_serie'], income['invoice_number']),

					smrid = create_smr(income, ou)
					if smrid:
						print ". Ingreso ID en ERP: %s" % smrid
						sql = "insert into erpid values (%s, %s, 'cxckardex', 'sync_morsa_refunds'); " \
									% (income['id_kar'], smrid)
						try:
							execute_query(cn, sql, 0, True)
							applied += 1
						except Exception as err:
							create_log( ou,  repr(err) )
							pass

					else:
						print ". NO Sincronizado"
						notapplied += 1

				applied, notapplied, incomes = \
							get_credits(cn, ou, date['fechadoc'], partner[0]['property_account_receivable'][0])

				# for income in incomes:
				# 	print "Pagos En Sucursal %s, Tipo Mov: %s, Cliente: %s, Docto %s-%s, Factura: %s-%s" % \
				# 		  (ou['name'], income['tipomov'], income['numcte'], income['serie'],
				# 		   income['referencia'],
				# 		   income['invoice_serie'], income['invoice_number']),
				#
				# 	smrid = create_smr(income, ou)
				# 	if smrid:
				# 		print ". ID en ERP: %s" % smrid
				# 		sql = "insert into erpid values (%s, %s, 'cxckardex', 'sync_morsa_refunds'); " \
				# 			  % (income['id_kar'], smrid)
				# 		#execute_query(cn, sql, 0, True)
				# 		applied += 1
				# 	else:
				# 		print ". NO Sincronizado"
				# 		notapplied += 1

				create_log(ou, "Se procesaron el %s, Notas de Credito: %s. \
								Actualizadas: %s. Operaciones: %s. Tiempo en minutos: %s" %
							(date, applied, notapplied, incomes, (time.time() - start_time)/60))



	except Exception as err:
		create_log(None, repr(err))
		print repr(err)
	finally:
		if (conexion):
			conexion.close()


if __name__ == "__main__":
	doincomes()

	




