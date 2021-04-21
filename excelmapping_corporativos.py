import xmlrpclib
import psycopg2 as psql
import openpyxl as pyxl
import sys
from datetime import datetime

#def get_conexion(host='192.168.0.13', port=5432, dbname='GMM_OUv7', user= 'openerp', password='0p3n3rp'):
#	return psql.connect("host='%s' port=%s dbname='%s' user='%s' password='%s'" % (host, port, dbname, user, password))


url = 'http://localhost:8069'
db = 'GMM_OUv7'
username = 'admin'
password = 'victoria'
common = xmlrpclib.ServerProxy('{}/xmlrpc/2/common'.format(url))
uid = common.authenticate(db, username, password, {})
models = xmlrpclib.ServerProxy('{}/xmlrpc/2/object'.format(url))

OU_EXCELROW = {'501': 'B', '502': 'C', '503': 'D', '504': 'E', '505': 'F', '506': 'G',
			   '518': 'H', '517': 'I', '508': 'K', '509': 'L', '510': 'M', '515': 'N', '516': 'O'
			   }

SHACUM = 'Gastos Corp. Acum.'
SHMONTH  = 'Gastos Corp. Mens.'

def createexcel():

	try:
		exfile = '/media/psf/Home/Downloads/EdoResAcumSuc_Template.xlsx'
		wb = pyxl.load_workbook(exfile)
		ws = wb.active
		icol = 1

		ctl = 'account.balance.reporting.template.line'
		ctle = 'account.balance.reporting.template.line.excel'
		ctleou = 'account.balance.reporting.template.line.excel.ou'
		ctleid = ''

		domain = [[['template_id', '=', 19]]]
		fields = ['id', 'code', 'name']
		tlids = models.execute_kw(db, uid, password, ctl, 'search_read', domain, {'order': 'code', 'fields': fields})

		for tl in tlids:
			lcode = tl['code']

			for irow in range(8, 135):
				cellvalue = ws.cell(row=irow, column=icol).value
				if cellvalue and cellvalue == tl['name']:
					print tl['name'],
					for ou in OU_EXCELROW:
						ous = [ou]
						domain = [[['code', 'in', ous]]]
						ouids = models.execute_kw(db, uid, password, 'operating.unit', 'search_read',
												domain, {'fields': ['id', 'code', 'name']})
						cell = OU_EXCELROW[ou]+str(irow)
						resex = {
							'sheet_acum': SHACUM,
							'sheet_month': SHMONTH,
							'name': lcode + '-' + ouids[0]['name'],
							'template_line_id': tl['id'],
							'cell_acum_current': cell,
							'cell_month_current': cell,
							'corporate_expense': True
						}
						ctleid = models.execute_kw(db, uid, password, ctle, 'create', [resex])
						for ouid in ouids:
							resex = {'excel_id': ctleid,
									'operating_unit_id': ouid['id']
									}
							models.execute_kw(db, uid, password, ctleou, 'create', [resex])

					break

		wb.close()

	except Exception as err:
		print repr(err)
#	finally:
#		if (conexion):
#			conexion.close()

if __name__ == "__main__":
    createexcel()

	




