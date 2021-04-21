import xmlrpclib
#import psycopg2 as psql
import openpyxl as pyxl
import sys
from datetime import datetime

#def get_conexion(host='192.168.0.13', port=5432, dbname='GMM_OUv7', user= 'openerp', password='0p3n3rp'):
#	return psql.connect("host='%s' port=%s dbname='%s' user='%s' password='%s'" % (host, port, dbname, user, password))


url = 'http://localhost:8069'
db = 'GMM'
username = 'admin'
password = 'victoria'
common = xmlrpclib.ServerProxy('{}/xmlrpc/2/common'.format(url))
uid = common.authenticate(db, username, password, {})
models = xmlrpclib.ServerProxy('{}/xmlrpc/2/object'.format(url))

OU_EXCELROW = {'12': 'B', '07': 'C', '10': 'D', '39': 'E', '03': 'F', '40': 'G', '15': 'H', '44': 'I',
			   '01': 'K', '02': 'L', '31': 'M', '34': 'N', '05': 'O',
			   '21': 'Q', '18': 'R',
			   '04': 'T', '14': 'U', '06': 'V', '09': 'W', '41': 'X',
			   '16': 'Z', '29': 'AA', '24': 'AB', '26': 'AC', '25': 'AD', '37': 'AE',
			   '30': 'AF', '28': 'AG', '35': 'AH', '38': 'AI', '42': 'AJ', '43': 'AK'
			   }

SHACUM = 'Edo. de Res. Acum x Sucl.'
SHMONTH  = 'Edo. de Res. Mens x Sucl.'

def createexcel():

	try:
		exfile = 'EdoFrosTemplate Nov 20.xlsx'
		wb = pyxl.load_workbook(exfile)
		ws = wb.active
		icol = 1

		ctl = 'account.balance.reporting.template.line'
		ctle = 'account.balance.reporting.template.line.excel'
		ctleou = 'account.balance.reporting.template.line.excel.ou'
		ctleid = ''

		domain = [[['template_id', '=', 18]]]
		fields = ['id', 'code', 'name']
		tlids = models.execute_kw(db, uid, password, ctl, 'search_read', domain, {'order': 'code', 'fields': fields})

		for tl in tlids:

			lcode = tl['code']
			if lcode[:3] > '105':
				continue

			exname  = tl['name'] + ' '

			for irow in range(8, 57):
				cellvalue = ws.cell(row=irow, column=icol).value
				if cellvalue and cellvalue == tl['name']:
					print tl['name']
					if lcode[:3] <= '105':
						for ou in OU_EXCELROW:
							ous = [ou]
							if len(lcode) > 3:
								ous = [ou + '-' + lcode[7:10]]
							domain = [[['code', 'in', ous]]]
							ouids = models.execute_kw(db, uid, password, 'operating.unit', 'search_read',
													domain, {'fields': ['id', 'code', 'name']})
							cell = OU_EXCELROW[ou]+str(irow)
							resex = {
								'sheet_acum': SHACUM,
								'sheet_month': SHMONTH,
								'name': exname + ouids[0]['name'],
								'template_line_id': tl['id'],
								'cell_acum_current': cell,
								'cell_month_current': cell,
								'corporate_expense': False
							}
							ctleid = models.execute_kw(db, uid, password, ctle, 'create', [resex])
							for ouid in ouids:
								resex = { 'excel_id': ctleid,
										  'operating_unit_id': ouid['id']
										}
								models.execute_kw(db, uid, password, ctleou, 'create', [resex])

					else:
						lous = lcode[4:].split(",")
						resex = {
							'sheet_acum': SHACUM,
							'sheet_month': SHMONTH,
							'name': tl['name'],
							'template_line_id': tl['id'],
							'corporate_expense': True
						}
						ctleid = models.execute_kw(db, uid, password, ctle, 'create', [resex])

						if lcode[:3] < '170':
							ous = []
							for l in lous:
								ous.append(l[3:6])

							domain = [[['code', 'in', ous]]]
							ouids = models.execute_kw(db, uid, password, 'operating.unit', 'search_read',
														domain, {'fields': ['id', 'code', 'name']})
							for ouid in ouids:
								resex = { 'excel_id': ctleid,
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
