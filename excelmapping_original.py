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

OU_EXCELROW = {'12': 'B', '07': 'C', '10': 'D', '39': 'E', '03': 'F', '40': 'G', '15': 'H',
			   '01': 'J', '02': 'K', '31': 'L', '34': 'M', '05': 'N',
			   '21': 'P', '18': 'Q',
			   '04': 'S', '14': 'T', '06': 'U', '09': 'V',
			   '16': 'X', '29': 'Y', '24': 'Z', '26': 'AA', '25': 'AB', '37': 'AC', '30': 'AD', '28': 'AE', '35': 'AF', '38': 'AG',
			   }

SHACUM = 'Edo. de Res. Acum x Sucl.'
SHMONTH  = 'Edo. de Res. Mens x Sucl.'
#conexion = get_conexion()

# def valida_conexion():
#
# 	cn = conexion
# 	if cn:
# 		return cn
# 	else:
# 		cn = get_conexion()
#
# 	return cn
#
# def execute_query(sql,tofetch=0,tocommit=False):
#
# 	records = ''
# 	cn = valida_conexion()
# 	crs = cn.cursor()
# 	crs.execute(sql)
# 	if tocommit:
# 		cn.commit()
# 		return records
#
# 	if tofetch == 1:
# 		records = crs.fetchone()
# 	elif tofetch > 1:
# 		records = crs.fetchmany(tofetch)
# 	else:
# 		records = crs.fetchall()
# 	crs.close()
#
# 	return records

def createexcel():

	try:
		exfile = '/media/psf/Home/Downloads/EdoResAcumSuc.xlsx'
		wb = pyxl.load_workbook(exfile)
		ws = wb.active
		icol = 1

		ctl = 'account.balance.reporting.template.line'
		ctle = 'account.balance.reporting.template.line.excel'
		ctleou = 'account.balance.reporting.template.line.excel.ou'
		ctleid = ''

		domain = [[['template_id', '=', 22],
				   ['code', 'in', ['045-[x-601]', '055']]
				   ]]
		fields = ['id', 'code', 'name']
		tlids = models.execute_kw(db, uid, password, ctl, 'search_read', domain, {'order': 'code', 'fields': fields})

		for tl in tlids:
			#ouids = models.execute_kw(db, uid, password, 'operating.unit', 'search', domain,
			#						  {'order': 'code', 'fields': ['id', 'name']})

			lcode = tl['code']

			for irow in range(8,97):
				cellvalue = ws.cell(row=irow, column=icol).value
				if cellvalue and cellvalue == tl['name']:
					print tl['name'],
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
								'name': tl['name'] + ' ' + ouids[0]['name'],
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

	




