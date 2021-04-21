import psycopg2 as psql
import sys
import csv
from datetime import datetime

def get_conexion(host='192.168.0.12', port=5432, dbname='GMM_OUv7', user= 'openerp', password='0p3n3rp'):
	return psql.connect("host='%s' port=%s dbname='%s' user='%s' password='%s'" % (host, port, dbname, user, password))

period_arg = sys.argv[1]

conexion = get_conexion()

def valida_conexion():

	cn = conexion
	if cn:
		return cn
	else:
		cn = get_conexion()

	return cn

def execute_query(sql, tofetch=0, tocommit=False):

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

def get_polizas(period_id):

	sql = """
		select id, name, ref, date
		from account_move am
		where am.state = 'posted' and am.period_id = %s
		order by am.date, id
	""" % period_id

	return execute_query(sql)

def get_periodo(period_code):

	sql = """
		select id 
		from account_period ap
		where ap.code = '%s'
	""" % period_code

	return execute_query(sql)

def get_detpoliza(move_id):

	sql = """
		select aa.code, aa.name, aml.debit, aml.credit 
		from account_move_line aml inner join account_account aa on (aml.account_id = aa.id) 
		where aml.move_id = %s
		order by aml.id;
	""" % move_id
	return execute_query(sql)

def doexportcsv(period=period_arg):

	try:

		if period in ['0', '']:
			period = '01/2019'

		period_id = get_periodo(period)[0]
		polizas = get_polizas(period_id)
		filename = 'polizas_' + period[-4:] + period[:2] + '.csv'
		myfile = open(filename, 'wb')
		myfields = ['poliza', 'referencia', 'fecha', 'cuenta', 'nombre cuenta', 'debe', 'haber']
		writer = csv.DictWriter(myfile, fieldnames=myfields)
		writer.writeheader()

		for poliza in polizas:

			dpol = get_detpoliza(poliza[0])
			totdebe = 0
			tothaber = 0
			for move in dpol:

				if move[2] == 0.0 and move[3] == 0.0:
					continue

				if totdebe == 0 and tothaber == 0:
					rowdict = {'poliza': poliza[1],
							   'referencia': poliza[2],
							   'fecha': poliza[3],
							   'cuenta': move[0],
							   'nombre cuenta': move[1],
							   'debe': move[2],
							   'haber': move[3]
							   }
				else:
					rowdict = {'poliza': '',
							   'referencia': '',
							   'fecha': '',
							   'cuenta': move[0],
							   'nombre cuenta': move[1],
							   'debe': move[2],
							   'haber': move[3]
							   }

				totdebe += move[2]
				tothaber += move[3]
				writer.writerow(rowdict)

			rowdict = {'poliza': '',
					   'referencia': '',
					   'fecha': '',
					   'cuenta': '',
					   'nombre cuenta': 'Total Poliza:',
					   'debe': totdebe,
					   'haber': tothaber
					   }
			writer.writerow(rowdict)

	except Exception as err:
		print repr(err)
	finally:
		if (conexion):
			conexion.close()


if __name__ == "__main__":
    doexportcsv()


