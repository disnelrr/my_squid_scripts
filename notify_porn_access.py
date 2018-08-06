#!/usr/bin/python
# -*- coding: iso-8859-15 -*-

import subprocess, os, smtplib, re, ldap, locale, sys, sqlite3
from datetime import *
from email.mime.text import MIMEText

# Variables
#====================================================================================
# Fichero de logs a examinar
LOGS_FILE = "/var/log/squid3/access.log"
# Fichero donde se encuentran los criterios de busqueda, uno por linea, DEBE EXISTIR
CRIT_FILE = "/etc/squid/crit_file_porno"
# Fichero donde se encuentran las excepciones, valores que no seran analizados ni reportados, DEBE EXISTIR
EXCP_FILE = "/etc/squid/exceptions_file_porno"
# Fichero con lineas a exportar a la base de datos de Drupal con feeds
FEEDS_FILE = '/etc/squid3/feeds/logs.csv'
# Base de datos sqlite con el analisis de lineas
BD_FILE = '/root/lineas.db'
# Direcciones email de los contactos a notificar separados por espacio
ADMINS=""
# Direcciones email de los contactos a notificar fuera del horario laboral
ADMINS_OUT=""
# Listado de correos de los supervisores por empresa"
SUPERVISORES=""
# Ficheros con los mensajes de notificacion
USER_ALERT_MESSAGE="/etc/squid3/user_alert_message"
SUPERVISOR_ALERT_MESSAGE="/etc/squid3/supervisor_alert_message"
# Parametros del LDAP
LDAP_URI = ""
USERS_BASE = ""
GROUPS_BASE = ""
INTERNET_GROUP_ID = 502
#====================================================================================

# Funciones
#====================================================================================

def inicializar_bd():
    conn = sqlite3.connect(BD_FILE)
    c = conn.cursor()
    try:
        c.execute("SELECT * FROM lineas")
    except sqlite3.OperationalError:
        print "Error, no existe el fichero con la base de datos"
        sys.exit(0)
    return (conn, c,)

# Las direcciones en una lista separadas por espacio, en el fichero el contenido del mensaje
def enviar_correo(asunto, direcciones, fichero):
    fp = open(fichero, 'rb')
    msg = MIMEText(fp.read(), _charset="UTF-8")
    me = 'proxy@gr.azcuba.cu'
    msg['Subject'] = asunto
    msg['From'] = me
    msg['To'] = ', '.join(direcciones.split())
    s = smtplib.SMTP('localhost')
    s.sendmail(me, direcciones.split(), msg.as_string())
    s.quit()
    fp.close()
    
def horario_laboral():
    hora = datetime.now().hour
    dia_semana = datetime.now().weekday()
    if dia_semana < 5 and hora >= 8 and hora < 17:
        return True
    return False

# Determina el correo y nombre completo del usuario
# Los devuelve en una tupla.
def obtener_datos_usuario(user):
    l = ldap.initialize(LDAP_URI)
    l.simple_bind_s()
    mail = ""
    cn = ""
    try:
        cn = l.search_s(USERS_BASE, ldap.SCOPE_SUBTREE, '(uid=%s)' % user, ['cn'])[0][1]['cn'][0]
        mail = l.search_s(USERS_BASE, ldap.SCOPE_SUBTREE, '(uid=%s)' % user, ['mail'])[0][1]['mail'][0]
        print "Determinado el correo <" + mail + "> para el usuario: [" + cn + "]."
    except IndexError:
        print "Error de acceso para determinar el correo del usuario " + user
    l.unbind_s()
    return (mail, cn)

# Devuelve True si el usuario tiene acceso a internet
# Se utiliza para no enviar el mensaje de notificacion
# en caso de que el usuario solo tenga acceso nacional
def es_usuario_internet(user):
    l = ldap.initialize(LDAP_URI)
    l.simple_bind_s()
    members = ""
    try:
        members = l.search_s(GROUPS_BASE, ldap.SCOPE_SUBTREE, '(gidNumber=%s)' % INTERNET_GROUP_ID, ['memberUid'])[0][1]['memberUid']
    except IndexError:
        print "Error de acceso para determinar los miembros del grupo", INTERNET_GROUP_ID
    l.unbind_s()
    return user in members

# Enviar notificacion al supervisor correspondiente, se debe pasar
# el usuario para localizar al supervisor en la lista, el asunto
# y el fichero con la notificacion base
def enviar_correo_a_supervisor(user, asunto, fichero):
    datos = obtener_datos_usuario(user)
    mail = datos[0]
    nombre_completo = datos[1]
    for sup_mail in SUPERVISORES.split():
        # Si el usuario es el propio supervisor, no notificar
        if sup_mail == mail:
            break
        if mail.split('@')[1] in sup_mail:
            enviar_correo(asunto, sup_mail, fichero)
            fecha = subprocess.check_output(['date', '+%d/%m/%Y : %H:%M'])
            noti_file = open('/root/noti_sup', 'a')
            noti_file.write("Notificación enviada a: <" + sup_mail + "> sobre el acceso de: [" + nombre_completo + "] el: " + fecha)
            noti_file.close()
            break

# Formatea el mensaje a enviar al usuario, uniendo las dos partes:
# el mensaje y el registro de los accesos
def formatear_mensaje(users_logs, user, fichero_mensaje):
    subprocess.call(['cp', fichero_mensaje, 'msg_tmp'])
    tmp = open('msg_tmp', 'a')
    tmp.write('\n')
    ulen = 0
    for linea in users_logs[user]:
        if len(linea) > ulen:
            ulen = len(linea)
    tmp.write('|' + "".ljust(ulen, '=') + '\n')
    for linea in users_logs[user]:
        tmp.write('|' + linea + '|\n')
    tmp.close()
    return 'msg_tmp'

# Analiza la linea y decide si se puede hacer la excepcion en base
# al criterio y la excepcion, retorna True si la linea es es_linea_sucia
# (se incluye en el reporte)
def es_linea_sucia(linea, criterio):
    # Expresion regular que define las paginas nacionales
    nav_nacional = re.compile('(https?://)?[a-zA-Z0-9.-_]*\.cu')
    pos_crit = linea.find(criterio)
    if pos_crit == -1:
        return False
    posf_crit = pos_crit + len(criterio) - 1
    # Si es una pagina nacional no se considera sucia
    if nav_nacional.search(linea):
            return False
    for excp in open(EXCP_FILE):
        pos_excp = linea.find(excp.rstrip('\n'))
        posf_excp = pos_excp + len(excp.rstrip('\n')) - 1
        # En caso de no encontrarse la excepcion se pasa al siguiente
        # elemento de excepcion
        if pos_excp != -1:
            if pos_crit >= pos_excp and posf_crit <= posf_excp:
                print excp.rstrip('\n') + " engloba al criterio [" + criterio + "]. La linea no es sucia."
                return False
            else:
                print excp.rstrip('\n') + " presente fuera del criterio, la linea es sucia."
                return True
    return True

def registrar_linea_feeds(linea, criterio):
    c = linea.split() 
    date = c[0].split('.')[0]
    elapsed = str(float(c[1]) / 1000)
    ip = c[2]    
    code = c[3]
    data = c[4]
    method1 = c[5]
    url = c[6]    
    user = c[7]
    method2 = c[8]
    rtype = c[9]
    if not os.access(FEEDS_FILE, os.F_OK):
        feeds_file = open(FEEDS_FILE, 'w')
        feeds_file.write('time,elapsed,ip,code,bytes,method1,url,user,method2,type,criterion\n')
        feeds_file.write(date + ',' + elapsed + ',' + ip + ',' + code + ',' + data + ',' + method1 + ',' + url + ',' + user + ',' + method2 + ',' + rtype + ',' + criterio + '\n')
        feeds_file.close()
    else:
        feeds_file = open(FEEDS_FILE, 'a')
        feeds_file.write(date + ',' + elapsed + ',' + ip + ',' + code + ',' + data + ',' + method1 + ',' + url + ',' + user + ',' + method2 + ',' + rtype + ',' + criterio + '\n')
        feeds_file.close()

#=====================================================================================

# Marcar el inicio del script
start = datetime.now()

# Expresion regular que define las paginas nacionales
nav_nacional = re.compile('(https?://)?[a-zA-Z0-9.-_]*\.cu')

# Definir las locales a utilizar
locale.setlocale(locale.LC_ALL, 'es_CU')

# Crear ficheros iniciales en la primera ejecucion
if os.access(LOGS_FILE + ".f", os.F_OK) == False:
    subprocess.call(["cp", LOGS_FILE, LOGS_FILE + ".f"])
    
# Copiar los archivos de inicio, fin y transicion
subprocess.call(["cp", LOGS_FILE, LOGS_FILE + ".x"])
subprocess.call(["cp", LOGS_FILE + ".f", LOGS_FILE + ".i"])
subprocess.call(["cp", LOGS_FILE + ".x", LOGS_FILE + ".f"])

# Determinar la diferencia entre el inicio y fin
inicio = len(open(LOGS_FILE + ".i").readlines())
final = len(open(LOGS_FILE + ".f").readlines())
dif = final - inicio
print "Analizando " + str(dif) + " líneas..."

file_dif = subprocess.check_output(["tail", "-n " + str(dif), LOGS_FILE + ".f"])

# Preparar el fichero para en analisis de cuota
f = open('dif.tmp', 'w')
f.write(file_dif)
f.close()
subprocess.call(['python', '/root/test/cuotas_db.py', '/root/dif.tmp'])
subprocess.call(['rm', 'dif.tmp'])
# Fin del subproceso de analisis de cuota

# Buscar las lineas sucias y pasarlas al fichero
ls = 0
crit_lines = open('crit_lines_excp', 'w')
for linea in file_dif.split('\n'):
    for criterio in open(CRIT_FILE):
        criterio = criterio.rstrip('\n')
        if es_linea_sucia(linea, criterio): 
            inicio = linea.find(criterio)
            fin = inicio + len(criterio)
            if linea[inicio:fin].isupper():
                crit_lines.write(linea[:inicio] + criterio.lower() + linea[fin:] + '\n')
            else:
                crit_lines.write(linea[:inicio] + criterio.upper() + linea[fin:] + '\n')
            #crit_lines.write(linea + '\n')
            print "Encontrada línea sucia para el criterio: " + criterio
            registrar_linea_feeds(linea, criterio)
            ls += 1
            break
crit_lines.close()
print "De ellas " + str(ls) + " determinadas como sucias..."

# Actualizar la base de datos con la informacion de la lineas analizadas
conn, c = inicializar_bd()
data = (start.hour, start.minute, dif, ls,)
hora = (data[0], data[1],)
c.execute("SELECT * FROM lineas WHERE hora = ? AND minuto = ?", hora)
try:
    record = c.next()
    c.execute("UPDATE lineas SET la = ?, ls = ? WHERE hora = ? AND minuto = ?", hora)
except:
    c.execute("INSERT INTO lineas VALUES (?, ?, ?, ?)", data)
conn.commit()
conn.close()

# A partir de aqui enviar las notificaciones por correo. 
# Solo enviar el correo si contiene algo el fichero con las incidencias
lineas = int(subprocess.check_output(["wc", "-l", 'crit_lines_excp']).split()[0])
if lineas > 0:
    enviar_correo('Alerta, posible acceso a sitios prohibidos...', ADMINS, 'crit_lines_excp')
    if not horario_laboral():
        enviar_correo('Alerta, posible acceso a sitios prohibidos...', ADMINS_OUT, 'crit_lines_excp')
        
# Personalizar los correos a enviar para el usuario encartado y el supervisor correspondiente
users_logs = dict()
for linea in open('crit_lines_excp'):
    campos = linea.split()
    user = campos[7]
    if user != '-':
        ip = campos[2]
        url = campos[6]
        fecha = datetime.fromtimestamp(float(campos[0])).strftime('%a, %d/%b/%Y, %H:%M:%S')
        registro = fecha + " || " + ip + " || " + url
        try:
            users_logs[user].append(registro)
        except (KeyError, IndexError):
            users_logs[user] = []
            users_logs[user].append(registro)
        
for user in users_logs.keys():
    if es_usuario_internet(user):
        datos = obtener_datos_usuario(user)
        fich_mens = formatear_mensaje(users_logs, user, SUPERVISOR_ALERT_MESSAGE)
        enviar_correo_a_supervisor(user, "Aviso, posible acceso indebido del usuario: [" + datos[1] + "]", fich_mens)
        fich_mens = formatear_mensaje(users_logs, user, USER_ALERT_MESSAGE)
        enviar_correo("Posible acceso indebido a internet, revise el cuerpo del correo", datos[0], fich_mens) 
    
# Marcar el fin del script y calculo del tiempo empleado
end = datetime.now()
elapsed = end - start
print "Análisis realizado en: " + str(elapsed.total_seconds()) + " segundos."
