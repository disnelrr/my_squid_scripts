#!/usr/bin/python
# -*- coding: iso-8859-15 -*-

import re, sqlite3, sys, os, smtplib, subprocess, ldap, ConfigParser
from datetime import *
from email.mime.text import MIMEText

LOGS = sys.argv[1]
CONFIG = '/etc/squid3/scripts_conf/cuotas_db_config.ini'

# Retorna el valor de la variable v en la configuracion
def get_option(section, option):
    if option.isupper():
        option = option.lower()
    config = ConfigParser.ConfigParser()
    config.read(CONFIG)
    return config.get(section, option)


# Incializa la base de datos. Retorna en una tupla el objeto conexion
# y el cursor para el resto de las operaciones
def inicializar_bd():
    conn = sqlite3.connect(get_option('FICHEROS', 'BD_FILE'))
    c = conn.cursor()
    try:
        c.execute("SELECT * FROM inetcons")
    except sqlite3.OperationalError:
        c.execute(
            '''CREATE TABLE inetcons (user text, data real, dataw real, datam real, elapsed real, overquota text)''')
    return (conn, c,)


def obtener_hora_actual():
    dt = datetime.now()
    return (dt.day, dt.weekday(), dt.hour, dt.minute,)


def get_squidlog_fields(linea):
    campos = dict()
    c = linea.split()
    campos['user'] = c[7]
    campos['data'] = float(c[4])
    campos['date'] = c[0]
    campos['elapsed'] = float(c[1]) / 1000
    campos['ip'] = c[2]
    return campos


def format_bytes(cons):
    if cons > 1048576:
        return str(round(cons / 1048576, 2)) + ' MB'
    elif cons > 1024:
        return str(round(cons / 1024, 2)) + ' kB'
    else:
        return str(cons) + '  B'


# Adiciona o elimina al usuario del grupo overquota
# en dependencia del valor de la variable action
# action = 'ADD' adiciona al usuario
# cualquier otro valor lo elimina del grupo
def update_user_in_group(user, action):
    l = ldap.initialize(get_option('LDAP', 'LDAP_URI'))
    l.simple_bind_s(get_option('LDAP', 'ADMIN_DN'), get_option('LDAP', 'ADMIN_PASSWD'))
    dn = get_option('LDAP', 'OVERQUOTA_GROUP')
    if action == 'ADD':
        try:
            l.modify_s(dn, [(ldap.MOD_ADD, 'memberUid', user), ])
            print "Adicionando al usuario", user, "al grupo overquota."
        except ldap.TYPE_OR_VALUE_EXISTS:
            pass
    else:
        try:
            l.modify_s(dn, [(ldap.MOD_DELETE, 'memberUid', user), ])
            print "Eliminando al usuario", user, "del grupo overquota."
        except ldap.NO_SUCH_ATTRIBUTE:
            pass
    l.unbind_s()

# Obtiene la cuota del usuario correspondiente y la devuelve en una tupla
def get_user_quota(user):
    cd, cs, cm = [int(i) for i in get_option('PACKS', 'default').split()]
    try:
        pack_name = get_option('PACKS_X_USUARIO', user)
        cd, cs, cm = [int(i) for i in get_option('PACKS', pack_name).split()]
    except:
        pass
    return (cd, cs, cm)

# Coloca al usuario en el grupo overquota en caso de que sobrepase
# el consumo diario, semanal o mensual
def update_overquota_user(user, data, dataw, datam):
    cd, cs, cm = get_user_quota(user)
    if not user in get_option('OTROS', 'EXCLUDED').split():
        if data >= cd or dataw >= cs or datam >= cm:
            update_user_in_group(user, 'ADD')
        else:
            update_user_in_group(user, 'DEL')


def enviar_correo(asunto, direcciones, fichero):
    fp = open(fichero, 'rb')
    msg = MIMEText(fp.read())
    me = 'proxy@gr.azcuba.cu'
    msg['Subject'] = asunto
    msg['From'] = me
    msg['To'] = ', '.join(direcciones.split())
    s = smtplib.SMTP('localhost')
    s.sendmail(me, direcciones.split(), msg.as_string())
    s.quit()
    fp.close()


# Volcar la salida de la base de datos hacia un archivo de texto
def generar_fichero_reporte(c):
    rf = open('reg_file', 'w')
    c.execute("SELECT max(length(user)) FROM inetcons")
    ulen = c.next()[0]
    TOTAL_WIDTH = 66 + ulen
    COL_WIDTH = 15
    rf.write("".ljust(TOTAL_WIDTH, '=') + '\n')
    rf.write("|" + "USUARIO".center(ulen) + "|" + "HOY".center(COL_WIDTH) + "|" + "ESTA SEMANA".center(
        COL_WIDTH) + "|" + "ESTE MES".center(COL_WIDTH) + "|" + "ESTADO".center(COL_WIDTH) + "|\n")
    rf.write("".ljust(TOTAL_WIDTH, '=') + '\n')
    for record in c.execute("SELECT * FROM inetcons ORDER BY datam DESC"):
        cd, cs, cm = get_user_quota(record[0])
        octext = "NORMAL"
        if record[3] > cm:
            octext = "OC MENSUAL"
        elif record[2] > cs:
            octext = "OC SEMANAL"
        elif record[1] > cd:
            octext = "OC DIARIO"
        rf.write("|" + record[0].ljust(ulen) + '|' + format_bytes(record[1]).rjust(COL_WIDTH) + '|' + format_bytes(
            record[2]).rjust(COL_WIDTH) + '|' + format_bytes(record[3]).rjust(COL_WIDTH) + "|" + octext.center(
            COL_WIDTH) + "|\n")
    rf.write("".ljust(TOTAL_WIDTH, '=') + "\n")
    rf.close()


#def registrar_consumo_feeds(user, data, dataw, datam, elapsed):
    #feed_line = user + ',' + str(data) + ',' + str(dataw) + ',' + str(datam) + ',' + str(elapsed) + '\n'
    #if not os.access(FEEDS_FILE, os.F_OK):
        #feeds_file = open(FEEDS_FILE, 'w')
        #feeds_file.write('user,data,dataw,datam,elapsed\n')
        #feeds_file.write(feed_line)
        #feeds_file.close()
    #else:
        #feeds_file = open(FEEDS_FILE, 'a')
        #feeds_file.write(feed_line)
        #feeds_file.close()

# Retorna en un diccionario los consumos correspondientes
# a cada usuario (el usuario es la clave), el valor es una
# tupla con los tres consumos
def obtener_consumos(c):
    consumos = dict()
    c.execute("SELECT user, data, dataw, datam FROM inetcons")
    for record in c.fetchall():
        consumos[record[0]] = (record[1], record[2], record[3],)
    return consumos


# Resetea los datos de consumo del usuario dado teniendo en cuenta
# si sobrepasa el 5% de cada consumo para incorporarlo al periodo
# siguiente.
def resetear_consumos(c):
    dia_mes_actual, dia_semana_actual, hora_actual, minuto_actual = obtener_hora_actual()
    if hora_actual == 8 and minuto_actual == 0:
        consumos = obtener_consumos(c)
        for user in consumos.keys():
            cd, cs, cm = get_user_quota(user)
            dat = (0, user)
            data = consumos[user][0]
            dataw = consumos[user][1]
            datam = consumos[user][2]
            if dia_mes_actual == 1:
                if datam / cm > float(get_option('OTROS', 'INDICE_SOBRECONSUMO')) and datam / cm < 2:
                    dat = (datam - cm, user)
                c.execute("UPDATE inetcons SET datam = ? WHERE user = ?", dat)
            if dia_semana_actual == 0:
                if dataw / cs > float(get_option('OTROS', 'INDICE_SOBRECONSUMO')) and dataw / cs < 2:
                    dat = (dataw - cs, user)
                c.execute("UPDATE inetcons SET dataw = ? WHERE user = ?", dat)
            if data / cd > float(get_option('OTROS', 'INDICE_SOBRECONSUMO')) and data / cd < 2:
                dat = (data - cd, user)
            c.execute("UPDATE inetcons SET data = ? WHERE user = ?", dat)


# Examina los logs parciales del squid y escanea los consumos registrados
# en ese momento para cada usuario. Necesita como parametro el cursor para
# las operaciones en la base de datos
def obtener_consumos_desde_logs(c):
    nav_nacional = re.compile('(https?://)?[a-zA-Z0-9.-_]*\.cu')
    ips_locales = re.compile('172\.26\.\d{1,3}\.\d{1,3}')
    for linea in open(LOGS):
        res = nav_nacional.search(linea)
        res1 = ips_locales.search(linea.split()[6])
        if not res and not res1 and linea.find('TCP_DENIED') == -1:
            campos = get_squidlog_fields(linea)
            if campos['user'] != '-':
                c.execute("SELECT * FROM inetcons WHERE user = '%s'" % campos['user'])
                try:
                    record = c.next()
                    rdata = record[1] + campos['data']
                    rdataw = record[2] + campos['data']
                    rdatam = record[3] + campos['data']
                    relapsed = record[4] + campos['elapsed']
                    update_overquota_user(campos['user'], rdata, rdataw, rdatam)
                    #registrar_consumo_feeds(campos['user'], rdata, rdataw, rdatam, relapsed)
                    cons = (rdata, rdataw, rdatam, relapsed, campos['user'])
                    c.execute("UPDATE inetcons SET data = ?, dataw = ?, datam = ?, elapsed = ? WHERE user = ?", cons)
                except StopIteration:
                    cons = (campos['user'], campos['data'], campos['data'], campos['data'], campos['elapsed'], "test")
                    c.execute("INSERT INTO inetcons VALUES (?, ?, ?, ?, ?, ?)", cons)


def enviar_reporte_correo():
    # Obtener los datos de la hora actual
    dia_mes_actual, dia_semana_actual, hora_actual, minuto_actual = obtener_hora_actual()

    # Enviar reporte por correo del resumen diario de consumo
    if hora_actual == 16 and minuto_actual == 57:
        enviar_correo('Reporte diario de overquota', 'disnelr@nauta.cu', 'reg_file')
        # Si es el ultimo dia del mes hacer una copia del registro
        dt = datetime.now()
        if (dt + timedelta(days=1)).day == 1:
            subprocess.call(['cp', 'reg_file', 'reg_file' + '_' + dt.isoformat().split('T')[0]])


conn, c = inicializar_bd()
resetear_consumos(c)

# Ejecutar el script en dependencia del valor de la variable AL_FULL
dia_mes_actual, dia_semana_actual, hora_actual, minuto_actual = obtener_hora_actual()
if get_option('OTROS', 'AL_FULL') and (dia_semana_actual > 4 or hora_actual < 8 or hora_actual >= 17):
    sys.exit(0)
else:
    obtener_consumos_desde_logs(c)
    generar_fichero_reporte(c)
    # enviar_reporte_correo()

conn.commit()
conn.close()
