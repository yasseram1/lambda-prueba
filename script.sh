#!/bin/bash
 
# Define las variables necesarias
PROCESS_SUCCESS_MESSAGE="Process sucessfull"
PATH_MYSQL="/usr/local/mariadb/columnstore/mysql/bin"
IpAddrRerportServer="172.19.242.108"
UserReportServer="nifi"
DatabaseReportServer="query_reports"
TableReportServer="query_events_exec"
 
START_DATE=$(date +"%Y-%m-%d %H:%M:%S")
DIVISION="RED"
AREA="ANALITICA Y EXPERIENCIA DE SERVICIOS"
CONTACT="DIEGO MORENO"
RESPONSIBLE="YASSER AVALOS"
HOSTNAME=$(hostname)
NAME="IFI_OPSITEL"
PATH_REPORT='/space/scripts/johan' # cambiar path '/space/scripts/reportes_fija_osiptel/'
FILE='script_ifi_opsitel'
FULLPATH_REPORT="${PATH_REPORT}/${FILE}"
 
semana_restar=$(( $1 * 7 ))
dateFormat=$(date +%Y-%m-%d)
fecha=$(date -d "$dateFormat - $semana_restar day" +%Y-%m-%d)
semana_actual=$(clickhouse-client -h 172.19.242.57 -u nifi --password=nifi --query \
"SELECT week(toDate('${fecha}'),1)")
semana_actual_sin_cero=${semana_actual#0}
semana_final=$((semana_actual_sin_cero))
GREEN_COLOUR='\x1B[32m'
BLUE_COLOUR='\e[34m'
RED_COLOUR='\e[31m'
END_MOD_COLOUR='\e[0m'
flagTrackingReport=1
flagSendReport=1
messageReport=""
argumento=1
argumento2=7
argumento3=3
ERROR_PROCESS_MESSAGE="Error en el proceso"
SUCCES_MESSAGE="Procesado correctamente"
fecha=$(date -d "$dateFormat - $semana_restar day" +%Y-%m-%d)
fecha_inicial=$(date -d "$fecha - $argumento2 day" +%Y-%m-%d)
fecha_final=$(date -d "$fecha - $argumento day" +%Y-%m-%d)
# fecha_file_sin_guiones=$(date -d "$dateFormat + $dias_fecha_file day" +%Y%m%d)
fecha_sin_guiones=$(date -d "$dateFormat + $argumento3 day"  +%Y%m%d)
 
# Bases de datos y tablas
db_ltetdd="ltetdd"
table_cdr_subscriber_1day="cdr_subscriber_1day"
table_opsitel_ifi_7day_test_v2="opsitel_ifi_7day_test_v2"
 
db_gsma="gsma"
table_equipment_gsma="equipment_gsma"
 
db_mlac="mlac"
table_vw_smartcare_cells2_rx="vw_smartcare_cells2_rx"
 
exportFilePath="/space/data/sftpserver/datawh/files/BASE_IFI/output/USRSIFITDD_${fecha_sin_guiones}.csv"
exportFilePathTemporal="/space/tmp/ifi_tdd_elastic/USRSIFITDD_${fecha_sin_guiones}.csv"
 
PROCESS_SUCCESS_MESSAGE="Process sucessfull"
INCOMPLETE_INPUT_MESSAGE="Incomplete data input"
INCOMPLETE_OUTPUT_MESSAGE="Incomplete data output"
ERROR_PROCESS_MESSAGE="Error during the execution process"
 
# Verifica si se proporcionó un argumento numérico
if ! [[ $semana_restar =~ ^[0-9]+$ ]]; then
  echo "Debes proporcionar un número como argumento."
  exit 1
fi
 
# Nuevo validateDataInput
validateDataInput() {
    echo -e "\n[$(date +"%Y-%m-%d %H:%M:%S")] validateDataInput(): exec[][]"
 
    local servidor="localhost"
    local databases=($db_ltetdd $db_ltetdd $db_gsma $db_mlac)
    local tables=($table_opsitel_ifi_7day_test_v2 $table_cdr_subscriber_1day $table_equipment_gsma $table_vw_smartcare_cells2_rx)
 
    for i in "${!databases[@]}"; do
        local database="${databases[$i]}"
        local table="${tables[$i]}"
        
        validateExistence $servidor $database $table
        validateData $servidor $database $table $i

        echo $i
    done
 
}

 
 
validateExistence(){
    local servidor=$1
    local database=$2
    local table=$3
 
    flagExistenceTable=$(clickhouse-client -h 172.19.242.57 -u nifi --password=nifi --query \
    "SELECT COUNT(1) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '${database}' AND TABLE_NAME = '${table}'")
 
    if [[ ${flagExistenceTable} -eq 0 ]]; then
        flagTrackingReport=0
        echo -e "\t${RED_COLOUR}La tabla ${database}.${table} no existe${END_MOD_COLOUR}"
        echo -e "\t${RED_COLOUR}${ERROR_PROCESS_MESSAGE}${END_MOD_COLOUR}"
        messageReport=${INCOMPLETE_INPUT_MESSAGE}
        registerReportEvent
        removeFiles
        exit 1
    fi
}
 
 
 
validateData() {
    local servidor=$1
    local database=$2;
    local table=$3;
   
    local quantityDestRows
    quantityDestRows=$(clickhouse-client -h 172.19.242.57 -u nifi --password=nifi --query \
      "SELECT COUNT(1) FROM ${database}.${table} where fecha between '${fecha_inicial}' AND '${fecha_final}'")
   
    if [[ ${quantityDestRows} -eq 0 ]]; then
      echo -e "\t${GREEN_COLOUR}La tabla ${database}.${table} no tiene datos entre el rango de fechas ${fecha_inicial} y ${fecha_final} ${END_MOD_COLOUR}"
      messageReport=${INCOMPLETE_INPUT_MESSAGE}
      registerReportEvent
      removeFiles
    elif [[ ${quantityDestRows} -gt 0 ]]; then
      echo -e "\t${RED_COLOUR}En la tabla ${database}.${table} si hay registros para la semana ${semana_final} ${END_MOD_COLOUR}"
      exit 1
    fi
}
 
 
 
InsertData_IFI() {
    echo -e "\n[$(date +"%Y-%m-%d %H:%M:%S")] InsertData_IFI(): exec[][]"
 
    clickhouse-client -h 172.19.242.57 -u nifi --password='nifi'\
     --query="insert into ${db_ltetdd}.${table_opsitel_ifi_7day_test_v2}
                select
                x.id,
                x.dia,
                year(toDate('${fecha}')),
                '${semana_final}' semana,
                fono,
                x.imsi,
                x.imei,        
                x.brand_name,
                x.marketing_name,
                CASE WHEN toString(fono) LIKE '51871%'
                      THEN 'ifi'
                      ELSE 'tdd'
                      END AS tipo_ifi_tdd
                from (select b.id, a.dia, a.anio,a.semana, a.msisdn fono , a.imsi, a.imei, c.brand_name, c.marketing_name
                      from (select msisdn, imsi, cellid, lac, semana, anio, imei, dia
                            from (select a.msisdn, a.imsi, a.cellid, a.lac, a.dia, week(a.dia,1) semana, year(a.dia) anio, a.imei,
                                  row_number() over (partition by a.msisdn order by a.uplink + a.downlink desc)
                                  ranking
                                  from ${db_ltetdd}.${table_cdr_subscriber_1day} a
                                  where a.dia BETWEEN '${fecha_inicial}' AND '${fecha_final}'
                            ) a where ranking = 1
                group by msisdn, imsi, cellid, lac, dia, semana, anio, imei) a
                left join ${db_gsma}.${table_equipment_gsma} c on c.tac = left(a.imei,8)
                left join ${db_mlac}.${table_vw_smartcare_cells2_rx} b on b.cellid = a.cellid and b.lac = a.lac
                group by a.dia, a.anio, a.semana, fono, a.imei, a.imsi, c.brand_name, c.marketing_name, b.id) x";
}
 
 
 
validateDataOutput(){
  echo -e "\n[$(date +"%Y-%m-%d %H:%M:%S")] validateDataOutput(): exec[][]"
 
  local database="${db_ltetdd}"
  local table="${table_opsitel_ifi_7day_test_v2}"
 
  for ((j=0; j<${#database[@]}; j++)); do
    local quantityDestRows
    quantityDestRows=$(clickhouse-client -h 172.19.242.57 -u nifi --password=nifi --query \
    "SELECT COUNT(1) FROM ${database[$j]}.${table[$j]} where fecha between '${fecha_inicial}' AND '${fecha_final}'")
 
    if [[ ${quantityDestRows} -eq 0 ]]; then
      echo -e "\t${RED_COLOUR} No se insertaron los datos en la tabla ${database[$j]}.${table[$j]}${END_MOD_COLOUR}"
      flagTrackingReport=0
      messageReport=${SUCCES_MESSAGE}
      registerReportEvent
    elif [[ ${quantityDestRows} -gt 0 ]]; then
      echo -e "\t${GREEN_COLOUR} Se insertaron los datos correctamente en la tabla ${database[$j]}.${table[$j]} para la semana ${semana_final}${END_MOD_COLOUR}"
      exit 1
    fi
  done
}
 
 
 
registerReportEvent() {
    echo -e "\n[$(date +"%Y-%m-%d %H:%M:%S")] registerReportEvent(): exec[][]"
 
    if [[ "${flagTrackingReport}" -eq 1 ]]; then
        messageReport=$PROCESS_SUCCESS_MESSAGE
    fi
 
    END_DATE=$(date +"%Y-%m-%d %H:%M:%S")
    DURATION=$(( ($(date --date "$END_DATE" +%s) - $(date --date "$START_DATE" +%s) ) ))
    ${PATH_MYSQL}/mysql -h ${IpAddrRerportServer} -u ${UserReportServer} --password='nifi' ${DatabaseReportServer} -e \
    "INSERT INTO ${TableReportServer} value (null, '$HOSTNAME', '$NAME', '$DIVISION', '$AREA', '$CONTACT',
    '$RESPONSIBLE', '$FULLPATH_REPORT', '$START_DATE', '$END_DATE', '$DURATION', '$flagTrackingReport',
    '$messageReport')"
 
}
 
validateDataInput
# InsertData_IFI
# validateDataOutput
# registerReportEvent