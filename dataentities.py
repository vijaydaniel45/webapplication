import json

from django.db import connections
from django.db import DatabaseError
from .connectobjectserversybase import ObjectServerAlarmStatus
from .connectopenshift import ClusterResourses
from .connectnfserverdiamonddb import DiabondDBServer

import datetime as dtm
from datetime import date
from datetime import timedelta
import time, sys
import copy
from collections import namedtuple
import re
import pandas as pd

from observertool import app
class DataEntities :

    @staticmethod
    def s2iData(forhours, startdate, enddate, datapart):
        if forhours == 0 or forhours == 99999:
            if startdate != '' and enddate != '':
                startdate += ' 00:00:00'
                enddate += ' 23:59:59'
                # enddate = time.mktime(datetime.datetime.strptime(enddate, "%Y-%m-%d %H:%M:%S").timetuple())
                # startdate = time.mktime(datetime.datetime.strptime(startdate, "%Y-%m-%d %H:%M:%S").timetuple())
                sqlpart_reporter_db = f" AND lastoccurrence BETWEEN '{startdate}' AND '{enddate}'"
            elif startdate != '' and enddate == '':
                sqlpart_reporter_db = f" AND DATE_FORMAT(lastoccurrence, '%Y-%m-%d') = '{startdate}'"
            else:  # default case
                sqlpart_reporter_db = f" AND lastoccurrence >= (NOW() - Interval 5 day)"
                description_daterange_str = f" Data for 5 days"
        else:
            sqlpart_reporter_db = f" AND lastoccurrence >= (NOW() - Interval {forhours} hour)"


        to_json = {'data': 'no records found'}
        recs_s2i = {}
        try:
            with connections['logsnapsreporter_non_others_db'].cursor() as cursor:
                if 'problem_elligible' in datapart:
                    sql = f"SELECT COUNT(*) AS CNT from reporter_status WHERE Domain = 'Wireline'  and Severity > 0 and Node not like '%SVS%' and NmosEntityId != 100 AND NFType is not NULL and NFType NOT LIKE '' {sqlpart_reporter_db};"
                    cursor.execute(sql)
                    desc = cursor.description
                    nt_result = namedtuple('Result', [col[0] for col in desc])
                    recs = [nt_result(*row) for row in cursor.fetchall()]

                    recs_s2i['problem_elligible'] = recs[0].CNT

                if 'problem_sent' in datapart:
                    sql = f"SELECT COUNT(*) AS CNT from reporter_status WHERE Domain = 'Wireline'  and Severity > 0 and Node not like '%SVS%' and NmosEntityId != 100 AND NFType is not NULL and NFType NOT LIKE '' and EventData like '%201%' {sqlpart_reporter_db};"
                    cursor.execute(sql)
                    desc = cursor.description
                    nt_result = namedtuple('Result', [col[0] for col in desc])
                    recs = [nt_result(*row) for row in cursor.fetchall()]

                    recs_s2i['problem_sent'] = recs[0].CNT

                if 'prob_total_conn_timeout_alarms' in datapart:
                    sql = f"SELECT COUNT(*) AS CNT from reporter_status WHERE EventData like '%408%' and Type = 1 and Domain = 'Wireline' and NFType != '' and NmosEntityId != 100 and Severity > 0 {sqlpart_reporter_db};"
                    cursor.execute(sql)
                    desc = cursor.description
                    nt_result = namedtuple('Result', [col[0] for col in desc])
                    recs = [nt_result(*row) for row in cursor.fetchall()]

                    recs_s2i['prob_total_conn_timeout_alarms'] = recs[0].CNT

                if 'prob_total_invalid_req_alarms' in datapart:
                    sql = f"SELECT COUNT(*) AS CNT from reporter_status WHERE EventData like '%504%' and Type = 1 and Domain = 'Wireline' and NFType != '' and NmosEntityId != 100 and Severity > 0 {sqlpart_reporter_db};"
                    cursor.execute(sql)
                    desc = cursor.description
                    nt_result = namedtuple('Result', [col[0] for col in desc])
                    recs = [nt_result(*row) for row in cursor.fetchall()]

                    recs_s2i['prob_total_invalid_req_alarms'] = recs[0].CNT

                if 'prob_total_bad_gateway_alarms' in datapart:
                    sql = f"SELECT COUNT(*) AS CNT from reporter_status WHERE EventData like '%400%' and Type = 1 and Domain = 'Wireline' and NFType != '' and NmosEntityId != 100 and Severity > 0 {sqlpart_reporter_db};"
                    cursor.execute(sql)
                    desc = cursor.description
                    nt_result = namedtuple('Result', [col[0] for col in desc])
                    recs = [nt_result(*row) for row in cursor.fetchall()]

                    recs_s2i['prob_total_bad_gateway_alarms'] = recs[0].CNT

                if 'clear_elligible' in datapart:
                    sql = f"SELECT COUNT(*) AS CNT from reporter_status WHERE Type = 1 and Severity = 0 and Domain = 'Wireline' and NFType != '' and NmosEntityId != 100 {sqlpart_reporter_db};"
                    cursor.execute(sql)
                    desc = cursor.description
                    nt_result = namedtuple('Result', [col[0] for col in desc])
                    recs = [nt_result(*row) for row in cursor.fetchall()]

                    recs_s2i['clear_elligible'] = recs[0].CNT

                if 'clear_sent' in datapart:
                    sql = f"SELECT COUNT(*) AS CNT from reporter_status WHERE Type = 1 and Severity = 0 and Domain = 'Wireline' and NFType != '' and NmosEntityId != 100 and EventData like '%200%' {sqlpart_reporter_db};"
                    cursor.execute(sql)
                    desc = cursor.description
                    nt_result = namedtuple('Result', [col[0] for col in desc])
                    recs = [nt_result(*row) for row in cursor.fetchall()]

                    recs_s2i['clear_sent'] = recs[0].CNT

                if 'clear_total_conn_timeout_alarms' in datapart:
                    sql = f"SELECT COUNT(*) AS CNT from reporter_status WHERE EventData like '%408%' and Type = 1 and Domain = 'Wireline' and NFType != '' and NmosEntityId != 100 and Severity = 0 {sqlpart_reporter_db};"
                    cursor.execute(sql)
                    desc = cursor.description
                    nt_result = namedtuple('Result', [col[0] for col in desc])
                    recs = [nt_result(*row) for row in cursor.fetchall()]

                    recs_s2i['clear_total_conn_timeout_alarms'] = recs[0].CNT

                if 'clear_total_invalid_req_alarms' in datapart:
                    sql = f"SELECT COUNT(*) AS CNT from reporter_status WHERE EventData like '%504%' and Type = 1 and Domain = 'Wireline' and NFType != '' and NmosEntityId != 100 and Severity = 0 {sqlpart_reporter_db};"
                    cursor.execute(sql)
                    desc = cursor.description
                    nt_result = namedtuple('Result', [col[0] for col in desc])
                    recs = [nt_result(*row) for row in cursor.fetchall()]

                    recs_s2i['clear_total_invalid_req_alarms'] = recs[0].CNT

                if 'clear_total_bad_gateway_alarms' in datapart:
                    sql = f"SELECT COUNT(*) AS CNT from reporter_status WHERE EventData like '%400%' and Type = 1 and Domain = 'Wireline' and NFType != '' and NmosEntityId != 100 and Severity = 0 {sqlpart_reporter_db};"
                    cursor.execute(sql)
                    desc = cursor.description
                    nt_result = namedtuple('Result', [col[0] for col in desc])
                    recs = [nt_result(*row) for row in cursor.fetchall()]

                    recs_s2i['clear_total_bad_gateway_alarms'] = recs[0].CNT

        except DatabaseError as e:
            print(e)

        if 's2i_health' in datapart:
            lastmins = 60
            lastsecs = lastmins * 60
            query = f"((Summary LIKE 'S2i_Enrichment_Service Stopped' OR Summary LIKE 'Filter_Rule Stopped' OR Summary LIKE 'Filter_Rule_Disable Stopped' OR Summary LIKE 'Threshold_Rule Stopped' OR Summary LIKE 'Threshold_Rule_Clear Stopped' OR Summary LIKE 'Wireline_Static_Topology_Enrichment Stopped') AND Severity > 0 AND LastOccurrence >= getdate()-{lastsecs})"
            collist = ['Summary', 'Severity', 'LastOccurrence']
            cr = ObjectServerAlarmStatus(None)
            recs_s2i['s2i_health'] = cr.impactServicesHealthCheck(query, collist, 'S2i')
        return recs_s2i

    @staticmethod
    def qwiltData(forhours, startdate, enddate, datapart):
        if forhours == 0 or forhours == 99999:
            if startdate != '' and enddate != '':
                startdate += ' 00:00:00'
                enddate += ' 23:59:59'
                # enddate = time.mktime(datetime.datetime.strptime(enddate, "%Y-%m-%d %H:%M:%S").timetuple())
                # startdate = time.mktime(datetime.datetime.strptime(startdate, "%Y-%m-%d %H:%M:%S").timetuple())
                sqlpart_reporter_db = f" AND lastoccurrence BETWEEN '{startdate}' AND '{enddate}'"
            elif startdate != '' and enddate == '':
                sqlpart_reporter_db = f" AND DATE_FORMAT(lastoccurrence, '%Y-%m-%d') = '{startdate}'"
            else:  # default case
                sqlpart_reporter_db = f" AND lastoccurrence >= (NOW() - Interval 5 day)"
                description_daterange_str = f" Data for 5 days"
        else:
            sqlpart_reporter_db = f" AND lastoccurrence >= (NOW() - Interval {forhours} hour)"

        recs_qwilt = {}
        try:
            with connections['logsnapsreporter_non_others_db'].cursor() as cursor:
                if 'problem_elligible' in datapart:
                    sql = f"SELECT COUNT(*) AS CNT from reporter_status WHERE Domain !='Others' and EventData='' and Vendor='Qwilt' and Type=1 and Severity > 0 and NFName !='' and Node like '%L-QW-CACHE-00-QN%' {sqlpart_reporter_db};"
                    cursor.execute(sql)
                    desc = cursor.description
                    nt_result = namedtuple('Result', [col[0] for col in desc])
                    recs = [nt_result(*row) for row in cursor.fetchall()]

                    recs_qwilt['problem_elligible'] = recs[0].CNT

                if 'problem_sent' in datapart:
                    sql = f"SELECT COUNT(*) AS CNT from reporter_status WHERE Domain !='Others' and EventData='' and Grade = 100 and Vendor='Qwilt' and Type=1 and Severity > 0 and NFName !='' and Node like '%L-QW-CACHE-00-QN%' {sqlpart_reporter_db};"
                    cursor.execute(sql)
                    desc = cursor.description
                    nt_result = namedtuple('Result', [col[0] for col in desc])
                    recs = [nt_result(*row) for row in cursor.fetchall()]

                    recs_qwilt['problem_sent'] = recs[0].CNT

                if 'clear_elligible' in datapart:
                    sql = f"SELECT COUNT(*) AS CNT from reporter_status WHERE Domain !='Others' and Vendor='Qwilt' and Type=1 and Severity=0 and Node like '%L-QW-CACHE-00-QN%' and EventData = '' {sqlpart_reporter_db};"
                    cursor.execute(sql)
                    desc = cursor.description
                    nt_result = namedtuple('Result', [col[0] for col in desc])
                    recs = [nt_result(*row) for row in cursor.fetchall()]

                    recs_qwilt['clear_elligible'] = recs[0].CNT

                if 'clear_sent' in datapart:
                    sql = f"SELECT COUNT(*) AS CNT from reporter_status WHERE Domain !='Others' and Vendor='Qwilt' and Type=1 and Severity=0 and Grade = 120 and Node like '%L-QW-CACHE-00-QN%' and EventData = '' {sqlpart_reporter_db};"
                    cursor.execute(sql)
                    desc = cursor.description
                    nt_result = namedtuple('Result', [col[0] for col in desc])
                    recs = [nt_result(*row) for row in cursor.fetchall()]

                    recs_qwilt['clear_sent'] = recs[0].CNT
        except DatabaseError as e:
            print(e)

        if 'qwilt_health' in datapart:
            lastmins = 60
            lastsecs = lastmins * 60
            query = f"((Summary LIKE 'Qwilt_Service_Problem Stopped' OR Summary LIKE 'Qwilt_Service_Clear Stopped') AND LastOccurrence >= getdate()-{lastsecs})"
            collist = ['Summary', 'Severity', 'LastOccurrence']
            cr = ObjectServerAlarmStatus(None)
            recs_qwilt['qwilt_health'] = cr.impactServicesHealthCheck(query, collist, 'Qwilt')
        return recs_qwilt

    @staticmethod
    def FMPMAlarmCountsData(fordays, startdate, enddate, datatable):

        recs_nf_type_alarms_nonothers = []
        recs_nf_type_alarms_others = []
        if fordays == 0:
            if startdate != '' and enddate != '':
                startdate += ' 00:00:00'
                enddate += ' 23:59:59'
                sqlpart_reporter_db = f" reportdate BETWEEN '{startdate}' AND '{enddate}'"
                description_daterange_str = f" Data between {startdate} AND {enddate}"
            else:  # default case
                sqlpart_reporter_db = f" reportdate >= (NOW() - Interval 1 day)"
                description_daterange_str = f" Data for 1 day"
        else:
            sqlpart_reporter_db = f" reportdate >= (NOW() - Interval {fordays} day)"
            description_daterange_str = f" Data for {fordays} days"

        recs_tncp_nftype_availability_status = {}
        try:
            with connections['default'].cursor() as cursor:
                #sql = f"SELECT nftype, DATE_FORMAT(reportdate,'%Y-%m-%d') reportdate, case when SUM(available)>0 then 1 else 0 end available FROM tncp_nftype_pm_reporting WHERE {sqlpart_reporter_db} GROUP BY DATE_FORMAT(reportdate,'%Y-%m-%d'), nftype;"
                sql = f"SELECT nftype, DATE_FORMAT(MAX(reportdate),'%Y-%m-%d') reportdate, available FROM tncp_nftype_pm_reporting WHERE available=1 GROUP BY nftype;"
                try:
                    cursor.execute(sql)
                    desc = cursor.description
                    fields = [col[0] for col in desc]
                    results = cursor.fetchall()
                    for row in results:
                        recs_tncp_nftype_availability_status[row[0] + row[1]] = ['-', row[0], row[1], 0, 0, 0, 0, 0,
                                                                                 0, 0, row[2]]

                except Exception as e:
                    print(e)
                    recs_tncp_nftype_availability_status = {
                        'error': "Some error happened while retrieving data. Please try after sometime."}

            my_nftype_dict = dict()
            with connections['logsnapsverizonedb'].cursor() as cursor:
                # sql = f"SELECT DISTINCT(LOWER(name)) nfname, NFType nftype FROM ASMData WHERE NFType IS NOT NULL and entitytype IN ('vnf','wirelineVNF');"
                sql = f"SELECT DISTINCT(LOWER(NFType)) nftype FROM AlarmSAPDetails WHERE NFType IS NOT NULL;"
                try:
                    cursor.execute(sql)
                    results = cursor.fetchall()
                    for row in results:
                        my_nftype_dict[row[0]] = ['-', row[0], 'N/A', 0, 0, 0, 0, 0, 0, 0, 0]
                except Exception as e:
                    print(e)
        except DatabaseError as e:
            print('Error from dataentities.FMPMAlarmCountsData------>default, verizon',e)
        # old logic fetching from app.NF_TYPES_LIST
        # for index, value in enumerate(app.NF_TYPES_LIST):
        #     my_nftype_dict[value.lower()] = [value, 0, 0, 0, 0, 0, 0, 0, 0, '']
        working_nftypes_list = copy.deepcopy(my_nftype_dict)
        try:
            with connections['logsnapsreporter_non_others_db'].cursor() as cursor:
                # sql = f"SELECT nl.nftype, tab.CRITICAL,tab.MAJOR,tab.MINOR,tab.WARNING,tab.INDETERMINATE,tab.CLEAR,tab.TOTAL, IF(tab.NFTYPE IS NULL,0,1) AVILABLE FROM nftypes_list nl LEFT JOIN (SELECT LOWER(NFTYPE) NFTYPE, SUM(CRITICAL) CRITICAL, SUM(MAJOR) MAJOR, SUM(MINOR) MINOR, SUM(WARNING) WARNING, SUM(INDETERMINATE) INDETERMINATE, SUM(CLEAR) CLEAR, (SUM(CRITICAL) + SUM(MAJOR) + SUM(MINOR) + SUM(WARNING) + SUM(INDETERMINATE) + SUM(CLEAR)) TOTAL FROM nftype_alerts_counts_report WHERE reportdate > Now() - INTERVAL 30 DAY GROUP BY `DOMAIN`,LOWER(NFTYPE) ) tab ON LOWER(tab.NFTYPE)=LOWER(nl.nftype);"
                # sql = f"SELECT LOWER(NFTYPE) NFTYPE, SUM(CRITICAL) CRITICAL, SUM(MAJOR) MAJOR, SUM(MINOR) MINOR, SUM(WARNING) WARNING, SUM(INDETERMINATE) INDETERMINATE, SUM(CLEAR) CLEAR, (SUM(CRITICAL) + SUM(MAJOR) + SUM(MINOR) + SUM(WARNING) + SUM(INDETERMINATE) + SUM(CLEAR)) TOTAL, 1 AVAILABLE FROM nftype_alerts_counts_report  WHERE {sqlpart_reporter_db} GROUP BY `DOMAIN`,LOWER(NFTYPE);"
                sql = f"SELECT `DOMAIN`, LOWER(NFTYPE) NFTYPE, DATE_FORMAT(reportdate,'%Y-%m-%d') reportdate, SUM(CRITICAL) CRITICAL, SUM(MAJOR) MAJOR, SUM(MINOR) MINOR, SUM(WARNING) WARNING, SUM(INDETERMINATE) INDETERMINATE, SUM(CLEAR) CLEAR, (SUM(CRITICAL) + SUM(MAJOR) + SUM(MINOR) + SUM(WARNING) + SUM(INDETERMINATE) + SUM(CLEAR)) TOTAL, 0 PM_AVAILABLE FROM nftype_alerts_counts_report  WHERE {sqlpart_reporter_db} GROUP BY DATE_FORMAT(reportdate,'%Y-%m-%d'), `DOMAIN`,LOWER(NFTYPE);"
                print('from dataentities.FMPMAlarmCountsData------>',sql)
                try:
                    cursor.execute(sql)
                    desc = cursor.description
                    fields = [col[0] for col in desc]
                    results = cursor.fetchall()
                    if datatable == 'yes':
                        recs_nf_type_alarms_nonothers = {'data': [], 'columns': []}
                        recs_nf_type_alarms_nonothers['columns'] = fields
                        for row in results:
                            if recs_tncp_nftype_availability_status.get(row[1].lower() + row[2], '') != '':
                                row[10] = recs_tncp_nftype_availability_status[row[1].lower() + row[2]][
                                    10]  # get the value of the PM availability
                                del recs_tncp_nftype_availability_status[
                                    row[1].lower() + row[2]]  # remove the utilized nftype from the PM data
                            recs_nf_type_alarms_nonothers['data'].append(row)
                            if working_nftypes_list.get(row[1], '') != '':
                                del working_nftypes_list[row[1]]
                        for pm_result in recs_tncp_nftype_availability_status.values():
                            recs_nf_type_alarms_nonothers['data'].append(pm_result)
                            if working_nftypes_list.get(pm_result[1], '') != '':
                                del working_nftypes_list[pm_result[1]]
                        for row in working_nftypes_list.values():
                            recs_nf_type_alarms_nonothers['data'].append(row)
                    else:
                        for row in results:
                            if recs_tncp_nftype_availability_status.get(row[1].lower() + row[2], '') != '':
                                row[10] = recs_tncp_nftype_availability_status[row[1].lower() + row[2]]
                                del recs_tncp_nftype_availability_status[row[1].lower() + row[2]]
                            recs_nf_type_alarms_nonothers.append(dict(zip(fields, row)))
                            if working_nftypes_list.get(row[1], '') != '':
                                del working_nftypes_list[row[1]]
                        for pm_result in recs_tncp_nftype_availability_status.values():
                            recs_nf_type_alarms_nonothers.append(dict(zip(fields, pm_result)))
                            if working_nftypes_list.get(pm_result[1], '') != '':
                                del working_nftypes_list[pm_result[1]]
                        for row in working_nftypes_list.values():
                            recs_nf_type_alarms_nonothers.append(dict(zip(fields, row)))
                except Exception as e:
                    print(e)
                    recs_nf_type_alarms_nonothers = {
                        'error': "Some error happened while retrieving data. Please try after sometime."}
        except DatabaseError as e:
            print('Error from dataentities.FMPMAlarmCountsData------>nonothers',e)

        try:
            with connections['logsnapsreporter_others_db'].cursor() as cursor:
                # sql = f"SELECT LOWER(NFTYPE) NFTYPE, SUM(CRITICAL) CRITICAL, SUM(MAJOR) MAJOR, SUM(MINOR) MINOR, SUM(WARNING) WARNING, SUM(INDETERMINATE) INDETERMINATE, SUM(CLEAR) CLEAR, (SUM(CRITICAL) + SUM(MAJOR) + SUM(MINOR) + SUM(WARNING) + SUM(INDETERMINATE) + SUM(CLEAR)) TOTAL, 1 AVAILABLE FROM nftype_alerts_counts_report  WHERE {sqlpart_reporter_db} GROUP BY `DOMAIN`,LOWER(NFTYPE);"
                sql = f"SELECT `DOMAIN`, LOWER(NFTYPE) NFTYPE, DATE_FORMAT(reportdate,'%Y-%m-%d') reportdate, SUM(CRITICAL) CRITICAL, SUM(MAJOR) MAJOR, SUM(MINOR) MINOR, SUM(WARNING) WARNING, SUM(INDETERMINATE) INDETERMINATE, SUM(CLEAR) CLEAR, (SUM(CRITICAL) + SUM(MAJOR) + SUM(MINOR) + SUM(WARNING) + SUM(INDETERMINATE) + SUM(CLEAR)) TOTAL FROM nftype_alerts_counts_report  WHERE {sqlpart_reporter_db} GROUP BY DATE_FORMAT(reportdate,'%Y-%m-%d'), `DOMAIN`,LOWER(NFTYPE);"
                try:
                    cursor.execute(sql)
                    desc = cursor.description
                    fields = [col[0] for col in desc]
                    results = cursor.fetchall()
                    if datatable == 'yes':
                        recs_nf_type_alarms_others = {'data': [], 'columns': []}
                        recs_nf_type_alarms_others['columns'] = fields
                        for row in results:
                            recs_nf_type_alarms_others['data'].append(row)
                    else:
                        for row in results:
                            recs_nf_type_alarms_others.append(dict(zip(fields, row)))

                except Exception as e:
                    print(e)
                    recs_nf_type_alarms_others = {
                        'error': "Some error happened while retrieving data. Please try after sometime."}
        except DatabaseError as e:
            print('Error from dataentities.FMPMAlarmCountsData------>others',e)

        return recs_nf_type_alarms_nonothers, recs_nf_type_alarms_others

    @staticmethod
    def FMPMAlarmCountsMailReportData(fordays, startdate, enddate, dataformat):

        recs_nf_type_alarms_nonothers = []
        # recs_nf_type_alarms_others = []
        if fordays == 0:
            if startdate != '' and enddate != '':
                startdate += ' 00:00:00'
                enddate += ' 23:59:59'
                sqlpart_reporter_db = f" reportdate BETWEEN '{startdate}' AND '{enddate}'"
            else:  # default case
                sqlpart_reporter_db = f" reportdate >= (NOW() - Interval 1 day)"
        else:
            sqlpart_reporter_db = f" reportdate >= (NOW() - Interval {fordays} day)"

        recs_tncp_nftype_availability_status = {}
        recs_platform_kpi_availability_status = {}
        PM_NFTYPES = []
        try:
            with connections['default'].cursor() as cursor:

                sql = f"SELECT UPPER(TRIM(nftype)), table_name, multiple, test_pattern_tags_resource FROM tncp_nftype_tables_association;"
                try:
                    cursor.execute(sql)
                    results = cursor.fetchall()
                    for row in results:
                        if row[0].find('PLATFORM_TYPE_KPI') == -1:  # Platform KPI name in disguise of NFType name
                            PM_NFTYPES.append(row[0])
                except Exception as e:
                    print(e)
                #retrieving PM report data
                # sql = f"SELECT nftype, DATE_FORMAT(MAX(reportdate),'%Y-%m-%d') reportdate FROM tncp_nftype_pm_reporting WHERE available = 1 AND {sqlpart_reporter_db} GROUP BY nftype HAVING SUM(available) > 0;"
                sql = f"SELECT UPPER(TRIM(nftype)), DATE_FORMAT(MAX(reportdate),'%Y-%m-%d') reportdate, is_platform_kpi FROM tncp_nftype_pm_reporting WHERE available = 1  GROUP BY nftype, is_platform_kpi HAVING SUM(available) > 0;"
                print('retrieving PM report data dataentities.FMPMAlarmCountsMailReportData------>',sql)
                try:
                    cursor.execute(sql)
                    desc = cursor.description
                    fields = [col[0] for col in desc]
                    results = cursor.fetchall()
                    for row in results:
                        if row[2] == 0: #checking whether Platform KPI (is_platform_kpi=1) or NFtype (is_platform_kpi=0)
                            recs_tncp_nftype_availability_status[row[0]] = row[1]
                        else:
                            recs_platform_kpi_availability_status[row[0]] = row[1] # this record belongs to Platform KPI Status report

                except Exception as e:
                    print(e)
                    recs_tncp_nftype_availability_status = {
                        'error': "Some error happened while retrieving data. Please try after sometime."}
                # print('PM data coming up----->',recs_tncp_nftype_availability_status)
        except DatabaseError as e:
            print('Error from dataentities.FMPMAlarmCountsMailReportData------>default, verizon',e)
        fm_pm_report_data = dict()
        platform_report_data = dict()
        recs_nf_type_alarms_nonothers = {}
        platform_vcptypes_alarms_nonothers = {}
        recs_fm_not_applicable_exclusionlist = []
        nftype_instance_counts = __class__.prepareNFCounts()
        # nftype_instance_counts = {'VNF TYPE': 1974, 'DMS': 33, 'ACPF': 956, 'VMDRASIGNALING': 2, 'AUPF': 944, 'VACS': 5, 'SVNFM': 16, 'VWIFI': 6, 'USM': 74, 'VCOMMON': 18, 'VDB': 14, 'VME': 5, 'FIS': 18, 'VZW-E-QWILT-M': 15, 'UPF': 5, 'RADSEC_EMS': 3, 'SEC_VNFM': 12, 'VOMA': 22, 'VASP': 7, 'VHOMEDEVICE': 6, 'TAS': 34, 'RADSECAPP': 3, 'VNFM': 1, 'IMS': 17, 'VZW-C-VSBLMRF-M': 5, 'VNOTIFICATION': 1, 'VZW-C-VSBLPCSCF-M': 2, 'VZW-C-VSBLSCEAS-M': 3, 'VZW-C-LB-L': 5, 'VDEVICE': 8, 'VZW-C-VSBLMCO-M': 2, 'VZW-C-VSBLPCSCF2-M': 1, 'VZW-C-VSBLMMSC-M': 2, 'VZW-C-VSBLPCSCF1-M': 0, 'VZW-C-VSBLPGW-M': 1, 'RADSECEMS': 6, 'VZW-C-VSBLTAS-M': 3, 'SIGNALING': 2, 'VPROFILEMGMT': 7, 'VZW-C-VSBLPSBC-M': 5, 'VZW-C-CIS-L': 1, 'VZW-C-VSBLIMC-M': 1, 'VZW-C-FW-L': 3, 'VZW-C-VSBLHSSFE-M': 0, 'CMS': 39, 'FW': 1, 'NETNUMBERTFNDB_EDGE': 2, 'NETNUMBERTFNDB_MSTR': 2, 'HAC': 4, 'VZW-E-WXGW-S10': 43, 'VZW-E-VCU-M': 2, 'VZW-E-AGGDRA-M': 19, 'VZW-E-WXGW-10SF': 8, 'VZW-E-WXGW-M': 4, 'VZW-E-EXGW-M': 3, 'VZW-E-PXGW-S10': 4, 'VZW-E-PXGW-M': 5, 'VZW-E-EXGW-S10': 30, 'VZW-E-PXGW-MED': 1, 'VZW-C-MPNP-M': 1, 'CMM': 74, 'EPDG': 12, 'VZW-E-VLESS-S': 7, 'SAEGW': 1, 'VMME': 1, 'VZW-E-ESC-S': 2, 'EMME': 0, 'DMC': 11, 'DRNOAM': 6, 'VISIBLE WSG': 0, 'VZW-C-PCSCF-M': 0, 'VMDRANOAM': 5, 'VZW-E-GVNFM-M': 1, 'ESAEGW': 0, 'VMDRADRNOAM': 0, 'NOAM': 1, 'ADPF': 33219, 'VZW-E-CORNINGIBVCU-S': 318, 'CUCP-NR': 39, 'CUUP-NR': 31, 'RAN-NR': 17}
        try:
            with connections['logsnapsreporter_non_others_db'].cursor() as cursor:
                # retrieving FM report data
                # sql = f"SELECT UPPER(NFTYPE) NFTYPE, DATE_FORMAT(MAX(reportdate),'%Y-%m-%d') reportdate FROM nftype_alerts_counts_report  WHERE NFTYPE<>'' AND {sqlpart_reporter_db} GROUP BY UPPER(NFTYPE) HAVING (SUM(CRITICAL) + SUM(MAJOR) + SUM(MINOR) + SUM(WARNING) + SUM(INDETERMINATE) + SUM(CLEAR)) > 0;"
                sql = f"SELECT UPPER(TRIM(NFTYPE)) NFTYPE, DATE_FORMAT(MAX(reportdate),'%Y-%m-%d') reportdate FROM nftype_alerts_counts_report  WHERE TRIM(NFTYPE)<>'' AND (available=1 OR (CRITICAL+MAJOR+MINOR+WARNING+INDETERMINATE+CLEAR)>0) GROUP BY UPPER(TRIM(NFTYPE)) ;"
                sql2 = f"SELECT nftype FROM fm_not_applicable_exclusion_list  WHERE active=1;"
                print('retrieving FM report data dataentities.FMPMAlarmCountsMailReportData------>',sql)
                try:
                    cursor.execute(sql)


                    desc = cursor.description
                    fields = [col[0] for col in desc]
                    results = cursor.fetchall()
                    for row in results:
                        recs_nf_type_alarms_nonothers[row[0]] = row[1]

                    cursor.execute(sql2)
                    results = cursor.fetchall()
                    for row in results:
                        recs_fm_not_applicable_exclusionlist.append(row[0])

                except Exception as e:
                    print(e)
                    recs_nf_type_alarms_nonothers = {
                        'error': "Some error happened while retrieving data. Please try after sometime."}

                # print('FM data coming up----->', recs_nf_type_alarms_nonothers)
                #retrieving Platform report data
                # sql = f"SELECT UPPER(VCPType) VCPType, DATE_FORMAT(MAX(reportdate),'%Y-%m-%d') reportdate FROM platform_alerts_counts_report  WHERE VCPType IN ('VCP Edge', 'VCP FarEdge', 'VCP Core', 'VCP Edge WS') AND {sqlpart_reporter_db} GROUP BY UPPER(VCPType) HAVING SUM(ALARMS)  > 0;"
                sql = f"SELECT UPPER(VCPType) VCPType, DATE_FORMAT(MAX(reportdate),'%Y-%m-%d') reportdate FROM platform_alerts_counts_report  WHERE (VCPType IN ('VCP Far Edge', 'VCP Core', 'VCP Edge WS', 'VCP Core WS') OR (VCPType LIKE 'VCP Edge%' AND VCPType <> ' VCP Edge WS') ) GROUP BY UPPER(VCPType) HAVING SUM(ALARMS)  > 0;"
                print('retrieving Platform report data dataentities.FMPMAlarmCountsMailReportData------> line 260', sql)
                try:
                    cursor.execute(sql)
                    desc = cursor.description
                    fields = [col[0] for col in desc]
                    results = cursor.fetchall()
                    for row in results:
                        platform_vcptypes_alarms_nonothers[row[0]] = row[1]

                except Exception as e:
                    print(e)
                    platform_vcptypes_alarms_nonothers = {
                        'error': "Some error happened while retrieving data. Please try after sometime."}
            yesterday = date.today() - timedelta(days=1)
            yesterday = yesterday.strftime("%Y-%m-%d")
            today = date.today().strftime("%Y-%m-%d")
            fm_source = {}

            with connections['logsnapsverizonedb'].cursor() as cursorvz:
                # sql = f"SELECT DISTINCT(LOWER(name)) nfname, NFType nftype FROM ASMData WHERE NFType IS NOT NULL and entitytype IN ('vnf','wirelineVNF');"
                sql = f"SELECT DISTINCT(UPPER(TRIM(NFType))) nftype, SapActiveCheck FROM AlarmSAPDetails WHERE NFType IS NOT NULL and TRIM(NFType) <> '' AND SapActiveCheck=1 AND UPPER(TRIM(NFType)) NOT IN ('EDGEHOST','KUBERNETESNODE','FAREDGEHOST') ORDER BY NFType;"
                sql_fm_src = f"SELECT UPPER(TRIM(NFType)) nftype,source FROM fm_alarm_source;"
                try:
                    print('retrieving Platform report data dataentities.FMPMAlarmCountsMailReportData line 280------>',sql)
                    cursorvz.execute(sql)
                    results = cursorvz.fetchall()

                    cursorvz.execute(sql_fm_src)
                    results_fm_src = cursorvz.fetchall()
                    for row in results_fm_src:
                        fm_source[row[0]] = row[1]

                    if dataformat == 'datatable':
                        pass
                    elif dataformat == 'htmltable':

                        # fm_pm_report_data = '<table border="1" bordercolor="#CC0000"><tr><th>NF Type</th><th>Alarm Last Received Date</th><th>KPI Last Received Date</th><th>Instance Count</th><th>Source</th></tr>'
                        fm_pm_report_data = '<table border="1" bordercolor="#CC0000"><tr><th>NF Type</th><th>Alarm Last Received Date</th><th>KPI Last Received Date</th><th>Instance Count</th></tr>'
                        markedred = 'style="color:#FF0000"'
                        # print('all nftypes------>',results)
                        for row in results:
                            nftype_coldata = row[0].replace('\r', '')
                            SapActiveCheck = row[1]
                            pm_val = f'<td >Not Receiving</td>' if nftype_coldata in PM_NFTYPES else '<td >Not Applicable</td>'
                            # fm_val = f'<td >Not Applicable</td>' if nftype_coldata in app.FM_NOT_COLLECTING else '<td >Not Receiving</td>'
                            fm_val = f'<td >Not Applicable</td>' if nftype_coldata in recs_fm_not_applicable_exclusionlist else '<td >Not Receiving</td>'

                            if recs_tncp_nftype_availability_status.get(nftype_coldata, '') != '':
                                pm_val = f'<td {markedred}>' + recs_tncp_nftype_availability_status.get(nftype_coldata, '') + '</td>' if (recs_tncp_nftype_availability_status.get(nftype_coldata, '') != yesterday and recs_tncp_nftype_availability_status.get(nftype_coldata, '') != today) else f'<td>' + recs_tncp_nftype_availability_status.get(nftype_coldata, '') + '</td>'
                            if recs_nf_type_alarms_nonothers.get(nftype_coldata, '') != '':
                                fm_val = f'<td {markedred}>' + recs_nf_type_alarms_nonothers.get(nftype_coldata, '') + '</td>' if (recs_nf_type_alarms_nonothers.get(nftype_coldata, '') != yesterday and recs_nf_type_alarms_nonothers.get(nftype_coldata, '') != today) else f'<td>' + recs_nf_type_alarms_nonothers.get(nftype_coldata, '') + '</td>'

                            nftype_instance_count_val = f'<td>{nftype_instance_counts[nftype_coldata]}</td>' if nftype_instance_counts.get(nftype_coldata, '') != '' else f'<td>-</td>'
                            # fm_src_val = '<td>' + fm_source.get(nftype_coldata) + '</td>' if fm_source.get(nftype_coldata, '') != '' else f'<td>-</td>'
                            # print('@@@@@@@@@@@looking for issue here#############>',nftype_instance_count_val,fm_src_val)
                            fm_pm_report_data += '<tr>'
                            fm_pm_report_data += ''.join(['<td>' + nftype_coldata + '</td>', fm_val, pm_val , nftype_instance_count_val])
                            fm_pm_report_data += '</tr>\n'
                        fm_pm_report_data += '</table>'

                    else:
                        fm_pm_report_data =[]
                        fm_pm_report_data.append(['NF Type', 'Alarm Last Received Date', 'KPI Last Received Date'])
                        for row in results:
                            nftype_coldata = row[0].replace('\r', '')
                            pm_val = 'Not Receiving' if recs_tncp_nftype_availability_status.get(nftype_coldata, '') == '' else recs_tncp_nftype_availability_status.get(nftype_coldata, '')
                            fm_val = 'Not Applicable' if recs_nf_type_alarms_nonothers.get(nftype_coldata, '') == '' else recs_nf_type_alarms_nonothers.get(nftype_coldata, '')
                            fm_pm_report_data.append([nftype_coldata, fm_val, pm_val])
                except Exception as e:
                    print(e)
        except DatabaseError as e:
            print('Error from dataentities.FMPMAlarmCountsData------>nonothers',e)
        Platform_VCP_Types = ['VCP Edge', 'VCP Far Edge', 'VCP Core', 'VCP Edge WS', 'VCP Core WS']

        res1 = [ __class__.format_platform_data( Platform_VCP_Type.upper(), dataformat, platform_vcptypes_alarms_nonothers, recs_platform_kpi_availability_status, yesterday, today) for Platform_VCP_Type in Platform_VCP_Types]
        # res2 = [__class__.format_platform_data( Platform_KPI, dataformat, recs_platform_kpi_availability_status, yesterday, today) for Platform_KPI in Platform_KPIs]
        if dataformat == 'datatable':
            pass
        elif dataformat == 'htmltable':
            platform_report_data = '<table border="1" bordercolor="#CC0000"><tr><th>Platform Name</th><th>Alarm Last Received Date</th><th>KPI Last Received Date</th></tr>'
            platform_report_data += "\n".join(res1)
            platform_report_data += '</table>'
            # platform_report_data += '<table border="1" bordercolor="#CC0000"><tr><th>Platform KPI Name</th><th>KPI Last Received Date</th></tr>'
            # platform_report_data += "\n".join(res2)
            # platform_report_data += '</table>'
        else:
            pass
        return fm_pm_report_data, platform_report_data

    @staticmethod
    def get_TNCP_Timeseries_API_delay_data():
        ddbdataaccessor = DiabondDBServer()
        notify_not_healthy = False
        message = "Timeseries is healthy"
        sql = f"select max(ts) from fact_30_1_qwilt"
        format = 'json'
        respdata = ddbdataaccessor._queryDiamondDB_unverified_ssl(sql, format)
        try:
            if respdata['success']=='success':
                print('######select max(ts) from fact_30_1_qwilt------------->' , respdata)

                checkTime = round(int(respdata['data'][0]['max(ts)'])) / 1000
                currentTime = round(time.mktime(dtm.datetime.now().timetuple()))
                timeDiff = currentTime - checkTime
                if timeDiff > 3600 :
                    notify_not_healthy = True
                    message = f'Timeseries is not healthy. \n {timeDiff} minutes behind.'
        except ValueError as ve:
            print(ve)
        except KeyError as ke:
            print(ke)
        return message, notify_not_healthy


    @staticmethod
    def format_platform_data(platformName, dataformat, alarm_rec, kpi_rec, yesterday, today):
        if dataformat == 'htmltable':
            platformNameKey = 'VCP CORE' if platformName == 'VCP CORE WS' else platformName
            platformAlarmDate = '<td >Not Receiving</td>' if alarm_rec.get(platformNameKey, '') == '' else ( f'<td style="color:#FF0000">{alarm_rec[platformNameKey]}</td>' if (alarm_rec.get(platformNameKey, '') != yesterday and alarm_rec.get(platformNameKey, '') != today) else f'<td >{alarm_rec[platformNameKey]}</td>')
            platformKPIDate = '<td >Not Receiving</td>' if kpi_rec.get(platformName, '') == '' else ( f'<td style="color:#FF0000">{kpi_rec[platformName]}</td>' if (kpi_rec.get(platformName, '') != yesterday and kpi_rec.get(platformName, '') != today) else f'<td >{kpi_rec[platformName]}</td>')
            return f"<tr><td>{platformName}</td>{platformAlarmDate}{platformKPIDate}</tr>"
        else:
            return { platformName : {'alarm_received':alarm_rec.get(platformName, 'Not Available'), 'kpi_received':kpi_rec.get(platformName, 'Not Available')} }

    @staticmethod
    def Mysql_transaction_monitoring_Data():
        mysql_transaction_proof = {'others':None,'non-others':None}
        try:
            with connections['logsnapsreporter_non_others_db'].cursor() as cursor_nonothers, connections['logsnapsreporter_others_db'].cursor() as cursor_others:
                sql = f"SELECT COUNT(*) FROM reporter_status;"
                try:
                    cursor_nonothers.execute(sql)
                    results = cursor_nonothers.fetchall()
                    if len(results) > 0:
                        mysql_transaction_proof['non-others'] = results[0][0]
                    cursor_others.execute(sql)
                    results = cursor_others.fetchall()
                    if len(results) > 0:
                        mysql_transaction_proof['others'] = results[0][0]

                except Exception as e:
                    print('call from Mysql_transaction_monitoring_Data:::', e)

        except DatabaseError as e:
            print('Error from job.Mysql_transaction_monitoring_Data------>nonothers and others', e)
        return mysql_transaction_proof

    @staticmethod
    def CL_OL_Alarm_Data(forhours, start_date, end_date , non_others_db_connection_key='logsnapsreporter_non_others_db', others_db_connection_key = 'logsnapsreporter_others_db', vz_db_connection_key='logsnapsverizonedb'):
        count = 0
        ServerSerials = []
        # sql = 'SELECT SERIALNO,CLOSED_LOOP_STATUS,CLOSEDLOOP_NAME,ALARM_SUMMARY,WORKFLOW_STATUS,FIRSTOCCURRENCE FROM WORKFLOW_STATS'
        # ------------------- Closed Loops services ---------------
        yesterday = dtm.datetime.now() - timedelta(days=1)
        yesterday_unixtime = round(time.mktime(yesterday.timetuple()))
        if forhours is None:
            if start_date!='' and end_date!='':
                start_date = start_date + ' 00:00:00'
                end_date = end_date + ' 00:00:00'
            elif start_date!='':
                start_date = start_date + ' 00:00:00'
                end_date = dtm.datetime.now().strftime(app.DATETIMEFORMAT)
            else:
                start_date = yesterday.strftime(app.DATETIMEFORMAT)
                end_date = dtm.datetime.now().strftime(app.DATETIMEFORMAT)

            start_date_obj = dtm.datetime.strptime(start_date, app.DATETIMEFORMAT)
            start_date_obj_unixtime = round(time.mktime(start_date_obj.timetuple()))
            end_date_obj = dtm.datetime.strptime(end_date, app.DATETIMEFORMAT)
            end_date_obj_unixtime = round(time.mktime(end_date_obj.timetuple()))

            sql_timecheck = f"and firstoccurrence > {start_date} and firstoccurrence < {end_date}"
            # sql_timecheck_workflow_triggered = f"and LASTOCCURRENCE > {start_date_obj_unixtime}0 and LASTSTOCCURRENCE < {end_date_obj_unixtime}0" # 11 digits unixtimestamp, hence added 0 at the end
            sql_timecheck_workflow_triggered = f"and FIRSTOCCURRENCE > {start_date} and FIRSTOCCURRENCE < {end_date}" # 11 digits unixtimestamp, hence added 0 at the end
            sql_timecheck_workflow_stats = f"and > {start_date_obj_unixtime}0 and FIRSTOCCURRENCE < {end_date_obj_unixtime}0"
        else:
            sql_timecheck = f" and firstoccurrence>= (NOW() - Interval {forhours} HOUR)"
            # sql_timecheck_workflow = f" and FIRSTOCCURRENCE>= (NOW() - Interval {forhours} HOUR)"
            # sql_timecheck_workflow_triggered = f" and LASTOCCURRENCE>= (UNIX_TIMESTAMP() - {yesterday_unixtime})" # 11 digits unixtimestamp, hence added 0 at the end
            sql_timecheck_workflow_triggered = f" and FIRSTOCCURRENCE>= (NOW() - Interval {forhours} HOUR)" # 11 digits unixtimestamp, hence added 0 at the end
            datetime_hoursback = dtm.datetime.now() - timedelta(hours=forhours)
            hoursback_unixtime = round(time.mktime(datetime_hoursback.timetuple()))
            sql_timecheck_workflow_stats = f" and FIRSTOCCURRENCE >= {hoursback_unixtime}"
        CL_results = {
            "Samsung_USM_VM_Healing": {'cl_name_str': 'Samsung USM VM Healing', 'alertGroup': "'samsungUSM_2121'",
                                       'alert_sql': f"Select serverserial from reporter_status where Type =1 and  AlertGroup = 'samsungUSM_2121' and originalseverity > 0 and Vendor = 'Samsung' {sql_timecheck};",
                                       'rec_elligible': None, 'rec_final': None, 'rec_triggered': None,
                                       'rec_alrms_status': None},
            "Samsung_USM_MCMMDM": {'cl_name_str': 'Samsung USM MCD/MCM',
                                   'alertGroup': "'samsungUSM_2163','samsungUSM_2164'",
                                   'alert_sql': f"Select serverserial from reporter_status where originalseverity > 3 and alertgroup in ('samsungUSM_2163','samsungUSM_2164') {sql_timecheck};",
                                   'rec_elligible': None, 'rec_final': None, 'rec_triggered': None,
                                   'rec_alrms_status': None},
            # "Samsung vDU POD Restart": {'cl_name_str': 'Samsung vDU Pod Restart', 'alertGroup': "'samsungUSM_2121'",
            #                             'alert_sql': f"Select serverserial from reporter_status where Type =1 and originalseverity > 0 and  alertgroup = 'samsungUSM_2121' {sql_timecheck};",
            #                             'rec_elligible': None, 'rec_final': None, 'rec_triggered': None,
            #                             'rec_alrms_status': None},
            "vSPC_Notification": {'cl_name_str': 'vSPC Notification', 'alertGroup': "'VSPC-PROCESS-START'",
                                  'alert_sql': f"Select serverserial from reporter_status where alertgroup ='VSPC-PROCESS-START' {sql_timecheck};",
                                  'rec_elligible': None, 'rec_final': None, 'rec_triggered': None,
                                  'rec_alrms_status': None},
            "vSPC_Site_Notification": {'cl_name_str': 'vSPC Site Monitoring Notification',
                                       'alertGroup': "'VSPC-SITE-MONITORING-ALERT'",
                                       'alert_sql': f"Select serverserial from reporter_status where alertgroup = 'VSPC-SITE-MONITORING-ALERT' and ((originalseverity=5) or (originalseverity = 0 and Manager = 'vSPC_Synthetic')) {sql_timecheck};",
                                       'rec_elligible': None, 'rec_final': None, 'rec_triggered': None,
                                       'rec_alrms_status': None},
            # "Common CLP Core": {'cl_name_str': 'Common CLP Core', 'alertGroup': "'node reboot','STATUS_NodeDown'",
            #                     'alert_sql': f"Select serverserial from reporter_status where alertgroup in ('node reboot','STATUS_NodeDown') and Agent='vcp-core' {sql_timecheck};",
            #                     'rec_elligible': None, 'rec_final': None, 'rec_triggered': None,
            #                     'rec_alrms_status': None},
            # "Common CLP Edge 1": {'cl_name_str': 'Common CLP Edge',
            #                       'alertGroup': "'HpCpqsm2_cpqSm2ServerReset_VCPE','STATUS_NodeDown_VCPE'",
            #                       'alert_sql': f"Select serverserial from reporter_status where alertgroup in ('HpCpqsm2_cpqSm2ServerReset_VCPE','STATUS_NodeDown_VCPE') and Manager ='clp_synthetic' and (Node like '%C7CN%' or Node like '%DLV%') {sql_timecheck};",
            #                       'rec_elligible': None, 'rec_final': None, 'rec_triggered': None,
            #                       'rec_alrms_status': None},
            # "Common CLP Edge 2": {'cl_name_str': 'Common CLP Edge',
            #                       'alertGroup': "'HpCpqsm2_cpqSm2ServerReset_VCPE','STATUS_NodeDown_VCPE'",
            #                       'alert_sql': f"Select serverserial from reporter_status where alertgroup in ('HpCpqsm2_cpqSm2ServerReset_VCPE','STATUS_NodeDown_VCPE') and  Manager not in ('clp_synthetic', 'clp_synthetic_dr') and originalseverity != 0 and (Node like '%C7CN%' or Node like '%DLV%') {sql_timecheck};",
            #                       'rec_elligible': None, 'rec_final': None, 'rec_triggered': None,
            #                       'rec_alrms_status': None},
            "AUPF_Performance_Service": {'cl_name_str': 'AUPF Performance',
                                         'alertGroup': "'Memory.MemUsageMax'",
                                         'alert_sql': f"Select serverserial from reporter_status where Type = 1 and originalseverity > 0 and AlertGroup = 'Memory.MemUsageMax' and Agent = 'Threshold' and NFType = 'AUPF'  and NFName != '' {sql_timecheck};",
                                         'rec_elligible': None, 'rec_final': None, 'rec_triggered': None,
                                         'rec_alrms_status': None},
            "ACPF_Performance_Service": {'cl_name_str': 'ACPF Performance',
                                              'alertGroup': "'Memory.MemUsageMax'",
                                              'alert_sql': f"Select serverserial from reporter_status where originalseverity >0  and  NFName != '' and alertgroup = 'Memory.MemUsageMax' and nftype='ACPF' and Agent = 'Threshold' {sql_timecheck};",
                                              'rec_elligible': None, 'rec_final': None, 'rec_triggered': None,
                                              'rec_alrms_status': None},
            "MEC_PGW_Notifications": {'cl_name_str': 'PGW_MEC',
                                      'alertGroup': "'PDSN_starSRPActive', 'PDSN_starSRPStandby'",
                                      'alert_sql': f"Select serverserial from  reporter_status where originalseverity = 5 and alertgroup in ('PDSN_starSRPActive', 'PDSN_starSRPStandby') {sql_timecheck};",
                                      'rec_elligible': None, 'rec_final': None, 'rec_triggered': None,
                                      'rec_alrms_status': None},
            # "KPI Based vDU Healing": {'cl_name_str': 'KPI Based vDU Healing',
            #                           'alertGroup': "'DU.vdu.restart.closedLoop'",
            #                           'alert_sql': f"Select serverserial from reporter_status where originalseverity > 0 and  Agent = 'Threshold' and NFName != '' and alertgroup = 'DU.vdu.restart.closedLoop' {sql_timecheck};",
            #                           'rec_elligible': None, 'rec_final': None, 'rec_triggered': None,
            #                           'rec_alrms_status': None},
            # "Remedy Ticket Enrichment": {'cl_name_str': 'Remedy Ticket Enrichment',
            #                              'alertGroup': "'samsungUSM_4500300-5G','samsungUSM_4500368-5G','samsungUSM_4532300-5G','samsungUSM_4532707-5G','samsungUSM_4533102-5G','samsungUSM_4533368-5G','samsungUSM_3599460-5G','samsungUSM_3500300-5G','samsungUSM_3500368-5G','samsungUSM_3532300-5G','samsungUSM_3532368-5G','samsungUSM_3533300-5G','samsungUSM_3533368-5G','samsungUSM_3599672-5G','samsungUSM_1500102-5G','samsungUSM_1501102-5G','samsungUSM_1502102-5G','samsungUSM_1502380-5G','samsungUSM_1503102-5G','samsungUSM_1532102-5G','samsungUSM_1532380-5G','samsungUSM_1532484-5G','samsungUSM_1533102-5G','samsungUSM_1533380-5G','samsungUSM_1599460-5G','samsungUSM_1599641-5G','samsungUSM_1599680-5G','samsungUSM_2006-5G','samsungUSM_2006-5G_COR','samsungUSM_2007-5G','samsungUSM_3500300-5G','samsungUSM_3500368-5G','samsungUSM_3532300-5G','samsungUSM_3532368-5G','samsungUSM_3533300-5G','samsungUSM_3533368-5G','samsungUSM_3599460-5G','samsungUSM_3599672-5G','samsungUSM_4500300-5G','samsungUSM_4500368-5G','samsungUSM_4532300-5G','samsungUSM_4532707-5G','samsungUSM_4533368-5G'",
            #                              'alert_sql': f"Select serverserial from reporter_status where TTNumber = '' and Type = 1 and originalseverity > 1 and Node != '' and alertgroup in ('samsungUSM_4500300-5G','samsungUSM_4500368-5G','samsungUSM_4532300-5G','samsungUSM_4532707-5G','samsungUSM_4533102-5G','samsungUSM_4533368-5G','samsungUSM_3599460-5G','samsungUSM_3500300-5G','samsungUSM_3500368-5G','samsungUSM_3532300-5G','samsungUSM_3532368-5G','samsungUSM_3533300-5G','samsungUSM_3533368-5G','samsungUSM_3599672-5G','samsungUSM_1500102-5G','samsungUSM_1501102-5G','samsungUSM_1502102-5G','samsungUSM_1502380-5G','samsungUSM_1503102-5G','samsungUSM_1532102-5G','samsungUSM_1532380-5G','samsungUSM_1532484-5G','samsungUSM_1533102-5G','samsungUSM_1533380-5G','samsungUSM_1599460-5G','samsungUSM_1599641-5G','samsungUSM_1599680-5G','samsungUSM_2006-5G','samsungUSM_2006-5G_COR','samsungUSM_2007-5G','samsungUSM_3500300-5G','samsungUSM_3500368-5G','samsungUSM_3532300-5G','samsungUSM_3532368-5G','samsungUSM_3533300-5G','samsungUSM_3533368-5G','samsungUSM_3599460-5G','samsungUSM_3599672-5G','samsungUSM_4500300-5G','samsungUSM_4500368-5G','samsungUSM_4532300-5G','samsungUSM_4532707-5G','samsungUSM_4533368-5G') {sql_timecheck};",
            #                              'rec_elligible': None, 'rec_final': None, 'rec_triggered': None,
            #                              'rec_alrms_status': None},
            # "Alarm Based vDU healing": {'cl_name_str': 'Alarm Based vDU healing',
            #                             'alertGroup': "'samsungUSM_1599460-5G'",
            #                             'alert_sql': f"Select serverserial from reporter_status where originalseverity > 0 and Type = 1 and Vendor = 'Samsung' and alertgroup = 'samsungUSM_1599460-5G' {sql_timecheck};",
            #                             'rec_elligible': None, 'rec_final': None, 'rec_triggered': None,
            #                             'rec_alrms_status': None},
            "OCS_TAS_Healing": {'cl_name_str': 'OCS TAS Healing', 'alertGroup': None,
                                'alert_sql': f"Select serverserial from reporter_status where originalseverity > 0 and Type = 1 and Agent = 'Threshold' and Summary like 'TAS' {sql_timecheck};",
                                'rec_elligible': None, 'rec_final': None, 'rec_triggered': None,
                                'rec_alrms_status': None},
            # "Performance Based DU or vDU healing": {'cl_name_str': 'Performance Based DU or vDU Healing',
            #                                         'alertGroup': "'DU.Performance'",
            #                                         'alert_sql': f"Select serverserial from reporter_status where originalseverity > 0 and Type = 1 and Agent = 'Threshold' and NFName !='' and alertgroup = 'DU.Performance' {sql_timecheck};",
            #                                         'rec_elligible': None, 'rec_final': None, 'rec_triggered': None,
            #                                         'rec_alrms_status': None},
            # "Accessibility Performance Based DU or vDU Healing": {
            #     'cl_name_str': 'Accessibility Performance Based DU or vDU Healing', 'alertGroup': "'DU.Accessibility'",
            #     'alert_sql': f"Select serverserial from reporter_status where originalseverity > 0 and Type = 1 and Agent = 'Threshold' and NFName !='' and alertgroup = 'DU.Accessibility' {sql_timecheck};",
            #     'rec_elligible': None, 'rec_final': None, 'rec_triggered': None, 'rec_alrms_status': None},
            # "Throughput Performance Based DU or vDU Healing": {
            #     'cl_name_str': 'Throughput Performance Based DU or vDU Healing', 'alertGroup': "'DU.Throughput'",
            #     'alert_sql': f"Select serverserial from reporter_status where originalseverity > 0 and Type = 1 and Agent = 'Threshold' and NFName !='' and alertgroup = 'DU.Throughput' {sql_timecheck};",
            #     'rec_elligible': None, 'rec_final': None, 'rec_triggered': None, 'rec_alrms_status': None},
            "Oracle_vRDRA_SOAM_Service": {'cl_name_str': 'vRDRA SOAM Notification',
                                          'alertGroup': "'Server.System.CPU.UtilPct.Average'",
                                          'alert_sql': f"Select serverserial from reporter_status where alertgroup = 'Server.System.CPU.UtilPct.Average' and Agent = 'Threshold' and originalseverity > 0 and Type = 1 {sql_timecheck};",
                                          'rec_elligible': None, 'rec_final': None, 'rec_triggered': None,
                                          'rec_alrms_status': None},
            # "vSPC_site_synthetic": {'cl_name_str': 'vSPC site synthetic', 'alertGroup': "'VSPC-SITE-MONITORING-ALERT'",
            #                         'alert_sql': f"Select serverserial from reporter_status where alertgroup = 'VSPC-SITE-MONITORING-ALERT' and  originalseverity = 0 {sql_timecheck};",
            #                         'rec_elligible': None, 'rec_final': None, 'rec_triggered': None,
            #                         'rec_alrms_status': None},
        }
        """
        CL_results = {
            "samsung USM VM healing": {'cl_name_str':'Samsung USM VM Healing', 'alertGroup': "'samsungUSM_2121'", 'alert_sql': f"Select serverserial from reporter_status where Type =1 and originalseverity > 0 and  alertgroup = 'samsungUSM_2121' {sql_timecheck};",'rec_elligible':None,'rec_final':None,'rec_triggered':None,'rec_alrms_status':None},
            "Samsung USM MCM MCD": {'cl_name_str':'Samsung USM MCD/MCM', 'alertGroup': "'samsungUSM_2163','samsungUSM_2164'", 'alert_sql': f"Select serverserial from reporter_status where originalseverity > 3 and alertgroup in ('samsungUSM_2163','samsungUSM_2164') {sql_timecheck};",'rec_elligible':None,'rec_final':None,'rec_triggered':None,'rec_alrms_status':None},
            "Samsung vDU POD Restart": {'cl_name_str':'Samsung vDU Pod Restart', 'alertGroup': "'samsungUSM_2121'", 'alert_sql': f"Select serverserial from reporter_status where Type =1 and originalseverity > 0 and  alertgroup = 'samsungUSM_2121' {sql_timecheck};",'rec_elligible':None,'rec_final':None,'rec_triggered':None,'rec_alrms_status':None},
            # "vRDRA SOAM Notifications": {'cl_name_str':'vRDRA SOAM Notification', 'alert_sql': f"Select serverserial from reporter_status where severity > 0 and Type=1 and alertgroup ='Server.System.CPU.UtilPct.Average' {sql_timecheck};",'rec_elligible':None,'rec_final':None,'rec_triggered':None,'rec_alrms_status':None},
            "vSPC Notification": {'cl_name_str':'vSPC Notification', 'alertGroup': "'VSPC-PROCESS-START'", 'alert_sql': f"Select serverserial from reporter_status where alertgroup ='VSPC-PROCESS-START' {sql_timecheck};",'rec_elligible':None,'rec_final':None,'rec_triggered':None,'rec_alrms_status':None},
            "vSPC site Monitoring Notification": {'cl_name_str':'vSPC site Monitoring Notification', 'alertGroup': "'VSPC-SITE-MONITORING-ALERT'", 'alert_sql': f"Select serverserial from reporter_status where alertgroup = 'VSPC-SITE-MONITORING-ALERT' {sql_timecheck};",'rec_elligible':None,'rec_final':None,'rec_triggered':None,'rec_alrms_status':None},
            # "vDU Protocol Notification": {'cl_name_str':'VDU Protocol Notification', 'alert_sql': f"Select serverserial from reporter_status where severity > 0 and Type=1 and alertgroup in ('samsungUSM_2006-5G', 'samsungUSM_2007-5G') {sql_timecheck};",'rec_elligible':None,'rec_final':None,'rec_triggered':None,'rec_alrms_status':None},
            "Common CLP Core": {'cl_name_str':'Common CLP Core', 'alertGroup': "'node reboot','STATUS_NodeDown'", 'alert_sql': f"Select serverserial from reporter_status where alertgroup in ('node reboot','STATUS_NodeDown') and Agent='vcp-core' {sql_timecheck};",'rec_elligible':None,'rec_final':None,'rec_triggered':None,'rec_alrms_status':None},
            "Common CLP Edge 1": {'cl_name_str':'Common CLP Edge', 'alertGroup': "'HpCpqsm2_cpqSm2ServerReset_VCPE','STATUS_NodeDown_VCPE'", 'alert_sql': f"Select serverserial from reporter_status where alertgroup in ('HpCpqsm2_cpqSm2ServerReset_VCPE','STATUS_NodeDown_VCPE') and Manager ='clp_synthetic' and (Node like '%C7CN%' or Node like '%DLV%') {sql_timecheck};",'rec_elligible':None,'rec_final':None,'rec_triggered':None,'rec_alrms_status':None},
            "Common CLP Edge 2": {'cl_name_str':'Common CLP Edge', 'alertGroup': "'HpCpqsm2_cpqSm2ServerReset_VCPE','STATUS_NodeDown_VCPE'", 'alert_sql': f"Select serverserial from reporter_status where alertgroup in ('HpCpqsm2_cpqSm2ServerReset_VCPE','STATUS_NodeDown_VCPE') and  Manager not in ('clp_synthetic', 'clp_synthetic_dr') and originalseverity != 0 and (Node like '%C7CN%' or Node like '%DLV%') {sql_timecheck};",'rec_elligible':None,'rec_final':None,'rec_triggered':None,'rec_alrms_status':None},
            "AUPF Performance": {'cl_name_str':'AUPF Performance', 'alertGroup': "'Memory.MemUsageMax'", 'alert_sql': f"Select serverserial from reporter_status where originalseverity > 0 and Agent = 'Threshold' and alertgroup = 'Memory.MemUsageMax' and nftype='AUPF' and NFName != '' {sql_timecheck};",'rec_elligible':None,'rec_final':None,'rec_triggered':None,'rec_alrms_status':None},
            "ACPF Performance": {'cl_name_str':'ACPF Performance', 'alertGroup': "'Memory.MemUsageMax'", 'alert_sql': f"Select serverserial from reporter_status where originalseverity >0  and  NFName != '' and alertgroup = 'Memory.MemUsageMax' and nftype='ACPF' {sql_timecheck};",'rec_elligible':None,'rec_final':None,'rec_triggered':None,'rec_alrms_status':None},
            "MEC_PGW": {'cl_name_str':'PGW_MEC', 'alertGroup': "'PDSN_starSRPActive', 'PDSN_starSRPStandby'", 'alert_sql': f"Select serverserial from  reporter_status where originalseverity = 5 and alertgroup in ('PDSN_starSRPActive', 'PDSN_starSRPStandby') {sql_timecheck};",'rec_elligible':None,'rec_final':None,'rec_triggered':None,'rec_alrms_status':None},
            "KPI Based vDU Healing": {'cl_name_str':'KPI Based vDU Healing', 'alertGroup': "'DU.vdu.restart.closedLoop'", 'alert_sql': f"Select serverserial from reporter_status where originalseverity > 0 and  Agent = 'Threshold' and NFName != '' and alertgroup = 'DU.vdu.restart.closedLoop' {sql_timecheck};",'rec_elligible':None,'rec_final':None,'rec_triggered':None,'rec_alrms_status':None},
            "Remedy Ticket Enrichment": {'cl_name_str':'Remedy Ticket Enrichment', 'alertGroup': "'samsungUSM_4500300-5G','samsungUSM_4500368-5G','samsungUSM_4532300-5G','samsungUSM_4532707-5G','samsungUSM_4533102-5G','samsungUSM_4533368-5G','samsungUSM_3599460-5G','samsungUSM_3500300-5G','samsungUSM_3500368-5G','samsungUSM_3532300-5G','samsungUSM_3532368-5G','samsungUSM_3533300-5G','samsungUSM_3533368-5G','samsungUSM_3599672-5G','samsungUSM_1500102-5G','samsungUSM_1501102-5G','samsungUSM_1502102-5G','samsungUSM_1502380-5G','samsungUSM_1503102-5G','samsungUSM_1532102-5G','samsungUSM_1532380-5G','samsungUSM_1532484-5G','samsungUSM_1533102-5G','samsungUSM_1533380-5G','samsungUSM_1599460-5G','samsungUSM_1599641-5G','samsungUSM_1599680-5G','samsungUSM_2006-5G','samsungUSM_2006-5G_COR','samsungUSM_2007-5G','samsungUSM_3500300-5G','samsungUSM_3500368-5G','samsungUSM_3532300-5G','samsungUSM_3532368-5G','samsungUSM_3533300-5G','samsungUSM_3533368-5G','samsungUSM_3599460-5G','samsungUSM_3599672-5G','samsungUSM_4500300-5G','samsungUSM_4500368-5G','samsungUSM_4532300-5G','samsungUSM_4532707-5G','samsungUSM_4533368-5G'",
                                         'alert_sql': f"Select serverserial from reporter_status where TTNumber = '' and Type = 1 and originalseverity > 1 and Node != '' and alertgroup in ('samsungUSM_4500300-5G','samsungUSM_4500368-5G','samsungUSM_4532300-5G','samsungUSM_4532707-5G','samsungUSM_4533102-5G','samsungUSM_4533368-5G','samsungUSM_3599460-5G','samsungUSM_3500300-5G','samsungUSM_3500368-5G','samsungUSM_3532300-5G','samsungUSM_3532368-5G','samsungUSM_3533300-5G','samsungUSM_3533368-5G','samsungUSM_3599672-5G','samsungUSM_1500102-5G','samsungUSM_1501102-5G','samsungUSM_1502102-5G','samsungUSM_1502380-5G','samsungUSM_1503102-5G','samsungUSM_1532102-5G','samsungUSM_1532380-5G','samsungUSM_1532484-5G','samsungUSM_1533102-5G','samsungUSM_1533380-5G','samsungUSM_1599460-5G','samsungUSM_1599641-5G','samsungUSM_1599680-5G','samsungUSM_2006-5G','samsungUSM_2006-5G_COR','samsungUSM_2007-5G','samsungUSM_3500300-5G','samsungUSM_3500368-5G','samsungUSM_3532300-5G','samsungUSM_3532368-5G','samsungUSM_3533300-5G','samsungUSM_3533368-5G','samsungUSM_3599460-5G','samsungUSM_3599672-5G','samsungUSM_4500300-5G','samsungUSM_4500368-5G','samsungUSM_4532300-5G','samsungUSM_4532707-5G','samsungUSM_4533368-5G') {sql_timecheck};",'rec_elligible':None,'rec_final':None,'rec_triggered':None,'rec_alrms_status':None},
            "Alarm Based vDU healing": {'cl_name_str':'Alarm Based vDU healing', 'alertGroup': "'samsungUSM_1599460-5G'", 'alert_sql': f"Select serverserial from reporter_status where originalseverity > 0 and Type = 1 and Vendor = 'Samsung' and alertgroup = 'samsungUSM_1599460-5G' {sql_timecheck};",'rec_elligible':None,'rec_final':None,'rec_triggered':None,'rec_alrms_status':None},
            # "OCS TAS Healing": {'cl_name_str':'OCS TAS Healing', 'alert_sql': f"Select serverserial from reporter_status where severity > 0 and Type=1 and summary like 'TAS' {sql_timecheck};",'rec_elligible':None,'rec_final':None,'rec_triggered':None,'rec_alrms_status':None},
            "Performance Based DU or vDU healing": {'cl_name_str':'Performance Based DU or vDU Healing', 'alertGroup': "'DU.Performance'", 'alert_sql': f"Select serverserial from reporter_status where originalseverity > 0 and Type = 1 and Agent = 'Threshold' and NFName !='' and alertgroup = 'DU.Performance' {sql_timecheck};",'rec_elligible':None,'rec_final':None,'rec_triggered':None,'rec_alrms_status':None},
            "Accessibility Performance Based DU or vDU Healing": {'cl_name_str':'Accessibility Performance Based DU or vDU Healing', 'alertGroup': "'DU.Accessibility'", 'alert_sql': f"Select serverserial from reporter_status where originalseverity > 0 and Type = 1 and Agent = 'Threshold' and NFName !='' and alertgroup = 'DU.Accessibility' {sql_timecheck};",'rec_elligible':None,'rec_final':None,'rec_triggered':None,'rec_alrms_status':None},
            "Throughput Performance Based DU or vDU Healing": {'cl_name_str':'Throughput Performance Based DU or vDU Healing', 'alertGroup': "'DU.Throughput'", 'alert_sql': f"Select serverserial from reporter_status where originalseverity > 0 and Type = 1 and Agent = 'Threshold' and NFName !='' and alertgroup = 'DU.Throughput' {sql_timecheck};",'rec_elligible':None,'rec_final':None,'rec_triggered':None,'rec_alrms_status':None},
            "vRDRA Notification": {'cl_name_str':'vRDRA Notification', 'alertGroup': "'Server.System.CPU.UtilPct.Average'", 'alert_sql': f"Select serverserial from reporter_status where alertgroup = 'Server.System.CPU.UtilPct.Average' and originalseverity > 0 and Type = 1 {sql_timecheck};",'rec_elligible':None,'rec_final':None,'rec_triggered':None,'rec_alrms_status':None},
            "vSPC site Notification": {'cl_name_str':'vSPC site Notification', 'alertGroup': "'VSPC-SITE-MONITORING-ALERT'", 'alert_sql': f"Select serverserial from reporter_status where alertgroup = 'VSPC-SITE-MONITORING-ALERT' {sql_timecheck};",'rec_elligible':None,'rec_final':None,'rec_triggered':None,'rec_alrms_status':None},
        }
        """
        try:
            #lets perform all DB queries under 1 set of opened connections unless all of the executions are done
            with connections[non_others_db_connection_key].cursor() as cursor, connections[others_db_connection_key].cursor() as cursorothers, connections[vz_db_connection_key].cursor() as cursorvz:
                for CL_OL_Name in list(CL_results):
                    ElligibleAlerts = 0
                    try:
                        sqlElligibleAlerts = CL_results[CL_OL_Name]['alert_sql']
                        print(f'from CL_OL_Alarm_Data : sqlElligibleAlerts({CL_OL_Name})------>', sqlElligibleAlerts)
                        serial_nos = []

                        cursor.execute(sqlElligibleAlerts) #executing queries in non-others DB
                        cursorothers.execute(sqlElligibleAlerts) #executing queries in others DB
                        del CL_results[CL_OL_Name]['alert_sql'] #for presenting the data after populating, no place for the sql in it

                        results_nonothers = cursor.fetchall()
                        results_others = cursorothers.fetchall()
                        #for Common CLP Edge there are 2 queries.. results to be combined
                        # if CL_results[CL_OL_Name]['cl_name_str'] == 'Common CLP Edge' and CL_results[CL_OL_Name]['rec_elligible'] is not None:
                        #     ElligibleAlerts += len(results_nonothers) + len(results_others) # summing the counts
                        # else:
                        #     ElligibleAlerts = len(results_nonothers) + len(results_others) # summing the counts
                        print('Elligible Alarms::>>>>>>', len(results_nonothers) , len(results_others))
                        if len(results_nonothers) > 0 or len(results_others) > 0:
                            if len(results_nonothers) > 0:
                                for row in results_nonothers:
                                    serial_nos.append(row[0])
                            if len(results_others) > 0:
                                for row in results_others:
                                    serial_nos.append(row[0])
                            if CL_results[CL_OL_Name]['cl_name_str'] == 'Common CLP Edge' and CL_results['Common CLP Edge 1']['rec_elligible'] is not None:
                                rec_elligible_CCLPE1 = CL_results['Common CLP Edge 1']['rec_elligible']
                                str_serial_nos = ','.join([str(s) for s in rec_elligible_CCLPE1]) + ',' + ','.join([str(s) for s in serial_nos])
                                CL_results['Common CLP Edge 2']['rec_elligible'] = rec_elligible_CCLPE1 + serial_nos
                            else:
                                str_serial_nos = ','.join([str(s) for s in serial_nos])
                                CL_results[CL_OL_Name]['rec_elligible'] = serial_nos
                            # print('outside-$$$$$$$$$$$$############->', CL_OL_Name)
                            if CL_OL_Name == 'Common CLP Edge 2': # when the data collection is done, lets change the key to Common CLP Edge and remove the 2 other queries 1 and 2
                                CL_OL_Name = 'Common CLP Edge'
                                # print('inside-$$$$$$$$$$$$############->',CL_OL_Name)
                                CL_results[CL_OL_Name] = CL_results['Common CLP Edge 2']
                                del CL_results['Common CLP Edge 1'], CL_results['Common CLP Edge 2']

                            if str_serial_nos != '':
                                cl_name_str = CL_results[CL_OL_Name]["cl_name_str"]
                                alertGroup = CL_results[CL_OL_Name]["alertGroup"]
                                alertGroupSqlPart = f" AND ALARM_NAME IN ({alertGroup})" if alertGroup is not None else ''
                                sqlAlertsTriggered = f"SELECT COUNT(*) trig_alarms_count FROM WORKFLOW_TRIGGERED_STATS WHERE LOOP_NAME='{cl_name_str}' {alertGroupSqlPart} {sql_timecheck_workflow_triggered};"
                                # sqlAlertsWorkFlowStatus = f"SELECT group_concat(SERIALNO) SERIALNOS, sum(case when WORKFLOW_STATUS = 'Success' then 1 else 0 end) SUCCESS, sum(case when WORKFLOW_STATUS = 'Failed' then 1 else 0 end) FAILED, sum(case when WORKFLOW_STATUS = 'In Progress' then 1 else 0 end) INPROGRESS, sum(case when WORKFLOW_STATUS = 'Waiting for Approval' then 1 else 0 end) WFA FROM WORKFLOW_STATS WHERE CLOSEDLOOP_NAME='{cl_name_str}' {sql_timecheck_workflow_stats};"
                                # sqlAlertsTriggered = f"SELECT COUNT(distinct SERIALNO) trig_alarms_count FROM WORKFLOW_TRIGGERED_STATS WHERE SERIALNO IN ({str_serial_nos}) {sql_timecheck_workflow_triggered};"
                                # sqlAlertsWorkFlowStatus = f"SELECT group_concat(SERIALNO) SERIALNOS, sum(case when WORKFLOW_STATUS = 'Success' then 1 else 0 end) SUCCESS, sum(case when WORKFLOW_STATUS = 'Failed' then 1 else 0 end) FAILED, sum(case when WORKFLOW_STATUS = 'In Progress' then 1 else 0 end) INPROGRESS, sum(case when WORKFLOW_STATUS = 'Waiting for Approval' then 1 else 0 end) WFA FROM WORKFLOW_STATS WHERE SERIALNO IN({str_serial_nos}) {sql_timecheck_workflow_stats};"
                                sqlAlertsWorkFlowStatus = f"SELECT group_concat(SERIALNO) SERIALNOS, sum(case when WORKFLOW_STATUS = 'Success' then 1 else 0 end) SUCCESS, sum(case when WORKFLOW_STATUS = 'Failed' then 1 else 0 end) FAILED, sum(case when WORKFLOW_STATUS = 'In Progress' then 1 else 0 end) INPROGRESS, sum(case when WORKFLOW_STATUS = 'Waiting for Approval' then 1 else 0 end) WFA FROM WORKFLOW_STATS WHERE CLOSEDLOOP_NAME='{cl_name_str}' {sql_timecheck_workflow_stats};"
                                print('sqlAlertsTriggered with time condition--------------------------->' )
                                print(sqlAlertsTriggered)
                                print('sqlAlertsWorkFlowStatus with time condition--------------------------->')
                                print(sqlAlertsWorkFlowStatus)

                                # sqlAlertsTriggered = f"SELECT COUNT(distinct SERIALNO) trig_alarms_count FROM WORKFLOW_TRIGGERED_STATS WHERE SERIALNO IN ({str_serial_nos});"
                                # sqlAlertsWorkFlowStatus = f"SELECT group_concat(SERIALNO) SERIALNOS, sum(case when WORKFLOW_STATUS = 'Success' then 1 else 0 end) SUCCESS, sum(case when WORKFLOW_STATUS = 'Failed' then 1 else 0 end) FAILED, sum(case when WORKFLOW_STATUS = 'In Progress' then 1 else 0 end) INPROGRESS, sum(case when WORKFLOW_STATUS = 'Waiting for Approval' then 1 else 0 end) WFA FROM WORKFLOW_STATS WHERE SERIALNO IN({str_serial_nos});"

                                # print('from dataentities.CL_OL_Alarm_Data: sqlAlertsTriggered------>', sqlAlertsTriggered)
                                # print('from dataentities.CL_OL_Alarm_Data: sqlAlertsWorkFlowStatus------>', sqlAlertsWorkFlowStatus)
                                try:
                                    # cursorvz.execute(sqlAlertsFinalStatus)
                                    cursorvz.execute(sqlAlertsTriggered)
                                    results = cursorvz.fetchall()
                                    for row in results:
                                        # CL_name_txt = row[0].split(':')[0] if row[0].find(':') != -1 else row[0]
                                        CL_results[CL_OL_Name]['rec_triggered'] = row[0]
                                    # for row in results:
                                    #     CL_name_txt = row[0].split(':')[0] if row[0].find(':')!=-1 else row[0]
                                    #     if CL_results[CL_OL_Name].get('rec_final') is not None:
                                    #         CL_results[CL_OL_Name]['rec_final'][CL_name_txt]['serial_nos'].append(row[1])
                                    #         CL_results[CL_OL_Name]['rec_final'][CL_name_txt]['count'] += 1
                                    #     else:
                                    #         CL_results[CL_OL_Name]['rec_final'][CL_name_txt]['serial_nos'] = [row[1]]
                                    #         CL_results[CL_OL_Name]['rec_final'][CL_name_txt]['count'] = 1
                                    # print('CL_OL_Name.resolved_status_report::', CL_results[CL_OL_Name]['rec_final'])
                                    cursorvz.execute(sqlAlertsWorkFlowStatus)
                                    results = cursorvz.fetchall()
                                    for row in results:
                                        # CL_name_txt = row[0].split(':')[0] if row[0].find(':') != -1 else row[0]
                                        CL_results[CL_OL_Name]['rec_alrms_status'] = {"SUCCESS":(row[1] if row[1] is not None else 0),
                                                                                      "FAILED": (row[2] if row[2] is not None else 0),
                                                                                      "INPROGRESS": (row[3] if row[3] is not None else 0),
                                                                                      "WFA": (row[4] if row[4] is not None else 0),
                                                                                      }

                                except Exception as e:
                                    print(e)
                        else:
                            if CL_OL_Name == 'Common CLP Edge 2': # when the data collection is done, lets change the key to Common CLP Edge and remove the 2 other queries 1 and 2
                                CL_OL_Name = 'Common CLP Edge'
                                if CL_results['Common CLP Edge 1']['rec_elligible'] is not None:
                                    CL_results[CL_OL_Name] = CL_results['Common CLP Edge 1']
                                else:
                                    CL_results[CL_OL_Name] = CL_results['Common CLP Edge 2']
                                del CL_results['Common CLP Edge 1'], CL_results['Common CLP Edge 2']
                        del CL_results[CL_OL_Name]['cl_name_str']
                    except Exception as e:
                        print(e)
                        recs_nf_type_alarms_nonothers = {
                            'error': "Some error happened while retrieving data. Please try after sometime."}

        except DatabaseError as e:
            print('Error from dataentities.FMPMAlarmCountsData------>nonothers or others or vz', e)
        return CL_results

    @staticmethod
    def ASM_Observer_Monitoring_Data():
        # API to fetch vnf and cnf id
        start_endpoint = "/1.0/topology/mgmt_artifacts?_field=*&_limit=400&_type=ASM_OBSERVER_JOB&_include_count=false"
        asm_observer_jobs_res = ClusterResourses._query_api_unverified_ssl(start_endpoint)
        # print('VNF-CNF data here--------------------->',asm_observer_jobs_res) # to be deleted later
        # forhours = 4
        ASM_JOBS_from_DB = {}
        ASM_FAILED_JOBS = []
        ASM_ZERO_OBS_COUNT_JOBS = []
        asm_failed_jobs_list = []
        ASM_JOBS_SUBSTANTIAL_COUNTS = []
        try:
            with connections['logsnapsreporter_non_others_db'].cursor() as cursor:
                # retrieving FM report data
                # sql = f"SELECT UPPER(NFTYPE) NFTYPE, DATE_FORMAT(MAX(reportdate),'%Y-%m-%d') reportdate FROM nftype_alerts_counts_report  WHERE NFTYPE<>'' AND {sqlpart_reporter_db} GROUP BY UPPER(NFTYPE) HAVING (SUM(CRITICAL) + SUM(MAJOR) + SUM(MINOR) + SUM(WARNING) + SUM(INDETERMINATE) + SUM(CLEAR)) > 0;"
                sql = f"SELECT upper(job_name), previous_count, current_count, last_run_status, current_status, prev_last_run_time, curr_last_run_time FROM asm_observer_job_completion_comparison_data;"
                print('retrieving ASM Job data job.queryASMObserverMonitoringAndDataUpdate------>')
                print(f'origin line number------->{sys._getframe().f_lineno} SQL-->', sql)
                try:
                    cursor.execute(sql)
                    desc = cursor.description
                    fields = [col[0] for col in desc]
                    results = cursor.fetchall()
                    if len(results) > 0:
                        for row in results:
                            ASM_JOBS_from_DB[row[0]] = {'previous_count': row[1], 'current_count': row[2],
                                                        'last_run_status': row[3], 'current_status': row[4],
                                                        'prev_last_run_time': row[5], 'curr_last_run_time': row[6]}

                    if asm_observer_jobs_res.get('_items', '') != '' and len(
                            asm_observer_jobs_res.get('_items', [])) > 0:
                        for asm_observer_job in asm_observer_jobs_res.get('_items'):
                            asm_observer_job_name = asm_observer_job.get('name').upper()

                            asm_observer_job_observedTime = int(
                                asm_observer_job['observedTime']) if \
                                asm_observer_job.get('observedTime', None) is not None else 0

                            # prepare a list of failed jobs
                            asm_observer_job_status_description = asm_observer_job.get('hasStateDescription', '')
                            # IDAVA 1135 Fix
                            asm_observer_job_status_description=asm_observer_job_status_description.replace("'","")
                            asm_observer_job_status = asm_observer_job.get('hasState','')
                            dt_object = dtm.datetime.utcfromtimestamp(
                                round(asm_observer_job_observedTime / 1000)).strftime(
                                app.DATETIMEFORMAT) if asm_observer_job_observedTime != 0 and asm_observer_job_observedTime is not None else ''
                            # mark failed if stattus FAILED and description does not have "finished" also exclude any MANUAL jobs
                            if (asm_observer_job_name.find('LISTENER') == -1 \
                                or asm_observer_job_name.upper() in app.ASM_FAILED_JOBS_INCLUSION_JOB_NAMES) \
                                and asm_observer_job_status != 'RUNNING' \
                                and asm_observer_job_status != 'FINISHING' \
                                and (asm_observer_job_status == 'FAILED' or asm_observer_job_status_description.lower().find(
                                    'job finished') == -1) :

                                # with description 'awaiting next scheduled run', there could be failures round teh corner so lets dig deeper
                                if asm_observer_job_status_description.lower().find(
                                    'awaiting next scheduled run') != -1:
                                    history_endpoint = f'/1.0/openstack-observer/observations/{asm_observer_job["_id"]}/history?_limit=10'
                                    asm_observer_job_history_res = ClusterResourses._query_api_unverified_ssl(history_endpoint)
                                    if asm_observer_job_history_res.get('_items', '') != '' and len(
                                            asm_observer_job_history_res.get('_items', [])) > 0 :
                                        for eachHistory in asm_observer_job_history_res.get('_items', []):
                                            if eachHistory['hasState'].upper() == 'FAILED' or eachHistory['hasState'].upper() != 'SKIPPED':
                                                asm_observer_job_status_description = eachHistory['hasStateDescription']
                                                asm_observer_job_status = eachHistory['hasState']
                                                asm_observer_job_observedTime = eachHistory['observedTime']
                                                dt_object = dtm.datetime.utcfromtimestamp(
                                                    round(asm_observer_job_observedTime / 1000)).strftime(
                                                    app.DATETIMEFORMAT) if asm_observer_job_observedTime != 0 and asm_observer_job_observedTime is not None else ''
                                                break
                                        ASM_FAILED_JOBS.append(
                                        {'asm_observer_job_name': asm_observer_job_name, 'lastruntime': dt_object,
                                         'job_status': asm_observer_job_status,
                                         'status_description': asm_observer_job_status_description})
                                        asm_failed_jobs_list.append(asm_observer_job_name.upper())

                                else:
                                    ASM_FAILED_JOBS.append(
                                        {'asm_observer_job_name': asm_observer_job_name, 'lastruntime': dt_object,
                                         'job_status': asm_observer_job_status,
                                         'status_description': asm_observer_job_status_description})
                                    asm_failed_jobs_list.append(asm_observer_job_name.upper())
                            # Zero count Jobs list
                            if asm_observer_job.get('observationCount', '') != '' \
                                    and int(asm_observer_job.get('observationCount', '')) == 0 \
                                    and asm_observer_job_name.upper() not in app.ZERO_OBS_COUNT_EXCLUSION_JOB_NAMES :
                                ASM_ZERO_OBS_COUNT_JOBS.append(
                                    {'asm_observer_job_name': asm_observer_job_name, 'lastruntime': dt_object,
                                     'job_status': asm_observer_job_status,
                                     'status_description': asm_observer_job_status_description})

                            if asm_observer_job.get('observationCount', '') != '' \
                                    and asm_observer_job.get('observedTime', '') != '' \
                                    and asm_observer_job.get('hasState', '') != '':

                                asm_observer_job_observationCount = int(asm_observer_job.get('observationCount')) if asm_observer_job.get(
                                    'observationCount', None) is not None else 0

                                if ASM_JOBS_from_DB.get(asm_observer_job_name, '') != '':
                                    # only update the DB if the lastRuntime has changed. I.e. it has run since last recorded
                                    if ASM_JOBS_from_DB.get(asm_observer_job_name)['curr_last_run_time'] != asm_observer_job_observedTime:
                                        # current Last Run Time from the DB is actually the previous Last Run Time for this status check instance
                                        curr_last_run_time = int(
                                            ASM_JOBS_from_DB.get(asm_observer_job_name)['curr_last_run_time']) if \
                                        ASM_JOBS_from_DB.get(asm_observer_job_name)['curr_last_run_time'] is not None else None
                                        # if current_status != asm_observer_job_status:
                                        # current count from the DB is actually the previous count for this status check instance
                                        current_count = ASM_JOBS_from_DB.get(asm_observer_job_name)['current_count'] if \
                                        ASM_JOBS_from_DB.get(asm_observer_job_name)['current_count'] is not None else 0
                                        current_status = ASM_JOBS_from_DB.get(asm_observer_job_name)['current_status']
                                        # lets calculate substantial diff
                                        sql_subs_diff_percentage_part = ''
                                        sql_subs_diff_percentage_part = f", substantial_difference = 0"
                                        if asm_observer_job_status_description.lower().find('finished') != -1 and int(
                                                asm_observer_job_observationCount) > 0:  # job must be finished in the previous run
                                            difference = int(asm_observer_job_observationCount) - int(current_count)
                                            subs_diff_percentage = round(difference / int(current_count) * 100,
                                                                         2) if current_count > 0 else 0
                                            sql_subs_diff_percentage_part = f", substantial_difference = {subs_diff_percentage}"
                                            # substantial threshold breach and also previous running job should have finished status
                                            if (subs_diff_percentage > app.ASM_OBS_SUBSTANTIAL_COUNT_THRESHOLD \
                                                    or subs_diff_percentage < (
                                                            -1 * app.ASM_OBS_SUBSTANTIAL_COUNT_THRESHOLD)):
                                                dt_object_prev_last_runtime = dtm.datetime.utcfromtimestamp(
                                                    round(curr_last_run_time / 1000)).strftime(
                                                    app.DATETIMEFORMAT) if curr_last_run_time > 0 else 'Not Available'
                                                dt_object_curr_last_runtime = dtm.datetime.utcfromtimestamp(
                                                    round(asm_observer_job_observedTime / 1000)).strftime(
                                                    app.DATETIMEFORMAT) if asm_observer_job_observedTime > 0 else 'Not Available'
                                                ASM_JOBS_SUBSTANTIAL_COUNTS.append(
                                                    {'asm_observer_job_name': asm_observer_job_name, 'previous_observedTime': dt_object_prev_last_runtime,
                                                     'current_observedTime': dt_object_curr_last_runtime,
                                                     'job_status': current_status, 'previous_observation_count': current_count,
                                                     'current_observation_count': asm_observer_job_observationCount, 'count_difference': difference,
                                                     'substantial_difference_percentage': subs_diff_percentage })
                                                # print('substantial diff reached::::',curr_last_run_time,asm_observer_job_observedTime,current_count,asm_observer_job_observationCount)
                                        sql_count_update_part = ''

                                        # update the current count and the prev counts only if the jobs have not failed
                                        if not asm_observer_job_name.upper() in asm_failed_jobs_list:
                                            sql_count_update_part = f", current_count = {asm_observer_job_observationCount}, previous_count = {current_count}"

                                        _sql = f"UPDATE asm_observer_job_completion_comparison_data " \
                                               f"SET current_status = '{asm_observer_job_status}'" \
                                               f", last_run_status = '{current_status}'" \
                                               f", status_description = '{asm_observer_job_status_description}'" \
                                               f"{sql_count_update_part}" \
                                               f", prev_last_run_time = {curr_last_run_time}" \
                                               f", curr_last_run_time = {asm_observer_job_observedTime}" \
                                               f"{sql_subs_diff_percentage_part}" \
                                               f" WHERE upper(job_name) = '{asm_observer_job_name}';"
                                        print(f'origin line number------->{sys._getframe().f_lineno} SQL-->', _sql)
                                        try:
                                            cursor.execute(_sql)
                                        except Exception as e:
                                            print(e)

                                else:
                                    _sql = f"INSERT INTO asm_observer_job_completion_comparison_data " \
                                           f"SET job_name = '{asm_observer_job_name}'" \
                                           f", current_count = {asm_observer_job_observationCount}" \
                                           f", previous_count = NULL" \
                                           f", current_status = '{asm_observer_job_status}'" \
                                           f", status_description = '{asm_observer_job_status_description}'" \
                                           f", prev_last_run_time = NULL" \
                                           f", curr_last_run_time = {asm_observer_job_observedTime}" \
                                           f", substantial_difference = NULL;"
                                    print(f'origin line number------->{sys._getframe().f_lineno} SQL-->', _sql)
                                    try:
                                        cursor.execute(_sql)
                                    except Exception as e:
                                        print(e)

                            else:
                                # IDAVA-1135 Fix

                                # The below code was causing insertion of duplicate rows when there is no change in the observed data. Also fix for exception when the map entry is not present causing invalid subscript exc.
                                # curr_last_run_time = int(
                                #     ASM_JOBS_from_DB.get(asm_observer_job_name)['curr_last_run_time']) if \
                                #     ASM_JOBS_from_DB.get(asm_observer_job_name)['curr_last_run_time'] is not None else None

                                # if ASM_JOBS_from_DB.get(asm_observer_job_name, '') != '' and curr_last_run_time != asm_observer_job_observedTime:
                                if asm_observer_job_name in ASM_JOBS_from_DB:
                                    curr_last_run_time = int(
                                        ASM_JOBS_from_DB.get(asm_observer_job_name)['curr_last_run_time']) if \
                                        ASM_JOBS_from_DB.get(asm_observer_job_name)['curr_last_run_time'] is not None else None
                                    if curr_last_run_time != asm_observer_job_observedTime:
                                        _sql = f"UPDATE asm_observer_job_completion_comparison_data " \
                                            f"SET current_count = NULL" \
                                            f", previous_count = NULL" \
                                            f", current_status = '{asm_observer_job_status}'" \
                                            f", status_description = '{asm_observer_job_status_description}'" \
                                            f", last_run_status = ''" \
                                            f", prev_last_run_time = {curr_last_run_time}" \
                                            f", curr_last_run_time = {asm_observer_job_observedTime}" \
                                            f", substantial_difference = NULL" \
                                            f" WHERE upper(job_name) = '{asm_observer_job_name}';"
                                        print(f'origin line number------->{sys._getframe().f_lineno} SQL-->', _sql)
                                else:
                                    _sql = f"INSERT INTO asm_observer_job_completion_comparison_data " \
                                           f"SET job_name = '{asm_observer_job_name}'" \
                                           f", previous_count = NULL" \
                                           f", current_count = NULL" \
                                           f", current_status = '{asm_observer_job_status}'" \
                                           f", last_run_status = ''" \
                                           f", status_description = '{asm_observer_job_status_description}'" \
                                           f", prev_last_run_time = NULL" \
                                           f", curr_last_run_time = {asm_observer_job_observedTime}" \
                                           f", substantial_difference = NULL;"
                                    print(f'origin line number------->{sys._getframe().f_lineno} SQL-->', _sql)
                                try:
                                    cursor.execute(_sql)
                                except Exception as e:
                                    print(e)

                except Exception as e:
                    print(e)
                    print("Global error")

        except DatabaseError as e:
            print('Error from job.queryASMObserverReportDataGatherer------>nonothers', e)
        # IDAVA-1135. I thought it's better to be sorted on email
        ASM_ZERO_OBS_COUNT_JOBS = sorted(ASM_ZERO_OBS_COUNT_JOBS, key=lambda d: d['lastruntime'], reverse=True)
        ASM_FAILED_JOBS = sorted(ASM_FAILED_JOBS, key=lambda d: d['lastruntime'], reverse=True)
        ASM_JOBS_SUBSTANTIAL_COUNTS = sorted(ASM_JOBS_SUBSTANTIAL_COUNTS, key=lambda d: d['count_difference'], reverse=True)
        return ASM_FAILED_JOBS, ASM_JOBS_SUBSTANTIAL_COUNTS, ASM_ZERO_OBS_COUNT_JOBS

    @staticmethod
    def ASM_Inventory_Datacenter_Data():
        ASMInventoryDataCenterCounts = []
        try:
            with connections['logsnapsverizonedb'].cursor() as cursorvz:
                # retrieving FM report data
                sql = f"select name as site_name, total_nfs from ASM_INVENTORY_DATACENTER_DATA ORDER BY total_nfs desc;"
                print('retrieving ASM inventory data ASM_Inventory_Datacenter_Data------>', sql)
                try:
                    cursorvz.execute(sql)
                    desc = cursorvz.description
                    fields = [col[0] for col in desc]
                    results = cursorvz.fetchall()
                    if len(results) > 0:
                        for row in results:
                            ASMInventoryDataCenterCounts.append( {'site_name': row[0], 'total_nfs': row[1], })

                except Exception as e:
                    print(e)

        except DatabaseError as e:
            print('Error from ASM_Inventory_Datacenter_Data------>verizon', e)
        return ASMInventoryDataCenterCounts

    @staticmethod
    def ASM_Observer_Data(dataformat):
        ASM_JOBS_from_DB = []
        try:
            with connections['logsnapsreporter_non_others_db'].cursor() as cursor:
                # retrieving FM report data
                # sql = f"SELECT UPPER(NFTYPE) NFTYPE, DATE_FORMAT(MAX(reportdate),'%Y-%m-%d') reportdate FROM nftype_alerts_counts_report  WHERE NFTYPE<>'' AND {sqlpart_reporter_db} GROUP BY UPPER(NFTYPE) HAVING (SUM(CRITICAL) + SUM(MAJOR) + SUM(MINOR) + SUM(WARNING) + SUM(INDETERMINATE) + SUM(CLEAR)) > 0;"
                sql = f"SELECT upper(job_name),previous_count, current_count, current_status, status_description, prev_last_run_time, curr_last_run_time, substantial_difference FROM asm_observer_job_completion_comparison_data;"
                print('retrieving ASM Job data job.ASM_Observer_Data------>', sql)
                try:
                    cursor.execute(sql)
                    desc = cursor.description
                    # fields = [col[0] for col in desc]
                    results = cursor.fetchall()
                    if len(results) > 0:
                        if dataformat == 'api':
                            for row in results:
                                dt_object1 = dtm.datetime.utcfromtimestamp(
                                    round(row[5] / 1000)).strftime(
                                    app.DATETIMEFORMAT) if (row[5] is not None and row[5] > 0) else '-'
                                dt_object2 = dtm.datetime.utcfromtimestamp(
                                    round(row[6] / 1000)).strftime(
                                    app.DATETIMEFORMAT) if (row[6] is not None and row[6] > 0) else '-'
                                ASM_JOBS_from_DB.append({'job_name': row[0], 'previous_count': row[1], 'current_count': row[2], 'current_status': row[3], 'status_description':row[4],
                                                         'prev_last_run_time': dt_object1, 'curr_last_run_time': dt_object2,'substantial_difference': row[7]})
                        elif dataformat == 'html':
                            for row in results:
                                dt_object1 = dtm.datetime.utcfromtimestamp(
                                    round(row[5] / 1000)).strftime(
                                    app.DATETIMEFORMAT) if row[5] is not None and row[5] > 0 else 'Not Available'
                                dt_object2 = dtm.datetime.utcfromtimestamp(
                                    round(row[6] / 1000)).strftime(
                                    app.DATETIMEFORMAT) if row[6] is not None and row[6] > 0 else 'Not Available'
                                ASM_JOBS_from_DB.append([row[0], row[1], row[2], row[3], row[4], dt_object1, dt_object2, row[7]])

                except Exception as e:
                    print(e)

        except DatabaseError as e:
            print('Error from job.ASM_Observer_Data------>nonothers', e)
        return ASM_JOBS_from_DB

    @staticmethod
    def prepareMissingDMLFiles7daysOldData(format, datapart=['vDU','ACPF','AUPF','DU']):
        resultMissingOldDMLAuditData = {}
        queries = {
            'vDU': f"Select fileId, CreateDate, neId, filename, sourceDirectory, marketId, processedTimestamp from DML_ADPF_MISSING_LIST where DATE_FORMAT(deletedTimestamp, '%Y-%m-%d') = (SELECT DATE_FORMAT(MAX(deletedTimestamp), '%Y-%m-%d') FROM DML_ADPF_MISSING_LIST)",
            'ACPF': f"Select fileId, CreateDate, neId, filename, sourceDirectory, marketId, processedTimestamp from DML_ACPF_MISSING_LIST where DATE_FORMAT(deletedTimestamp, '%Y-%m-%d') = (SELECT DATE_FORMAT(MAX(deletedTimestamp), '%Y-%m-%d') FROM DML_ACPF_MISSING_LIST)",
            'AUPF': f"Select fileId, CreateDate, neId, filename, sourceDirectory, marketId, processedTimestamp from DML_AUPF_MISSING_LIST where DATE_FORMAT(deletedTimestamp, '%Y-%m-%d') = (SELECT DATE_FORMAT(MAX(deletedTimestamp), '%Y-%m-%d') FROM DML_AUPF_MISSING_LIST)",
            'DU': f"Select fileId, CreateDate, neId, filename, sourceDirectory, marketId, processedTimestamp from DML_DU_MISSING_LIST where DATE_FORMAT(deletedTimestamp, '%Y-%m-%d') = (SELECT DATE_FORMAT(MAX(deletedTimestamp), '%Y-%m-%d') FROM DML_DU_MISSING_LIST)"
        }
        try:
            with connections['logsnapsverizonedb'].cursor() as cursorvz:
                for nftype, sql in queries.items():
                    if nftype in datapart:
                        print('retrieving ASM Job data job.prepareMissingDMLFiles7daysOldData------>', sql)
                        cursorvz.execute(sql)
                        results = cursorvz.fetchall()
                        if len(results) > 0:
                            desc = cursorvz.description
                            fields = [col[0] for col in desc]
                            if format == 'csv':
                                resultMissingOldDMLAuditData[nftype] = {}
                                resultMissingOldDMLAuditData[nftype]['data'] = ','.join(fields) + '\n'
                                for row in results:
                                    resultMissingOldDMLAuditData[nftype]['data'] += ','.join((str(v) if v is not None else '-') for v in row)  + '\n'
                            else:
                                if len(results) > 0:
                                    resultMissingOldDMLAuditData[nftype] = {}
                                    resultMissingOldDMLAuditData[nftype]['data'] = []

                                    for row in results:
                                        eachRow = {}
                                        for i in range(len(fields)):
                                            eachRow[fields[i]] = row[i]
                                        resultMissingOldDMLAuditData[nftype]['data'].append(eachRow)

        except DatabaseError as e:
            print('Error from dataentities.prepareMissingDMLFiles7daysOldData------>verizon', e)
        return resultMissingOldDMLAuditData

    @staticmethod
    def prepareDMLFileAuditSummaryData():
        # yesterday = date.today() - timedelta(days=1)
        # yesterday = yesterday.strftime("%Y-%m-%d")
        dataRes = {
            'resultDMLFileAuditSummaryData': f"select DATE_FORMAT(`Date`, '%Y-%m-%d') as `summary_date`,Domain,FilesNotReceived,FilesReceived,FilesProcessed from DML_Config_Summary_Report where  DATE_FORMAT(`Date`, '%Y-%m-%d')=DATE_FORMAT(CURRENT_DATE(), '%Y-%m-%d')",
            'resultDMLFileAuditSummaryData_SW': f"select DATE_FORMAT(`Date`, '%Y-%m-%d') as `summary_date`,Domain,FilesNotReceived,FilesReceived,FilesProcessed from DML_Software_Summary_Report where  DATE_FORMAT(`Date`, '%Y-%m-%d')=DATE_FORMAT(CURRENT_DATE(), '%Y-%m-%d')"
        }

        try:
            with connections['logsnapsverizonedb'].cursor() as cursorvz:
                for datatype, sql in dataRes.items():
                    print('retrieving ASM Job data job.prepareDMLFileAuditSummaryData------>', sql)
                    dataRes[datatype] = {}

                    cursorvz.execute(sql)
                    results = cursorvz.fetchall()

                    if len(results) > 0 :
                        dataRes[datatype]['data'] = []
                        desc = cursorvz.description
                        fields = [col[0] for col in desc]
                        for row in results:
                            eachRow = {}
                            for i in range(len(fields)):
                                eachRow[fields[i]] = row[i]
                            dataRes[datatype]['data'].append(eachRow)

        except DatabaseError as e:
            print('Error from dataentities.prepareDMLFileAuditSummaryData------>verizon', e)
            dataRes = {
                'resultDMLFileAuditSummaryData': {},
                'resultDMLFileAuditSummaryData_SW': {}
            }
        return dataRes

    @staticmethod
    def prepareDMLCountPerOSSData():
        # yesterday = date.today() - timedelta(days=1)
        # yesterday = yesterday.strftime("%Y-%m-%d")
        dataRes = {
            'ADPF': f"SELECT SourceDirectory AS Directory,count(*) AS Count_Of_Missing_Files FROM DML_ADPF_CONFIG where processedTimestamp <= CONCAT( DATE_FORMAT((NOW() - Interval 1 day) , '%Y-%m-%d'), ' 23:59:59') GROUP BY SourceDirectory;",
            'ACPF': f"SELECT SourceDirectory AS Directory,count(*) AS Count_Of_Missing_Files FROM DML_ACPF_CONFIG where processedTimestamp <= CONCAT( DATE_FORMAT((NOW() - Interval 1 day) , '%Y-%m-%d'), ' 23:59:59') GROUP BY SourceDirectory;",
            'AUPF': f"SELECT SourceDirectory AS Directory,count(*) AS Count_Of_Missing_Files FROM DML_AUPF_CONFIG where processedTimestamp <= CONCAT( DATE_FORMAT((NOW() - Interval 1 day) , '%Y-%m-%d'), ' 23:59:59') GROUP BY SourceDirectory;",
            'DU': f"SELECT SourceDirectory AS Directory,count(*) AS Count_Of_Missing_Files FROM DML_DU_CONFIG where processedTimestamp <= CONCAT( DATE_FORMAT((NOW() - Interval 1 day) , '%Y-%m-%d'), ' 23:59:59') GROUP BY SourceDirectory;"
        }

        try:
            with connections['logsnapsverizonedb'].cursor() as cursorvz:
                for NFType, sql in dataRes.items():
                    print('retrieving ASM Job data job.prepareDMLCountPerOSSData------>', sql)
                    dataRes[NFType] = {}
                    # dataRes[NFType]['data'] = [
                    #     {'Directory': 'sa_charlotte_oss',
                    #      'Count_Of_Missing_Files': 100},
                    #     {'Directory': 'sa_westborough_2_oss',
                    #      'Count_Of_Missing_Files': 100},
                    #     {'Directory': 'sa_memphis_oss',
                    #      'Count_Of_Missing_Files': 100},
                    # ]

                    cursorvz.execute(sql)
                    results = cursorvz.fetchall()

                    if len(results) > 0:
                        dataRes[NFType]['data'] = []
                        desc = cursorvz.description
                        fields = [col[0] for col in desc]
                        for row in results:
                            eachRow = {}
                            for i in range(len(fields)):
                                eachRow[fields[i]] = row[i]
                            dataRes[NFType]['data'].append(eachRow)


        except DatabaseError as e:
            print('Error from dataentities.prepareDMLFileAuditSummaryData------>verizon', e)
            dataRes = {
                'ADPF': {},
                'ACPF': {},
                'AUPF': {},
                'DU': {}
            }
        return dataRes

    @staticmethod
    def prepareMissingDMLFileAuditReportsData(format, datapart=['vDU','ACPF','AUPF','DU']):
        yesterday = date.today() - timedelta(days=1)
        yesterday = yesterday.strftime("%Y-%m-%d")
        resultDMLAuditReportsData = {}
        resultDMLAuditReportsData_SW = {}
        queries = {
            'vDU': f"Select neId, filename, sourceDirectory, gnodebId, marketId, processedTimestamp, modifydate from DML_ADPF_CONFIG where processedTimestamp <= '{yesterday} 23:59:59' and filename not like '%SW.xml.gz'",
            'ACPF': f"Select neId, filename, sourceDirectory, gnodebId, marketId, processedTimestamp, modifydate from DML_ACPF_CONFIG where processedTimestamp <= '{yesterday} 23:59:59' and filename not like '%SW.xml.gz'",
            'AUPF': f"Select neId, filename, sourceDirectory, gnodebId, marketId, processedTimestamp, modifydate from DML_AUPF_CONFIG where processedTimestamp <= '{yesterday} 23:59:59' and filename not like '%SW.xml.gz'",
            'DU': f"Select neId, filename, sourceDirectory, gnodebId, marketId, processedTimestamp, modifydate from DML_DU_CONFIG where processedTimestamp <= '{yesterday} 23:59:59' and filename not like '%SW.xml.gz'"
        }
        queries_sw = {
            'vDU': f"Select neId, filename, sourceDirectory, gnodebId, marketId, processedTimestamp, modifydate from DML_ADPF_CONFIG where processedTimestamp <= '{yesterday} 23:59:59' and filename like '%SW.xml.gz'",
            'ACPF': f"Select neId, filename, sourceDirectory, gnodebId, marketId, processedTimestamp, modifydate from DML_ACPF_CONFIG where processedTimestamp <= '{yesterday} 23:59:59' and filename like '%SW.xml.gz'",
            'AUPF': f"Select neId, filename, sourceDirectory, gnodebId, marketId, processedTimestamp, modifydate from DML_AUPF_CONFIG where processedTimestamp <= '{yesterday} 23:59:59' and filename like '%SW.xml.gz'",
            'DU': f"Select neId, filename, sourceDirectory, gnodebId, marketId, processedTimestamp, modifydate from DML_DU_CONFIG where processedTimestamp <= '{yesterday} 23:59:59' and filename like '%SW.xml.gz'"
        }

        try:
            with connections['logsnapsverizonedb'].cursor() as cursorvz, connections['logsnapsreporter_non_others_db'].cursor() as cursor_nonothers:
                for nftype, sql in queries.items():
                    if nftype in datapart:
                        print('retrieving ASM Job data job.prepareMissingDMLFileAuditReportsData------>', sql)
                        cursorvz.execute(sql)
                        results = cursorvz.fetchall()


                        cursorvz.execute(queries_sw[nftype])
                        results_sw = cursorvz.fetchall()

                        if len(results) > 0 or len(results_sw)>0:
                            neids = []
                            neids_sw = []

                            desc = cursorvz.description
                            fields = [col[0] for col in desc]
                            if format == 'csv':
                                if len(results) > 0:
                                    resultDMLAuditReportsData[nftype] = {}
                                    resultDMLAuditReportsData[nftype]['data'] = ','.join(fields) + '\n'
                                    for row in results:
                                        neids.append(row[0])
                                        resultDMLAuditReportsData[nftype]['data'] += ','.join((str(v).replace(',',';') if v is not None else '-') for v in row)  + '\n'
                                if len(results_sw) > 0:
                                    resultDMLAuditReportsData_SW[nftype] = {}
                                    resultDMLAuditReportsData_SW[nftype]['data'] = ','.join(fields) + '\n'
                                    for row in results_sw:
                                        neids_sw.append(row[0])
                                        resultDMLAuditReportsData_SW[nftype]['data'] += ','.join(
                                            (str(v) if v is not None else '-') for v in row) + '\n'
                            else:
                                if len(results) > 0:
                                    resultDMLAuditReportsData[nftype] = {}
                                    resultDMLAuditReportsData[nftype]['data'] = []

                                    for row in results:
                                        eachRow = {}
                                        neids.append(row[0])
                                        for i in range(len(fields)):
                                            eachRow[fields[i]] = row[i]
                                        resultDMLAuditReportsData[nftype]['data'].append(eachRow)
                                if len(results_sw) > 0:
                                    resultDMLAuditReportsData_SW[nftype] = {}
                                    resultDMLAuditReportsData_SW[nftype]['data'] = []

                                    for row in results_sw:
                                        eachRow = {}
                                        neids_sw.append(row[0])
                                        for i in range(len(fields)):
                                            eachRow[fields[i]] = row[i]
                                        resultDMLAuditReportsData_SW[nftype]['data'].append(eachRow)


                            resultDMLAuditReportsData[nftype]['valid'] = False
                            resultDMLAuditReportsData_SW[nftype]['valid'] = False
                            lenneids = len(neids)
                            print('######### returned :',lenneids, ' results #########')
                            if lenneids > 0:
                                if lenneids>100: # if it is a big list, lets break it into 100 items chunk and keep on searching
                                    turns = lenneids//100
                                    for i in range(turns):
                                        str_neids = "'" + "','".join(neids[100*i:(100*i+100)]) + "'"
                                        if str_neids!='':
                                            sql_validate_reporter_status = f"SELECT COUNT(*) FROM reporter_status WHERE nfname IN ({str_neids}) AND (Summary LIKE '%Netconf%' OR Summary LIKE '%Service off%') AND lastoccurrence>= (NOW() - INTERVAL 1 DAY);"
                                            cursor_nonothers.execute(sql_validate_reporter_status)
                                            validity_result = cursor_nonothers.fetchall()
                                            print(f'record processing:{100*i+100}---->',int(validity_result[0][0]))
                                            if int(validity_result[0][0]) > 0:
                                                break
                                else:
                                    str_neids = "'" + "','".join(neids) + "'"
                                    sql_validate_reporter_status = f"SELECT COUNT(*) FROM reporter_status WHERE nfname IN ({str_neids}) AND (Summary LIKE '%Netconf%' OR Summary LIKE '%Service off%') AND lastoccurrence>= (NOW() - INTERVAL 1 DAY);"
                                    cursor_nonothers.execute(sql_validate_reporter_status)
                                    validity_result = cursor_nonothers.fetchall()

                                resultDMLAuditReportsData[nftype]['valid'] = True
                                if len(validity_result) > 0 and len(validity_result[0]) > 0 and int(
                                        validity_result[0][0]) > 0:
                                    resultDMLAuditReportsData[nftype]['valid'] = False

                                lenneids = len(neids_sw)
                                print('######### returned :', lenneids, ' results for SW #########')
                                if lenneids > 0:
                                    if lenneids > 100:  # if it is a big list, lets break it into 100 items chunk and keep on searching
                                        turns = lenneids // 100
                                        for i in range(turns):
                                            str_neids = "'" + "','".join(neids_sw[100 * i:(100 * i + 100)]) + "'"
                                            if str_neids != '':
                                                sql_validate_reporter_status = f"SELECT COUNT(*) FROM reporter_status WHERE nfname IN ({str_neids}) AND (Summary LIKE '%Netconf%' OR Summary LIKE '%Service off%') AND lastoccurrence>= (NOW() - INTERVAL 1 DAY);"
                                                cursor_nonothers.execute(sql_validate_reporter_status)
                                                validity_result = cursor_nonothers.fetchall()
                                                print(f'record processing:{100 * i + 100}---->',
                                                      int(validity_result[0][0]))
                                                if int(validity_result[0][0]) > 0:
                                                    break
                                    else:
                                        str_neids = "'" + "','".join(neids_sw) + "'"
                                        sql_validate_reporter_status = f"SELECT COUNT(*) FROM reporter_status WHERE nfname IN ({str_neids}) AND (Summary LIKE '%Netconf%' OR Summary LIKE '%Service off%') AND lastoccurrence>= (NOW() - INTERVAL 1 DAY);"
                                        cursor_nonothers.execute(sql_validate_reporter_status)
                                        validity_result = cursor_nonothers.fetchall()
                                resultDMLAuditReportsData_SW[nftype]['valid'] = True
                                if len(validity_result)>0 and len(validity_result[0])>0 and int(validity_result[0][0]) > 0:
                                    resultDMLAuditReportsData_SW[nftype]['valid'] = False

        except DatabaseError as e:
            print('Error from dataentities.prepareMissingDMLFileAuditReportsData------>verizon', e)
        return resultDMLAuditReportsData, resultDMLAuditReportsData_SW

    @staticmethod
    def get_market_ids():
        # get a consolidated set of market_ids
        market_ids = {}
        market_id_sqls = [
            "Select distinct(marketId) from DML_ADPF_CONFIG",
            "Select distinct(marketId) from DML_ACPF_CONFIG",
            "Select distinct(marketId) from DML_AUPF_CONFIG",
            "Select distinct(marketId) from DML_DU_CONFIG"
        ]
        try:
            with connections['logsnapsverizonedb'].cursor() as cursorvz:
                for sql in market_id_sqls:
                    cursorvz.execute(sql)
                    market_results = cursorvz.fetchall()
                    for row in market_results:
                        market_ids[row[0]] = 1
                market_ids = list(market_ids.keys())
        except DatabaseError as e:
            print('Error from dataentities.get_market_ids------>verizon', e)
        return market_ids

    @staticmethod
    def prepareDuplicateUSMCountAllMarketIdsReportsData(format, datapart=['vDU', 'ACPF', 'AUPF', 'DU']):
        resultDuplicateUSMCountData = {}
        # queries = {
        #     'vDU': f"Select neId, marketId, JSON_LENGTH(data, '$.\"config\".\"managed-element\".\"common-management\".\"ems-configuration\".\"ems-ip-address\"') as USM_count, JSON_EXTRACT(data, '$.\"config\".\"managed-element\".\"common-management\".\"ems-configuration\".\"ems-ip-address\"') as USM_IP from DML_ADPF_CONFIG where marketId = \"MARKET_ID\";",
        #     'ACPF': f"Select neId, marketId, JSON_LENGTH(data, '$.\"config\".\"managed-element\".\"common-management\".\"ems-configuration\".\"ems-ip-address\"') as USM_count, JSON_EXTRACT(data, '$.\"config\".\"managed-element\".\"common-management\".\"ems-configuration\".\"ems-ip-address\"') as USM_IP from DML_ACPF_CONFIG where marketId = \"MARKET_ID\";",
        #     'AUPF': f"Select neId, marketId, JSON_LENGTH(data, '$.\"config\".\"managed-element\".\"common-management\".\"ems-configuration\".\"ems-ip-address\"') as USM_count, JSON_EXTRACT(data, '$.\"config\".\"managed-element\".\"common-management\".\"ems-configuration\".\"ems-ip-address\"') as USM_IP from DML_AUPF_CONFIG where marketId = \"MARKET_ID\";",
        #     'DU': f"Select neId, marketId, JSON_LENGTH(data, '$.\"config\".\"managed-element\".\"common-management\".\"ems-configuration\".\"ems-ip-address\"') as USM_count, JSON_EXTRACT(data, '$.\"config\".\"managed-element\".\"common-management\".\"ems-configuration\".\"ems-ip-address\"') as USM_IP from DML_DU_CONFIG where marketId = \"MARKET_ID\";"
        # }
        # queries = {
        #     'vDU': f"Select neId, marketId, JSON_LENGTH(data, '$.\"config\".\"managed-element\".\"common-management\".\"ems-configuration\".\"ems-ip-address\"') as USM_count, CASE WHEN JSON_VALID(Data) THEN JSON_EXTRACT(data, '$.\"config\".\"managed-element\".\"common-management\".\"ems-configuration\".\"ems-ip-address\"') ELSE null END as USM_IP from DML_ADPF_CONFIG where marketId = \"MARKET_ID\";",
        #     'ACPF': f"Select neId, marketId, JSON_LENGTH(data, '$.\"config\".\"managed-element\".\"common-management\".\"ems-configuration\".\"ems-ip-address\"') as USM_count, CASE WHEN JSON_VALID(Data) THEN JSON_EXTRACT(data, '$.\"config\".\"managed-element\".\"common-management\".\"ems-configuration\".\"ems-ip-address\"') ELSE null END as USM_IP from DML_ACPF_CONFIG where marketId = \"MARKET_ID\";",
        #     'AUPF': f"Select neId, marketId, JSON_LENGTH(data, '$.\"config\".\"managed-element\".\"common-management\".\"ems-configuration\".\"ems-ip-address\"') as USM_count, CASE WHEN JSON_VALID(Data) THEN JSON_EXTRACT(data, '$.\"config\".\"managed-element\".\"common-management\".\"ems-configuration\".\"ems-ip-address\"') ELSE null END as USM_IP from DML_AUPF_CONFIG where marketId = \"MARKET_ID\";",
        #     'DU': f"Select neId, marketId, JSON_LENGTH(data, '$.\"config\".\"managed-element\".\"common-management\".\"ems-configuration\".\"ems-ip-address\"') as USM_count, CASE WHEN JSON_VALID(Data) THEN JSON_EXTRACT(data, '$.\"config\".\"managed-element\".\"common-management\".\"ems-configuration\".\"ems-ip-address\"') ELSE null END as USM_IP from DML_DU_CONFIG where marketId = \"MARKET_ID\";"
        # }
        queries = {
            'vDU': f"Select dadpfc.neId, dadpfc.marketId, JSON_LENGTH(DC.usm_ip) as USM_count, DC.usm_ip as USM_IP from DML_ADPF_CONFIG dadpfc, DML_EXTRACTED_CONFIG DC where dadpfc.marketId = \"MARKET_ID\" and dadpfc.fileId = DC.fileId and DC.nftype='ADPF' and JSON_LENGTH(DC.usm_ip) > 1;",
            'ACPF': f"Select dacpfc.neId, dacpfc.marketId, JSON_LENGTH(DC.usm_ip) as USM_count, DC.usm_ip as USM_IP from DML_ACPF_CONFIG dacpfc, DML_EXTRACTED_CONFIG DC where dacpfc.marketId = \"MARKET_ID\" and dacpfc.fileId = DC.fileId and DC.nftype='ACPF' and JSON_LENGTH(DC.usm_ip) > 1;",
            'AUPF': f"Select daupfc.neId, daupfc.marketId, JSON_LENGTH(DC.usm_ip) as USM_count, DC.usm_ip as USM_IP from DML_AUPF_CONFIG daupfc, DML_EXTRACTED_CONFIG DC where daupfc.marketId = \"MARKET_ID\" and daupfc.fileId = DC.fileId and DC.nftype='AUPF' and JSON_LENGTH(DC.usm_ip) > 1;",
            'DU': f"Select dduc.neId, dduc.marketId, JSON_LENGTH(DC.usm_ip) as USM_count, DC.usm_ip as USM_IP from DML_DU_CONFIG dduc, DML_EXTRACTED_CONFIG DC where dduc.marketId = \"MARKET_ID\" and dduc.fileId = DC.fileId and DC.nftype='DU' and JSON_LENGTH(DC.usm_ip) > 1;"
        }
        """select
        count(daupfc. *) as AUPF_unlock
        from DML_AUPF_CONFIG daupfc, DML_EXTRACTED_CONFIG DC
        where
        daupfc.marketId = MARKET_ID
        and DC.administrative_state = \"unlocked\" and daupfc.fileId = DC.fileId"""

        market_ids = __class__.get_market_ids()
        if len(market_ids)>0:
            try:
                with connections['logsnapsverizonedb'].cursor() as cursorvz, connections['logsnapsreporter_non_others_db'].cursor() as cursor_nonothers:
                    for market_id in market_ids:
                        if market_id is not None:
                            for nftype, sql in queries.items():
                                sql = sql.replace('MARKET_ID', str(market_id))
                                if nftype in datapart:
                                    cursorvz.execute(sql)
                                    results = cursorvz.fetchall()
                                    print(f'##### {sql}\n######### returned :', len(results), ' results #########')
                                    if len(results) > 0:
                                        desc = cursorvz.description
                                        fields = [col[0] for col in desc]
                                        if resultDuplicateUSMCountData.get(nftype, '') == '':
                                            resultDuplicateUSMCountData[nftype] = {}
                                            resultDuplicateUSMCountData[nftype]['neids'] = []
                                        if format == 'csv':

                                            resultDuplicateUSMCountData[nftype]['data'] = ','.join(fields) + '\n'
                                            for row in results:
                                                resultDuplicateUSMCountData[nftype]['neids'].append(row[0])
                                                resultDuplicateUSMCountData[nftype]['data'] += ','.join((str(v) if v is not None else '-') for v in row) + '\n'

                                        else:
                                            resultDuplicateUSMCountData[nftype]['data'] = []

                                            for row in results:
                                                eachRow = {}
                                                resultDuplicateUSMCountData[nftype]['neids'].append(row[0])
                                                for i in range(len(fields)):
                                                    eachRow[fields[i]] = row[i]
                                                resultDuplicateUSMCountData[nftype]['data'].append(eachRow)

                        # for nftype, sql in queries.items():
                        #     resultDuplicateUSMCountData[nftype]['valid'] = False
                        #     lenneids = len(resultDuplicateUSMCountData[nftype]['neids'])
                        #     print('######### returned :', lenneids, ' results #########')
                        #     if lenneids > 0:
                        #         if lenneids > 100:  # if it is a big list, lets break it into 100 items chunk and keep on searching
                        #             turns = lenneids // 100
                        #             for i in range(turns):
                        #                 str_neids = "'" + "','".join(resultDuplicateUSMCountData[nftype]['neids'][100 * i:(100 * i + 100)]) + "'"
                        #                 if str_neids != '':
                        #                     sql_validate_reporter_status = f"SELECT COUNT(*) FROM reporter_status WHERE nfname IN ({str_neids}) AND (Summary LIKE '%Netconf%' OR Summary LIKE '%Service off%') AND lastoccurrence>= (NOW() - INTERVAL 1 DAY);"
                        #                     cursor_nonothers.execute(sql_validate_reporter_status)
                        #                     validity_result = cursor_nonothers.fetchall()
                        #                     print(f'record processing:{100 * i + 100}---->',
                        #                           int(validity_result[0][0]))
                        #                     if int(validity_result[0][0]) > 0:
                        #                         break
                        #         else:
                        #             str_neids = "'" + "','".join(resultDuplicateUSMCountData[nftype]['neids']) + "'"
                        #             sql_validate_reporter_status = f"SELECT COUNT(*) FROM reporter_status WHERE nfname IN ({str_neids}) AND (Summary LIKE '%Netconf%' OR Summary LIKE '%Service off%') AND lastoccurrence>= (NOW() - INTERVAL 1 DAY);"
                        #             cursor_nonothers.execute(sql_validate_reporter_status)
                        #             validity_result = cursor_nonothers.fetchall()
                        #         resultDuplicateUSMCountData[nftype]['valid'] = True
                        #         if len(validity_result) > 0 and len(validity_result[0]) > 0 and int(
                        #                 validity_result[0][0]) > 0:
                        #             resultDuplicateUSMCountData[nftype]['valid'] = False

            except DatabaseError as e:
                print('Error from dataentities.prepareDuplicateUSMCountAllMarketIdsReportsData------>verizon', e)
        return resultDuplicateUSMCountData


    @staticmethod
    def prepareDMLAuditMarketLevelReportData():
        from itertools import chain
        base_result_structure = [
            {'Market': "",
             'Type Of NFs': 'vDU',
             'Count of NFs in SQL Query': "Select count(*) from DML_ADPF_CONFIG where marketId = MARKET_ID",
             'Band Sub': "Select DC.band from DML_ADPF_CONFIG dadpfc, DML_EXTRACTED_CONFIG DC where dadpfc.marketId = MARKET_ID and dadpfc.fileId = DC.fileId and DC.nftype='ADPF'",
             'Band Sub3': 0,
             'Band Sub4': 0,
             'Band SubMMW': 0,
             'Count of gNodeBs': "Select count(distinct(gnodebId)) from DML_ADPF_CONFIG where marketId = MARKET_ID",
             'Count of Cells': "Select JSON_LENGTH(DC.cell_identity) as cell_count from DML_ADPF_CONFIG dadpfc, DML_EXTRACTED_CONFIG DC where dadpfc.marketId = MARKET_ID and dadpfc.fileId = DC.fileId and DC.nftype='ADPF'",
             'Admin Status - Unlocked': "select count(*) as ADPF_unlock from DML_ADPF_CONFIG dadpfc, DML_EXTRACTED_CONFIG DC where dadpfc.marketId = MARKET_ID and DC.administrative_state = '\"unlocked\"' and dadpfc.fileId = DC.fileId and DC.nftype='ADPF'",
             'Admin Status - Locked': "select count(*) as ADPF_unlock from DML_ADPF_CONFIG dadpfc, DML_EXTRACTED_CONFIG DC where dadpfc.marketId = MARKET_ID and DC.administrative_state = '\"locked\"' and dadpfc.fileId = DC.fileId and DC.nftype='ADPF'",
             'Band Other': 0,
             'Admin Status - Other': 0,
             },
            {'Market': "",
             'Type Of NFs': 'ACPF',
             'Count of NFs in SQL Query': "Select count(*) from DML_ACPF_CONFIG where marketId = MARKET_ID",
             'Band Sub': None,
             'Band Sub3': 0,
             'Band Sub4': 0,
             'Band SubMMW': 0,
             'Count of gNodeBs': "Select JSON_LENGTH(concat('[',gnodebId,']')) from DML_ACPF_CONFIG where marketId = MARKET_ID",
             'Count of Cells': 0,
             'Admin Status - Unlocked': "select count(*) as ACPF_unlock from DML_ACPF_CONFIG dacpfc, DML_EXTRACTED_CONFIG DC where dacpfc.marketId = MARKET_ID and DC.administrative_state = '\"unlocked\"' and dacpfc.fileId = DC.fileId and DC.nftype='ACPF'",
             'Admin Status - Locked': "select count(*) as ACPF_unlock from DML_ACPF_CONFIG dacpfc, DML_EXTRACTED_CONFIG DC where dacpfc.marketId = MARKET_ID and DC.administrative_state = '\"locked\"' and dacpfc.fileId = DC.fileId and DC.nftype='ACPF'",
             'Band Other': 0,
             'Admin Status - Other': 0,
             },
            {'Market': "",
             'Type Of NFs': 'AUPF',
             'Count of NFs in SQL Query': "Select count(*) from DML_AUPF_CONFIG where marketId = MARKET_ID",
             'Band Sub': None,
             'Band Sub3': 0,
             'Band Sub4': 0,
             'Band SubMMW': 0,
             'Count of gNodeBs': "Select JSON_LENGTH(concat('[',gnodebId,']')) from DML_AUPF_CONFIG where marketId = MARKET_ID",
             'Count of Cells': 0,
             'Admin Status - Unlocked': "select count(*) as AUPF_unlock from DML_AUPF_CONFIG daupfc, DML_EXTRACTED_CONFIG DC where daupfc.marketId = MARKET_ID and DC.administrative_state = '\"unlocked\"' and daupfc.fileId = DC.fileId and DC.nftype='AUPF'",
             'Admin Status - Locked': "select count(*) as AUPF_unlock from DML_AUPF_CONFIG daupfc, DML_EXTRACTED_CONFIG DC where daupfc.marketId = MARKET_ID and DC.administrative_state = '\"locked\"'and daupfc.fileId = DC.fileId and DC.nftype='AUPF'",
             'Band Other': 0,
             'Admin Status - Other': 0,
             },
            {'Market': "",
             'Type Of NFs': 'DU',
             'Count of NFs in SQL Query': "Select count(*) from DML_DU_CONFIG where marketID = MARKET_ID",
             'Band Sub': "Select DC.band from DML_DU_CONFIG dduc, DML_EXTRACTED_CONFIG DC where dduc.marketId = MARKET_ID and dduc.fileId = DC.fileId and DC.nftype='DU'",
             'Band Sub3': 0,
             'Band Sub4': 0,
             'Band SubMMW': 0,
             'Count of gNodeBs': "Select count(distinct(gnodebId)) from DML_DU_CONFIG where marketId = MARKET_ID",
             'Count of Cells': "Select JSON_LENGTH(DC.cell_identity) as cell_count from DML_DU_CONFIG dduc, DML_EXTRACTED_CONFIG DC where dduc.marketId = MARKET_ID and dduc.fileId = DC.fileId and DC.nftype='DU'",
             'Admin Status - Unlocked': "select count(*) as DU_unlock from DML_DU_CONFIG dduc, DML_EXTRACTED_CONFIG DC where dduc.marketId = MARKET_ID and DC.administrative_state = '\"unlocked\"' and dduc.fileId = DC.fileId and DC.nftype='DU'",
             'Admin Status - Locked': "select count(*) as DU_unlock from DML_DU_CONFIG dduc, DML_EXTRACTED_CONFIG DC where dduc.marketId = MARKET_ID and DC.administrative_state = '\"locked\"' and dduc.fileId = DC.fileId and DC.nftype='DU'",
             'Band Other': 0,
             'Admin Status - Other': 0,
             },
        ]
        result = []

        # get a consolidated set of market_ids
        market_ids = __class__.get_market_ids()
        start_time = time.time()
        try:
            with connections['logsnapsverizonedb'].cursor() as cursorvz:
                for market_id in market_ids:
                    for eachNFTypeDML in base_result_structure:
                        # result_dict = copy.deepcopy(eachNFTypeDML)
                        result_dict = {}
                        # if len(row) > 0:
                        result_dict['Market'] = market_id
                        result_dict["Type Of NFs"] = eachNFTypeDML["Type Of NFs"]
                        result_dict['Count of NFs in SQL Query'] = 0
                        result_dict['Count of gNodeBs'] = 0
                        result_dict['Count of Cells'] = 0
                        result_dict['Admin Status - Unlocked'] = 0
                        result_dict['Admin Status - Locked'] = 0
                        result_dict['Admin Status - Other'] = 0
                        result_dict['Band Sub3'] = 0
                        result_dict['Band Sub6'] = 0
                        result_dict['Band MMW'] = 0
                        result_dict['Band Other'] = 0

                        for eachItem in eachNFTypeDML.items():
                            if eachItem[1] is not None and eachItem[1] != 0 and eachItem[0] != 'Type Of NFs' and \
                                    eachItem[0] != 'Market' and market_id is not None:
                                try:
                                    sql = eachItem[1].replace('MARKET_ID', '"'+str(market_id)+'"')
                                    print(
                                        f'retrievingDML audit count for {eachNFTypeDML["Type Of NFs"]}---{eachItem[0]} prepareDMLAuditCounts line 1104------>',
                                        sql)
                                    cursorvz.execute(sql)
                                    results = cursorvz.fetchall()

                                    if len(results)>0 and len(results[0])>0:
                                        if eachItem[0] == 'Band Sub' and (eachNFTypeDML['Type Of NFs'] == 'vDU' or eachNFTypeDML['Type Of NFs'] == 'DU'):
                                            for eachObj in results:
                                                jsonstr = eachObj[0] if eachObj[0] is not None else ''
                                                if jsonstr != '':
                                                    obj = json.loads(jsonstr)
                                                    # if 2 in obj or 5 in obj or 6 in obj:
                                                    #     result_dict['Band Sub3'] += 1
                                                    # elif 77 in obj or 46 in obj:
                                                    #     result_dict['Band Sub6'] += 1
                                                    # else:
                                                    #     result_dict['Band Other'] += 1
                                                    if 2 in obj or 5 in obj or 66 in obj:
                                                        result_dict['Band Sub3'] += 1
                                                    elif 7 in obj or 48 in obj:
                                                        result_dict['Band Sub6'] += 1
                                                    elif 261 in obj or 262 in obj:
                                                        result_dict['Band MMW'] += 1
                                                    # else:
                                                    #     result_dict['Band Other'] += 1
                                            result_dict['Band Other'] = int(result_dict['Count of NFs in SQL Query']) - (int(result_dict['Band Sub3']) + int(result_dict['Band Sub6']) + int(result_dict['Band MMW']) )
                                        # elif eachItem[0] == 'Band Sub' and eachNFTypeDML['Type Of NFs'] == 'DU':
                                        #     for eachObj in results:
                                        #         jsonstr = eachObj[0] if eachObj[0] is not None else ''
                                        #         if jsonstr != '':
                                        #             obj = json.loads(jsonstr)
                                        #             if len(obj) > 0:
                                        #                 if 261 in obj or 262 in obj:
                                        #                     result_dict['Band MMW'] += 1
                                                        # else:
                                                        #     result_dict['Band Other'] += 1
                                            # result_dict['Band Other'] = int(result_dict['Count of NFs in SQL Query']) - int(result_dict['Band MMW'])
                                        else:
                                            if sql.find('JSON_LENGTH') != -1:
                                                print(eachItem[0],f' for nftype {eachNFTypeDML["Type Of NFs"]}:',results, list(chain.from_iterable(list(filter(lambda x: x[0] is not None,results)))))
                                                # result_dict[eachItem[0]] = len(list(filter(lambda x: x[0] is not None,results)))
                                                result_dict[eachItem[0]] = sum(list(chain.from_iterable(list(filter(lambda x: x[0] is not None,results)))))
                                            else:
                                                result_dict[eachItem[0]] = results[0][0]

                                    # del result_dict['Band Sub']

                                except Exception as e:
                                    print(
                                        f'Error {e} ::::: for retrievingDML audit count for {eachNFTypeDML["Type Of NFs"]}---{eachItem[0]} prepareDMLAuditCounts line 1104------>',
                                        eachItem[1])
                                    if str(e).find('Lost connection to MySQL server during query')!=-1:
                                        cursorvz = connections['logsnapsverizonedb'].cursor()
                            elif eachItem[0] == 'Admin Status - Other' and market_id is not None:
                                try:
                                    print('\nCount of NFs in SQL Query:', result_dict['Count of NFs in SQL Query'], '\nAdmin Status - Unlocked:',result_dict['Admin Status - Unlocked'] ,'\nAdmin Status - Locked:', result_dict['Admin Status - Locked'])
                                    result_dict[eachItem[0]] = int(result_dict['Count of NFs in SQL Query']) - (int(result_dict['Admin Status - Unlocked']) + int(result_dict['Admin Status - Locked']))
                                except Exception as ve:
                                    print('------->line number 1444 result_dict[Admin Status - Other]',ve)
                                    result_dict['Admin Status - Other'] = 0

                        if market_id is not None and market_id != '' and result_dict['Count of NFs in SQL Query'] > 0:
                            result.append(result_dict)
        except DatabaseError as e:
                print('Error from dataentities.prepareDMLAuditCounts------>verizon', e)
        print(f"--- {(time.time() - start_time)} seconds ---")
        return result, (time.time() - start_time)

    @staticmethod
    def prepareRemedyEnrichmentCounts(startTime, endTime):
        remedy_data_REA = {}
        remedy_data_RES = {}
        remedy_data_NoASMI = {}
        remedy_data_other = {}
        nftypes_masterlist = []
        RemedyEnrichmentCounts = []
        summary_data = [0,0,0,0]
        queryRemedyEnhancementAttempted = f"select UPPER(nftype), count(*) from WORKFLOW_STATS where CLOSEDLOOP_NAME = 'Remedy Ticket Enrichment' and FIRSTOCCURRENCE > {startTime} and FIRSTOCCURRENCE < {endTime} and nftype <> '' GROUP BY UPPER(nftype);"
        queryRemedyEnhancementSuccessful = f"select UPPER(nftype), count(*) from WORKFLOW_STATS where CLOSEDLOOP_NAME = 'Remedy Ticket Enrichment' and FIRSTOCCURRENCE > {startTime} and FIRSTOCCURRENCE < {endTime} and WORKFLOW_STATUS = 'Success' and nftype <> '' GROUP BY UPPER(nftype);"
        queryNoNFInASMInventory = f"select UPPER(nftype), count(*) from WORKFLOW_STATS where CLOSEDLOOP_NAME = 'Remedy Ticket Enrichment' and FIRSTOCCURRENCE > {startTime} and FIRSTOCCURRENCE < {endTime} and NOTES like '%No Tickets Available%' and nftype <> '' GROUP BY UPPER(nftype);"
        queryOther = f"select UPPER(nftype), count(*) from WORKFLOW_STATS where CLOSEDLOOP_NAME = 'Remedy Ticket Enrichment' and FIRSTOCCURRENCE > {startTime} and FIRSTOCCURRENCE < {endTime} and NOTES like '%No data retrieved from ASM resource%' and nftype <> '' GROUP BY UPPER(nftype);"
        try:
            with connections['logsnapsverizonedb'].cursor() as cursorvz:
                try:
                    print('retrieving Platform report data dataentities.prepareRemedyEnrichmentCounts line 827------>', queryRemedyEnhancementAttempted)
                    cursorvz.execute(queryRemedyEnhancementAttempted)
                    results = cursorvz.fetchall()
                    for row in results:
                        remedy_data_REA[row[0]] = row[1]
                        summary_data[0] += int(row[1]) if row[1] is not None else 0
                    nftypes_masterlist.extend(remedy_data_REA.keys())

                    print('retrieving Platform report data dataentities.prepareRemedyEnrichmentCounts line 827------>',queryRemedyEnhancementSuccessful)
                    cursorvz.execute(queryRemedyEnhancementSuccessful)
                    results = cursorvz.fetchall()
                    for row in results:
                        remedy_data_RES[row[0]] = row[1]
                        summary_data[1] += int(row[1]) if row[1] is not None else 0
                    nftypes_masterlist.extend(remedy_data_RES.keys())

                    print('retrieving Platform report data dataentities.prepareRemedyEnrichmentCounts line 827------>', queryNoNFInASMInventory)
                    cursorvz.execute(queryNoNFInASMInventory)
                    results = cursorvz.fetchall()
                    for row in results:
                        remedy_data_NoASMI[row[0]] = row[1]
                        summary_data[2] += int(row[1]) if row[1] is not None else 0
                    nftypes_masterlist.extend(remedy_data_NoASMI.keys())

                    print('retrieving Platform report data dataentities.prepareRemedyEnrichmentCounts line 827------>',queryOther)
                    cursorvz.execute(queryOther)
                    results = cursorvz.fetchall()
                    for row in results:
                        remedy_data_other[row[0]] = row[1]
                        summary_data[3] += int(row[1]) if row[1] is not None else 0
                    nftypes_masterlist.extend(remedy_data_other.keys())
                    nftypes_masterlist = list(dict.fromkeys(nftypes_masterlist))

                    for each_nftype in nftypes_masterlist:
                        RemedyEnrichmentCounts.append({'NFType': each_nftype, 'REA':(remedy_data_REA[each_nftype] if remedy_data_REA.get(each_nftype,'')!='' else 0), 'RES': (remedy_data_RES[each_nftype] if remedy_data_RES.get(each_nftype,'')!='' else 0) , 'NoASMI': (remedy_data_NoASMI[each_nftype] if remedy_data_NoASMI.get(each_nftype,'')!='' else 0), 'other': (remedy_data_other[each_nftype] if remedy_data_other.get(each_nftype,'')!='' else 0)})

                    RemedyEnrichmentCounts.append({'NFType': '<span style="font-weight:bold">Summary</span>', 'REA': summary_data[0], 'RES': summary_data[1], 'NoASMI': summary_data[2], 'other': summary_data[3]})

                except Exception as e:
                    print(e)
        except DatabaseError as e:
            print('Error from dataentities.FMPMAlarmCountsData------>nonothers', e)
        return RemedyEnrichmentCounts

    @staticmethod
    def prepareNFCounts():
        #API to fetch vnf and cnf id
        start_endpoint = "/1.0/topology/types?_filter=name:vnf,cnf&_filter=description:resource&_field=*&_include_count=false&_include_metadata=false"
        vnf_cnf_id_res = ClusterResourses._query_api_unverified_ssl(start_endpoint)
        # print('VNF-CNF data here--------------------->',vnf_cnf_id_res) # to be deleted later
        NFCOUNT_PER_NFTYPES = {}
        # resource_status_list = {}
        if vnf_cnf_id_res.get('_items','') != '' and len(vnf_cnf_id_res.get('_items',[])) > 0:
            for vnf_cnf_id in vnf_cnf_id_res.get('_items'):
                vnf_cnf_id = vnf_cnf_id.get('_id')
                # call for fetching VNFs/CNFs datai= iteratively
                referenceNo = 0
                offset = 0 #to start of with
                reccount = 500
                remaining_count = 1000000 #arbitrarily lets set to a higher number
                while remaining_count > 0:
                    # endpoint = f"/1.0/topology/resources/{vnf_cnf_id}/references/out/definesType?_filter=referenceNo>{referenceNo}&_field=name&_field=entityTypes&_field=type&_field=referenceNo&_return=nodes&_return_composites=true&_limit={reccount}&_offset={offset}&_include_count=true&_include_status=false&_include_status_severity=false&_include_metadata=false"
                    endpoint = f"/1.0/topology/resources/{vnf_cnf_id}/references/out/definesType?_filter=referenceNo>{referenceNo}&_field=name&_field=entityTypes&_field=type&_field=nfType&_field=referenceNo&_field=_status&_return=nodes&_return_composites=true&_limit={reccount}&_include_count=true&_include_status=true&_include_status_severity=false&_include_metadata=false"
                    vnf_cnf_res = ClusterResourses._query_api_unverified_ssl(endpoint)
                    print('vnf cnf data endpoint-------------->',endpoint)
                    # if offset == 0: # to be deleted later
                    #     print('More VNFCNF data here--------------------->', vnf_cnf_res)
                    remaining_count = (int(vnf_cnf_res.get('_count',0))-reccount) if offset==0 else (remaining_count-reccount)
                    print('remaining count--------->',remaining_count)
                    offset = offset+reccount
                    print('offset--------->',offset)
                    if vnf_cnf_res.get('_items', '') != '' and len(vnf_cnf_res.get('_items', [])) > 0:
                        vnf_cnf_res = vnf_cnf_res.get('_items')
                        for each_vnf_cnf in vnf_cnf_res:
                            resource_status = ''
                            # if each_vnf_cnf.get('_status', '') != '' and len(each_vnf_cnf['_status'])>0:
                            #     resource_status = each_vnf_cnf['_status'][0]['description']
                                # resource_status_list[resource_status] = 1
                            if each_vnf_cnf.get('referenceNo', '') != '':
                                referenceNo = int(each_vnf_cnf.get('referenceNo'))
                            # if resource_status in ['Vnf is currently active','Cnf is currently active']:
                            if each_vnf_cnf.get('type', '') != '' :
                                NFCOUNT_PER_NFTYPES[each_vnf_cnf['type'].upper()] = (NFCOUNT_PER_NFTYPES[each_vnf_cnf['type'].upper()]+1) if NFCOUNT_PER_NFTYPES.get(each_vnf_cnf['type'].upper(),0) > 0 else 1
                            if each_vnf_cnf.get('nfType', '') != '' and each_vnf_cnf.get('type', '').upper() != each_vnf_cnf.get('nfType', '').upper():
                                NFCOUNT_PER_NFTYPES[each_vnf_cnf['nfType'].upper()] = (NFCOUNT_PER_NFTYPES[each_vnf_cnf['nfType'].upper()]+1) if NFCOUNT_PER_NFTYPES.get(each_vnf_cnf['nfType'].upper(),0) > 0 else 1

        broadsoft_query_endpoint = '/1.0/topology/resources?_filter=entityTypes%3AwirelineVNF&_filter=tags%3AwirelineVNF_Consumer-Broadsoft&_field=name&_field=uniqueId&_field=vnfType&_include_global_resources=false&_include_count=true&_include_status=false&_include_status_severity=false&_include_metadata=false&_return_composites=true'
        broadsoft_res = ClusterResourses._query_api_unverified_ssl(broadsoft_query_endpoint)
        if broadsoft_res.get('_items', '') != '' and len(broadsoft_res.get('_items', [])) > 0:
            broadsoft_res = broadsoft_res.get('_items')
            for each_broadsoft in broadsoft_res:
                if each_broadsoft.get('vnfType', '') != '':
                    NFCOUNT_PER_NFTYPES[each_broadsoft['vnfType'].upper()] = (
                                NFCOUNT_PER_NFTYPES[each_broadsoft['vnfType'].upper()] + 1) if NFCOUNT_PER_NFTYPES.get(
                        each_broadsoft['vnfType'].upper(), 0) > 0 else 1
        # print('available statuses for all resource active/inactive------------------>',resource_status_list.keys())
        print('nftypes:instance_counts from resource API calls------------------>',NFCOUNT_PER_NFTYPES)
        return NFCOUNT_PER_NFTYPES

    def get_object_server_data(self, usecase_keys, lastmins=5):
        lastsecs = lastmins * 60
        osa = ObjectServerAlarmStatus(None)
        cr = ClusterResourses(None)
        usecases = {
            'netcool-health-probe-status' :{
                'filter': f"(AlertGroup = 'ProbeStatus' or AlertGroup = 'probestat' or AlertGroup = 'Probe' or Manager = 'ProbeWatch') and Severity=5 AND LastOccurrence >= getdate()-{lastsecs}",
                'collist': ['Summary','Severity','LastOccurrence'],
                'table': 'alerts/status',
                'column_formatting': {'LastOccurrence': 'datetime'},
                'process_handler': osa.netcoolMonitoring,
                'type': 'os_api',
                'params': [],
            },
            'netcool-health-WebGUI-status': {
                'filter': f"AlertGroup='WebGUI Status' and Severity=5 AND LastOccurrence >= getdate()-{lastsecs}",
                'collist': ['Summary', 'Severity', 'LastOccurrence'],
                'table': 'alerts/status',
                'column_formatting': {'LastOccurrence': 'datetime'},
                'process_handler': osa.netcoolMonitoring,
                'type': 'os_api',
                'params': [],
            },
            'netcool-health-avg-time-to-display-events': {
                'filter': f"Identifier='time_to_display' AND LastOccurrence >= getdate()-{lastsecs}",
                'collist': ['Summary', 'Severity', 'LastOccurrence'],
                'table': 'alerts/status',
                'column_formatting': {'LastOccurrence': 'datetime'},
                'process_handler': osa.netcoolMonitoring,
                'type': 'os_api',
                'params': [],
            },
            'netcool-health-gateway-status': {
                'filter': f"(AlertGroup='Gateway' or (Manager = 'SystemWatch' and Identifier like 'gateway_resync_')) and Severity =5 AND LastOccurrence >= getdate()-{lastsecs}",
                'collist': ['Summary', 'Severity', 'LastOccurrence'],
                'table': 'alerts/status',
                'column_formatting': {'LastOccurrence': 'datetime'},
                'process_handler': osa.netcoolMonitoring,
                'type': 'os_api',
                'params': [],
            },
            'netcool-health-top-nodes': {
                'filter': f"AlertGroup='TopNodes' and Severity >= 2 AND LastOccurrence >= getdate()-{lastsecs}",
                'collist': ['Summary', 'Severity', 'LastOccurrence'],
                'table': 'alerts/status',
                'column_formatting': {'LastOccurrence': 'datetime'},
                'process_handler': osa.netcoolMonitoring,
                'type': 'os_api',
                'params': [],
            },
            'netcool-impact-cluster-monitoring': {
                'filter': f"AlertGroup = 'ClusterStatus' and ServerName = 'AGG_P' and Summary like '%about to shutdown%' AND LastOccurrence >= getdate()-{lastsecs}",
                'collist': ['Summary', 'Grade'],
                'table': 'alerts/status',
                'column_formatting': {},
                'process_handler': osa.netcoolMonitoring,
                'type': 'os_api',
                'params': [],
            },
            'netcool-impact-datasource-monitoring': {
                'filter': f"AlertGroup = 'DataSourceStatus' and Agent = 'Impact SelfMonitoring' and ServerName = 'AGG_P' and Summary like '%Failed%' and Severity>0 AND LastOccurrence >= getdate()-{lastsecs}",
                'collist': ['Summary'],
                'table': 'alerts/status',
                'column_formatting': {},
                'process_handler': osa.netcoolMonitoring,
                'type': 'os_api',
                'params': [],
            },
            'netcool-impact-memory-monitoring': {
                'filter': f"AlertGroup = 'MemoryStatus' and Agent = 'Impact SelfMonitoring' and ServerName = 'AGG_P' and Severity>0 and Summary like '%Failed%' AND LastOccurrence >= getdate()-{lastsecs}",
                'collist': ['Summary', 'Grade'],
                'table': 'alerts/status',
                'column_formatting': {},
                'process_handler': osa.netcoolMonitoring,
                'type': 'os_api',
                'params': [{'field':'Grade', 'op':'gt', 'val': 80}],
            },
            'netcool-impact-queue-status-monitoring': {
                'filter': f"AlertGroup = 'QueueStatus' and Agent = 'Impact SelfMonitoring' and ServerName = 'AGG_P' and Severity>0 AND LastOccurrence >= getdate()-{lastsecs}",
                'collist': ['Summary'],
                'table': 'alerts/status',
                'column_formatting': {},
                'process_handler': osa.netcoolMonitoring,
                'type': 'os_api',
                'params': [{'field':'Summary', 'op':'regex', 'pattern': r'QueueSize:\s([0-9]*)\s\(Min:\s([0-9]*)\sMax:\s([0-9]*)\)\s+DeltaQueue:\s([0-9]*)', 'pattern_matched_index': 3, 'val_check_op': 'gt', 'val': 0}],
            },
            'netcool-impact-service-monitoring': {
                'filter': f"AlertGroup = 'ServiceStatus' and Agent = 'Impact SelfMonitoring' and ServerName = 'AGG_P' and Severity>0 AND LastOccurrence >= getdate()-{lastsecs}",
                'collist': ['Summary'],
                'table': 'alerts/status',
                'column_formatting': {},
                'process_handler': osa.netcoolMonitoring,
                'type': 'os_api',
                'params': [{'field':'Summary', 'op':'like', 'val': 'Stopped'}],
            },
            'netcool-impact-event-checkbox-monitoring': {
                'command': 'tail -n 200 $IMPACT_HOME/logs/impactserver.log | grep -i "ServiceUIResource"',
                'collist': ['Error'],
                'process_handler': cr.exec_cmd_in_pod_container,
                'type': 'kube_commands',
                'params': [app.OPENSHIFT_DEFAULT_MONITORED_NAMESPACE, 'noi-nciserver-0', 'nciserver'],
            },
            'netcool-objectserver-alertstatus-monitoring': {
                'filter': f"AlertGroup = 'DBStatus' and ServerName = 'AGG_P' and Summary like 'Event count' AND LastOccurrence >= getdate()-{lastsecs}",
                'collist': ['Summary', 'Grade'],
                'table': 'alerts/status',
                'column_formatting': {},
                'process_handler': osa.netcoolMonitoring,
                'type': 'os_api',
                'params': [{'field':'Grade', 'op':'ge', 'val': 100000}],
            },
            'netcool-objectserver-SLMA-monitoring': {
                'filter': f"AlertGroup = 'MemstoreStatus' and ServerName = 'AGG_P' AND LastOccurrence >= getdate()-{lastsecs}",
                'collist': ['Summary', 'Grade'],
                'table': 'alerts/status',
                'column_formatting': {},
                'process_handler': osa.netcoolMonitoring,
                'type': 'os_api',
                'params': [{'field':'Grade', 'op':'gt', 'val': 80}],
            },
            'netcool-objectserver-individual-trigger-single-granularity-period': {
                'filter': f"TriggerName like '[a-zA-Z]+'",
                'collist': ['TriggerName', 'PeriodTime'],
                'table': 'catalog/trigger_stats',
                'column_formatting': {},
                'process_handler': osa.netcoolMonitoring,
                'type': 'os_api',
                'params': [{'field':'PeriodTime', 'op':'gt', 'val': 5.0}],
            },
            'netcool-objectserver-app-conn-single-granularity-period': {
                'filter': f"AlertGroup = 'ClientStatus' and ServerName = 'AGG_P' AND LastOccurrence >= getdate()-{lastsecs}",
                'collist': ['Summary', 'Grade'],
                'table': 'alerts/status',
                'column_formatting': {},
                'process_handler': osa.netcoolMonitoring,
                'type': 'os_api',
                'params': [{'field':'Grade', 'op':'gt', 'val': 60.0}],
            },
            'netcool-objectserver-profiler-reporting-period': {
                'filter': f"AlertGroup = 'TriggerStatus' and ServerName = 'AGG_P' and Identifier like 'Profiler Report Status for AGG_P' AND LastOccurrence >= getdate()-{lastsecs}",
                'collist': ['Summary', 'Grade'],
                'table': 'alerts/status',
                'column_formatting': {},
                'process_handler': osa.netcoolMonitoring,
                'type': 'os_api',
                'params': [{'field':'Grade', 'op':'gt', 'val': 60.0}],
            },
            'netcool-WebGUI-monitoring': {
                'filter': f"AlertGroup = 'WebGUI Status' and ServerName = 'AGG_P' and Severity >= 3 AND LastOccurrence >= getdate()-{lastsecs}",
                'collist': ['Summary', 'Severity', 'LastOccurrence'],
                'column_formatting': {'LastOccurrence': 'datetime'},
                'table': 'alerts/status',
                'process_handler': osa.netcoolMonitoring,
                'type': 'os_api',
                'params': [],
            },

        }
        DataResult = {}
        # print('param received------------->',usecase_keys)
        # print('usecases------------->',usecases)
        for usecase_key, item in usecases.items():
            if usecase_key in usecase_keys:
                if item['type']=='os_api':
                    DataResult[usecase_key] = {}
                    DataResult[usecase_key]['notify'], DataResult[usecase_key]['data'] = usecases[usecase_key]['process_handler'](usecases[usecase_key]['filter'], usecases[usecase_key]['collist'], usecases[usecase_key]['table'], usecases[usecase_key]['params'], usecases[usecase_key]['column_formatting'])
                if item['type'] == 'kube_commands':
                    DataResult[usecase_key] = {}
                    DataResult[usecase_key]['data'] = usecases[usecase_key]['process_handler'](*usecases[usecase_key]['params'], usecases[usecase_key]['command'])
                    if DataResult[usecase_key]['data']!= '':
                        DataResult[usecase_key]['data'] = self.stringToStructuredData(usecases[usecase_key]['collist'], DataResult[usecase_key]['data'], '')
                    DataResult[usecase_key]['notify'] = len(DataResult[usecase_key]['data'])>0
        return DataResult

    def stringToStructuredData(self,collist,rawdata, split_row_by_str=''):
        data = rawdata.split('\n')
        result = []
        for eachrow in data:
            eachdata = {}
            i=0
            if split_row_by_str != '':
                split_row_by_str = eachrow.split(split_row_by_str)
            else:
                eachrow = [eachrow]
            for col in collist:
                eachdata[col] = eachrow[i]
                i += 1
            result.append(eachdata)
        return result

    def getASMObserverLogs(self, namespace, since_seconds, tail_lines):
        """
        Addded as part of # IDAVA-1165. it is what it is !
        Added support for sorting the log statements based on the log level received in log lines.
        Added support for multiple ASM Observer Pods.
        Added support to show file names extracted from logs
        Added as new function to not touch existing ones. It would have been easier if we fixed the getASMLogErrorData but not sure what's the design reason behind it. So working around.
        """

        observer_names = [ 'noi-topology-openstack-observer', 'noi-topology-file-observer', 'noi-topology-hpnfvd-observer', 'noi-topology-bigcloudfabric-observer']

        error_result_sorted = dict()
        pod_to_error_dict = dict()
        LOG_LEVEL_PATTERN = r'(INFO|ERROR|WARN|TRACE|DEBUG|FATAL|INFORMATION|WARNING)\s+.*'
        LOG_LEVEL_ORDER = {'DEBUG':4,'INFO':3, 'FATAL': 1,
                            'WARN':2,'ERROR':0, 'WARNING': 2,
                            'INFORMATION':2, 'TRACE':5, 'MESSAGE':6}
        FILE_NAME_PATTERN = r'file_name=(.*?)\.txt\]'

        for pod in observer_names:
            error_result_per_pod = self.getASMLogErrorData(pod, namespace, since_seconds, tail_lines)
            pod_to_error_dict[pod]=error_result_per_pod
#         print(json.dumps(
#     pod_to_error_dict,
#     sort_keys=True,
#     indent=4,
#     separators=(',', ': ')
# ))

        for pod_name,pod_output in pod_to_error_dict.items():
            for key, value in pod_output.items():
                for each_error in value:
                    if match := re.search(LOG_LEVEL_PATTERN, each_error, re.IGNORECASE):
                        log_level = match.group(1).upper()
                    else:
                        log_level='MESSAGE' # Just in case !
                    if match := re.search(FILE_NAME_PATTERN, each_error):
                        file_name = match.group(1)
                    else:
                        file_name = ''
                    message={
                            'logType': key,
                            'logMessage': each_error,
                            'fileName': file_name,
                            'observerName': pod_name
                            }
                    if log_level in error_result_sorted:
                        error_result_sorted[log_level].append(message)
                    else:
                        error_result_sorted[log_level]=[message]
        error_result_sorted=dict(sorted(error_result_sorted.items(), key= lambda x: LOG_LEVEL_ORDER[x[0]]))
        return error_result_sorted

    def getASMLogErrorData(self, podname, namespace, since_seconds, tail_lines):
        cr = ClusterResourses(None)
        lograw = cr.read_namespaced_logs(podname, namespace, False, since_seconds, tail_lines)
        lines = lograw.split("\n")
        errors = {}
        for line in lines:
            if line.find('ERROR') != -1 and line.find('Failed to validate connection') != -1:
                if errors.get('Connection Error', '') != '':
                    errors['Connection Error'].append(line)
                else:
                    errors['Connection Error'] = [line]
            elif line.find('ERROR') != -1:
                if errors.get('Syntax Error', '') != '':
                    errors['Syntax Error'].append(line)
                else:
                    errors['Syntax Error'] = [line]
                ## IDAVA-1135-  this as a bug which was not visible earlier.Commenting out.
                # if errors.get('Syntax Error', '') != '':
                #     errors['Syntax Error'].append(line)
                # else:
                #     errors['Syntax Error'] = [line]
            if (line.find('WARN') != -1 and line.find('Connection to') != -1 and line.find(
                    'could not be established') != -1):
                if errors.get('Kafka Issue', '') != '':
                    errors['Kafka Issue'].append(line)
                else:
                    errors['Kafka Issue'] = [line]
            if (line.find('INFO') != -1 and line.find('No space left on device') != -1 and line.find(
                    'could not be established') != -1):
                if errors.get('PVC Issue', '') != '':
                    errors['PVC Issue'].append(line)
                else:
                    errors['PVC Issue'] = [line]
            elif line.find('_error') != -1:
                if errors.get('Other Errors', '') != '':
                    errors['Other Errors'].append(line)
                else:
                    errors['Other Errors'] = [line]
        return errors