import sys
sys.path.insert(0, './sql_scripts/data_transformation/')
sys.path.insert(0, './sql_scripts/deviation_report/')
sys.path.insert(0, './sql_scripts/')
sys.path.insert(0, './resources/')
import os
import yaml
import schedule
from datetime import datetime

from sisense.ecube.ecube import build_development_cube, get_ecube
from sisense.dashboards.dashboards import create_dashboards,get_dashboard_details
from sisense.alerts.alerts import create_alerts,get_alert_details
from sql_scripts.stage_to_mart import fact_stage_to_base,dim_stage_to_base,base_to_mart,db_user_access,update_static_entries
from sql_scripts.deviation_report.deviation_report import deviation_report
from post_run.stage_vs_base import stage_vs_base
from post_run.notification_utilities import notification_mail_body
from resources.utilities import send_email, create_log_file
from pre_run.config_sanity import check_config


config = yaml.safe_load(open("config.yml"))
config = eval(str(config).replace("./",os.getcwd()+"/"))

def refresh_L5():
    # Creating logs folder & file
    try:
        os.makedirs('./../L5_Logs/')
    except FileExistsError:
        pass
    logger, log_file = create_log_file('./../L5_Logs/')
    config['logger'] = logger

    script_start = datetime.now()
    print('\n')
    logger.debug('\n')
    dims_time_taken, facts_time_taken, static_update_time_taken, mart_time_taken, dim_status, fact_status,\
    base_status, cube_status, dashboards_status, alerts_status = None, None, None, None, True, None, None,\
                                                                 False, False, False

    if config['l5']['run_pre_run_checklist']:
        check_config(config)

    if config['l5']['run_deviation_report']:
        deviation_report(config)

    if config['l5']['transfer_dim_stage_to_base']:
        print('Starting to transfer data for dimension tables...')
        logger.debug('Starting to transfer for dimension tables...')
        try:
            dims_time_taken, dim_status = dim_stage_to_base(config)
        except Exception as e:
            dim_status = False
            print("Error running stage to base scripts with Exception as -", e)
            logger.exception("Error transferring data for fact tables")
            base_status = False
        print('\n')
        logger.debug('\n')

    if config['l5']['transfer_fact_stage_to_base'] and dim_status == True:
        print('Starting to transfer data for fact tables...')
        logger.debug('Starting to transfer for fact tables...')
        try:
            facts_time_taken, fact_status = fact_stage_to_base(config)
        except Exception as e:
            print("Error running stage to base scripts with Exception as -", e)
            logger.exception("Error transferring data for fact tables")
            base_status = False
        print('\n')
        logger.debug('\n')

    if config['l5']['update_static_entries']:
        print('Starting to update static entries...')
        logger.debug('Starting to update static entries...')
        try:
            static_update_time_taken, static_status = update_static_entries(config)
        except Exception as e:
            print("Error updating static entries with Exception as -", e)
            logger.exception("Error in updating static entries")
            base_status = False
        print('\n')
        logger.debug('\n')

    if config['l5']['refresh_mart']:
        try:
            mart_time_taken, mart_status = base_to_mart(config)
        except Exception as e:
            print("Error in refreshing mart with Exception as -", e)
            logger.exception("Error in refreshing mart")
            base_status = False
        print('\n')
        logger.debug('\n')

    if config['base']['redshift_access_user_list']:
        grant_access_time_taken, access_status=db_user_access(config)

    if base_status is None:
        base_status = True

    if config['l5']['build_cube']:
        print('Building cube...')
        logger.debug('Building cube...')
        try:
            cube_status = build_development_cube(config)
        except Exception as e:
            print("Error building cube with Exception as -", e)
            logger.exception("Error building cube...")

        print('\n')
        logger.debug('\n')
        print('\n')
        logger.debug('\n')

    if config['l5']['build_dashboards']:
        print('Creating dashboards...')
        logger.debug('Creating dashboards...')
        try:
            dashboards_status = create_dashboards(config)
        except Exception as e:
            print("Error creating dashboards with Exception as -", e)
            logger.exception("Error creating dashboards...")

        print('\n')
        logger.debug('\n')
        print('\n')
        logger.debug('\n')

    if config['l5']['create_alerts']:
        print('Creating alerts...')
        logger.debug('Creating alerts...')
        try:
            alerts_status = create_alerts(config)
        except Exception as e:
            print("Error creating alerts with Exception as -", e)
            logger.exception("Error creating alerts...")

        print('\n')
        logger.debug('\n')
        print('\n')
        logger.debug('\n')

    if config['l5']['archive_cube']:
        print('Archiving ecube at ' + config['sisense']['archived_cubes_directory'])
        logger.debug('Archiving ecube at ' + config['sisense']['archived_cubes_directory'])
        try:
            get_ecube(config)
        except Exception as e:
            print("Error archiving ecube with Exception as - ", e)
            logger.exception("Error archiving ecube...")

    if config['l5']['archive_dashboards']:
        print ('Archiving dashboards at '+config['sisense']['archived_dashboards_directory'])
        logger.debug('Archiving dashboards at '+config['sisense']['archived_dashboards_directory'])
        try:
            dashboard_archival_status = get_dashboard_details(config)
        except Exception as e:
            print("Error archiving dashboards with Exception as - ",e)
            logger.exception("Error archiving dashboards...")

    if config['l5']['archive_alerts']:
        print('Archiving pulse alerts at ' + config['sisense']['archived_alerts_directory'])
        logger.debug('Archiving pulse alerts at ' + config['sisense']['archived_alerts_directory'])
        try:
            alert_archival_status = get_alert_details(config)
        except Exception as e:
            print("Error archiving alerts with Exception as - ", e)
            logger.exception("Error archiving alerts...")

    print('------------------------------------------ L5 refresh complete  ------------------------------------------\n')
    logger.debug('------------------------------------------ L5 refresh complete  ------------------------------------------\n')
    total_time_taken = (datetime.now()-script_start).seconds/60
    total_time_taken = round(total_time_taken, 0)
    print('Total time taken -', total_time_taken, 'minutes')
    logger.debug('Total time taken - ' + str(total_time_taken) + 'minutes')

    checklist = None
    if(config['l5']['checklist']):
        print('\n------------------------------------------ Running post run checklist  ------------------------------------------\n')
        logger.debug('\n------------------------------------------ Running post run checklist  ------------------------------------------\n')
        checklist = stage_vs_base(config)
        print('\n------------------------------------------ Checklist finished ------------------------------------------\n')
        logger.debug('\n------------------------------------------ Checklist finished ------------------------------------------\n')

    if config['email']['recipients']:
        print('Sending email...')
        logger.debug('Sending email...')
        email_body = notification_mail_body(config, total_time_taken, dims_time_taken, facts_time_taken, base_status, cube_status, dashboards_status, alerts_status, checklist)
        subject_line = config['project_name']+' - Report for L5_Scheduler'
        send_email(config['email']['sender'], config['email']['recipients'], subject_line, config['email']['login_key'], log_file, email_body)


if config['l5']['schedule_type'] == 'weekly':
    if config['l5']['schedule_day'] == 'sunday':
        schedule.every().sunday.at(config['l5']['schedule_time']).do(refresh_L5)
    elif config['l5']['schedule_day'] == 'monday':
        schedule.every().monday.at(config['l5']['schedule_time']).do(refresh_L5)
    elif config['l5']['schedule_day'] == 'tuesday':
        schedule.every().tuesday.at(config['l5']['schedule_time']).do(refresh_L5)
    elif config['l5']['schedule_day'] == 'wednesday':
        schedule.every().wednesday.at(config['l5']['schedule_time']).do(refresh_L5)
    elif config['l5']['schedule_day'] == 'thursday':
        schedule.every().thursday.at(config['l5']['schedule_time']).do(refresh_L5)
    elif config['l5']['schedule_day'] == 'friday':
        schedule.every().friday.at(config['l5']['schedule_time']).do(refresh_L5)
    elif config['l5']['schedule_day'] == 'saturday':
        schedule.every().saturday.at(config['l5']['schedule_time']).do(refresh_L5)
elif config['l5']['schedule_type'] == 'daily':
    schedule.every().day.at(config['l5']['schedule_time']).do(refresh_L5)
elif config['l5']['schedule_type'] == 'weeks':
    schedule.every(config['l5']['schedule_frequency']).weeks.at(config['l5']['schedule_time']).do(refresh_L5)
elif config['l5']['schedule_type'] == 'days':
    schedule.every(config['l5']['schedule_frequency']).days.at(config['l5']['schedule_time']).do(refresh_L5)
elif config['l5']['schedule_type'] == 'hours':
    schedule.every(config['l5']['schedule_frequency']).hours.do(refresh_L5)
elif config['l5']['schedule_type'] == 'minutes':
    schedule.every(config['l5']['schedule_frequency']).minutes.do(refresh_L5)
elif config['l5']['schedule_type'] == 'seconds':
    schedule.every(config['l5']['schedule_frequency']).seconds.do(refresh_L5)
elif config['l5']['schedule_type'] == 'once':
    refresh_L5()
    sys.exit('Exiting L5_refresh event...')
else:
    raise ValueError('schedule_type '+config['l5']['schedule_type']+' is not supported.')

while True:
    schedule.run_pending()