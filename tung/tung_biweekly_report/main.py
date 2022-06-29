import os
import slack
import logging
from datetime import date, datetime

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s:%(levelname)s:%(name)s:%(message)s')
stream_handler = logging.StreamHandler()
stream_handler.setFormatter(formatter)
logger.addHandler(stream_handler)

# key = 'SLACK_TOKEN'
REPORT = 'daily_edge_detection_report'
TOKEN = 'xoxp-2740212806-3599153334503-3732954917666-b0d899d011cb2739ca1a63ac22070793'
# TOKEN = os.getenv(key) 
FILE = REPORT+'.html'
CHANNEL = 'trial'
date_format='%Y-%m-%d'


def get_time_zone(date_format):
    date = datetime.utcnow()
    dateTimezone=date.strftime(date_format)
    return dateTimezone


def generate_report():
    logger.info('executing Junpyter Notebook to create report')
    os.system(f'jupyter nbconvert --to html --no-input {REPORT}.ipynb' )
    file_path = os.getcwd()+ '/' + FILE
    logger.info(f'report generated: {REPORT}')
    send_message(file_path)


def send_message(file_path):
    logger.info(f'sending slack message to {CHANNEL}')
    client = slack.WebClient(token=TOKEN)
    report_date = get_time_zone(date_format)
    with open(file_path, 'rb') as att:
        r = client.api_call("files.upload", 
            files={'file': att,
            }, data={
            'channels': CHANNEL,
            'filename': FILE,
            'title': REPORT,
            'initial_comment': f'{report_date}, {REPORT}: please download the HTML file to view the report.',
            })
        assert r.status_code == 200

    logger.info('sent slack message')


def main():
    logger.info('Starting with report')
    generate_report()
    logger.info('Completed with report')


if __name__ == "__main__":
    main()
