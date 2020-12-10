import re
from numpy import random

from typing import Dict, Text, List, Optional, Any

from rasa_sdk import Tracker
from rasa_sdk.events import EventType, SlotSet
from rasa_sdk.executor import CollectingDispatcher
from rasa_sdk.forms import FormValidationAction
from rasa_sdk.types import DomainDict

from rasa_sdk import Action

import logging
logger = logging.getLogger(__name__)

class ValidateTbshootForm(FormValidationAction):

    def __init__(
        self
    ) -> None:
        self.org_id = None
        self.form_state = {}
        self.returned_slots = ['mac']
        self.first_call = False

        self.condition = 0
        self.count = 0

    def name(self) -> Text:
        return "validate_form_tbshoot"

    async def run(
        self,
        dispatcher: "CollectingDispatcher",
        tracker: "Tracker",
        domain: "DomainDict",
    ) -> List[EventType]:

        # restore the form state from slots
        self.form_state = tracker.slots.get('tb_form_state', {})
        if self.form_state is None:
            self.form_state = {}

        # original form implementation for the run()
        events = await super(ValidateTbshootForm, self).run(dispatcher, tracker, domain)

        # persist the stats into slots
        events = events + [SlotSet(key='tb_form_state', value=self.form_state)]
        return events

    async def required_slots(
        self,
        slots_mapped_in_domain: List[Text],
        dispatcher: "CollectingDispatcher",
        tracker: "Tracker",
        domain: "DomainDict",
    ) -> Optional[List[Text]]:

        logger.info('START of RS')
        logger.info('Executed condition inside RS : {}'.format(self.condition))
        # logger.info('count is : {}'.format(self.count))
        intent = tracker.latest_message.get('intent', {}).get('name', '')
        logger.info('detcted intent inside RS: {}'.format(intent))
        # logger.info('count RS: {}'.format(self.count))

        #reset condition
        if self.count == 1 and self.condition == 'finish':
            del self.condition

        # if self.count == self.condition:
        logger.info('count is : {}'.format(self.count))
        self.count += 1
        if self.count == 3:
            self.count = 0
            # del self.condition


        # self.count += 1
        if self.count  == 1:
            # logger.info('count inside condition loop : {}'.format(self.count))

            # Condition #1 : user doesn't know the mac --> we ask to provide hostname
            if self.condition == 0 and intent == 'deny':
                self.condition = 1
                self.returned_slots = ["hostname"]
                

            
            elif self.condition == 0 and intent == 'inform':
                mac = tracker.get_slot('mac')
                # Condition # : user provides the correct mac
                if check_mac_format(mac)[0] == True:
                    dispatcher.utter_message('Troublshoot for client with mac {}'.format(mac))
                    self.condition = 'finish'
                    self.returned_slots = []
                    
                # Condition #2 : user provides the wrong mac so we ask for hostname
                else:
                    self.returned_slots = ["hostname"]
                    self.condition = 2
                    
            # After we ask for hostname different scenarios might happend
            elif self.condition in [1, 2]:
                # User knows the hostname
                if intent == 'inform':
                    hostname = tracker.get_slot('hostname')
                    jj = find_hostname(hostname)
                    # There is not such hostname
                    if jj == 0:
                        dispatcher.utter_message('The client with host name {} was not found'.format(hostname))
                        self.condition = 'finish'
                        self.returned_slots = []
                    # There is only one client with that hostname
                    elif jj == 1:
                        dispatcher.utter_message('Troublshoot for client with hostname {}'.format(hostname))
                        self.condition = 'finish'
                        self.returned_slots = []
                    # There are multiple clients with that hostname
                    elif jj == 2:
                        dispatcher.utter_message('There are multiple clients with that hostname {}'.format(hostname))
                        self.returned_slots = ["sitename"]
                        self.condition = 3
                        
                # User does not know the hostname so we ask for site name (no mac, no hostname)
                elif intent == 'deny':
                    self.returned_slots = ["sitename"]
                    self.condition = 4
            # condition #3(no mac, multiple hostname), condition #4(no mac, no hostname) --> for both we ask site name
            elif self.condition in [3, 4]:
                # User gives you the site name
                if intent == 'inform':
                    sitename = tracker.get_slot('sitename')
                    # we know the hostname and sitename
                    if self.condition == 3:
                        hostname = tracker.get_slot('hostname')
                        jj = find_sitename(sitename)
                        # There is not such sitename
                        if jj == 0:
                            # There is not such client with that sitename and hostname
                            dispatcher.utter_message('The client with site name {} and hostname {} was not found'.format(sitename, hostname))
                            self.condition = 'finish'
                            self.returned_slots = []
                        # There is only one client with that sitename and hostname
                        elif jj == 1:
                            # del self.condition
                            dispatcher.utter_message('Troublshoot for client with sitename {} and hostname {}'.format(sitename, hostname))
                            self.condition = 'finish'
                            self.returned_slots = []
                        # There are multiple clients with that sitename and hostname so we ask for device type
                        elif jj == 2:
                            dispatcher.utter_message('There are multiple clients with that site name {} and hostname {}'.format(sitename, hostname))
                            self.returned_slots = ["deviceType"]
                            self.condition = 5
                    # we don't know the hostname, just the sitename so we ask for device type
                    elif self.condition == 4:
                            self.returned_slots = ["deviceType"]
                            self.condition = 6
                # User dosen't know the sitename 
                elif intent == 'deny':
                    # User know the hostname but No mac, and no sitename
                    if self.condition == 3:
                        hostname = tracker.get_slot('hostname')
                        dispatcher.utter_message('Please select from list of hostname {} shown here'.format(hostname))
                        self.condition = 'finish'
                        self.returned_slots = []
                    # No mac, no hostname, no sitename
                    if self.condition == 4:
                        dispatcher.utter_message('Dude, without knowing your mac, hostname, and sitename there is not much I can do!')
                        self.condition = 'finish'
                        self.returned_slots = []
            
            # condition #5(no mac, multiple hostname, multiple sitename), condition #6(no mac, no hostname, we know the sitename) --> for both we ask deviceType
            elif self.condition in [5, 6]:
                # user knows the device type
                if intent == 'inform':
                    deviceType = tracker.get_slot('deviceType')
                    # We know the hostname, sitename, and device type
                    if self.condition == 5:
                        sitename = tracker.get_slot('sitename')
                        hostname = tracker.get_slot('hostname')
                        dispatcher.utter_message('Troubleshoot client with hostname {}, sitename {}, and device type {}'.format(hostname,sitename,deviceType))
                        self.condition = 'finish'
                        self.returned_slots = []
                    # We know the sitename, and device type
                    elif self.condition == 6:
                        sitename = tracker.get_slot('sitename')
                        dispatcher.utter_message('Here is what I can do knowing your sitename {} and device type as  {}'.format(sitename,deviceType))
                        self.condition = 'finish'
                        self.returned_slots = []

                # user doesn't know the device type
                elif intent == 'deny':
                    # we know the sitename and hostname
                    if self.condition == 5:
                        sitename = tracker.get_slot('sitename')
                        hostname = tracker.get_slot('hostname')
                        dispatcher.utter_message('Here is what I can do knowing your hostname {} and sitename {}'.format(hostname,sitename))
                        self.condition = 'finish'
                        self.returned_slots = []

                    # we only know the sitename 
                    elif self.condition == 6:
                        sitename = tracker.get_slot('sitename')
                        dispatcher.utter_message('I only know your sitename as {}, here are unhappy clients as your site'.format(sitename))
                        self.condition = 'finish'
                        self.returned_slots = []

            # # logger.info('Executed condition 2: {}'.format(self.condition))
            logger.info('Updated required slots RS: {}'.format(self.returned_slots))

        # if self.condition == 'finish':
        #     self.finish_count += 1


        logger.info('END of RS')
        return self.returned_slots


    async def extract_hostname(
        self, dispatcher: CollectingDispatcher, tracker: Tracker, domain: Dict
    ) -> Dict[Text, Any]:

        # self.count = self.condition

        logger.info('inside extract_hostname')
        hostname = tracker.get_slot('hostname')

        return {"hostname": hostname}

    async def extract_sitename(
        self, dispatcher: CollectingDispatcher, tracker: Tracker, domain: Dict
    ) -> Dict[Text, Any]:

        # self.count = self.condition

        logger.info('inside extract_sitename')
        sitename = tracker.get_slot('sitename')

        return {"sitename": sitename}
    
    async def extract_deviceType(
        self, dispatcher: CollectingDispatcher, tracker: Tracker, domain: Dict
    ) -> Dict[Text, Any]:

        # self.count = self.condition

        logger.info('inside extract_deviceType')
        deviceType = tracker.get_slot('deviceType')

        return {"deviceType": deviceType}


def check_mac_format(name):
    if re.match("[0-9a-f]{2}([-:]?)[0-9a-f]{2}(\\1[0-9a-f]{2}){4}$", name.lower()):
        replaced = re.sub('[-:]', '', name)
        return True, replaced
    return False, name

def find_hostname(hostname):
    if hostname == 'amin0':
        number_found = 0
    if hostname == 'amin1':
        number_found = 1
    if hostname == 'amin2':
        number_found = 2
    return number_found

def find_sitename(sitename):
    if sitename == 'mist0':
        number_found = 0
    if sitename == 'mist1':
        number_found = 1
    if sitename == 'mist2':
        number_found = 2
    return number_found


