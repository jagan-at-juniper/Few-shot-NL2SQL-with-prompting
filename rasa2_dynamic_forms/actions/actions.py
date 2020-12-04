import re
from numpy import random

from typing import Dict, Text, List, Optional, Any

from rasa_sdk import Tracker
from rasa_sdk.executor import CollectingDispatcher
from rasa_sdk.forms import FormValidationAction
from rasa_sdk.types import DomainDict

from rasa_sdk import Action

import logging
logger = logging.getLogger(__name__)

class ValidateTbshootForm(FormValidationAction):
    def name(self) -> Text:
        try:
            self.condition
            # self.returned_slots
        except AttributeError:
            # condition #0 asks for mac address
            self.condition = 0
            self.returned_slots = ['mac']
            self.count = 0
            # self.finish_count = 0

        # logger.info('count name: {}'.format(self.count))
        # logger.info('condition name: {}'.format(self.condition))
        # logger.info('finish count name: {}'.format(self.finish_count))

        # if self.condition == 'finish' and self.finish_count == 1:
        #     logger.info('reset condition')
        #     del self.condition
            
        
        logger.info('Executed condition inside name : {}'.format(self.condition))

        return "validate_form_tbshoot"

    async def required_slots(
        self,
        slots_mapped_in_domain: List[Text],
        dispatcher: "CollectingDispatcher",
        tracker: "Tracker",
        domain: "DomainDict",
    ) -> Optional[List[Text]]:


        # logger.info('count is : {}'.format(self.count))
        intent = tracker.latest_message.get('intent', {}).get('name', '')
        logger.info('detcted intent inside RS: {}'.format(intent))
        logger.info('count RS: {}'.format(self.count))

        self.count += 1
        if self.count % 2 == 0:
            # logger.info('count inside condition loop : {}'.format(self.count))

            # Condition #1 : user doesn't know the mac --> we ask to provide hostname
            if self.condition == 0 and intent == 'deny':
                self.condition = 1
                self.returned_slots = ["hostname"]
                

            
            elif self.condition == 0 and intent == 'inform':
                mac = tracker.get_slot('mac')
                # Condition # : user provides the correct mac
                if check_mac_format(mac)[0] == True:
                    # del self.condition
                    dispatcher.utter_message('Troublshoot for client with mac {}'.format(mac))
                    self.condition = 'finish'
                    self.returned_slots = []
                    
                # Condition #2 : user provides the wrong mac so we ask for hostname
                else:
                    self.returned_slots = ["hostname"]
                    self.condition = 2
                    
            # After we ask for hostname differen scenarios might happend
            elif self.condition in [1, 2]:
                # User knows the hostname
                if intent == 'inform':
                    hostname = tracker.get_slot('hostname')
                    jj = find_hostname(hostname)
                    # There is not such hostname
                    if jj == 0:
                        # del self.condition
                        dispatcher.utter_message('The client with host name {} was not found'.format(hostname))
                        self.condition = 'finish'
                        self.returned_slots = []
                    # There is only one client with that hostname
                    elif jj == 1:
                        # del self.condition
                        dispatcher.utter_message('Troublshoot for client with hostname {}'.format(hostname))
                        self.condition = 'finish'
                        self.returned_slots = []
                    # There are multiple clients with that hostname
                    elif jj == 2:
                        dispatcher.utter_message('There are multiple clients with that hostname {}'.format(hostname))
                        self.returned_slots = ["site_name"]
                        self.condition = 3
                        
                # User does not know the hostname so we ask for site name
                elif intent == 'deny':
                    self.returned_slots = ["site_name"]
                    self.condition = 4
                
            elif self.condition in [3, 4]:
                if intent == 'inform':
                    site_name = tracker.get_slot('site_name')
                    jj = find_site_name(site_name)
                    # There is not such sitename
                    if jj == 0:
                        # del self.condition
                        dispatcher.utter_message('The client with site name {} was not found'.format(site_name))
                        self.condition = 'finish'
                        self.returned_slots = []
                    # There is only one client with that sitename
                    elif jj == 1:
                        # del self.condition
                        dispatcher.utter_message('Troublshoot for client with sitename {}'.format(site_name))
                        self.condition = 'finish'
                        self.returned_slots = []
                    # There are multiple clients with that  sitename
                    elif jj == 2:
                        dispatcher.utter_message('There are multiple clients with that site name {}'.format(site_name))
                        self.returned_slots = ["deviceType"]
                        self.condition = 5
                
                elif intent == 'deny':
                    self.returned_slots = ["deviceType"]
                    self.condition = 6

            elif self.condition in [5, 6]:
                deviceType = tracker.get_slot('deviceType')
                site_name = tracker.get_slot('site_name')
                dispatcher.utter_message('Troubleshoot client with device type {} and site name {}'.format(deviceType, site_name))
                self.condition = 'finish'
                self.returned_slots = []
            # # logger.info('Executed condition 2: {}'.format(self.condition))
            logger.info('Updated required slots RS: {}'.format(self.returned_slots))

        # if self.condition == 'finish':
        #     self.finish_count += 1
        
        
        return self.returned_slots


    async def extract_hostname(
        self, dispatcher: CollectingDispatcher, tracker: Tracker, domain: Dict
    ) -> Dict[Text, Any]:

        logger.info('inside extract_hostname')
        hostname = tracker.get_slot('hostname')

        return {"hostname": hostname}

    async def extract_site_name(
        self, dispatcher: CollectingDispatcher, tracker: Tracker, domain: Dict
    ) -> Dict[Text, Any]:

        logger.info('inside extract_site_name')
        site_name = tracker.get_slot('site_name')

        return {"site_name": site_name}
    
    async def extract_deviceType(
        self, dispatcher: CollectingDispatcher, tracker: Tracker, domain: Dict
    ) -> Dict[Text, Any]:

        logger.info('inside extract_deviceType')
        deviceType = tracker.get_slot('deviceType')

        return {"deviceType": deviceType}


def check_mac_format(name):
    if re.match("[0-9a-f]{2}([-:]?)[0-9a-f]{2}(\\1[0-9a-f]{2}){4}$", name.lower()):
        replaced = re.sub('[-:]', '', name)
        return True, replaced
    return False, name

def find_hostname(hostname):
    number_found = random.choice([0, 1, 2])
    return number_found

def find_site_name(site_name):
    number_found = random.choice([2])
    return number_found


