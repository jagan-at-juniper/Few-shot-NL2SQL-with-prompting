import logging
import re
from typing import Dict, Text, List, Optional, Any

from rasa_sdk import Tracker
from rasa_sdk.events import EventType, SlotSet
from rasa_sdk.executor import CollectingDispatcher
from rasa_sdk.forms import FormValidationAction
from rasa_sdk.types import DomainDict

logger = logging.getLogger(__name__)


class ValidateTbshootForm(FormValidationAction):

    def __init__(
        self
    ) -> None:
        self.org_id = None
        self.form_state = {}
        self.returned_slots = []

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

        logger.info('START of RS {}'.format(self.form_state))

        count = self.form_state.get('count', 0)
        intent = tracker.latest_message.get('intent', {}).get('name', '')
        required_slots = self.form_state.get('required_slots', [])
        logger.info('Form intent: {}'.format(intent))
        logger.info('Form state: {}'.format(self.form_state))

        if intent == 'deny':
            return []

        if self.condition == 0:
            # retrieve slot names which need to extract user input
            logger.info('1st call, return default slots {}'.format(required_slots))

            if 'required_slots' not in self.form_state or not self.form_state['required_slots']:
                self.form_state['required_slots'] = slots_mapped_in_domain
                required_slots = self.form_state['required_slots']

            self.form_state['count'] = count
            self.condition += 1
            logger.info('update form_state: {}'.format(self.form_state))
            logger.info('END of RS\n\n')
            return required_slots

        elif self.condition == 2:
            # retrieve slot names which need to extract user input
            logger.info('3rd call, return slots {}'.format(required_slots))

            self.form_state['required_slots'] = required_slots
            self.form_state['count'] = count
            self.condition = 0
            logger.info('update form_state: {}'.format(self.form_state))
            logger.info('END of RS\n\n')
            return required_slots

        elif self.condition == 1:
            logger.info('2nd call, checkout counter: {}'.format(count))

            if count >= 30:

                required_slots = []
                self.form_state = dict()
            else:

                count += 1
                status = self.check_form_slots(tracker, required_slots)
                logger.info('2nd call, extraction status: {}'.format(status))

                required_slots = []
                if count == 1 and status == 0:

                    required_slots = ['mac']
                elif count > 1 and status == 0:
                    dispatcher.utter_message('I can not find the mac. Next ask for hostname')
                    required_slots = ['hostname']

                elif 2 <= status < 5:

                    dispatcher.utter_message('I found multiple. What is the site name')
                    dispatcher.utter_message('Please select one to continue.')
                    required_slots = []

                elif 5 <= status:
                    dispatcher.utter_message('I found multiple. What is the site name')
                    required_slots = ['sitename']
                    
                elif status == 1:
                    dispatcher.utter_message('Yes, Found the device')
                    required_slots = []

            # save all form state into variable form_state and will be
            # persist into slot later
            self.form_state['required_slots'] = required_slots
            self.form_state['count'] = count
            self.condition += 1
            logger.info('update form_state: {}'.format(self.form_state))

            logger.info('END of RS {}\n\n'.format(required_slots))
            return required_slots

    def reset_form_state(self):
        self.form_state = dict()

    def check_form_slots(self, tracker, returned_slots):

        # based on form state and current user filled in slots values
        # to validate the data

        slot = returned_slots[0]
        logger.info('what is the name of the slot? {}'.format(slot))
        extracted_value = tracker.slots.get(slot, '')
        logger.info('slot = {} value = {}'.format(slot, extracted_value))
        if extracted_value == 'shirley_2':
            return 2
        elif extracted_value == 'shirley_1':
            return 1
        elif extracted_value == 'shirley_3':
            return 3
        else:
            return 0


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

    def validate_mac(
        self,
        slot_value: Any,
        dispatcher: CollectingDispatcher,
        tracker: Tracker,
        domain: DomainDict,
    ) -> Dict[Text, Any]:
        """Validate cuisine value."""

        logger.info('validate_mac {}'.format(slot_value))
        check, formatted_mac = check_mac_format(slot_value)
        if check:
            return {"mac": formatted_mac}
        else:
            return {"mac": None}

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


