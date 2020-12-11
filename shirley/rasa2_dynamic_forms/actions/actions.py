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

        logger.info('START of RS {}'.format(self.form_state))

        count = self.form_state.get('count', 0)
        intent = tracker.latest_message.get('intent', {}).get('name', '')
        required_slots = self.form_state.get('required_slots', ['mac'])
        logger.info('Form intent: {}'.format(intent))

        if intent == 'deny':
            return []

        self.first_call = not self.first_call
        if self.first_call:
            # retrieve slot names which need to extract user input
            logger.info('1st call, return default slots {}'.format(required_slots))
            return required_slots

        else:
            logger.info('2nd call, checkout counter: {}'.format(count))

            if count >= 3:

                required_slots = []
                self.form_state = dict()
            else:

                count += 1
                required_slots = []

                extracted_mac = tracker.slots.get('mac', '')
                logger.info('2nd call, extracted_mac: {}'.format(extracted_mac))

                if count == 1 and extracted_mac is None:

                    required_slots.append('mac')
                elif count > 1 and extracted_mac is None:
                    dispatcher.utter_message('I can not find the mac. Next ask for hostname')
                    required_slots.append('hostname')

                elif extracted_mac:

                    check, formatted_mac = check_mac_format(extracted_mac)
                    if not check:
                        required_slots.append('hostname')

            # save all form state into variable form_state and will be
            # persist into slot later
            self.form_state['required_slots'] = required_slots
            self.form_state['count'] = count
            logger.info('2nd call, update form_state: {}'.format(self.form_state))

        logger.info('END of RS')
        return required_slots


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


