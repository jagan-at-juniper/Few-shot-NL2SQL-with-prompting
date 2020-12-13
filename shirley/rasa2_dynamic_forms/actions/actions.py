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

        intent = tracker.latest_message.get('intent', {}).get('name', '')
        logger.info('Form intent: {}'.format(intent))
        logger.info('Form state: {}'.format(self.form_state))

        if intent == 'deny':
            # User does not want to continue, get out the form
            dispatcher.utter_message('It seems you do not want to continue this topic. Stop form')
            return []
        elif not self.form_state:
            # User just enters the form, return the default required slots
            self.form_state['asking_slots'] = ['mac']
            return self.form_state['asking_slots']

        elif 'asking_slots' in self.form_state and not self.form_state.get('asking_slots'):
            # In the form state, it has all the required information and
            # validation method determine it's time to get out
            return []

        elif 'asking_slots' in self.form_state and self.form_state['asking_slots']:
            # The from stats has slot names in 'asking_slots' field
            # if the the first one is an empty slot, continue ask user
            # otherwise, end the form

            slots_check = self.form_state['asking_slots']
            logger.info('checking slot {}'.format(slots_check))
            empty_ones = list(filter(lambda s: tracker.get_slot(s) is None, slots_check))

            if len(empty_ones) == 0:
                self.form_state.pop('asking_slots')
                logger.info('End the form!!!!')
                return []
            else:
                return self.form_state['asking_slots']

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
        elif self.form_state.get('mac'):
            return {"mac": self.form_state.get('mac')}
        else:
            dispatcher.utter_message('Can not find device of this mac. I will ask user for something else \n\n')
            self.form_state['asking_slots'] = ['hostname']
            return {"mac": None}

    def validate_hostname(
        self,
        slot_value: Any,
        dispatcher: CollectingDispatcher,
        tracker: Tracker,
        domain: DomainDict,
    ) -> Dict[Text, Any]:
        """Validate cuisine value."""

        logger.info('validate_mac {}'.format(slot_value))
        count = find_hostname(slot_value)
        if count == 1:
            dispatcher.utter_message('Perfect! Found one host')
            dispatcher.utter_message('Stop the form \n')
            self.form_state['asking_slots'] = []
            return {"hostname": slot_value}
        elif 2 <= count < 5:
            # TODO
            dispatcher.utter_message('Found multiple devices match the name. Please click one from the list..')
            dispatcher.utter_message('Stop the form \n')
            self.form_state['asking_slots'] = []
            return {"hostname": None}
        else:
            dispatcher.utter_message('Can not find device of this name. I will ask user for something else \n')
            self.form_state['asking_slots'] = ['sitename']
            return {"hostname": None}

    def validate_sitename(
        self,
        slot_value: Any,
        dispatcher: CollectingDispatcher,
        tracker: Tracker,
        domain: DomainDict,
    ) -> Dict[Text, Any]:
        """Validate cuisine value."""

        logger.info('validate_mac {}'.format(slot_value))
        count = find_sitename(slot_value)
        if count == 1:
            self.form_state['asking_slots'] = []
            return {"sitename": slot_value}
        elif 2 <= count < 5:
            # TODO
            dispatcher.utter_message('Found multiple sites match the name: mist0, mist1, mist2... which is the one?')
            self.form_state['asking_slots'] = []
            return {"sitename": None}
        else:
            dispatcher.utter_message('Hmm, can not pinpoint the site for the given name. '
                                     'Maybe I should redirect you to different page?')
            self.form_state['asking_slots'] = []
            return {"sitename": None}

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


def validate(tracker, org_id, config, form_state, slot_name, slot_value):
    """
    Generic validation method:

    entity_type: client

    entity_search_flow:

        - entity_type: client
          entity_property: mac
          enforce_single_match: True

        - entity_type: client
          entity_property: hostname
          user_input:

        - entity_type: site
          entity_property: sitename
          enforce_single_match: True
          retries_if_empty: 2

        - entity_type: client
          entity_property: deviceType

    :param tracker:
    :param disorg_id:
    :param config:
    :param form_state:
    :return:
    """
    form_config = form_state.get('form_control')
    current_flow_inx = form_state.get('form_control')
    current_flow = form_config.get('entity_search_flow')[current_flow_inx]

    # a dict of user validated user inputs
    # which may contain the site_id (based user inputted side_name)
    # AP mac (based on user inputted ap name)
    # etc
    validated_user_inputs = form_state['validated_user_inputs']

    # compose entity_search based on previous validated user inputs and current slot value
    # to validate current slot value


    # Based on current validated slot value to search the entity required by the form
    pass


def check_mac_format(name):
    if re.match("[0-9a-f]{2}([-:]?)[0-9a-f]{2}(\\1[0-9a-f]{2}){4}$", name.lower()):
        replaced = re.sub('[-:]', '', name)
        return True, replaced
    return False, name

def find_hostname(hostname):
    number_found = 5
    if hostname == 'amin0':
        number_found = 0
    if hostname == 'amin1':
        number_found = 1
    if hostname == 'amin2':
        number_found = 2
    return number_found

def find_sitename(sitename):
    number_found = 5
    if sitename == 'mist0':
        number_found = 0
    if sitename == 'mist1':
        number_found = 1
    if sitename == 'mist2':
        number_found = 2
    return number_found


