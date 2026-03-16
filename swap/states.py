from aiogram.dispatcher.filters.state import State, StatesGroup

class ExtraWashStates(StatesGroup):
    WAIT_CITY = State()
    WAIT_SERVICE = State()
    WAIT_PLATE = State()
    WAIT_LOCATION = State()
    WAIT_TICKET = State()
    WAIT_PHOTOS = State()
    WAIT_CONFIRMATION = State()