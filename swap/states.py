from aiogram.dispatcher.filters.state import State, StatesGroup

class OrderWheelStates(StatesGroup):
    WAIT_COMPANY  = State()
    WAIT_TYPE     = State()
    WAIT_SIZE     = State()
    WAIT_NAME     = State()
    WAIT_MODEL    = State()
    WAIT_QUANTITY = State()
    WAIT_ASSEMBLY = State()

class TransferStates(StatesGroup):
    WAIT_BY_REQUEST = State()        # вопрос "Выдача по заявке?"
    WAIT_REQUEST_SELECT = State()    # выбор номера заявки
    WAIT_REQUEST_ACTION = State()    # "Отменить" / "Выдать"
    WAIT_RECIPIENT_SELECTION = State()
    WAIT_SHM_COMPANY = State()
    WAIT_SHM_LOCATION = State()
    WAIT_MATERIAL_SELECTION = State()
    WAIT_MANUAL_MATERIAL = State()
    WAIT_CELL_SELECTION = State()
    WAIT_QUANTITY = State()
    WAIT_MORE = State()
    WAIT_CONFIRM = State()
    WAIT_PHOTO = State()

class RequestTmcStates(StatesGroup):
    WAIT_DEPARTMENT = State()
    WAIT_MATERIAL_SELECTION = State()
    WAIT_QUANTITY = State()
    WAIT_MORE = State()
    WAIT_CONFIRM = State()
    WAIT_OVERFLOW_CONFIRM = State()