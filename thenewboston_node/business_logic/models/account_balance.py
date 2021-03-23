from dataclasses import dataclass
from typing import Optional

from dataclasses_json import dataclass_json

from thenewboston_node.business_logic.exceptions import ValidationError
from thenewboston_node.core.utils.constants import SENTINEL
from thenewboston_node.core.utils.dataclass import fake_super_methods


@dataclass_json
@dataclass
class AccountBalance:
    value: int
    lock: str

    def validate(self, validate_balance_lock=True):
        if not isinstance(self.value, int):
            raise ValidationError('Balance must be an integer')

        if validate_balance_lock:
            if not isinstance(self.lock, str):
                raise ValidationError('Balance lock must be a string')

            if not self.lock:
                raise ValidationError('Balance lock must be set')


@fake_super_methods
@dataclass_json
@dataclass
class BlockAccountBalance(AccountBalance):
    lock: Optional[str] = None  # type: ignore

    def override_to_dict(self):  # this one turns into to_dict()
        dict_ = self.super_to_dict()

        # TODO(dmu) LOW: Implement a better way of removing optional fields or allow them in normalized message
        value = dict_.get('lock', SENTINEL)
        if value is None:
            del dict_['lock']

        return dict_

    def validate(self):
        super().validate(validate_balance_lock=False)
