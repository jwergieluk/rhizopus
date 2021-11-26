from enum import Enum
from typing import Any, Dict, List, Type


def enum_member_to_int(member: Enum) -> int:
    for i, (name, m) in enumerate(member.__class__.__members__.items()):
        if member == m:
            return i


def enum_member_from_value(enum_class: Type[Enum], value: Any) -> Any:
    for member in enum_class:
        if member.value == value:
            return member
    raise LookupError(f'Value not found: {value}')


def enum_member_from_name(enum_class: Type[Enum], name: str) -> Enum:
    for member in enum_class:
        if member.name == name:
            return member
    raise LookupError(f'Value not found: {name}')


def enum_values_map(enum_class: Type[Enum]) -> Dict[str, str]:
    # This should preserve the member definition order, but it doesn't:
    # https://docs.python.org/3/library/enum.html
    return {i.name: i.value for i in enum_class}


def enum_names_list(enum_class: Type[Enum]) -> List[str]:
    return [str(i.name) for i in enum_class]
